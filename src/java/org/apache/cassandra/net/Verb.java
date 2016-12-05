/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.utils.TimeoutSupplier;
import org.apache.cassandra.utils.versioning.Version;

/**
 * Represents the definition of an inter-node exchange (read request, gossip ack message, etc...).
 * <p>
 * A verb describes a particular type of request, and the response associated to it (if any), as well as informations
 * associated to this message exchange (timeout, stage to use for executing the request, ...). Most importantly, the verb
 * defines the {@link #handler} that is used to execute its requests.
 * <p>
 * A verb should be registered against a {@link VerbGroup} (a verb is always associated to a group) that itself must be
 * listed in {@link Verbs} to be concretely used by the messaging service and you should almost always rely on
 * {@link VerbGroup.RegistrationHelper} to create verbs for you.
 * <p>
 * Also note that while {@link Verb} is the generic "interface" for messaging exchanges, each concrete verb will
 * actually be of one of the 3 more precise sub- and inner- classes:
 * <ul>
 *     <li>{@link OneWay}: for fire-and-forget messages, where no response is sent (and thus no callback set).</li>
 *     <li>{@link AckedRequest}: for request-response exchanges where the response is a simple ack (it has no payload).</li>
 *     <li>{@link RequestResponse}: for standard request-response messages having a non-empty response.</li>
 * <ul/>
 *
 * @param <P> the type of the payload of the request of this verb.
 * @param <Q> the type of the payload of the responses of this verb.
 */
public abstract class Verb<P, Q>
{
    final Consumer<Response<Q>> EMPTY_RESPONSE_CONSUMER = r -> {};

    /**
     * A simple utility class that groups most infos of the verb to avoid code repetition in the subclass definitions
     * at the end of this class and in {@link VerbGroup}. This can be ignored otherwise.
     */
    public static class Info
    {
        private final VerbGroup<?> group;
        private final int groupIdx;
        private final String name;
        private final Stage requestStage;
        private final boolean supportsBackPressure;

        Info(VerbGroup<?> group,
             int groupIdx,
             String name,
             Stage requestStage,
             boolean supportsBackPressure)
        {
            assert group != null && name != null && requestStage != null;
            this.group = group;
            this.groupIdx = groupIdx;
            this.name = name;
            this.requestStage = requestStage;
            this.supportsBackPressure = supportsBackPressure;
        }

        @Override
        public String toString()
        {
            return String.format("%s.%s", group, name);
        }
    }

    private final Info info;
    private final TimeoutSupplier<P> timeoutSupplier;
    private final VerbHandler<P, Q> handler;

    protected Verb(Info info,
                   TimeoutSupplier<P> timeoutSupplier,
                   VerbHandler<P, Q> handler)
    {
        this.info = info;
        this.timeoutSupplier = timeoutSupplier;
        this.handler = handler;

        assert isOneWay() == (timeoutSupplier == null) : "Oneway verbs must not have a timeout supplier, but other verbs must";
    }

    /**
     * The group this verb is part of.
     *
     * @param <V> the type of the versions of the group this is a verb of.
     * @return the group of this verb.
     */
    @SuppressWarnings("unchecked")
    public <V extends Enum<V> & Version<V>> VerbGroup<V> group()
    {
        return (VerbGroup<V>) info.group;
    }

    /**
     * The "index" of the verb inside its group. This is only used by groups to make lookups faster and shouldn't be
     * used by any other class than {@link VerbGroup}.
     *
     * @return the index of the verb within its group.
     */
    int groupIdx()
    {
        return info.groupIdx;
    }

    /**
     * The timeout for the exchange represented by this verb.
     * <p>
     * Note that one-way verbs don't have a timeout supplier as by definition nobody is waiting on them. Any other verb
     * must define a timeout supplier however.
     *
     * @return a supplier for the timeouts used by exchanges of this verb, or {@code null} if this is a one-way verb.
     */
    public TimeoutSupplier<P> timeoutSupplier()
    {
        return timeoutSupplier;
    }

    /**
     * The stage on which the request must be executed.
     */
    Stage requestStage()
    {
        return info.requestStage;
    }

    /**
     * The handler for the requests of this verb.
     */
    VerbHandler<P, Q> handler()
    {
        return handler;
    }

    /**
     * Whether this verb supports backpressure.
     */
    boolean supportsBackPressure()
    {
        return info.supportsBackPressure;
    }

    /**
     * Whether this verb defines one-way exchanges, that is no responses is sent for the requests.
     */
    public abstract boolean isOneWay();

    private long computeRequestPayloadSize(P payload)
    {
        return MessagingService.current_version.serializer(this).requestSerializer.serializedSize(payload);
    }

    Message.Data<P> makeMessageData(P payload, boolean skipPayloadSizeComputation)
    {
        return new Message.Data<>(payload,
                                  skipPayloadSizeComputation ? -1 : computeRequestPayloadSize(payload),
                                  System.currentTimeMillis(),
                                  timeoutSupplier == null ? -1 : timeoutSupplier.get(payload));
    }

    /**
     * Creates a request for this verb.
     *
     * @param to the destination for the request.
     * @param payload the payload for the request.
     * @return a newly created request to {@code to} with payload {@code payload}.
     */
    public Request<P, Q> newRequest(InetAddress to, P payload)
    {
        return newRequest(to, makeMessageData(payload, to == Request.local));
    }

    /**
     * Creates a request for this verb given the message data.
     * <p>
     * Note: this shouldn't be public and one should avoid using this outside of this package, but it's public due to
     * the crazy hack we're using for tracing, see {@link org.apache.cassandra.tracing.Tracing#TRACE_MSG_DEF}.
     */
    public Request<P, Q> newRequest(InetAddress to, Message.Data<P> messageData)
    {
        return new Request<>(Request.local,
                             to,
                             MessagingService.newMessageId(),
                             this,
                             messageData);
    }

    Request<P, Q> newRequestWithForwards(InetAddress to, Message.Data<P> messageData, List<InetAddress> forwards)
    {
        return new Request<>(Request.local,
                             to,
                             MessagingService.newMessageId(),
                             this,
                             messageData,
                             Request.Forward.from(forwards));
    }

    boolean isOnlyLocal(Collection<InetAddress> endpoints)
    {
        return endpoints.size() == 1 && endpoints.iterator().next() == Request.local;
    }

    /**
     * Creates a new (non-forwarding) request dispatcher (a facility to dispatch a request to multiple destinations) for
     * this verb.
     * <p>
     * The dispatcher created by this method is not a forwarding one, meaning that requests will be sent to all
     * destination directly, without using request forwarding for remote DCs. Use {@link #newForwardingDispatcher} if
     * you want such forwarding.
     *
     * @param endpoints the destinations for the request dispatcher.
     * @param payload the payload of the dispatch.
     * @return a dispatcher for requests to {@code endpoints} using payload {@code payload}.
     */
    public Request.Dispatcher<P, Q> newDispatcher(Collection<InetAddress> endpoints, P payload)
    {
        return new Request.Dispatcher<>(MessageTargets.createSimple(endpoints),
                                        this,
                                        makeMessageData(payload, isOnlyLocal(endpoints)));
    }

    /**
     * Creates a new forwarding request dispatcher (a facility to dispatch a request to multiple destinations) for
     * this verb.
     *
     * @param endpoints the destinations for the request dispatcher.
     * @param localDc the local datacenter of this node (use to decide which nodes below to the local dc and which are
     *                remote for forwarding purposes).
     * @param payload the payload of the dispatch.
     * @return a dispatcher for requests to {@code endpoints} using payload {@code payload}, using forwarding for remote
     * DCs.
     */
    public Request.Dispatcher<P, Q> newForwardingDispatcher(Collection<InetAddress> endpoints, String localDc, P payload)
    {
        return new Request.Dispatcher<>(MessageTargets.createWithFowardingForRemoteDCs(endpoints, localDc),
                                        this,
                                        makeMessageData(payload, isOnlyLocal(endpoints)));
    }

    @Override
    public final boolean equals(Object o)
    {
        // The way the code is made, each verb is it's own singleton object, so reference equality is fine and actually
        // what we want. And the only purpose of this (redundant) definition is to make it clear that this is intended,
        // especially as Tracing.TRACE_MSG_DEF currently rely on use not really acessing anything of the object.
        return this == o;
    }

    @Override
    public String toString()
    {
        return info.toString();
    }

    /**
     * Verbs for one-way exchanges, that is for requests to which we don't send any response (not even an acknowledgment.
     *
     * @param <P> the type for the payload of the requests.
     */
    public static class OneWay<P> extends Verb<P, NoResponse>
    {
        OneWay(Info info,
               VerbHandler<P, NoResponse> handler)
        {
            super(info, null, handler);
        }

        public boolean isOneWay()
        {
            return true;
        }

        @Override
        public OneWayRequest<P> newRequest(InetAddress to, P payload)
        {
            return newRequest(to, makeMessageData(payload, to == Request.local));
        }

        @Override
        public OneWayRequest<P> newRequest(InetAddress to, Message.Data<P> messageData)
        {
            return new OneWayRequest<>(Request.local,
                                       to,
                                       this,
                                       messageData);
        }

        @Override
        public OneWayRequest<P> newRequestWithForwards(InetAddress to, Message.Data<P> data, List<InetAddress> forwards)
        {
            return new OneWayRequest<>(Request.local,
                                       to,
                                       this,
                                       data,
                                       Request.Forward.from(forwards));
        }

        @Override
        public OneWayRequest.Dispatcher<P> newDispatcher(Collection<InetAddress> endpoints, P payload)
        {
            return new OneWayRequest.Dispatcher<>(MessageTargets.createSimple(endpoints),
                                                  this,
                                                  makeMessageData(payload, isOnlyLocal(endpoints)));
        }
    }

    /**
     * Generic verb for request-response exchanges.
     *
     * @param <P> the type for the payload of the requests.
     * @param <Q> the type for the payload of the responses.
     */
    public static class RequestResponse<P, Q> extends Verb<P, Q>
    {
        RequestResponse(Info info,
                        TimeoutSupplier<P> timeoutSupplier,
                        VerbHandler<P, Q> handler)
        {
            super(info, timeoutSupplier, handler);
        }

        public boolean isOneWay()
        {
            return false;
        }
    }

    /**
     * Verb for request-response exchanges where the response is a simple ack of the request but contain no additional
     * data itself.
     * <p>
     * Note: this exists mostly because we have a fair amount of such verbs and writing {@code AckedRequest<X>} is
     * shorter (and cleaner?) than {@code RequestResponse<X, EmptyPayload>}.
     *
     * @param <P> the type for the payload of the requests.
     */
    public static class AckedRequest<P> extends RequestResponse<P, EmptyPayload>
    {
        protected AckedRequest(Info info,
                               TimeoutSupplier<P> timeoutSupplier,
                               VerbHandler<P, EmptyPayload> handler)
        {
            super(info, timeoutSupplier, handler);
        }

        public boolean isOneWay()
        {
            return false;
        }
    }
}
