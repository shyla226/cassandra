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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.monitoring.AbortedOperationException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A request message for the inter-node messaging protocol.
 * <p>
 * Note that all requests are of this type, but it has two noticeable sub-classes:
 * <ul>
 *     <li>{@link OneWayRequest} for one-way requests, to which no response is sent.</li>
 *     <li>{@link ForwardRequest} for requests that are forwarded (see below), i.e. for which the response shouldn't be
 *     sent to the source of the message.</li>
 * </ul>
 * <p>
 * The messaging service has a forwarding mechanism for requests. A request can be created with "forwards", that is
 * essentially a list of nodes to which the request destination should forward the request to (on top of handling the
 * request itself that is). This is used to save cross-datacenter bandwidth when sending requests with large payload
 * (essentially mutations currently), by sending a request to only one node of remote DCs, but having that node forward
 * the request to any other node that needs to receive the request in the same datacenter. Every receiver then sends
 * its response directly to the originator of the request.
 * <p>
 * In practice, one can enable this forwarding by using a forwarding {@link Dispatcher} (created with
 * {@link Verb#newForwardingDispatcher}). In that case, on the sender, only a single request per remote datacenter will
 * be created, but that request will include a list of {@link Forward}. A receiver of a request with "forwards" will
 * both execute the request but also send a {@link ForwardRequest} to each node for which there is a {@link Forward}
 * (see {@link MessageDeliveryTask#run} for details). Each receiver of a {@link ForwardRequest} will execute the
 * request normally but will sent his response to the node that originated the operation, not the one having sent
 * the {@link ForwardRequest} to it (see {@link ForwardRequest#respond}).
 *
 * @param <P> the type of the payload of this request.
 * @param <Q> the type of the payload for the responses to this request.
 */
public class Request<P, Q> extends Message<P>
{
    private static final Logger logger = LoggerFactory.getLogger(Request.class);

    static final InetAddress local = FBUtilities.getBroadcastAddress();

    protected final Verb<P, Q> verb;
    private final List<Forward> forwards;

    /**
     * Package-protected because request are not meant to be created manually outside of the 'net' package. Instead,
     * one should use the {@link Verb#newRequest} methods.
     */
    Request(InetAddress from,
            InetAddress to,
            int id,
            Verb<P, Q> verb,
            Message.Data<P> data)
    {
        this(from,
             to,
             id,
             verb,
             data,
             Collections.emptyList());
    }

    Request(InetAddress from,
            InetAddress to,
            int id,
            Verb<P, Q> verb,
            Message.Data<P> data,
            List<Forward> forwards)
    {
        super(from, to, id, data);
        this.verb = verb;
        this.forwards = forwards;
    }

    @VisibleForTesting
    static <P, Q> Request<P, Q> fakeTestRequest(InetAddress to, int id, Verb<P, Q> verb, P payload)
    {
        return new Request<>(FBUtilities.getBroadcastAddress(),
                             to,
                             id,
                             verb,
                             new Data<>(payload));
    }

    public boolean isRequest()
    {
        return true;
    }

    /**
     * Whether that's a forwarded request. If it is, this can be safely cast to {@link ForwardRequest}.
     */
    boolean isForwarded()
    {
        return false;
    }

    public Verb<P, Q> verb()
    {
        return verb;
    }

    /**
     * The list of forwards for this request (can and will often be empty).
     */
    List<Forward> forwards()
    {
        return forwards;
    }

    /**
     * Creates a response message for this request given the payload to send in this response.
     *
     * @param payload the payload to send as response.
     * @return the response message for responding to this request with {@code payload}.
     */
    Response<Q> respond(Q payload)
    {
        return new Response<>(local,
                              from(),
                              id(),
                              verb,
                              responseData(payload));
    }

    Data<Q> responseData(Q payload)
    {
        long serializedSize = MessagingService.current_version.serializer(verb()).responseSerializer.serializedSize(payload);
        return messageData.withPayload(payload, serializedSize);
    }

    public Request<P, Q> addParameters(MessageParameters parameters)
    {
        return new Request<>(from(), to(), id(), verb(), messageData.withAddedParameters(parameters), forwards);
    }

    /**
     * Creates a response message for this request in the case an error occurred.
     *
     * @param reason the reason for which the request failed.
     * @return the failure response for responding to this request with the error of reason {@code reason}.
     */
    FailureResponse<Q> respondWithFailure(RequestFailureReason reason)
    {
        return new FailureResponse<>(local,
                                     from(),
                                     id(),
                                     verb,
                                     reason,
                                     messageData.withPayload(null, -1));
    }

    /**
     * Execute the request using the handler of the verb this is a request of, and feed the response to the provided
     * consumer.
     * <p>
     * Note that this method mostly delegates the work to {@code verb().handler()} which decides if the method is
     * synchronous or not and so this method may or may not block.
     *
     * @param responseHandler a consumer to which the response to this request is provided.
     * @param onAborted a runnable that is run if the request execution is aborted due to monitoring.
     */
    public void execute(Consumer<Response<Q>> responseHandler, Runnable onAborted)
    {
        try
        {
            CompletableFuture<Response<Q>> future = verb.handler().handle(this);
            assert future != null || verb.isOneWay();
            if (future != null)
                future.thenAccept(responseHandler)
                      .exceptionally(e ->
                                     {
                                         if (e instanceof AbortedOperationException)
                                             onAborted.run();
                                         else if (e instanceof DroppingResponseException)
                                             logger.debug("Dropping response to {} as requested by the handler (it threw DroppingResponseException)", to());
                                         else
                                             // Handlers are not supposed to throw anything else than the 2 previous exceptions by contract, but
                                             // let's not completely silence a programmer bug.
                                             logger.error("Unexpected error thrown by {} handler; this should have been caught in the handler", verb, e);

                                         return null;
                                     });
        }
        catch (AbortedOperationException e)
        {
            onAborted.run();
        }
        catch (DroppingResponseException e)
        {
            // Doing nothing on purpose as this is what this exception is about
            logger.debug("Dropping response to {} as requested by the handler (it threw DroppingResponseException)", to());
        }
    }

    Iterable<ForwardRequest<P, Q>> forwardRequests()
    {
        return Iterables.transform(forwards,
                                   fwd -> new ForwardRequest<>(local,
                                                               fwd.to,
                                                               from(),
                                                               fwd.id,
                                                               verb,
                                                               messageData));
    }

    long payloadSerializedSize(MessagingVersion version)
    {
        return messageData.payloadSize >= 0 && version == MessagingService.current_version
               ? messageData.payloadSize
               : version.serializer(verb()).requestSerializer.serializedSize(payload());
    }

    /**
     * Represents a node to which the request should be forwarded. It also includes the id that allows to associate the
     * future response from {@code to} to the proper callback.
     */
    static class Forward
    {
        final InetAddress to;
        final int id;

        Forward(InetAddress to, int id)
        {
            this.to = to;
            this.id = id;
        }

        private Forward(InetAddress to)
        {
            this(to, MessagingService.newMessageId());
        }

        static List<Forward> from(List<InetAddress> hosts)
        {
            return Lists.transform(hosts, Forward::new);
        }
    }

    /**
     * A dispatcher is used to send a given request to multiple hosts. It basically generates requests from a list of
     * destinations, but also provides options like forwarding as discussed above.
     */
    public static class Dispatcher<P, Q>
    {
        protected final MessageTargets targets;
        protected final Verb<P, Q> verb;
        protected final Data<P> messageData;

        /**
         * Creates a new dispatcher. Not meant to be used directly, use {@link Verb#newDispatcher} or
         * {@link Verb#newForwardingDispatcher} instead.
         */
        Dispatcher(MessageTargets targets,
                   Verb<P, Q> verb,
                   Data<P> data)
        {
            this.targets = targets;
            this.verb = verb;
            this.messageData = data;
        }

        Verb<P, Q> verb()
        {
            return verb;
        }

        boolean hasLocalRequest()
        {
            return targets.hasLocal();
        }

        Iterable<? extends Request<P, Q>> remoteRequests()
        {
            Iterable<Request<P, Q>> withoutForwards = Iterables.transform(targets.nonForwardingRemotes(),
                                                                          to -> verb.newRequest(to, messageData));

            if (!targets.hasForwards())
                return withoutForwards;

            return Iterables.concat(withoutForwards,
                                    Iterables.<MessageTargets.WithForwards, Request<P, Q>>transform(targets.remotesWithForwards(), t -> verb.newRequestWithForwards(t.target, messageData, t.forwards)));
        }

        Request<P, Q> localRequest()
        {
            return verb.newRequest(local, messageData);
        }
    }
}
