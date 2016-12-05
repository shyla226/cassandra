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

import java.io.IOException;
import java.net.InetAddress;

import javax.annotation.Nullable;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tracing.Tracing;

/**
 * A message for the inter-node messaging protocol.
 * <p>
 * In pratice, every message is either a {@link Request} or a {@link Response} and this class groups the data shared
 * by both types of message.
 * <p>
 * A message is an immutable object.
 *
 * @param <P> the type of the payload of this message.
 */
public abstract class Message<P>
{
    public enum Kind {
        GOSSIP, SMALL, LARGE;

        @Override
        public String toString()
        {
            return name().substring(0, 1) + name().substring(1).toLowerCase();
        }
    }

    /**
     * Groups data contained in a message that we often have to pass around without modification.
     * This class mainly serves in making the code more readable. It also shouldn't be public, but is due to the Tracing
     * ugly hack, {@link Tracing#TRACE_MSG_DEF}.
     */
    public static class Data<P>
    {
        private final P payload;
        // The size of the payload serialized in the current messaging version. This is cached here because we need this
        // both to segregate messages by size (big/small for OutboundTcpConnectionPool) and then for the actual serialization
        final long payloadSize;

        private final long createdAtMillis;
        private final long timeoutMillis;

        private final MessageParameters parameters;

        @Nullable
        private final Tracing.SessionInfo tracingInfo; // null if not tracing

        Data(P payload,
             long payloadSize,
             long createdAtMillis,
             long timeoutMillis,
             MessageParameters parameters,
             Tracing.SessionInfo tracingInfo)
        {
            this.payload = payload;
            this.payloadSize = payloadSize;
            this.createdAtMillis = createdAtMillis;
            this.timeoutMillis = timeoutMillis;
            this.parameters = parameters;
            this.tracingInfo = tracingInfo;
        }

        /**
         * Only public because of the tracing ugly hack (see comment on {@link Tracing#TRACE_MSG_DEF} for details on
         * said hack).
         */
        public Data(P payload,
                    long payloadSize,
                    long createdAtMillis,
                    long timeoutMillis)
        {
            this(payload,
                 payloadSize,
                 createdAtMillis,
                 timeoutMillis,
                 MessageParameters.EMPTY,
                 Tracing.isTracing() ? Tracing.instance.sessionInfo() : null);
        }

        // Mostly for tests
        Data(P payload)
        {
            this(payload, -1, System.currentTimeMillis(), Long.MAX_VALUE);

        }

        <Q> Data<Q> withPayload(Q payload, long payloadSize)
        {
            return new Data<>(payload, payloadSize, createdAtMillis, timeoutMillis, parameters, tracingInfo);
        }

        Data<P> withAddedParameters(MessageParameters newParameters)
        {
            return new Data<>(payload, payloadSize, createdAtMillis, timeoutMillis, parameters.unionWith(newParameters), tracingInfo);
        }
    }

    static final Message<?> CLOSE_SENTINEL = new Message<Void>(null, null, -1, null)
    {
        public Verb<?, ?> verb()
        {
            return null;
        }

        public boolean isRequest()
        {
            return false;
        }

        public Message<Void> addParameters(MessageParameters parameters)
        {
            throw new UnsupportedOperationException();
        }

        long payloadSerializedSize(MessagingVersion version)
        {
            return 0;
        }
    };

    private final InetAddress from;
    private final InetAddress to;
    private final int id;

    final Data<P> messageData;

    protected Message(InetAddress from,
                      InetAddress to,
                      int id,
                      Data<P> messageData)
    {
        this.from = from;
        this.to = to;
        this.id = id;
        this.messageData = messageData;
    }

    /**
     * Creates a message serializer for the provided messaging protocol version.
     *
     * @param version the version for which to create the serializer.
     * @param baseTimestampMillis the base timestamp to use for the serializer. This value is exchanged on connection
     *                            during the handshake and is used to delta-encode message timestamps.
     * @return a newly created message serializer.
     */
    public static Serializer createSerializer(MessagingVersion version, long baseTimestampMillis)
    {
        return version.isDSE()
               ? new MessageSerializer(version, baseTimestampMillis)
               : new OSSMessageSerializer(version);
    }

    /**
     * The source of the message.
     */
    public InetAddress from()
    {
        return from;
    }

    /**
     * The destination of the message.
     */
    public InetAddress to()
    {
        return to;
    }

    /**
     * The group of the message.
     * <p>
     * This is basically just a shortcut for {@code verb().group()}.
     */
    public VerbGroup<?> group()
    {
        return verb().group();
    }

    /**
     * The verb this is a message of.
     */
    public abstract Verb<?, ?> verb();

    /**
     * The ID of the message, used to associate responses to their initial requests.
     * <p>
     * Request that are of one-way message definitions will always return -1.
     */
    int id()
    {
        return id;
    }

    /**
     * Whether this is a request message.
     *
     * @return {@code true} if it is a request message (in which case it can be safely casted to a {@link Request}),
     * {@code false} if it is a response one (in which case it can be safely casted to a {@link Response}).
     */
    public abstract boolean isRequest();

    /**
     * Whether this is a response message.
     * <p>
     * This is a shortcut for {@code !isRequest()}.
     */
    public boolean isResponse()
    {
        return !isRequest();
    }

    /**
     * The time at which the "operation" this is a message of started in milliseconds.
     * <p>
     * This time is used for the purpose of timeouting messages and monitoring of the operation the message is for.
     * Message are sent to perform some form of operation, and evey operation that require inter-node request-response
     * communication must have a timeout to deal with the possibility of a remote never responding. And we want to
     * drop message that are for operations that we know have timed out, since they are not useful anymore. So this time
     * represents the starting time from which to count the {@link #timeoutMillis}, that is the start of the operation
     * of which this is a message of.
     * <p>
     * Note that for requests, this is roughly the creation time of the message, but responses inherit the starting time
     * of the request they are a response to (since they're still part of the same overall operation).
     */
    long operationStartMillis()
    {
        return messageData.createdAtMillis;
    }

    /**
     * The timeout in milliseconds for the message/operation this is a message of.
     */
    public long timeoutMillis()
    {
        return messageData.timeoutMillis;
    }
    
    /**
     * Whether the message is timed out and can be dropped.
     */
    boolean isTimedOut()
    {
        // Note: we used to have a list of droppable verbs (it's still in OSS too) but we don't anymore (since APOLLO-497).
        // Instead, all two-way verbs can be dropped once timed out. The rational is that any two-way verb better have a
        // timeout (failure happens, we should not wait indefinitely), but then once that timeout elapses, whomever sent
        // the message will have given up on the response _and_ will have taken any actions necessary on the assumption
        // that the message didn't make it through. So there is no reason not to drop a timed out message, and dropping them
        // is desirable to help recover more quickly when overwhelmed. And not having a list that we're bound to forget
        // to update at times is a plus.
        return !verb().isOneWay() && lifetimeMillis() > timeoutMillis();
    }

    /**
     * The lifetime of the message (or rather of the operation it represents), which is since how long the message has been created.
     */
    long lifetimeMillis()
    {
        return System.currentTimeMillis() - operationStartMillis();
    }

    Stage stage()
    {
        return isRequest()
               ? verb().requestStage()
               : (group().isInternal() ? Stage.INTERNAL_RESPONSE : Stage.REQUEST_RESPONSE);
    }

    /**
     * Whether the message is a locally delivered one, meaning if {@code to() == from()}.
     */
    public boolean isLocal()
    {
        return to.equals(from);
    }

    /**
     * Message custom parameters.
     */
    public MessageParameters parameters()
    {
        return messageData.parameters;
    }

    /**
     * Returns a new message identical to this one except that the provided parameters are added to the existing parameters in
     * this message.
     *
     * @param parameters the additional parameters for the returned message.
     * @return a newly created message equivalent to this one, but with {@code parameters} added to the message parameters.
     */
    public abstract Message<P> addParameters(MessageParameters parameters);

    /**
     * Whether tracing is enabled for the operation this is a message of.
     */
    public boolean isTraced()
    {
        return messageData.tracingInfo != null;
    }

    /**
     * If tracing is enabled, the tracing session information, {@code null} otherwise.
     */
    public Tracing.SessionInfo tracingInfo()
    {
        return messageData.tracingInfo;
    }

    public P payload()
    {
        return messageData.payload;
    }

    abstract long payloadSerializedSize(MessagingVersion version);

    public Kind kind()
    {
        if (group() == Verbs.GOSSIP)
            return Kind.GOSSIP;

        // Note that payloadSize is in the current messaging version, which may not be the version we're gonna send
        // the message in if there is mixed-versions nodes. This doesn't matter in practice though, as the version
        // won't dramatically change the size, which is good enough.
        return messageData.payloadSize > OutboundTcpConnectionPool.LARGE_MESSAGE_THRESHOLD ? Kind.LARGE : Kind.SMALL;
    }

    @Override
    public String toString()
    {
        return String.format("%s@%d: %s %s %s", verb(), id(), from(), isRequest() ? "->" : "<-", to());
    }

    public interface Serializer
    {
        public <P> void serialize(Message<P> message, DataOutputPlus out) throws IOException;
        public <P> long serializedSize(Message<P> message);
        public <P> Message<P> deserialize(DataInputPlus in, InetAddress from) throws IOException;

        // Only abstracted here because the OSS side doesn't write the fully message serialized size (and so override this)
        default public void writeSerializedSize(int serializedSize, DataOutputPlus out) throws IOException
        {
            out.writeInt(serializedSize);
        }

        // Only abstracted here because the OSS side doesn't write the fully message serialized size (and so override this)
        default public int readSerializedSize(DataInputPlus in) throws IOException
        {
            return in.readInt();
        }
    }
}
