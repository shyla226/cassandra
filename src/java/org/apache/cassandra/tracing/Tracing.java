/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.tracing;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.concurrent.ExecutorLocal;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb.AckedRequest;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.WrappedRunnable;


/**
 * A trace session context. Able to track and store trace sessions. A session is usually a user initiated query, and may
 * have multiple local and remote events before it is completed.
 */
public abstract class Tracing implements ExecutorLocal<TraceState>
{
    // A "fake" message definition that exists only to reuse the droppedMessagesMap in MessagingService when the
    // tracing executor backlog and we have to drop tracing tasks on the floor (see onDroppedTask in particular).
    // Note that most stuffs are null, so we totally rely on the fact that this definition and its message are never
    // used for anything else.
    public static final AckedRequest<EmptyPayload> TRACE_MSG_DEF = new AckedRequest<EmptyPayload>(null, request -> 0, null)
    {
        @Override
        public String toString()
        {
            return "_TRACE";
        }
    };

    public enum TraceType
    {
        NONE,
        QUERY,
        REPAIR;

        private static final TraceType[] ALL_VALUES = values();

        public static TraceType deserialize(byte b)
        {
            if (b < 0 || ALL_VALUES.length <= b)
                return NONE;
            return ALL_VALUES[b];
        }

        public static byte serialize(TraceType value)
        {
            return (byte) value.ordinal();
        }

        private static final int[] TTLS = { DatabaseDescriptor.getTracetypeQueryTTL(),
                                            DatabaseDescriptor.getTracetypeQueryTTL(),
                                            DatabaseDescriptor.getTracetypeRepairTTL() };

        public int getTTL()
        {
            return TTLS[ordinal()];
        }
    }

    protected static final Logger logger = LoggerFactory.getLogger(Tracing.class);

    private final InetAddress localAddress = FBUtilities.getLocalAddress();

    private final FastThreadLocal<TraceState> state = new FastThreadLocal<>();

    protected final ConcurrentMap<UUID, TraceState> sessions = new ConcurrentHashMap<>();

    public static final Tracing instance;

    /**
     * Called by the tracing executor when a tracing task cannot be executed due to a backlog of tracing tasks.
     *
     * @param runnable the task whose execution has been rejected.
     */
    public static void onDroppedTask(Runnable runnable)
    {
        long createdAtMillis = runnable instanceof TracingRunnable
                               ? ((TracingRunnable) runnable).createdAtMillis
                               : ApproximateTime.currentTimeMillis();
        Message.Data<EmptyPayload> data = new Message.Data<>(EmptyPayload.instance, 0, createdAtMillis, -1);
        MessagingService.instance().incrementDroppedMessages(TRACE_MSG_DEF.newRequest(FBUtilities.getBroadcastAddress(), data));
    }

    static
    {
        Tracing tracing = null;
        String customTracingClass = System.getProperty("cassandra.custom_tracing_class");
        if (null != customTracingClass)
        {
            try
            {
                tracing = FBUtilities.construct(customTracingClass, "Tracing");
                logger.info("Using {} as tracing queries (as requested with -Dcassandra.custom_tracing_class)", customTracingClass);
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.error(String.format("Cannot use class %s for tracing, ignoring by defaulting to normal tracing", customTracingClass), e);
            }
        }
        instance = null != tracing ? tracing : new TracingImpl();
    }

    public UUID getSessionId()
    {
        assert isTracing();
        return state.get().sessionId;
    }

    public TraceType getTraceType()
    {
        assert isTracing();
        return state.get().traceType;
    }

    public int getTTL()
    {
        assert isTracing();
        return state.get().ttl;
    }

    /**
     * Indicates if the current thread's execution is being traced.
     */
    public static boolean isTracing()
    {
        return instance.get() != null;
    }

    public UUID newSession(Map<String, ByteBuffer> customPayload)
    {
        return newSession(
        TimeUUIDType.instance.compose(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())),
        TraceType.QUERY,
        customPayload);
    }

    public UUID newSession(TraceType traceType)
    {
        return newSession(
        TimeUUIDType.instance.compose(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())),
        traceType,
        Collections.emptyMap());
    }

    public UUID newSession(UUID sessionId, Map<String, ByteBuffer> customPayload)
    {
        return newSession(sessionId, TraceType.QUERY, customPayload);
    }

    /**
     * This method is intended to be overridden in tracing implementations that need access to the customPayload
     */
    protected UUID newSession(UUID sessionId, TraceType traceType, Map<String, ByteBuffer> customPayload)
    {
        assert get() == null;

        TraceState ts = newTraceState(localAddress, sessionId, traceType);
        set(ts);
        sessions.put(sessionId, ts);

        return sessionId;
    }

    public void doneWithNonLocalSession(TraceState state)
    {
        if (state.releaseReference() == 0)
            sessions.remove(state.sessionId);
    }


    /**
     * Stop the session and record its complete.  Called by coodinator when request is complete.
     */
    public void stopSession()
    {
        TraceState state = get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
        {
            //logger.trace("request complete");
        }
        else
        {
            stopSessionImpl();

            state.stop();
            sessions.remove(state.sessionId);
            set(null);
        }
    }

    protected abstract void stopSessionImpl();

    public TraceState get()
    {
        return state.get();
    }

    public TraceState get(UUID sessionId)
    {
        return sessions.get(sessionId);
    }

    public void set(final TraceState tls)
    {
        state.set(tls);
    }

    public TraceState begin(final String request, final Map<String, String> parameters)
    {
        return begin(request, null, parameters);
    }

    public abstract TraceState begin(String request, InetAddress client, Map<String, String> parameters);

    /**
     * Determines the tracing context from a message.  Does NOT set the threadlocal state.
     *
     * @param message The internode message
     */
    public TraceState initializeFromMessage(final Message message)
    {
        if (!message.isTraced())
            return null;

        SessionInfo info = message.tracingInfo();

        TraceState ts = get(info.sessionId);
        if (ts != null && ts.acquireReference())
            return ts;

        ts = newTraceState(message.from(), info.sessionId, info.traceType);

        if (message.isRequest())
        {
            // Received a request on which tracing is enabled. We save the session
            // locally, it will be cleaned up on sending back the response in
            // onMessageSend()
            sessions.put(info.sessionId, ts);
            return ts;
        }
        else
        {
            return new ExpiredTraceState(ts);
        }
    }

    public void onMessageSend(Message message, int messageSerializedSize)
    {
        if (!message.isTraced())
            return;

        SessionInfo info = message.tracingInfo();
        TraceState state = get(info.sessionId);

        // Even if tracing is set on the message, it's possible for the tracing
        // session to have been removed locally, see CASSANDRA-5668.
        if (state == null)
            state = newTraceState(message.from(), info.sessionId, info.traceType);

        state.trace(String.format("Sending %s message to %s, size=%s bytes",
                                  message.verb(),
                                  message.to(),
                                  messageSerializedSize));

        // If it's the response to a request, we've set the session locally on receiving the
        // request (in initializeFromMessage) and should remove it now
        if (!message.isRequest())
            Tracing.instance.doneWithNonLocalSession(state);
    }

    public SessionInfo sessionInfo()
    {
        TraceState state = get();
        assert state != null;
        return new SessionInfo(state);
    }

    protected abstract TraceState newTraceState(InetAddress coordinator, UUID sessionId, Tracing.TraceType traceType);

    // repair just gets a varargs method since it's so heavyweight anyway
    public static void traceRepair(String format, Object... args)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(format, args);
    }

    // normal traces get zero-, one-, and two-argument overloads so common case doesn't need to create varargs array
    public static void trace(String message)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(message);
    }

    public static void trace(String format, Object arg)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(format, arg);
    }

    public static void trace(String format, Object arg1, Object arg2)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(format, arg1, arg2);
    }

    public static void trace(String format, Object... args)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(format, args);
    }

    /**
     * Called from {@link org.apache.cassandra.net.OutboundTcpConnection} for non-local traces (traces
     * that are not initiated by local node == coordinator).
     */
    public abstract void trace(ByteBuffer sessionId, String message, int ttl);

    public static class SessionInfo
    {
        final UUID sessionId;
        final TraceType traceType;

        private SessionInfo(TraceState state)
        {
            this(state.sessionId, state.traceType);
        }

        public SessionInfo(UUID sessionId, TraceType traceType)
        {
            this.sessionId = sessionId;
            this.traceType = traceType;
        }

        public void serialize(DataOutputPlus out) throws IOException
        {
            UUIDSerializer.serializer.serialize(sessionId, out);
            out.writeByte(TraceType.serialize(traceType));
        }

        public long serializedSize()
        {
            return UUIDSerializer.serializer.serializedSize(sessionId) + 1;
        }

        public static SessionInfo deserialize(DataInputPlus in) throws IOException
        {
            UUID sessionId = UUIDSerializer.serializer.deserialize(in);
            TraceType traceType = TraceType.deserialize((byte)in.readUnsignedByte());
            return new SessionInfo(sessionId, traceType);
        }
    }

    static abstract class TracingRunnable extends WrappedRunnable
    {
        final long createdAtMillis = System.currentTimeMillis();
    }
}
