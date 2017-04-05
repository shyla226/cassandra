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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Connection;

/**
 * Represents the state related to a given query.
 */
public class QueryState
{
    private final ClientState clientState;
    private final int streamId;
    private volatile UUID preparedTracingSession;

    public QueryState(ClientState clientState)
    {
        this(clientState, 0);
    }

    public QueryState(ClientState clientState, int streamId)
    {
        this.clientState = clientState;
        this.streamId = streamId;
    }

    /**
     * @return a QueryState object for internal C* calls (not limited by any kind of auth).
     */
    public static QueryState forInternalCalls()
    {
        return new QueryState(ClientState.forInternalCalls());
    }

    public ClientState getClientState()
    {
        return clientState;
    }

    /**
     * This clock guarantees that updates for the same QueryState will be ordered
     * in the sequence seen, even if multiple updates happen in the same millisecond.
     */
    public long getTimestamp()
    {
        return clientState.getTimestamp();
    }

    public boolean shouldTraceRequest(boolean tracingRequested)
    {
        if (tracingRequested)
            return true;

        // If no tracing is explicitly requested in the message, eventually trace the query according to configured trace probability.
        double traceProbability = StorageService.instance.getTraceProbability();
        return traceProbability != 0 && ThreadLocalRandom.current().nextDouble() < traceProbability;
    }

    public void createTracingSession()
    {
        createTracingSession(Collections.emptyMap());
    }

    public void createTracingSession(Map<String,ByteBuffer> customPayload)
    {
        preparedTracingSession = Tracing.instance.newSession(customPayload);
    }

    public InetAddress getClientAddress()
    {
        return clientState.isInternal
             ? null
             : clientState.getRemoteAddress().getAddress();
    }

    public int getStreamId()
    {
        return streamId;
    }

    public Connection getConnection()
    {
        return clientState.connection;
    }

    public UUID getPreparedTracingSession()
    {
        return preparedTracingSession;
    }
}
