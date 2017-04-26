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
package org.apache.cassandra.transport.messages;

import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;
import io.reactivex.Single;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.JVMStabilityInspector;

public class PrepareMessage extends Message.Request
{
    public static final Message.Codec<PrepareMessage> codec = new Message.Codec<PrepareMessage>()
    {
        public PrepareMessage decode(ByteBuf body, ProtocolVersion version)
        {
            String query = CBUtil.readLongString(body);
            return new PrepareMessage(query);
        }

        public void encode(PrepareMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeLongString(msg.query, dest);
        }

        public int encodedSize(PrepareMessage msg, ProtocolVersion version)
        {
            return CBUtil.sizeOfLongString(msg.query);
        }
    };

    private final String query;

    public PrepareMessage(String query)
    {
        super(Message.Type.PREPARE);
        this.query = query;
    }

    public Single<? extends Response> execute(QueryState state, long queryStartNanoTime)
    {
        try
        {
            if (state.shouldTraceRequest(isTracingRequested()))
            {
                state.createTracingSession(getCustomPayload());
                Tracing.instance.begin("Preparing CQL3 query", state.getClientAddress(), ImmutableMap.of("query", query));
            }

            return ClientState.getCQLQueryHandler().prepare(query, state, getCustomPayload())
                              .map(response ->
                                  {
                                      response.setTracingId(state.getPreparedTracingSession());
                                      return response;
                                  })
                             .doFinally(Tracing.instance::stopSession);
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            return Single.just(ErrorMessage.fromException(e));
        }
    }

    @Override
    public String toString()
    {
        return "PREPARE " + query;
    }
}
