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

import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;
import io.reactivex.Single;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.flow.RxThreads;

/**
 * A CQL query
 */
public class QueryMessage extends Message.Request
{
    public static final Message.Codec<QueryMessage> codec = new Message.Codec<QueryMessage>()
    {
        public QueryMessage decode(ByteBuf body, ProtocolVersion version)
        {
            String query = CBUtil.readLongString(body);
            return new QueryMessage(query, QueryOptions.codec.decode(body, version));
        }

        public void encode(QueryMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeLongString(msg.query, dest);
            if (version == ProtocolVersion.V1)
                CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
            else
                QueryOptions.codec.encode(msg.options, dest, version);
        }

        public int encodedSize(QueryMessage msg, ProtocolVersion version)
        {
            int size = CBUtil.sizeOfLongString(msg.query);

            if (version == ProtocolVersion.V1)
            {
                size += CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency());
            }
            else
            {
                size += QueryOptions.codec.encodedSize(msg.options, version);
            }
            return size;
        }
    };

    public final String query;
    public final QueryOptions options;

    public QueryMessage(String query, QueryOptions options)
    {
        super(Type.QUERY);
        this.query = query;
        this.options = options;
    }

    public Single<? extends Response> execute(Single<QueryState> state, long queryStartNanoTime)
    {
        try
        {
            final UUID tracingID = setUpTracing();
            QueryHandler handler = ClientState.getCQLQueryHandler();

            return state.flatMap( s ->
            {
                checkIsLoggedIn(s);

                Single<ResultMessage> resp = Single.defer(() ->
                {
                    return handler.process(query, s, options, getCustomPayload(), queryStartNanoTime)
                                      .map(response -> 
                                      {
                                          if (options.skipMetadata() && response instanceof ResultMessage.Rows)
                                              ((ResultMessage.Rows) response).result.metadata.setSkipMetadata();

                                          response.setTracingId(tracingID);

                                          return response;
                                      })
                                      .flatMap(response -> Tracing.instance.stopSessionAsync().toSingleDefault(response));
                });

            // If some Auth data was not in the caches we might be on an IO thread. In which case we need to switch
            // back to the TPC thread.
            if (!TPC.isTPCThread())
                return RxThreads.subscribeOn(resp, TPC.bestTPCScheduler(), TPCTaskType.EXECUTE_STATEMENT);

            return resp;
        });

        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            if (!((e instanceof RequestValidationException) || (e instanceof RequestExecutionException)))
                logger.error("Unexpected error during query", e);
            return Tracing.instance.stopSessionAsync()
                                   .toSingleDefault(ErrorMessage.fromException(e));
        }
    }

    private UUID setUpTracing()
    {
        if(!shouldTraceRequest())
            return null;

        final UUID sessionId = Tracing.instance.newSession(getCustomPayload());

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put("query", query);
        if (options.getPagingOptions() != null)
            builder.put("page_size", Integer.toString(options.getPagingOptions().pageSize().rawSize()));
        if (options.getConsistency() != null)
            builder.put("consistency_level", options.getConsistency().name());
        if (options.getSerialConsistency() != null)
            builder.put("serial_consistency_level", options.getSerialConsistency().name());

        Tracing.instance.begin("Execute CQL3 query", getClientAddress(), builder.build());
        return sessionId;
    }

    @Override
    public String toString()
    {
        return "QUERY " + query + (options.getPagingOptions() == null ? "" : "[pageSize = " + options.getPagingOptions().pageSize() + "]");
    }
}
