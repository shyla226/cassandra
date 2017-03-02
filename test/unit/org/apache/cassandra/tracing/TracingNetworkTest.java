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

package org.apache.cassandra.tracing;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.junit.Assert.*;

public class TracingNetworkTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
        DatabaseDescriptor.setBatchSizeWarnThresholdInKB(1);
    }

    @Test
    public void testTracingOn() throws Exception
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, ProtocolVersion.V4))
        {
            client.connect(false);

            QueryMessage query = new QueryMessage(String.format("INSERT INTO %s.%s (pk, v) VALUES (1, 'abc')", KEYSPACE, currentTable()),
                                                  QueryOptions.DEFAULT);
            query.setTracingRequested();
            Message.Response resp = client.execute(query);

            UUID id = resp.getTracingId();
            assertNotNull(id);

            query = new QueryMessage(String.format("SELECT * from system_traces.events where session_id = %s", id),
                                     QueryOptions.DEFAULT);
            resp = client.execute(query);
            assertNotNull(resp);
            assertTrue(resp instanceof ResultMessage.Rows);

            ResultSet tracingEvents = ((ResultMessage.Rows)resp).result;
            assertNotNull(tracingEvents);

            Set<String> loggingThreads = new HashSet<>();
            for (List<ByteBuffer> row : tracingEvents.rows)
            {
                StringBuilder rowText = new StringBuilder("TRACING: ");
                for (int i = 0; i < row.size(); i++)
                {
                    String name = tracingEvents.metadata.names.get(i).name.toCQLString();
                    String val = tracingEvents.metadata.names.get(i).type.asCQL3Type().toCQLLiteral(row.get(i), ProtocolVersion.V4);
                    rowText.append(String.format("%s=%s, ", name, val));

                    if (name.equals("thread"))
                        loggingThreads.add(val);
                }

                logger.debug(rowText.toString());
            }

            // for inserts at least two threads should participate in the insert
            // because the mutation is applied on the rx thread corresponding to the correct
            // token range, which for pk=1 is monitoredEpollEventLoopGroup-1-3, whilst the request
            // is handled by monitoredEpollEventLoopGroup-1-2
            assertTrue(loggingThreads.size() > 1);
        }
    }
}
