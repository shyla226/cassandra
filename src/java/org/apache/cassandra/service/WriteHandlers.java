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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Response;

abstract class WriteHandlers
{
    // Not a real class, just a 'namespace' to group the basic implementations
    private WriteHandlers() {}

    /**
     * Handles writes for any non-datacenter specific consistency level (ONE, ANY, TWO, THREE, QUORUM, and ALL).
     */
    static class SimpleHandler extends AbstractWriteHandler
    {
        protected final AtomicInteger responses = new AtomicInteger(0);

        SimpleHandler(WriteEndpoints endpoints,
                      ConsistencyLevel consistency,
                      WriteType writeType,
                      long queryStartNanos)
        {
            super(endpoints, consistency, writeType, queryStartNanos);
        }

        public void onResponse(Response<EmptyPayload> response)
        {
            if (!waitingFor(response.from()))
                return;

            if (responses.incrementAndGet() == blockFor)
                complete(null);
        }

        protected int pendingToBlockFor()
        {
            // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
            // guarantees (see #833)
            return endpoints.pendingCount();
        }

        protected int ackCount()
        {
            return responses.get();
        }
    }

    /**
     * Handles DC local consistency level (CL.LOCAL_QUORUM).
     */
    static class DatacenterLocalHandler extends SimpleHandler
    {
        DatacenterLocalHandler(WriteEndpoints endpoints,
                               ConsistencyLevel consistency,
                               WriteType writeType,
                               long queryStartNanos)
        {
            super(endpoints, consistency, writeType, queryStartNanos);
        }

        protected int pendingToBlockFor()
        {
            // during bootstrap, include pending endpoints (only local here) in the count
            // or we may fail the consistency level guarantees (see #833, #8058)
            return consistency.countLocalEndpoints(endpoints.pending());
        }

        @Override
        protected boolean waitingFor(InetAddress from)
        {
            return consistency.isLocal(from);
        }
    }

    /**
     * Handles EACH_QUORUM consistency level, blocking for a quorum of responses _in all datacenters_.
     */
    static class DatacenterSyncHandler extends AbstractWriteHandler
    {
        private static final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

        private final Map<String, AtomicInteger> responses = new HashMap<>();
        private final AtomicInteger acks = new AtomicInteger(0);

        DatacenterSyncHandler(WriteEndpoints endpoints,
                              ConsistencyLevel consistency,
                              WriteType writeType,
                              long queryStartNanos)
        {
            super(endpoints, consistency, writeType, queryStartNanos);
            assert consistency == ConsistencyLevel.EACH_QUORUM;

            NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) endpoints.keyspace().getReplicationStrategy();

            for (String dc : strategy.getDatacenters())
            {
                int rf = strategy.getReplicationFactor(dc);
                responses.put(dc, new AtomicInteger((rf / 2) + 1));
            }

            // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
            // guarantees (see #833)
            for (InetAddress pending : endpoints.pending())
                responses.get(snitch.getDatacenter(pending)).incrementAndGet();
        }

        public void onResponse(Response<EmptyPayload> response)
        {
            responses.get(snitch.getDatacenter(response.from())).getAndDecrement();
            acks.incrementAndGet();

            for (AtomicInteger i : responses.values())
            {
                if (i.get() > 0)
                    return;
            }

            // all the quorum conditions are met
            complete(null);
        }

        protected int ackCount()
        {
            return acks.get();
        }
    }
}
