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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WriteHandlerTest
{
    private static Keyspace ks;
    private static List<InetAddress> targets;

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        // Register peers with expected DC for NetworkTopologyStrategy.
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        metadata.updateHostId(UUID.randomUUID(), InetAddress.getByName("127.1.0.255"));
        metadata.updateHostId(UUID.randomUUID(), InetAddress.getByName("127.2.0.255"));

        DatabaseDescriptor.setEndpointSnitch(new IEndpointSnitch()
        {
            public String getRack(InetAddress endpoint)
            {
                return null;
            }

            public String getDatacenter(InetAddress endpoint)
            {
                byte[] address = endpoint.getAddress();
                if (address[1] == 1)
                    return "datacenter1";
                else
                    return "datacenter2";
            }

            public List<InetAddress> getSortedListByProximity(InetAddress address, Collection<InetAddress> unsortedAddress)
            {
                return null;
            }

            public void sortByProximity(InetAddress address, List<InetAddress> addresses)
            {

            }

            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }

            public void gossiperStarting()
            {

            }

            public boolean isWorthMergingForRangeQuery(List<InetAddress> merged, List<InetAddress> l1, List<InetAddress> l2)
            {
                return false;
            }
        });
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.1.0.1"));
        SchemaLoader.createKeyspace("Foo", KeyspaceParams.nts("datacenter1", 3, "datacenter2", 3), SchemaLoader.standardCFMD("Foo", "Bar"));
        ks = Keyspace.open("Foo");
        targets = ImmutableList.of(InetAddress.getByName("127.1.0.255"), InetAddress.getByName("127.1.0.254"), InetAddress.getByName("127.1.0.253"),
                                   InetAddress.getByName("127.2.0.255"), InetAddress.getByName("127.2.0.254"), InetAddress.getByName("127.2.0.253"));
    }

    @Before
    public void resetCounters()
    {
        ks.metric.writeFailedIdealCL.dec(ks.metric.writeFailedIdealCL.getCount());
    }

    /**
     * Validate that a successful write at ideal CL logs latency information. Also validates
     * DatacenterSyncWriteResponseHandler
     */
    @Test
    public void idealCLLatencyTracked() throws Throwable
    {
        long startingCount = ks.metric.idealCLWriteLatency.latency.getCount();

        //Specify query start time in past to ensure minimum latency measurement
        WriteHandler handler = writeHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM, System.nanoTime() - TimeUnit.DAYS.toNanos(1));

        //dc1
        handler.onResponse(createDummyMessage(0));
        handler.onResponse(createDummyMessage(1));
        //dc2
        handler.onResponse(createDummyMessage(4));
        handler.onResponse(createDummyMessage(5));

        //Don't need the others
        handler.onTimeout(targets.get(2));
        handler.onTimeout(targets.get(3));

        assertEquals(0,  ks.metric.writeFailedIdealCL.getCount());
        assertTrue( TimeUnit.DAYS.toMicros(1) < ks.metric.idealCLWriteLatency.totalLatency.getCount());
        assertEquals(startingCount + 1, ks.metric.idealCLWriteLatency.latency.getCount());
    }

    /**
     * Validate that WriteResponseHandler does the right thing on success.
     */
    @Test
    public void idealCLWriteResponeHandlerWorks() throws Throwable
    {
        long startingCount = ks.metric.idealCLWriteLatency.latency.getCount();

        WriteHandler handler = writeHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.ALL, System.nanoTime());

        //dc1
        handler.onResponse(createDummyMessage(0));
        handler.onResponse(createDummyMessage(1));
        handler.onResponse(createDummyMessage(2));
        //dc2
        handler.onResponse(createDummyMessage(3));
        handler.onResponse(createDummyMessage(4));
        handler.onResponse(createDummyMessage(5));

        assertEquals(0,  ks.metric.writeFailedIdealCL.getCount());
        assertEquals(startingCount + 1, ks.metric.idealCLWriteLatency.latency.getCount());
    }

    /**
     * Validate that DatacenterWriteResponseHandler does the right thing on success.
     */
    @Test
    public void idealCLDatacenterWriteResponeHandlerWorks() throws Throwable
    {
        long startingCount = ks.metric.idealCLWriteLatency.latency.getCount();
        WriteHandler handler = writeHandler(ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_QUORUM, System.nanoTime());

        //dc1
        handler.onResponse(createDummyMessage(0));
        handler.onResponse(createDummyMessage(1));
        handler.onResponse(createDummyMessage(2));
        //dc2
        handler.onResponse(createDummyMessage(3));
        handler.onResponse(createDummyMessage(4));
        handler.onResponse(createDummyMessage(5));

        assertEquals(0,  ks.metric.writeFailedIdealCL.getCount());
        assertEquals(startingCount + 1, ks.metric.idealCLWriteLatency.latency.getCount());
    }

    /**
     * Validate that failing to achieve ideal CL increments the failure counter
     */
    @Test
    public void failedIdealCLIncrementsStat() throws Throwable
    {
        WriteHandler handler = writeHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM, System.nanoTime());

        //Succeed in local DC
        handler.onResponse(createDummyMessage(0));
        handler.onResponse(createDummyMessage(1));
        handler.onResponse(createDummyMessage(2));

        //Fail in remote DC
        handler.onTimeout(targets.get(3));
        handler.onTimeout(targets.get(4));
        handler.onTimeout(targets.get(5));
        assertEquals(1, ks.metric.writeFailedIdealCL.getCount());
        assertEquals(0, ks.metric.idealCLWriteLatency.totalLatency.getCount());
    }

    private WriteHandler writeHandler(ConsistencyLevel cl, ConsistencyLevel idealCl, long nanoTime)
    {
        return WriteHandler.builder(WriteEndpoints.withLive(ks, targets), cl, WriteType.SIMPLE, nanoTime)
                           .withIdealConsistencyLevel(idealCl)
                           .build();
    }

    private static Response<EmptyPayload> createDummyMessage(int target)
    {
        return Response.testResponse(targets.get(target),
                                     FBUtilities.getBroadcastAddress(),
                                     Verbs.WRITES.WRITE,
                                     EmptyPayload.instance);
    }
}
