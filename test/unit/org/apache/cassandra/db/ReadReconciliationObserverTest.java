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
package org.apache.cassandra.db;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.interceptors.InterceptionContext;
import org.apache.cassandra.net.interceptors.Interceptor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ReadCallback;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.flow.Flow;

import static org.junit.Assert.*;

// Using CQLTester really just to make table creation and general initialization easier.
public class ReadReconciliationObserverTest extends CQLTester
{
    // counter to generate the last byte of the respondent's address in a ReadResponse message
    private int addressSuffix = 10;

    @Before
    public void injectMessageSink()
    {
        // install an interceptor to discard all messages so tests don't try to create connections to nodes that don't exist
        MessagingService.instance().addInterceptor(new Interceptor()
        {
            public <M extends Message<?>> void intercept(M message, InterceptionContext<M> context)
            {
                context.drop(message);
            }
        });
    }

    @After
    public void removeMessageSink()
    {
        // should be unnecessary, but good housekeeping
        MessagingService.instance().clearInterceptors();
    }

    @Test
    public void testReconciliationObserverOnDigestMatch()
    {
        createTable("CREATE TABLE %s (k int, t int, v int, PRIMARY KEY (k, t))");

        TableMetadata table = currentTableMetadata();

        ReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(table,
                                                                       FBUtilities.nowInSeconds(),
                                                                       ByteBufferUtil.bytes(0));

        InetAddress peer1 = peer();
        InetAddress peer2 = peer();
        InetAddress peer3 = peer();

        ReadContext ctx = readContext(cmd, true);
        List<InetAddress> peers = Arrays.asList(peer1, peer2, peer3);
        ReadCallback<FlowablePartition> callback = ReadCallback.forInitialRead(cmd, peers, ctx);

        TestObserver observer = (TestObserver) ctx.readObserver;
        assert observer != null;

        PartitionUpdate full = update(table, row(0, 0), row(1, 1), row(2, 2));

        callback.onResponse(dataResponse(peer1, full, cmd));
        callback.onResponse(digestResponse(peer2, full, cmd));
        callback.onResponse(digestResponse(peer3, full, cmd));

        consume(callback);

        assertEquals(peers, observer.responded);
        assertTrue(observer.hadDigestMatch);
        assertFalse(observer.hadDigestMismatch);
    }

    @Test
    public void testReconciliationObserverOnDigestMismatch()
    {
        createTable("CREATE TABLE %s (k int, t int, v int, PRIMARY KEY (k, t))");

        TableMetadata table = currentTableMetadata();

        ReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(table,
                                                                       FBUtilities.nowInSeconds(),
                                                                       ByteBufferUtil.bytes(0));
        InetAddress peer1 = peer();
        InetAddress peer2 = peer();
        InetAddress peer3 = peer();

        ReadContext ctx = readContext(cmd, true);
        List<InetAddress> peers = Arrays.asList(peer1, peer2, peer3);
        ReadCallback<FlowablePartition> callback = ReadCallback.forInitialRead(cmd, peers, ctx);

        TestObserver observer = (TestObserver) ctx.readObserver;
        assert observer != null;

        PartitionUpdate full = update(table, row(0, 0), row(1, 1), row(2, 2));
        PartitionUpdate partial = update(table, row(0, 0), row(2, 2));

        callback.onResponse(dataResponse(peer1, full, cmd));
        callback.onResponse(digestResponse(peer2, partial, cmd));
        callback.onResponse(digestResponse(peer3, full, cmd));

        consume(callback);

        assertEquals(peers, observer.responded);
        assertFalse(observer.hadDigestMatch);
        assertTrue(observer.hadDigestMismatch);
    }

    @Test
    public void testReconciliationObserverDataRead()
    {
        createTable("CREATE TABLE %s (k int, t int, v int, PRIMARY KEY (k, t))");

        TableMetadata table = currentTableMetadata();

        ReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(table,
                                                                       FBUtilities.nowInSeconds(),
                                                                       ByteBufferUtil.bytes(0));
        InetAddress peer1 = peer();
        InetAddress peer2 = peer();
        InetAddress peer3 = peer();

        ReadContext ctx = readContext(cmd, false);
        List<InetAddress> peers = Arrays.asList(peer1, peer2, peer3);
        ReadCallback<FlowablePartition> callback = ReadCallback.forInitialRead(cmd, peers, ctx);

        TestObserver observer = (TestObserver) ctx.readObserver;
        assert observer != null;

        PartitionUpdate full = update(table, row(0, 0), row(1, 1), row(2, 2));
        full.addPartitionDeletion(new DeletionTime(1, 1));
        full.add(new RangeTombstone(Slice.make(new BufferClustering(ByteBufferUtil.bytes(3))),
                                    new DeletionTime(2, 2)));

        PartitionUpdate partial1 = update(table, row(0, 0), row(2, 2));

        PartitionUpdate partial2 = update(table, row(0, 0), row(2, 2));
        partial2.addPartitionDeletion(new DeletionTime(1, 1));

        callback.onResponse(dataResponse(peer1, full, cmd));
        callback.onResponse(dataResponse(peer2, partial1, cmd));
        callback.onResponse(dataResponse(peer3, partial2, cmd));

        consume(callback);

        assertEquals(peers, observer.responded);

        // Not a digest query, so neither a match or a mismatch
        assertFalse(observer.hadDigestMatch);
        assertFalse(observer.hadDigestMismatch);

        assertEquals(1, observer.inconsistentPartitionDeletions);
        assertEquals(0, observer.consistentPartitionDeletions);

        assertEquals(1, observer.inconsistentRows);
        assertEquals(2, observer.consistentRows);

        assertEquals(2, observer.inconsistentMarkers);
        assertEquals(0, observer.consistentMarkers);

        assertEquals(2, observer.repairSent); // All inconsistencies are in the same partition, but we sent it
                                                       // to 2 nodes
    }

    private void consume(ReadCallback<FlowablePartition> callback)
    {
        // Note that we're not waiting on the completion of the future. This is on purpose, because the underlying
        // processing will potentially block on read-repair responses for a data read and we actually throw away
        // network messages (see injectMessageSink()) so said response will never come and we'd timeout if we where
        // to wait on completion. All we care is that all content gets processed. Note that this is all in-memory
        // anyway so we're not really leaking resources (not that it would matter much).
        callback.result().flatProcess(p -> p.content).processToFuture();
    }

    private PartitionUpdate update(TableMetadata table, int[]... rows)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(table, 0)
                                                               .timestamp(10);
        for (int[] row : rows)
            builder.row(row[0]).add("v", row[1]);
        return builder.build();
    }

    private int[] row(int t, int v)
    {
        return new int[]{ t, v };
    }

    private Flow<FlowableUnfilteredPartition> flow(PartitionUpdate update)
    {
        return FlowablePartitions.fromPartitions(new SingletonUnfilteredPartitionIterator(update.unfilteredIterator()), null);
    }

    private Response<ReadResponse> dataResponse(InetAddress peer, PartitionUpdate values, ReadCommand cmd)
    {
        return response(peer, ReadResponse.createRemoteDataResponse(flow(values), cmd));
    }

    private Response<ReadResponse> digestResponse(InetAddress peer, PartitionUpdate values, ReadCommand cmd)
    {
        return response(peer, ReadResponse.createDigestResponse(flow(values), cmd).blockingGet());
    }

    private Response<ReadResponse> response(InetAddress peer, ReadResponse payload)
    {
        return Response.testResponse(peer, FBUtilities.getBroadcastAddress(), Verbs.READS.SINGLE_READ, payload);
    }

    private ReadContext readContext(ReadCommand command, boolean useDigests)
    {
        // Note that we need to  use "block on all targets" because we're faking 3 nodes but we only have one so by
        // default even CL.ALL would only wait for 1.
        return ReadContext.builder(command, ConsistencyLevel.ALL)
                          .observer(new TestObserver())
                          .blockForAllTargets()
                          .useDigests(useDigests)
                          .build(System.nanoTime());
    }

    private InetAddress peer()
    {
        try
        {
            return InetAddress.getByAddress(new byte[]{ 127, 0, 0, (byte) addressSuffix++ });
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static class TestObserver implements ReadReconciliationObserver
    {
        private Collection<InetAddress> responded;
        private boolean hadDigestMatch;
        private boolean hadDigestMismatch;

        private int consistentPartitionDeletions;
        private int inconsistentPartitionDeletions;

        private int consistentRows;
        private int inconsistentRows;

        private int consistentMarkers;
        private int inconsistentMarkers;

        private int repairSent;

        public void responsesReceived(Collection<InetAddress> responded)
        {
            this.responded = responded;
        }

        public void onDigestMatch()
        {
            hadDigestMatch = true;
        }

        public void onDigestMismatch()
        {
            hadDigestMismatch = true;
        }

        public void onPartition(DecoratedKey partitionKey)
        {
        }

        public void onPartitionDeletion(DeletionTime deletion, boolean isConsistent)
        {
            if (isConsistent)
                ++consistentPartitionDeletions;
            else
                ++inconsistentPartitionDeletions;
        }

        public void onRow(Row row, boolean isConsistent)
        {
            if (isConsistent)
                ++consistentRows;
            else
                ++inconsistentRows;
        }

        public void onRangeTombstoneMarker(RangeTombstoneMarker marker, boolean isConsistent)
        {
            if (isConsistent)
                ++consistentMarkers;
            else
                ++inconsistentMarkers;
        }

        public void onRepair(InetAddress endpoint, PartitionUpdate repair)
        {
            ++repairSent;
        }
    }
}