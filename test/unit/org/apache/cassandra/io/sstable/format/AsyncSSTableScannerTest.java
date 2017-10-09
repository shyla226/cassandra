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

package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cache.ChunkCacheMocks;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.FlowablePartitionBase;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.Threads;

import static org.junit.Assert.*;
import static org.apache.cassandra.io.sstable.format.SSTableScannerUtils.*;

public class AsyncSSTableScannerTest
{
    public static final String KEYSPACE = "AsyncSSTableScannerTest";
    public static final String TABLE = "Standard1";

    private static final Random random = new Random(1233698798L);

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        DatabaseDescriptor.daemonInitialization();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    private static List<DecoratedKey> toKeys(Flow<FlowableUnfilteredPartition> partitions)
    {
        return partitions.map(FlowablePartitionBase::partitionKey).toList().blockingSingle();
    }

    private static void assertScanMatches(SSTableReader sstable, int scanStart, int scanEnd, int ... boundaries)
    {
        assert boundaries.length % 2 == 0;
        for (DataRange range : dataRanges(sstable.metadata(), scanStart, scanEnd))
        {
            List<DecoratedKey> keys = toKeys(sstable.getAsyncScanner(ColumnFilter.all(sstable.metadata()),
                                                                     range,
                                                                     SSTableReadsListener.NOOP_LISTENER));
            int current = 0;
            for (int b = 0; b < boundaries.length; b += 2)
                for (int i = boundaries[b]; i <= boundaries[b + 1]; i++)
                    assertEquals(toKey(i), new String(keys.get(current++).getKey().array()));

            assertEquals(keys.size(), current); // no more keys
        }
    }

    private static void assertScanEmpty(SSTableReader sstable, int scanStart, int scanEnd)
    {
        assertScanMatches(sstable, scanStart, scanEnd);
    }

    private static void assertScanContainsRanges(Flow<FlowableUnfilteredPartition> partitions, int ... rangePairs)
    {
        assert rangePairs.length % 2 == 0;

        List<DecoratedKey> keys = toKeys(partitions);
        int k = 0;

        for (int pairIdx = 0; pairIdx < rangePairs.length; pairIdx += 2)
        {
            int rangeStart = rangePairs[pairIdx];
            int rangeEnd = rangePairs[pairIdx + 1];

            for (int expected = rangeStart; expected <= rangeEnd; expected++)
            {
                assertTrue(String.format("Expected to see key %03d", expected), k < keys.size());
                assertEquals(toKey(expected), new String(keys.get(k++).getKey().array()));
            }
        }

        assertEquals(keys.size(), k);
    }

    @Test
    public void testSingleDataRange() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(TABLE);
        store.clearUnsafe();

        // disable compaction while flushing
        store.disableAutoCompaction();

        for (int i = 2; i < 10; i++)
            insertRowWithKey(store.metadata(), i);
        store.forceBlockingFlush();

        assertEquals(1, store.getLiveSSTables().size());
        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        // full range scan
        List<DecoratedKey> keys = toKeys(sstable.getAsyncScanner());
        int k = 0;
        for (int i = 2; i < 10; i++)
            assertEquals(toKey(i), new String(keys.get(k++).getKey().array()));

        // a simple read of a chunk in the middle
        assertScanMatches(sstable, 3, 6, 3, 6);

        // start of range edge conditions
        assertScanMatches(sstable, 1, 9, 2, 9);
        assertScanMatches(sstable, 2, 9, 2, 9);
        assertScanMatches(sstable, 3, 9, 3, 9);

        // end of range edge conditions
        assertScanMatches(sstable, 1, 8, 2, 8);
        assertScanMatches(sstable, 1, 9, 2, 9);
        assertScanMatches(sstable, 1, 9, 2, 9);

        // single item ranges
        assertScanMatches(sstable, 2, 2, 2, 2);
        assertScanMatches(sstable, 5, 5, 5, 5);
        assertScanMatches(sstable, 9, 9, 9, 9);

        // empty ranges
        assertScanEmpty(sstable, 0, 1);
        assertScanEmpty(sstable, 10, 11);

        // wrapping, starts in middle
        assertScanMatches(sstable, 5, 3, 2, 3, 5, 9);
        assertScanMatches(sstable, 5, 2, 2, 2, 5, 9);
        assertScanMatches(sstable, 5, 1, 5, 9);
        assertScanMatches(sstable, 5, Integer.MIN_VALUE, 5, 9);
        // wrapping, starts at end
        assertScanMatches(sstable, 9, 8, 2, 8, 9, 9);
        assertScanMatches(sstable, 9, 3, 2, 3, 9, 9);
        assertScanMatches(sstable, 9, 2, 2, 2, 9, 9);
        assertScanMatches(sstable, 9, 1, 9, 9);
        assertScanMatches(sstable, 9, Integer.MIN_VALUE, 9, 9);
        assertScanMatches(sstable, 8, 3, 2, 3, 8, 9);
        assertScanMatches(sstable, 8, 2, 2, 2, 8, 9);
        assertScanMatches(sstable, 8, 1, 8, 9);
        assertScanMatches(sstable, 8, Integer.MIN_VALUE, 8, 9);
        // wrapping, starts past end
        assertScanMatches(sstable, 10, 9, 2, 9);
        assertScanMatches(sstable, 10, 5, 2, 5);
        assertScanMatches(sstable, 10, 2, 2, 2);
        assertScanEmpty(sstable, 10, 1);
        assertScanEmpty(sstable, 10, Integer.MIN_VALUE);
        assertScanMatches(sstable, 11, 10, 2, 9);
        assertScanMatches(sstable, 11, 9, 2, 9);
        assertScanMatches(sstable, 11, 5, 2, 5);
        assertScanMatches(sstable, 11, 2, 2, 2);
        assertScanEmpty(sstable, 11, 1);
        assertScanEmpty(sstable, 11, Integer.MIN_VALUE);
        // wrapping, starts at start
        assertScanMatches(sstable, 3, 1, 3, 9);
        assertScanMatches(sstable, 3, Integer.MIN_VALUE, 3, 9);
        assertScanMatches(sstable, 2, 1, 2, 9);
        assertScanMatches(sstable, 2, Integer.MIN_VALUE, 2, 9);
        assertScanMatches(sstable, 1, 0, 2, 9);
        assertScanMatches(sstable, 1, Integer.MIN_VALUE, 2, 9);
        // wrapping, starts before
        assertScanMatches(sstable, 1, -1, 2, 9);
        assertScanMatches(sstable, 1, Integer.MIN_VALUE, 2, 9);
        assertScanMatches(sstable, 1, 0, 2, 9);
    }

    @Test
    public void testMultipleRanges() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(TABLE);
        store.clearUnsafe();

        // disable compaction while flushing
        store.disableAutoCompaction();

        for (int i = 0; i < 3; i++)
            for (int j = 2; j < 10; j++)
                insertRowWithKey(store.metadata(), i * 100 + j);
        store.forceBlockingFlush();

        assertEquals(1, store.getLiveSSTables().size());
        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        // full range scan
        Flow<FlowableUnfilteredPartition> fullScanner = sstable.getAsyncScanner();
        assertScanContainsRanges(fullScanner,
                                 2, 9,
                                 102, 109,
                                 202, 209);


        // scan all three ranges separately
        Flow<FlowableUnfilteredPartition> scanner = sstable.getAsyncScanner(makeRanges(1, 9,
                                                                                       101, 109,
                                                                                       201, 209));
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 102, 109,
                                 202, 209);

        // skip the first range
        scanner = sstable.getAsyncScanner(makeRanges(101, 109,
                                                     201, 209));
        assertScanContainsRanges(scanner,
                                 102, 109,
                                 202, 209);

        // skip the second range
        scanner = sstable.getAsyncScanner(makeRanges(1, 9,
                                                     201, 209));
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 202, 209);


        // skip the last range
        scanner = sstable.getAsyncScanner(makeRanges(1, 9,
                                                     101, 109));
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 102, 109);

        // the first scanned range stops short of the actual data in the first range
        scanner = sstable.getAsyncScanner(makeRanges(1, 5,
                                                     101, 109,
                                                     201, 209));
        assertScanContainsRanges(scanner,
                                 2, 5,
                                 102, 109,
                                 202, 209);

        // the first scanned range requests data beyond actual data in the first range
        scanner = sstable.getAsyncScanner(makeRanges(1, 20,
                                                     101, 109,
                                                     201, 209));
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 102, 109,
                                 202, 209);


        // the middle scan range splits the outside two data ranges
        scanner = sstable.getAsyncScanner(makeRanges(1, 5,
                                                     6, 205,
                                                     206, 209));
        assertScanContainsRanges(scanner,
                                 2, 5,
                                 7, 9,
                                 102, 109,
                                 202, 205,
                                 207, 209);

        // empty ranges
        scanner = sstable.getAsyncScanner(makeRanges(0, 1,
                                                     2, 20,
                                                     101, 109,
                                                     150, 159,
                                                     201, 209,
                                                     1000, 1001));
        assertScanContainsRanges(scanner,
                                 3, 9,
                                 102, 109,
                                 202, 209);

        // out of order ranges
        scanner = sstable.getAsyncScanner(makeRanges(201, 209,
                                                     1, 20,
                                                     201, 209,
                                                     101, 109,
                                                     1000, 1001,
                                                     150, 159));
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 102, 109,
                                 202, 209);

        // only empty ranges
        scanner = sstable.getAsyncScanner(makeRanges(0, 1,
                                                     150, 159,
                                                     250, 259));
        assertTrue(toKeys(scanner).isEmpty());

        // no ranges is equivalent to a full scan
        scanner = sstable.getAsyncScanner(new ArrayList<>());
        assertTrue(toKeys(scanner).isEmpty());
    }

    @Test
    public void testSingleKeyMultipleRanges() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(TABLE);
        store.clearUnsafe();

        // disable compaction while flushing
        store.disableAutoCompaction();

        insertRowWithKey(store.metadata(), 205);
        store.forceBlockingFlush();

        assertEquals(1, store.getLiveSSTables().size());
        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        // full range scan
        Flow<FlowableUnfilteredPartition> fullScanner = sstable.getAsyncScanner();
        assertScanContainsRanges(fullScanner, 205, 205);

        // scan three ranges separately
        Flow<FlowableUnfilteredPartition> scanner = sstable.getAsyncScanner(makeRanges(101, 109,
                                                                                       201, 209));

        // this will currently fail
        assertScanContainsRanges(scanner, 205, 205);
    }

    @Test
    public void testParallelAccess() throws Exception
    {
        testParallelAccess(false);
    }

    @Test
    public void testParallelAccessWithCacheIntercept() throws Exception
    {
        testParallelAccess(true);
    }

    private void testParallelAccess(boolean interceptCache) throws Exception
    {
        // intercepting the cache with the cache mocks should result in a
        // NotInCacheException on average once every 4 reads
        if (interceptCache)
            ChunkCacheMocks.interceptCache(random);

        try
        {
            Keyspace keyspace = Keyspace.open(KEYSPACE);
            ColumnFamilyStore store = keyspace.getColumnFamilyStore(TABLE);
            store.clearUnsafe();

            // disable compaction while flushing
            store.disableAutoCompaction();

            for (int i = 0; i < 4; i++)
                for (int j = 2; j < 20; j++)
                    insertRowWithKey(store.metadata(), i * 100 + j);

            store.forceBlockingFlush();

            assertEquals(1, store.getLiveSSTables().size());
            SSTableReader sstable = store.getLiveSSTables().iterator().next();

            final int numCores = TPC.getNumCores();
            final CountDownLatch latch = new CountDownLatch(numCores);
            final AtomicReference<Throwable> error = new AtomicReference<>(null);
            for (int i = 0; i < numCores; i++)
            {
                final int coreNum = i;
                // use the io scheduler because assertScanContainsRanges blocks and so we cannot do everything on a core
                // thread but requestOnCore will guarantee we are requesting the next item on the core thread
                TPC.ioScheduler().scheduleDirect(() ->
                                                 {
                                                     try
                                                     {
                                                         Flow<FlowableUnfilteredPartition> scanner = sstable.getAsyncScanner()
                                                                                                            .lift(Threads.requestOnCore(coreNum, TPCTaskType.UNKNOWN));
                                                         assertScanContainsRanges(scanner,
                                                                                  2, 19,
                                                                                  102, 119,
                                                                                  202, 219,
                                                                                  302, 319);
                                                     }
                                                     catch (Throwable err)
                                                     {
                                                         error.compareAndSet(null, err);
                                                     }
                                                     finally
                                                     {
                                                         latch.countDown();
                                                     }
                                                 });
            }

            latch.await(1, TimeUnit.MINUTES);
            assertNull(error.get());
        }
        finally
        {
            if (interceptCache)
                ChunkCacheMocks.clearIntercept();
        }
    }
}
