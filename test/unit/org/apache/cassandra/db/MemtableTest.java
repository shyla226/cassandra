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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.concurrent.TPCBoundaries;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.flow.Flow;

import static org.junit.Assert.*;

public class MemtableTest extends CQLTester
{
    @Test
    public void testAbortingFlushRunnables() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value int)");

        for (int i = 0; i < 10000; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, i);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();

        OpOrder.Barrier barrier = cfs.keyspace.writeOrder.newBarrier();
        memtable.setDiscarding(barrier, new AtomicReference<>(CommitLog.instance.getCurrentPosition()));
        barrier.issue();

        // this determines the number of flush writers created, the FlushRunnable will convert a null location into an sstable location for us
        Directories.DataDirectory[] locations = new Directories.DataDirectory[24];
        List<Range<Token>> sortedLocalRanges = Range.sort(StorageService.instance.getLocalRanges(cfs.keyspace.getName()));
        DiskBoundaries mockDiskBoundary = DiskBoundaryManager.createDiskBoundaries(cfs, locations, sortedLocalRanges);
        ExecutorService executor = Executors.newFixedThreadPool(locations.length / 3);

        // abort without starting
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.FLUSH))
        {
            List<Memtable.FlushRunnable> flushRunnables = memtable.createFlushRunnables(txn, mockDiskBoundary);
            assertNotNull(flushRunnables);

            for (Memtable.FlushRunnable flushRunnable : flushRunnables)
                assertEquals(Memtable.FlushRunnableWriterState.IDLE, flushRunnable.state());

            for (Memtable.FlushRunnable flushRunnable : flushRunnables)
                assertNull(flushRunnable.abort(null));

            for (Memtable.FlushRunnable flushRunnable : flushRunnables)
                assertEquals(Memtable.FlushRunnableWriterState.ABORTED, flushRunnable.state());
        }

        // abort after starting
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.FLUSH))
        {
            List<Memtable.FlushRunnable> flushRunnables = memtable.createFlushRunnables(txn, mockDiskBoundary);

            List<Future<SSTableMultiWriter>> futures = flushRunnables.stream().map(executor::submit).collect(Collectors.toList());

            for (Memtable.FlushRunnable flushRunnable : flushRunnables)
                assertNull(flushRunnable.abort(null));

            FBUtilities.waitOnFutures(futures);

            for (Memtable.FlushRunnable flushRunnable : flushRunnables)
                assertEquals(Memtable.FlushRunnableWriterState.ABORTED, flushRunnable.state());
        }

        // abort before starting
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.FLUSH))
        {
            List<Memtable.FlushRunnable> flushRunnables = memtable.createFlushRunnables(txn, mockDiskBoundary);

            for (Memtable.FlushRunnable flushRunnable : flushRunnables)
                assertNull(flushRunnable.abort(null));

            List<Future<SSTableMultiWriter>> futures = flushRunnables.stream().map(executor::submit).collect(Collectors.toList());

            FBUtilities.waitOnFutures(futures);

            for (Memtable.FlushRunnable flushRunnable : flushRunnables)
                assertEquals(Memtable.FlushRunnableWriterState.ABORTED, flushRunnable.state());
        }
    }

    @Test
    public void testMakePartitionFlow() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value int)");

        for (int i = 0; i < 100; i++) // with Murmur3 hashing, these are sufficient to populate all sub-ranges on an 8 core machine
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, i);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
        TPCBoundaries boundaries = memtable.getBoundaries();
        assertTrue("Expected boundaries for more than 1 core", boundaries.supportedCores() > 1);

        final ColumnFilter columnFilter = ColumnFilter.all(cfs.metadata());
        Flow<FlowableUnfilteredPartition> partitions;
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();

        Map<Range<Token>, List<FlowableUnfilteredPartition>> partitionsByRange = new HashMap<>();
        List<Range<Token>> ranges = boundaries.asRanges();
        for (Range<Token> range : ranges)
        {
            final DataRange dataRange = DataRange.forTokenRange(range);
            partitions = memtable.makePartitionFlow(columnFilter, dataRange);
            assertNotNull(partitions);
            partitionsByRange.put(range, partitions.toList().blockingSingle());
        }

        assertEquals(boundaries.supportedCores(), partitionsByRange.size());
        assertEquals(100, (int)partitionsByRange.values().stream().map(list -> list.size()).reduce(0, Integer::sum));

        // query all data in memtable
        checkPartitions(partitionsByRange, ranges, memtable.makePartitionFlow(columnFilter, DataRange.allData(partitioner)).toList().blockingSingle());

        // aggregate ranges
        for (int i = 1; i < boundaries.supportedCores(); i++)
        {
            partitions = memtable.makePartitionFlow(columnFilter, DataRange.forKeyRange(Range.makeRowRange(partitioner.getMinimumToken(),
                                                                                                           ranges.get(i).right.getToken())));

            checkPartitions(partitionsByRange, ranges.subList(0, i + 1), partitions.toList().blockingSingle());
        }

        // split ranges
        for (Range<Token> range : ranges)
        {
            Pair<AbstractBounds<Token>, AbstractBounds<Token>> splitRanges = range.split(partitioner.midpoint(range.left, range.right));
            partitions = Flow.concat(memtable.makePartitionFlow(columnFilter, DataRange.forKeyRange(Range.makeRowRange(splitRanges.left.left,
                                                                                                                       splitRanges.left.right))),
                                     memtable.makePartitionFlow(columnFilter, DataRange.forKeyRange(Range.makeRowRange(splitRanges.right.left,
                                                                                                                       splitRanges.right.right))));

            checkPartitions(partitionsByRange, Collections.singletonList(range), partitions.toList().blockingSingle());

        }
    }

    void checkPartitions(Map<Range<Token>, List<FlowableUnfilteredPartition>> partitionsByRange, List<Range<Token>> ranges, List<FlowableUnfilteredPartition> partitions)
    {
        List<FlowableUnfilteredPartition> expectedPartitions = new ArrayList<>();
        for (Range<Token> range : ranges)
            expectedPartitions.addAll(partitionsByRange.get(range));

        assertEquals(expectedPartitions.size(), partitions.size());

        expectedPartitions.sort(Comparator.comparing(fup -> fup.partitionKey()));
        partitions.sort(Comparator.comparing(fup -> fup.partitionKey()));

        for (int i = 0; i < expectedPartitions.size(); i++)
            assertEquals(expectedPartitions.get(i).partitionKey(), partitions.get(i).partitionKey());
    }

    @Test
    public void testMakePartitionFlow_Index() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value int)");
        createIndex("CREATE INDEX ON %s (value)");

        for (int i = 0; i < 100; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, i);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore().indexManager.getAllIndexColumnFamilyStores().iterator().next();
        IPartitioner partitioner = cfs.metadata().partitioner;
        assertTrue(partitioner instanceof LocalPartitioner);

        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
        TPCBoundaries boundaries = memtable.getBoundaries();
        assertTrue("Expected boundaries for more than 1 core", boundaries.supportedCores() > 1);

        final ColumnFilter columnFilter = ColumnFilter.all(cfs.metadata());

        checkPartitions(0, 100, memtable.makePartitionFlow(columnFilter, DataRange.allData(partitioner)));

        checkPartitions(0, 51,  memtable.makePartitionFlow(columnFilter,
                                                           DataRange.forKeyRange(Range.makeRowRange(partitioner.getMinimumToken(),
                                                                                                    partitioner.getToken(Int32Type.instance.decompose(50))))));
        checkPartitions(51, 49, memtable.makePartitionFlow(columnFilter,
                                                           DataRange.forKeyRange(Range.makeRowRange(partitioner.getToken(Int32Type.instance.decompose(50)),
                                                                                                    partitioner.getMinimumToken()))));

        checkPartitions(1, 99, memtable.makePartitionFlow(columnFilter,
                                                          DataRange.forKeyRange(Range.makeRowRange(partitioner.getToken(Int32Type.instance.decompose(0)),
                                                                                                   partitioner.getToken(Int32Type.instance.decompose(100))))));

        checkPartitions(51, 10, memtable.makePartitionFlow(columnFilter,
                                                           DataRange.forKeyRange(Range.makeRowRange(partitioner.getToken(Int32Type.instance.decompose(50)),
                                                                                                    partitioner.getToken(Int32Type.instance.decompose(60))))));
    }


    void checkPartitions(int min, int num, Flow<FlowableUnfilteredPartition> flow)
    {
        List<FlowableUnfilteredPartition> partitions = flow.toList().blockingSingle();
        assertEquals(num, partitions.size());

        for (int i = 0; i < num; i++)
            assertEquals(min + i, (int)Int32Type.instance.compose(partitions.get(i).partitionKey().getKey()));
    }

}
