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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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

        List<Range<Token>> ranges = Range.sort(StorageService.instance.getLocalRanges(cfs.keyspace.getName()));
        // this determines the number of flush writers created, the FlushRunnable will convert a null location into an sstable location for us
        Directories.DataDirectory[] locations = new Directories.DataDirectory[24];
        ExecutorService executor = Executors.newFixedThreadPool(locations.length / 3);

        // abort without starting
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.FLUSH))
        {
            List<Memtable.FlushRunnable> flushRunnables = memtable.createFlushRunnables(ranges, locations, txn);
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
            List<Memtable.FlushRunnable> flushRunnables = memtable.createFlushRunnables(ranges, locations, txn);

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
            List<Memtable.FlushRunnable> flushRunnables = memtable.createFlushRunnables(ranges, locations, txn);

            for (Memtable.FlushRunnable flushRunnable : flushRunnables)
                assertNull(flushRunnable.abort(null));

            List<Future<SSTableMultiWriter>> futures = flushRunnables.stream().map(executor::submit).collect(Collectors.toList());

            FBUtilities.waitOnFutures(futures);

            for (Memtable.FlushRunnable flushRunnable : flushRunnables)
                assertEquals(Memtable.FlushRunnableWriterState.ABORTED, flushRunnable.state());
        }
    }
}
