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

package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionInfo.Unit;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompactionManagerTest
{
    private static ColumnFamilyStore CFS1;
    private static ColumnFamilyStore CFS2;
    private static ColumnFamilyStore CFS3;
    private static SSTableReader SSTABLE1;
    private static SSTableReader SSTABLE2;
    private static SSTableReader SSTABLE3;
    private static SSTableReader SSTABLE4;
    private static SSTableReader SSTABLE5;
    private static CompactionManager cm;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        MockSchema.cleanup();
        SystemKeyspace.finishStartupBlocking();
        cm = CompactionManager.instance;
        CFS1 = MockSchema.newCFS();
        CFS2 = MockSchema.newCFS();
        CFS3 = MockSchema.newCFS();
        SSTABLE1 = MockSchema.sstable(0, true, CFS1);
        SSTABLE2 = MockSchema.sstable(1, true, CFS1);
        SSTABLE3 = MockSchema.sstable(2, true, CFS1);
        SSTABLE4 = MockSchema.sstable(0, true, CFS2);
        SSTABLE5 = MockSchema.sstable(0, true, CFS3);
        CFS1.addSSTables(Lists.newArrayList(SSTABLE1, SSTABLE2, SSTABLE3));
        CFS2.addSSTables(Lists.newArrayList(SSTABLE4));
        CFS2.addSSTables(Lists.newArrayList(SSTABLE5));
    }

    @AfterClass
    public static void afterClass()
    {
        SSTABLE1.selfRef().release();
        SSTABLE2.selfRef().release();
        SSTABLE3.selfRef().release();
    }

    @Test
    public void testIsCompactingNoPredicateSingleTable()
    {
        try (LifecycleTransaction txn = CFS1.getTracker().tryModify(Lists.newArrayList(SSTABLE1, SSTABLE3), OperationType.COMPACTION))
        {
            assertTrue(cm.isCompacting(Collections.singleton(CFS1)));
            assertFalse(cm.isCompacting(Collections.singleton(CFS2)));
            assertFalse(cm.isCompacting(Collections.singleton(CFS3)));
        }
        assertFalse(cm.isCompacting(Collections.singleton(CFS1)));
        assertFalse(cm.isCompacting(Collections.singleton(CFS2)));
        assertFalse(cm.isCompacting(Collections.singleton(CFS3)));
    }

    @Test
    public void testIsCompactingNoPredicateMultipleTables()
    {
        LifecycleTransaction txn1 = CFS1.getTracker().tryModify(Lists.newArrayList(SSTABLE1, SSTABLE3), OperationType.COMPACTION);
        LifecycleTransaction txn2 = CFS2.getTracker().tryModify(Lists.newArrayList(SSTABLE4), OperationType.COMPACTION);
        assertTrue(cm.isCompacting(Collections.singleton(CFS1)));
        assertTrue(cm.isCompacting(Collections.singleton(CFS2)));
        assertFalse(cm.isCompacting(Collections.singleton(CFS3)));
        txn1.abort();
        txn2.abort();
        assertFalse(cm.isCompacting(Collections.singleton(CFS1)));
        assertFalse(cm.isCompacting(Collections.singleton(CFS2)));
        assertFalse(cm.isCompacting(Collections.singleton(CFS3)));
    }

    @Test
    public void testIsCompactingNoSSTables()
    {
        try (LifecycleTransaction txn = CFS1.getTracker().tryModify(Collections.emptyList(), OperationType.COMPACTION))
        {
            // Even if there is a LifecycleTransaction, if there are no SSTables it should return false
            assertFalse(cm.isCompacting(Collections.singleton(CFS1)));
        }
        assertFalse(cm.isCompacting(Collections.singleton(CFS2)));
    }

    @Test
    public void testIsCompactingFilterBySSTable()
    {
        try (LifecycleTransaction txn = CFS1.getTracker().tryModify(Lists.newArrayList(SSTABLE1, SSTABLE3), OperationType.COMPACTION))
        {
            assertTrue(cm.isCompacting(Collections.singleton(CFS1), Predicates.alwaysTrue(), s -> s == SSTABLE1 || s == SSTABLE2));
            assertFalse(cm.isCompacting(Collections.singleton(CFS1), Predicates.alwaysTrue(), s -> s == SSTABLE2 || s == SSTABLE5));
            assertTrue(cm.isCompacting(Collections.singleton(CFS1), Predicates.alwaysTrue(), s -> s == SSTABLE5 || s == SSTABLE3));
            // even though sstable1 is compacting, it should return false because it's not from CFS2
            assertFalse(cm.isCompacting(Collections.singleton(CFS2), Predicates.alwaysTrue(), s -> s == SSTABLE1));
        }
    }

    @Test
    public void testIsCompactingFilterByOperation()
    {
        try (LifecycleTransaction txn = CFS1.getTracker().tryModify(Lists.newArrayList(SSTABLE1, SSTABLE3), OperationType.COMPACTION))
        {
            assertTrue(cm.isCompacting(Collections.singleton(CFS1), o -> o == OperationType.COMPACTION, Predicates.alwaysTrue()));
            assertFalse(cm.isCompacting(Collections.singleton(CFS1), o -> o == OperationType.VALIDATION, Predicates.alwaysTrue()));
            assertTrue(cm.isCompacting(Collections.singleton(CFS1), o -> o == OperationType.VALIDATION || o == OperationType.COMPACTION, Predicates.alwaysTrue()));
        }
    }

    @Test
    public void testIsCompactingFilterBySSTableAndOperation()
    {
        try (LifecycleTransaction txn = CFS1.getTracker().tryModify(Lists.newArrayList(SSTABLE1, SSTABLE3), OperationType.COMPACTION))
        {
            assertTrue(cm.isCompacting(Collections.singleton(CFS1), o -> o == OperationType.COMPACTION, s -> s == SSTABLE1 || s == SSTABLE3));
            assertFalse(cm.isCompacting(Collections.singleton(CFS1), o -> o == OperationType.VALIDATION, s -> s == SSTABLE1 || s == SSTABLE3));
            assertFalse(cm.isCompacting(Collections.singleton(CFS1), o -> o == OperationType.COMPACTION, s -> s == SSTABLE2));
        }
    }


    public void testRunWithCompactionDisabledShouldWaitForMatchingPredicate(boolean matchOp, boolean matchAll) throws Exception
    {
        // Create two transactions
        LifecycleTransaction txn1 = CFS1.getTracker().tryModify(SSTABLE1, OperationType.COMPACTION);
        LifecycleTransaction txn2 = CFS1.getTracker().tryModify(SSTABLE2, OperationType.VALIDATION);

        Integer RETURN_VALUE = 10;

        // Define predicates based on parameters
        Predicate<OperationType> opPredicate = !matchAll && matchOp ? (o -> o == OperationType.COMPACTION) : Predicates.alwaysTrue();
        Predicate<SSTableReader> ssTablePredicate = !matchAll && !matchOp? (s -> s == SSTABLE1) : Predicates.alwaysTrue();

        // Run with compaction disabled
        CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> CFS1.runWithCompactionsDisabled(() -> RETURN_VALUE, opPredicate,
                                                                                                               ssTablePredicate, false));

        // Wait 2 seconds to ensure runWithCompactionsDisabled is started
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        // Check that runWithCompactionsDisabled is still running
        assertFalse(future.isDone());

        // Abort txn1
        txn1.abort();
        assertEquals(Transactional.AbstractTransactional.State.ABORTED, txn1.state());

        // Since we aborted txn1, the op or sstable predicate should allow runWithCompactionsDisabled to progress
        // When matchAll=true, there is no filtering by predicate, so we should also abort txn2 to allow runWithCompactionsDisabled to progress
        if (matchAll)
        {
            // Check that runWithCompactionsDisabled is still running
            assertFalse(future.isDone());
            // Abort txn2
            txn2.abort();
            assertEquals(Transactional.AbstractTransactional.State.ABORTED, txn2.state());
        }

        // Check that runWithCompactionsDisabled finished
        assertEquals(RETURN_VALUE, future.get(10, TimeUnit.SECONDS));
        txn2.abort();
    }

    @Test
    public void testRunWithCompactionDisabledShouldWaitForMatchingOperation() throws Exception
    {
        testRunWithCompactionDisabledShouldWaitForMatchingPredicate(true, false);
    }

    @Test
    public void testRunWithCompactionDisableShouldWaitForMatchingSSTables() throws Exception
    {
        testRunWithCompactionDisabledShouldWaitForMatchingPredicate(false, false);
    }

    @Test
    public void testRunWithCompactionDisableShouldWaitForAllWhenNoFiltering() throws Exception
    {
        testRunWithCompactionDisabledShouldWaitForMatchingPredicate(false, true);
    }

    @Test
    public void testRunWithCompactionDisabledShouldStopMatchingCompaction() throws Exception
    {
        // Create and register two mock holders to simulate a running compaction
        MockHolder compaction1 = new MockHolder(CFS1, OperationType.COMPACTION, Collections.singleton(SSTABLE1));
        MockHolder compaction2 = new MockHolder(CFS1, OperationType.VALIDATION, Collections.singleton(SSTABLE2));
        CompactionManager.instance.getMetrics().beginCompaction(compaction1);
        CompactionManager.instance.getMetrics().beginCompaction(compaction2);

        // Run with compaction disabled should stop compactions matching the predicate
        assertEquals((Integer)10, CFS1.runWithCompactionsDisabled(() -> 10, op -> op == OperationType.COMPACTION,
                                                                  s -> s == SSTABLE1 || s == SSTABLE2, false));

        // In this case it should only stop compaction1 and not compaction2
        assertTrue(compaction1.isStopRequested());
        assertFalse(compaction2.isStopRequested());
    }

    @Test
    public void testRunWithCompactionDisabledShouldInterruptNonStartedCompaction() throws Exception
    {
        // Create compaction task
        LifecycleTransaction txn = CFS1.getTracker().tryModify(Lists.newArrayList(SSTABLE1, SSTABLE2), OperationType.COMPACTION);
        Assert.assertNotNull(txn);
        CompactionTask task = new CompactionTask(CFS1, txn, 0);

        Integer RETURN_VALUE = 10;

        // Run with compactions disabled from another thread to simulate a race where this is called
        // after the compaction task was created but before it was executed.
        // This needs to be run from another thread because runWithCompactionsDisabled
        // will wait until all ongoing compactions are finished
        CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> CFS1.runWithCompactionsDisabled(() -> RETURN_VALUE, false));

        // Wait 2 seconds to ensure runWithCompactionsDisabled is started
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        // Check that runWithCompactionsDisabled is still running
        assertFalse(future.isDone());

        // Now execute compaction task and expect it to be interrupted
        try
        {
            task.execute(null);
            Assert.fail("Expected CompactionInterruptedException");
        }
        catch (CompactionInterruptedException e)
        {
            // expected
        }
        assertEquals(Transactional.AbstractTransactional.State.ABORTED, txn.state());

        // Check that runWithCompactionsDisabled finished
        assertEquals(RETURN_VALUE, future.get(10, TimeUnit.SECONDS));
    }

    public class MockHolder extends CompactionInfo.Holder
    {
        private final Collection<SSTableReader> readers;
        private final OperationType type;
        private final ColumnFamilyStore cfs;

        public MockHolder(ColumnFamilyStore cfs, OperationType type, Collection<SSTableReader> readers)
        {
            this.cfs = cfs;
            this.type = type;
            this.readers = readers;
        }

        protected boolean maybeStop(Predicate<SSTableReader> predicate)
        {
            return readers.stream().anyMatch(s -> predicate.apply(s));
        }

        public CompactionInfo getCompactionInfo()
        {
            return new CompactionInfo(cfs.metadata(), type, 0, 0, Unit.BYTES, UUIDGen.getTimeUUID());
        }
    }
}
