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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;

import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.apache.cassandra.Util.getFullRange;
import static org.apache.cassandra.Util.getRange;
import static org.apache.cassandra.Util.writeSSTable;
import static org.apache.cassandra.Util.writeSSTableWithTombstones;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class AntiCompactionTest
{
    public static final String KEYSPACE2 = "Keyspace2";
    public static final String CF_STANDARD1 = "Standard1";
    public static final long REPAIRED_AT = 123456;
    private static final String KEYSPACE1 = "AntiCompactionTest";
    private static final String CF = "AntiCompactionTest";
    private static CFMetaData cfm;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        cfm = SchemaLoader.standardCFMD(KEYSPACE1, CF);
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    cfm);
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    KeyspaceParams.simple(2),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD1));
    }

    @After
    public void truncateCF()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF).truncateBlocking();
        Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_STANDARD1).truncateBlocking();
    }

    @Test
    public void antiCompactOne() throws Exception
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        assertEquals(store.getLiveSSTables().size(), sstables.size());
        List<Range<Token>> ranges = Arrays.asList(getRange("0", "4"));

        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION))
        {
            if (txn == null)
                throw new IllegalStateException();
            long repairedAt = 1000;
            UUID parentRepairSession = UUID.randomUUID();
            new LocalAntiCompactionTask(store, ranges, parentRepairSession, repairedAt, txn).run();
        }

        assertEquals(2, store.getLiveSSTables().size());
        verifyAntiCompaction(store.getLiveSSTables(), ranges, 4, 6);

        for (SSTableReader sstable : store.getLiveSSTables())
        {
            assertFalse(sstable.isMarkedCompacted());
            assertEquals(1, sstable.selfRef().globalCount());
        }
        assertEquals(0, store.getTracker().getCompacting().size());
    }

    private void verifyAntiCompaction(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges, int expectedRepairedKeys, int expectedNonRepairedKeys)
    {
        int repairedKeys = 0;
        int nonRepairedKeys = 0;
        for (SSTableReader sstable : sstables)
        {
            try (ISSTableScanner scanner = sstable.getScanner((RateLimiter) null))
            {
                while (scanner.hasNext())
                {
                    UnfilteredRowIterator row = scanner.next();
                    if (sstable.isRepaired())
                    {
                        assertTrue(ranges.stream().anyMatch(r -> r.contains(row.partitionKey().getToken())));
                        repairedKeys++;
                    }
                    else
                    {
                        assertTrue(ranges.stream().noneMatch(r -> r.contains(row.partitionKey().getToken())));
                        nonRepairedKeys++;
                    }
                }
            }
        }
        assertEquals(expectedRepairedKeys, repairedKeys);
        assertEquals(expectedNonRepairedKeys, nonRepairedKeys);
    }

    @Test
    public void antiCompactionSizeTest() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.disableAutoCompaction();
        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Range<Token> range = getRange(0, 500);
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        UUID parentRepairSession = UUID.randomUUID();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION))
        {
            new LocalAntiCompactionTask(cfs, Arrays.asList(range), parentRepairSession, 12345, txn).run();
        }
        long sum = 0;
        long rows = 0;
        for (SSTableReader x : cfs.getLiveSSTables())
        {
            sum += x.bytesOnDisk();
            rows += x.getTotalRows();
        }
        assertEquals(sum, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(rows, 1000 * (1000 * 5));//See writeFile for how this number is derived
    }

    private SSTableReader writeFile(ColumnFamilyStore cfs, int count)
    {
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        String filename = cfs.getSSTablePath(dir);

        try (SSTableTxnWriter writer = SSTableTxnWriter.create(cfs, filename, 0, 0, new SerializationHeader(true, cfm, cfm.partitionColumns(), EncodingStats.NO_STATS)))
        {
            for (int i = 0; i < count; i++)
            {
                UpdateBuilder builder = UpdateBuilder.create(cfm, ByteBufferUtil.bytes(i));
                for (int j = 0; j < count * 5; j++)
                    builder.newRow("c" + j).add("val", "value1");
                writer.append(builder.build().unfilteredIterator());

            }
            Collection<SSTableReader> sstables = writer.finish(true);
            assertNotNull(sstables);
            assertEquals(1, sstables.size());
            return sstables.iterator().next();
        }
    }

    public void generateSStable(ColumnFamilyStore store, String Suffix)
    {
        for (int i = 0; i < 10; i++)
        {
            String localSuffix = Integer.toString(i);
            new RowUpdateBuilder(cfm, System.currentTimeMillis(), localSuffix + "-" + Suffix)
                    .clustering("c")
                    .add("val", "val" + localSuffix)
                    .build()
                    .applyUnsafe();
        }
        store.forceBlockingFlush();
    }

    @Test
    public void antiCompactTenSTC() throws Exception
    {
        antiCompactTen("SizeTieredCompactionStrategy");
    }

    @Test
    public void antiCompactTenLC() throws Exception
    {
        antiCompactTen("LeveledCompactionStrategy");
    }

    public void antiCompactTen(String compactionStrategy) throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.disableAutoCompaction();

        for (int table = 0; table < 10; table++)
        {
            generateSStable(store,Integer.toString(table));
        }
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        assertEquals(store.getLiveSSTables().size(), sstables.size());

        Range<Token> range = getRange("0", "4");
        List<Range<Token>> ranges = Arrays.asList(range);

        long repairedAt = 1000;
        UUID parentRepairSession = UUID.randomUUID();
        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION))
        {
            new LocalAntiCompactionTask(store, ranges, parentRepairSession, repairedAt, txn).run();
        }
        /*
        Anticompaction will be anti-compacting 10 SSTables but will be doing this two at a time
        so there will be no net change in the number of sstables
         */
        assertEquals(10, store.getLiveSSTables().size());
        int repairedKeys = 0;
        int nonRepairedKeys = 0;
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner((RateLimiter) null))
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator row = scanner.next())
                    {
                        if (sstable.isRepaired())
                        {
                            assertTrue(range.contains(row.partitionKey().getToken()));
                            assertEquals(repairedAt, sstable.getSSTableMetadata().repairedAt);
                            repairedKeys++;
                        }
                        else
                        {
                            assertFalse(range.contains(row.partitionKey().getToken()));
                            assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, sstable.getSSTableMetadata().repairedAt);
                            nonRepairedKeys++;
                        }
                    }
                }
            }
        }
        assertEquals(repairedKeys, 40);
        assertEquals(nonRepairedKeys, 60);
    }

    @Test
    public void shouldMutateRepairedAt() throws Exception
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        assertEquals(store.getLiveSSTables().size(), sstables.size());
        Range<Token> range = getRange("0", "9999");
        List<Range<Token>> ranges = Arrays.asList(range);
        UUID parentRepairSession = UUID.randomUUID();

        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION))
        {
            new LocalAntiCompactionTask(store, ranges, parentRepairSession, 1, txn).run();
        }

        assertThat(store.getLiveSSTables().size(), is(1));
        assertThat(Iterables.get(store.getLiveSSTables(), 0).isRepaired(), is(true));
        assertThat(Iterables.get(store.getLiveSSTables(), 0).selfRef().globalCount(), is(1));
        assertThat(store.getTracker().getCompacting().size(), is(0));
    }


    @Test
    public void shouldSkipAntiCompactionForNonIntersectingRange() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.disableAutoCompaction();

        for (int table = 0; table < 10; table++)
        {
            generateSStable(store,Integer.toString(table));
        }
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        assertEquals(store.getLiveSSTables().size(), sstables.size());

        Range<Token> range = getRange("-1", "-10");
        List<Range<Token>> ranges = Arrays.asList(range);
        UUID parentRepairSession = UUID.randomUUID();

        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION))
        {
            new LocalAntiCompactionTask(store, ranges, parentRepairSession, 1, txn);
        }

        assertThat(store.getLiveSSTables().size(), is(10));
        assertThat(Iterables.get(store.getLiveSSTables(), 0).isRepaired(), is(false));
    }

    private ColumnFamilyStore prepareColumnFamilyStore()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.disableAutoCompaction();
        for (int i = 0; i < 10; i++)
        {
            new RowUpdateBuilder(cfm, System.currentTimeMillis(), Integer.toString(i))
                .clustering("c")
                .add("val", "val")
                .build()
                .applyUnsafe();
        }
        store.forceBlockingFlush();
        return store;
    }

    @After
    public void truncateCfs()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.truncateBlocking();
    }

    private static Set<SSTableReader> getUnrepairedSSTables(ColumnFamilyStore cfs)
    {
        return ImmutableSet.copyOf(cfs.getTracker().getView().sstables(SSTableSet.LIVE, (s) -> !s.isRepaired()));
    }

    @Test
    public void testMixedLocalAndRemoteSSTables() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE2);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.disableAutoCompaction();

        SSTableReader localA = writeSSTable(cfs, cfs.metadata, 0, 10, 0, 1, 0);
        SSTableReader localB = writeSSTable(cfs, cfs.metadata, 4, 6, 0, 1, 0);
        SSTableReader localC = writeSSTable(cfs, cfs.metadata, 5, 9, 0, 1, 2);
        SSTableReader remoteA = writeSSTableWithTombstones(cfs, cfs.metadata, 0, 3, 0, 1, 5);
        SSTableReader remoteB = writeSSTableWithTombstones(cfs, cfs.metadata, 3, 6, 0, 1, 6);
        SSTableReader remoteC = writeSSTableWithTombstones(cfs, cfs.metadata, 7, 12, 0, 1, 7);

        Set<SSTableReader> allSSTables = Sets.newHashSet(localA, localB, localC, remoteA, remoteB, remoteC);

        Set<SSTableReader> localSSTables = Sets.newHashSet(localA, //1
                                                           localB, //2
                                                           localC); //3

        Set<SSTableReader> remoteSSTables = Sets.newHashSet(remoteA, //4
                                                            remoteB, //5
                                                            remoteC); //6

        cfs.addSSTables(allSSTables);

        // Case 1: Local and remote SSTables fully contained in repaired range - all should be mutated repaired at
        Set<LifecycleTransaction> remoteSSTableTxns = remoteSSTables.stream().map(s -> getTransaction(cfs, Collections.singleton(s))).collect(Collectors.toSet());
        Set<SSTableReader> unrepairedSet = Collections.emptySet();
        LocalAntiCompactionTask anticompactionTask = new LocalAntiCompactionTask(cfs, getFullRange(cfs), UUIDGen.getTimeUUID(),
                                                         REPAIRED_AT, getTransaction(cfs, localSSTables),
                                                         remoteSSTableTxns, unrepairedSet);
        Set<SSTableReader> sstablesToMutateRepairedAt = allSSTables;
        runAntiCompactionAndVerify(cfs, anticompactionTask, sstablesToMutateRepairedAt, Collections.emptySet(), 27, 0);
        cfs.mutateRepairedAt(allSSTables, 0, false); //cleanup

        // Case 2: Same case as above, but with two unrepaired SSTables (one local and one remote)
        remoteSSTableTxns = remoteSSTables.stream().map(s -> getTransaction(cfs, Collections.singleton(s))).collect(Collectors.toSet());
        unrepairedSet = Sets.newHashSet(localC, remoteC);
        anticompactionTask = new LocalAntiCompactionTask(cfs, getFullRange(cfs), UUIDGen.getTimeUUID(),
                                                         REPAIRED_AT, getTransaction(cfs, localSSTables),
                                                         remoteSSTableTxns, unrepairedSet);
        sstablesToMutateRepairedAt = Sets.difference(allSSTables, unrepairedSet);
        runAntiCompactionAndVerify(cfs, anticompactionTask, sstablesToMutateRepairedAt, Collections.emptySet(), 18, 0);
        cfs.mutateRepairedAt(allSSTables, 0, false); //cleanup

        // Case 3:
        // - Repaired ranges: (-1,3] and (2,6]
        //   - Fully contained in range: localB[4,6], remoteA[0,3], remoteB[3,6]
        //   - Partially contained in range: localA[0,10] (will be anti-compacted)
        //   - Not contained in range: remoteC[7,12] (should not be mutatedRepaired at)
        // - Unrepaired set: localC[5,9] (even though it overlaps with repaired range, it's ignored if it's on unrepaired set)
        remoteSSTableTxns = remoteSSTables.stream().map(s -> getTransaction(cfs, Collections.singleton(s))).collect(Collectors.toSet());
        unrepairedSet = Sets.newHashSet(localC);
        HashSet<Range<Token>> successfulRanges = Sets.newHashSet(getRange(-1, 3), getRange(2, 6));
        anticompactionTask = new LocalAntiCompactionTask(cfs, successfulRanges, UUIDGen.getTimeUUID(),
                                                         REPAIRED_AT, getTransaction(cfs, localSSTables),
                                                         remoteSSTableTxns, unrepairedSet);
        sstablesToMutateRepairedAt = Sets.newHashSet(localB, remoteA, remoteB);
        runAntiCompactionAndVerify(cfs, anticompactionTask, sstablesToMutateRepairedAt, Collections.singleton(localA), 15, 3);
        cfs.mutateRepairedAt(allSSTables, 0, false); //cleanup
    }

    @Test
    public void testTransactionsAreAbortedOnFailedAntiCompactionTest() throws IOException
    {
        class BrokenAntiCompactionTask extends LocalAntiCompactionTask
        {
            public BrokenAntiCompactionTask(ColumnFamilyStore cfs, Collection<Range<Token>> ranges,
                                           UUID parentSessionId, long repairedAt,
                                           LifecycleTransaction localSSTablesTxn,
                                           Set<LifecycleTransaction> remoteSSTablesTxns,
                                           Set<SSTableReader> unrepairedSet)
            {
                super(cfs, ranges, parentSessionId, repairedAt, localSSTablesTxn, remoteSSTablesTxns, unrepairedSet);
            }

            protected int antiCompactGroup(LifecycleTransaction groupTxn)
            {
                //inject failure during anti-compaction
                throw new RuntimeException();
            }
        }

        Keyspace keyspace = Keyspace.open(KEYSPACE2);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.disableAutoCompaction();

        SSTableReader localA = writeSSTable(cfs, cfs.metadata, 0, 10, 0, 1, 0);
        SSTableReader localB = writeSSTable(cfs, cfs.metadata, 4, 6, 0, 1, 0);
        SSTableReader localC = writeSSTable(cfs, cfs.metadata, 5, 9, 0, 1, 2);
        SSTableReader remoteA = writeSSTableWithTombstones(cfs, cfs.metadata, 0, 3, 0, 1, 5);
        SSTableReader remoteB = writeSSTableWithTombstones(cfs, cfs.metadata, 3, 6, 0, 1, 6);
        SSTableReader remoteC = writeSSTableWithTombstones(cfs, cfs.metadata, 7, 12, 0, 1, 7);

        Set<SSTableReader> allSSTables = Sets.newHashSet(localA, localB, localC, remoteA, remoteB, remoteC);

        Set<SSTableReader> localSSTables = Sets.newHashSet(localA, //1
                                                           localB, //2
                                                           localC); //3

        Set<SSTableReader> remoteSSTables = Sets.newHashSet(remoteA, //4
                                                            remoteB, //5
                                                            remoteC); //6

        cfs.addSSTables(allSSTables);

        Set<LifecycleTransaction> remoteSSTableTxns = remoteSSTables.stream().map(s -> getTransaction(cfs, Collections.singleton(s))).collect(Collectors.toSet());
        Set<SSTableReader> unrepairedSet = Sets.newHashSet(localC);
        HashSet<Range<Token>> successfulRanges = Sets.newHashSet(getRange(-1, 3), getRange(2, 6));
        LocalAntiCompactionTask antiCompactionTask = new BrokenAntiCompactionTask(cfs, successfulRanges, UUIDGen.getTimeUUID(),
                                                                                  REPAIRED_AT, getTransaction(cfs, localSSTables),
                                                                                  remoteSSTableTxns, unrepairedSet);

        try
        {
            antiCompactionTask.run();
            fail("Should have thrown RuntimeException");
        } catch (RuntimeException e)
        {
            //expected
        }

        // Compacting set must be empty after anti-compaction
        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        // Tracker should contain all validated sstables
        assertEquals(allSSTables, cfs.getLiveSSTables());

        // Check that all SSTables are still unrepaired
        assertRepaired(allSSTables, ActiveRepairService.UNREPAIRED_SSTABLE);

        // Check remote sstable transaction is ABORTED
        assertTrue(antiCompactionTask.getRemoteSSTablesTransactions().stream().allMatch(s -> s.state() == Transactional.AbstractTransactional.State.ABORTED));
        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        // Check local sstable transaction is ABORTED
        assertEquals(Transactional.AbstractTransactional.State.ABORTED, antiCompactionTask.getLocalSSTablesTransaction().state());
    }

    private LifecycleTransaction getTransaction(ColumnFamilyStore cfs, Set<SSTableReader> localSSTables)
    {
        return cfs.getTracker().tryModify(localSSTables, OperationType.ANTICOMPACTION);
    }

    private void runAntiCompactionAndVerify(ColumnFamilyStore cfs, LocalAntiCompactionTask antiCompactionTask,
                                            Set<SSTableReader> sstablesToMutateRepairedAt,
                                            Set<SSTableReader> sstablesToAntiCompact,
                                            int repairedCount, int nonRepairedCount)
    {
        Set<SSTableReader> validatedSSTables = Sets.newHashSet(antiCompactionTask.getLocalSSTablesTransaction().originals());
        validatedSSTables.addAll(antiCompactionTask.getRemoteSSTablesTransactions().stream().flatMap(s -> s.originals().stream()).collect(Collectors.toSet()));
        Set<SSTableReader> unrepairedSet = Sets.difference(validatedSSTables, Sets.union(sstablesToMutateRepairedAt, sstablesToAntiCompact));

        // Check that all SSTables to anti-compact are unrepaired and marked as compacting
        assertEquals(validatedSSTables, cfs.getTracker().getCompacting());
        assertRepaired(validatedSSTables, ActiveRepairService.UNREPAIRED_SSTABLE);

        // Check that sstables not to be repaired are either in anticompaction task unrepaired set or are not contained in repair range
        Set<SSTableReader> notContainedInRepairedRange = validatedSSTables.stream().filter(s -> antiCompactionTask.getSuccessfulRanges()
                                                                                                                  .stream()
                                                                                                                  .noneMatch(r -> r.intersects(s.getRange())))
                                                                          .collect(Collectors.toSet());
        assertEquals(unrepairedSet, Sets.union(antiCompactionTask.getUnrepairedSet(), notContainedInRepairedRange));

        // Check that all SSTables not to be anti-compacted
        assertTrue(unrepairedSet.containsAll(antiCompactionTask.getUnrepairedSet()));

        // Run anti-compaction
        antiCompactionTask.run();

        // Compacting set must be empty after anti-compaction
        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        // Tracker should contain all validated sstables
        assertTrue(cfs.getLiveSSTables().containsAll(sstablesToMutateRepairedAt));

        // Check that repair-set is repaired, while unrepaired set is not
        assertRepaired(sstablesToMutateRepairedAt, REPAIRED_AT);
        assertRepaired(unrepairedSet, ActiveRepairService.UNREPAIRED_SSTABLE);
        // Both should be present on the tracker
        assertTrue(cfs.getLiveSSTables().containsAll(Sets.union(sstablesToMutateRepairedAt, unrepairedSet)));

        // SSTables to be anti-compacted should not be marked repaired
        assertRepaired(sstablesToAntiCompact, ActiveRepairService.UNREPAIRED_SSTABLE);
        // SSTables to be anti-compacted should be removed from the tracker
        assertTrue(sstablesToAntiCompact.stream().noneMatch(s -> cfs.getLiveSSTables().contains(s)));

        // Check remote sstable transaction is ABORTED
        assertTrue(antiCompactionTask.getRemoteSSTablesTransactions().stream().allMatch(s -> s.state() == Transactional.AbstractTransactional.State.ABORTED));
        assertTrue(cfs.getTracker().getCompacting().isEmpty());

        // Check local sstable transaction is COMMITTED and contains no sstables since they were either contained on repaired range or anti-compacted
        assertTrue(antiCompactionTask.getLocalSSTablesTransaction().originals().isEmpty());
        assertTrue(antiCompactionTask.getLocalSSTablesTransaction().state() == Transactional.AbstractTransactional.State.COMMITTED);

        Set<SSTableReader> sstablesToVerify = Sets.difference(cfs.getLiveSSTables(), unrepairedSet);
        verifyAntiCompaction(sstablesToVerify, antiCompactionTask.getSuccessfulRanges(), repairedCount, nonRepairedCount);
    }

    private void assertRepaired(Set<SSTableReader> repaired, long repairedAt)
    {
        for (SSTableReader reader : repaired)
        {
            assertEquals(repairedAt, reader.getSSTableMetadata().repairedAt);
        }
    }

}
