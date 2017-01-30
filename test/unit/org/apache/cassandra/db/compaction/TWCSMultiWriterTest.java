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

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TWCSMultiWriterTest extends CQLTester
{
    @Test
    public void testSimpleSplittingPartitions() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, PRIMARY KEY (id, id2))");
        long now = System.currentTimeMillis();
        for (int i = 0; i < 10; i++)
            execute(String.format("insert into %%s (id, id2, t) values (1, %d, '%dx%d') using timestamp %d", i, i, i, now - i * 60000));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        int nowInSeconds = FBUtilities.nowInSeconds();
        Range<Token> r = new Range<>(cfs.getPartitioner().getMinimumToken(), cfs.getPartitioner().getMaximumToken());
        DataRange dr = new DataRange(Range.makeRowRange(r), new ClusteringIndexSliceFilter(Slices.ALL, false));
        PartitionRangeReadCommand rc = new PartitionRangeReadCommand(cfs.metadata(),
                                                                     nowInSeconds,
                                                                     ColumnFilter.all(cfs.metadata()),
                                                                     RowFilter.NONE,
                                                                     DataLimits.NONE,
                                                                     dr,
                                                                     Optional.empty());
        TWCSMultiWriter.BucketIndexer indexes = TWCSMultiWriter.createBucketIndexes(TimeUnit.MINUTES, 1);
        try (ReadExecutionController executionController = rc.executionController();
             UnfilteredPartitionIterator pi = rc.executeLocally(executionController))
        {
            while (pi.hasNext())
            {
                try (UnfilteredRowIterator ri = pi.next())
                {
                    int count = 0;
                    UnfilteredRowIterator [] iterators = TWCSMultiWriter.splitPartitionOnTime(ri, indexes, new TWCSMultiWriter.TWCSConfig(TimeUnit.MINUTES, 1, TimeUnit.MILLISECONDS));
                    for (int i = iterators.length - 1; i >= 0; i--)
                    {
                        UnfilteredRowIterator iterator = iterators[i];
                        if (iterator == null)
                            continue;
                        assertTrue(iterator.partitionKey().equals(cfs.decorateKey(ByteBufferUtil.bytes(1))));
                        while (iterator.hasNext())
                        {
                            Row row = (Row)iterator.next();
                            for (Cell c : row.cells())
                            {
                                assertEquals(ByteBufferUtil.string(c.value()), String.format("%dx%d", count, count));
                                assertEquals(now - (count * 60000), c.timestamp());
                                count++;
                            }
                        }
                    }
                    assertEquals(10, count);

                }
            }

        }
    }

    @Test
    public void testSplittingPartitionsStaticRow() throws Throwable
    {
        createTable("create table %s (id int, id2 int, s text static, s2 text static, t text, PRIMARY KEY (id, id2)) WITH compaction = {'class':'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES', 'timestamp_resolution': 'MILLISECONDS', 'split_during_flush':'true'}");

        long now = System.currentTimeMillis();
        execute("insert into %s (id, id2, s, s2, t) values (1, 1, 'static_old', 'static_old_2', 'some value') using timestamp 1");
        execute("update %s using timestamp "+now+" set s='static_new' where id=1");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.forceBlockingFlush();

        boolean checkedOld = false;
        boolean checkedNew = false;
        for (SSTableReader s : cfs.getSSTables(SSTableSet.LIVE))
        {
            long mints = s.getSSTableMetadata().minTimestamp;
            long maxts = s.getSSTableMetadata().maxTimestamp;
            assertTrue(mints == maxts);
            try (ISSTableScanner scanner = s.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator rows = scanner.next())
                    {
                        Row staticRow = rows.staticRow();
                        for (Cell c : staticRow.cells())
                        {
                            if (mints == 1)
                            {
                                checkedOld = true;
                                assertEquals("static_old_2", ByteBufferUtil.string(c.value()));
                            }
                            else if (mints == now)
                            {
                                checkedNew = true;
                                assertEquals("static_new", ByteBufferUtil.string(c.value()));
                            }
                            else
                                fail();
                        }
                    }

                }
            }
        }
        assertTrue(checkedNew);
        assertTrue(checkedOld);
    }

    @Test
    public void testSplittingPartitionsRowDeletion() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, PRIMARY KEY (id, id2)) WITH compaction = {'class':'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES', 'timestamp_resolution': 'MILLISECONDS', 'split_during_flush':'true'}");

        long now = System.currentTimeMillis();
        for (int i = 0; i < 10; i++)
            execute(String.format("insert into %%s (id, id2, t) values (1, %d, '%dx%d') using timestamp %d", i, i, i, now - TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES)));

        execute(String.format("delete from %%s using timestamp %d where id=1 and id2=2 ", now));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.forceBlockingFlush();

        for (SSTableReader s : cfs.getSSTables(SSTableSet.LIVE))
        {
            long mints = s.getSSTableMetadata().minTimestamp;
            long maxts = s.getSSTableMetadata().maxTimestamp;

            assertEquals(mints, maxts);

            try (ISSTableScanner scanner = s.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator rows = scanner.next())
                    {

                        while (rows.hasNext())
                        {
                            Row r = (Row) rows.next();
                            if (mints == now) // this should contain the row delete
                            {
                                assertFalse(r.deletion().isLive());
                                assertEquals(now, r.deletion().time().markedForDeleteAt());
                            }
                            else
                            {
                                assertTrue(r.deletion().isLive());
                            }
                        }
                    }

                }
            }
        }
    }

    @Test
    public void testWithPartitionDeletion() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, PRIMARY KEY (id, id2)) WITH compaction = {'class':'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES', 'timestamp_resolution': 'MILLISECONDS', 'split_during_flush':'true'}");

        long now = System.currentTimeMillis();
        for (int i = 0; i < 10; i++)
            execute(String.format("insert into %%s (id, id2, t) values (1, %d, '%dx%d') using timestamp %d", i, i, i, now - TimeUnit.MILLISECONDS.convert(7, TimeUnit.MINUTES)));

        execute(String.format("DELETE FROM %%s USING TIMESTAMP %d WHERE id = 1", now - TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES)));

        execute(String.format("INSERT INTO %%s (id, id2, t) VALUES (1, 2, 'xyz') USING TIMESTAMP %d", now));

        getCurrentColumnFamilyStore().forceBlockingFlush();

        long foundSSTables = 0;
        boolean foundPLD = false;
        boolean foundCell = false;
        for (SSTableReader s : getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE))
        {
            foundSSTables++;
            long mints = s.getSSTableMetadata().minTimestamp;
            long maxts = s.getSSTableMetadata().maxTimestamp;

            assertEquals(mints, maxts);

            try (ISSTableScanner scanner = s.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator rows = scanner.next())
                    {
                        if (!rows.partitionLevelDeletion().isLive())
                        {
                            assertEquals(now - TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES), rows.partitionLevelDeletion().markedForDeleteAt());
                            assertEquals(now - TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES), mints);
                            foundPLD = true;
                            assertFalse(rows.hasNext());
                        }
                        while (rows.hasNext())
                        {
                            Unfiltered uf = rows.next();
                            assertEquals(DeletionTime.LIVE, rows.partitionLevelDeletion());
                            for (Cell c : ((Row)uf).cells())
                            {
                                assertEquals(now, c.timestamp());
                                assertEquals(now, mints);
                                foundCell = true;
                            }
                        }
                    }
                }
            }
        }

        assertEquals(2, foundSSTables);
        assertTrue(foundPLD);
        assertTrue(foundCell);
    }

    @Test
    public void testWithComplexData() throws Throwable
    {
        createTable("create table %s (id int, id2 int, x set<text>, PRIMARY KEY (id, id2)) WITH compaction = {'class':'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES', 'timestamp_resolution': 'MILLISECONDS', 'split_during_flush':'true'}");

        long now = System.currentTimeMillis();

        long firstTS = now - TimeUnit.MILLISECONDS.convert(7, TimeUnit.MINUTES) - 2; // deletion is written at ts - 1, so move the timestamp a bit to make sure deletion and data get in the same window
        long secondTS = now - TimeUnit.MILLISECONDS.convert(3, TimeUnit.MINUTES) - 2;

        for (int i = 0; i < 10; i++)
            execute(String.format("INSERT INTO %%s (id, id2, x) VALUES (1, %d, {%s}) USING TIMESTAMP %d", i, "'a','b','c'", firstTS));

        execute(String.format("INSERT INTO %%s (id, id2, x) VALUES (1, 2, {'x','y','z'}) USING TIMESTAMP %d", secondTS));
        getCurrentColumnFamilyStore().forceBlockingFlush();
        int foundSSTables = 0;
        for (SSTableReader s : getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE))
        {
            foundSSTables++;
            long mints = s.getSSTableMetadata().minTimestamp;
            long maxts = s.getSSTableMetadata().maxTimestamp;
            assertEquals(mints + 1, maxts); // complex deletions are written at ts - 1
        }
        assertEquals(foundSSTables, 2);

        UntypedResultSet rs = execute("SELECT * FROM %s WHERE id = 1");

        Iterator<UntypedResultSet.Row> x = rs.iterator();
        boolean foundUpdate = false, found = false;
        while (x.hasNext())
        {
            UntypedResultSet.Row r = x.next();
            if (r.getInt("id2") == 2)
            {
                Set<String> expectedSet = Sets.newHashSet("x", "y", "z");
                assertEquals(expectedSet, r.getSet("x", UTF8Type.instance));
                foundUpdate = true;
            }
            else
            {
                Set<String> expectedSet = Sets.newHashSet("a", "b", "c");
                assertEquals(expectedSet, r.getSet("x", UTF8Type.instance));
                found = true;
            }
        }
        assertTrue(foundUpdate);
        assertTrue(found);
    }

    @Test
    public void testSplittingFlush() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, PRIMARY KEY (id, id2)) WITH compaction = {'class':'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES', 'timestamp_resolution': 'MILLISECONDS', 'split_during_flush':'true'}");

        long now = System.currentTimeMillis();

        for (int i = 0; i < 10; i++)
            execute(String.format("insert into %%s (id, id2, t) values (1, %d, '%dx%d') using timestamp %d", i, i, i, now - TimeUnit.MILLISECONDS.convert(10 - i, TimeUnit.MINUTES)));

        getCurrentColumnFamilyStore().forceBlockingFlush();
        assertEquals(10, Iterables.size(getCurrentColumnFamilyStore().getSSTables(SSTableSet.CANONICAL)));
    }

    @Test
    public void testFutureAndOldInsert() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, PRIMARY KEY (id, id2)) WITH compaction = {'class':'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES', 'timestamp_resolution': 'MILLISECONDS', 'split_during_flush':'true'}");
        long now = System.currentTimeMillis();

        for (int i = 0; i < 10; i++)
            execute(String.format("insert into %%s (id, id2, t) values (1, %d, '%dx%d') using timestamp %d", i, i, i, now - TimeUnit.MILLISECONDS.convert(9 - i, TimeUnit.MINUTES)));
        execute(String.format("insert into %%s (id, id2, t) values (1, %d, '%dx%d') using timestamp %d", 11, 11, 11, now + TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES)));
        execute(String.format("insert into %%s (id, id2, t) values (1, %d, '%dx%d') using timestamp %d", 12, 12, 12, now - TimeUnit.MILLISECONDS.convert(100, TimeUnit.MINUTES)));

        getCurrentColumnFamilyStore().forceBlockingFlush();
        assertEquals(12, Iterables.size(getCurrentColumnFamilyStore().getSSTables(SSTableSet.CANONICAL)));
    }

}
