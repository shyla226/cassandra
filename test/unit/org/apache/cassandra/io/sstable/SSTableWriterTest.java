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

package org.apache.cassandra.io.sstable;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SSTableWriterTest extends SSTableWriterTestBase
{
    @Test
    public void testAbortTxnWithOpenEarlyShouldRemoveSSTable() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
        try (SSTableWriter writer = getWriter(cfs, dir, txn))
        {
            final List<DecoratedKey> keys = generateKeys(cfs, 20000);
            addUpdatesToWriter(cfs, writer, keys.subList(0, 10000));

            SSTableReader s = writer.setMaxDataAge(1000).openEarly();
            assert s != null;
            assertFileCounts(dir.list());

            addUpdatesToWriter(cfs, writer, keys.subList(10000, 20000));

            SSTableReader s2 = writer.setMaxDataAge(1000).openEarly();
            assertTrue(s.last.compareTo(s2.last) < 0);
            assertFileCounts(dir.list());
            s.selfRef().release();
            s2.selfRef().release();

            int datafiles = assertFileCounts(dir.list());
            assertEquals(datafiles, 1);

            // These checks don't work on Windows because the writer has the channel still
            // open till .abort() is called (via the builder)
            if (!FBUtilities.isWindows)
            {
                LifecycleTransaction.waitForDeletions();
                assertFileCounts(dir.list());
            }
            writer.abort();
            txn.abort();
            LifecycleTransaction.waitForDeletions();
            datafiles = assertFileCounts(dir.list());
            assertEquals(datafiles, 0);
            validateCFS(cfs);
        }
    }

    private List<DecoratedKey> generateKeys(ColumnFamilyStore cfs, int num)
    {
        final TableMetadata metadata = cfs.metadata();
        final List<DecoratedKey> keys = new ArrayList(num);

        for (int i = 0; i < num; i++)
            keys.add(metadata.partitioner.decorateKey(random(i, 10)));

        Collections.sort(keys, DecoratedKey.comparator);
        return keys;
    }

    private void addUpdatesToWriter(ColumnFamilyStore cfs, SSTableWriter writer, List<DecoratedKey> keys)
    {
        for (DecoratedKey key : keys)
        {
            UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), key).withTimestamp(1);
            for (int j = 0; j < 100; j++)
                builder.newRow("" + j).add("val", ByteBuffer.allocate(1000));
            writer.append(builder.build().unfilteredIterator());
        }
    }


    @Test
    public void testAbortTxnWithClosedWriterShouldRemoveSSTable() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.STREAM);
        try (SSTableWriter writer = getWriter(cfs, dir, txn))
        {
            final List<DecoratedKey> keys = generateKeys(cfs, 20000);
            addUpdatesToWriter(cfs, writer, keys.subList(0, 10000));

            assertFileCounts(dir.list());

            addUpdatesToWriter(cfs, writer, keys.subList(10000, 20000));

            SSTableReader sstable = writer.finish(true);
            int datafiles = assertFileCounts(dir.list());
            assertEquals(datafiles, 1);

            sstable.selfRef().release();
            // These checks don't work on Windows because the writer has the channel still
            // open till .abort() is called (via the builder)
            if (!FBUtilities.isWindows)
            {
                LifecycleTransaction.waitForDeletions();
                assertFileCounts(dir.list());
            }

            txn.abort();
            LifecycleTransaction.waitForDeletions();
            datafiles = assertFileCounts(dir.list());
            assertEquals(datafiles, 0);
            validateCFS(cfs);
        }
        finally
        {
            txn.close();
        }
    }

    @Test
    public void testAbortTxnWithClosedAndOpenWriterShouldRemoveAllSSTables() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.STREAM);

        SSTableWriter writer1 = getWriter(cfs, dir, txn);
        SSTableWriter writer2 = getWriter(cfs, dir, txn);
        try
        {
            final List<DecoratedKey> keys = generateKeys(cfs, 20000);
            addUpdatesToWriter(cfs, writer1, keys.subList(0, 10000));

            assertFileCounts(dir.list());

            addUpdatesToWriter(cfs, writer2, keys.subList(10000, 20000));

            SSTableReader sstable = writer1.finish(true);
            txn.update(sstable, false);

            assertFileCounts(dir.list());

            int datafiles = assertFileCounts(dir.list());
            assertEquals(datafiles, 2);

            // These checks don't work on Windows because the writer has the channel still
            // open till .abort() is called (via the builder)
            if (!FBUtilities.isWindows)
            {
                LifecycleTransaction.waitForDeletions();
                assertFileCounts(dir.list());
            }
            txn.abort();
            LifecycleTransaction.waitForDeletions();
            datafiles = assertFileCounts(dir.list());
            assertEquals(datafiles, 0);
            validateCFS(cfs);
        }
        finally
        {
            writer1.close();
            writer2.close();

            txn.close();
        }
    }

    @Test
    public void testValueTooBigCorruption() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_SMALL_MAX_VALUE);
        truncate(cfs);

        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.STREAM);

        try (SSTableWriter writer1 = getWriter(cfs, dir, txn))
        {
            UpdateBuilder largeValue = UpdateBuilder.create(cfs.metadata(), "large_value").withTimestamp(1);
            largeValue.newRow("clustering").add("val", ByteBuffer.allocate(2 * 1024 * 1024));
            writer1.append(largeValue.build().unfilteredIterator());

            SSTableReader sstable = writer1.finish(true);

            txn.update(sstable, false);

            try
            {
                DecoratedKey dk = Util.dk("large_value");
                UnfilteredRowIterator rowIter = sstable.iterator(dk, Slices.ALL, ColumnFilter.all(cfs.metadata()), false);
                while (rowIter.hasNext())
                {
                    rowIter.next();
                    // no-op read, as values may not appear expected
                }
                fail("Expected a CorruptSSTableException to be thrown");
            }
            catch (CorruptSSTableException e)
            {
            }

            txn.abort();
            LifecycleTransaction.waitForDeletions();
        }
        finally
        {
            txn.close();
        }
    }

}
