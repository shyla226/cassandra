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

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.fail;

public class PerRangeReducingKeyIteratorTest
{
    public static final String KEYSPACE1 = "PerRangeReducingKeyIteratorTest";
    public static final String CF_STANDARD = "Standard1";

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.prepareServer();
        CompactionManager.instance.disableAutoCompaction();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));
    }

    @After
    public void afterTest() throws Exception
    {
        ColumnFamilyStore store = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD);
        store.truncateBlocking();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSSTablesAreRequired() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD);
        IPartitioner partitioner = store.getPartitioner();

        Collection<Range<Token>> bounds = ImmutableList.of(new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken()));
        PerRangeReducingKeyIterator reducingIterator = new PerRangeReducingKeyIterator(ImmutableList.of(), bounds);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBoundsAreRequired() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD);

        new RowUpdateBuilder(store.metadata(), 0, String.valueOf(0))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        store.forceBlockingFlush();

        Set<SSTableReader> sstables = store.getLiveSSTables();
        PerRangeReducingKeyIterator reducingIterator = new PerRangeReducingKeyIterator(sstables, ImmutableList.of());
    }

    @Test
    public void testHasNextIdempotency() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD);
        IPartitioner partitioner = store.getPartitioner();

        new RowUpdateBuilder(store.metadata(), 0, String.valueOf(0))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        store.forceBlockingFlush();

        Set<SSTableReader> sstables = store.getLiveSSTables();
        Collection<Range<Token>> bounds = ImmutableList.of(new Range<Token>(partitioner.getMinimumToken(), partitioner.getMaximumToken()));
        PerRangeReducingKeyIterator reducingIterator = new PerRangeReducingKeyIterator(sstables, bounds);

        Assert.assertTrue(reducingIterator.hasNext());
        Assert.assertTrue(reducingIterator.hasNext());

        Assert.assertNotNull(reducingIterator.next());

        Assert.assertFalse(reducingIterator.hasNext());
        Assert.assertFalse(reducingIterator.hasNext());
    }

    @Test
    public void testNextCanBeCalledWithoutHasNext() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD);
        IPartitioner partitioner = store.getPartitioner();

        new RowUpdateBuilder(store.metadata(), 0, String.valueOf(0))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        store.forceBlockingFlush();

        Set<SSTableReader> sstables = store.getLiveSSTables();
        Collection<Range<Token>> bounds = ImmutableList.of(new Range<Token>(partitioner.getMinimumToken(), partitioner.getMaximumToken()));
        PerRangeReducingKeyIterator reducingIterator = new PerRangeReducingKeyIterator(sstables, bounds);

        Assert.assertNotNull(reducingIterator.next());
        try
        {
            reducingIterator.next();
            fail("There should be no more elements!");
        }
        catch(IllegalStateException ex)
        {
            // expected
        }
    }

    @Test
    public void testIteration() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD);
        IPartitioner partitioner = store.getPartitioner();

        Range<Token> all = new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken());
        Pair<AbstractBounds<Token>, AbstractBounds<Token>> split = all.split(partitioner.midpoint(all.left, all.right));
        ImmutableList<AbstractBounds<Token>> bounds = ImmutableList.of(split.left, split.right);

        long key = 0;
        for (int i = 0; i < bounds.size(); i++)
        {
            for (int j = 0; j < 50; j++)
            {
                key = brutallyGetKeyFor(store.metadata(), partitioner, bounds.get(i), key);
                new RowUpdateBuilder(store.metadata(), System.currentTimeMillis(), String.valueOf(key))
                    .clustering("0")
                    .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

                if (j % 10 == 0)
                {
                    store.forceBlockingFlush();
                }
            }
        }
        store.forceBlockingFlush();

        Set<SSTableReader> sstables = store.getLiveSSTables();
        Assert.assertTrue(sstables.size() > 1);

        PerRangeReducingKeyIterator reducingIterator = new PerRangeReducingKeyIterator(sstables, bounds);

        int idx = 0;
        while (reducingIterator.hasNext())
        {
            DecoratedKey next = reducingIterator.next();
            Assert.assertTrue(bounds.get(idx++ % bounds.size()).contains(next.getToken()));
        }
        Assert.assertEquals(100, idx);
        Assert.assertEquals(reducingIterator.getTotalBytes(), reducingIterator.getBytesRead());
    }

    private long brutallyGetKeyFor(TableMetadata metadata, IPartitioner partitioner, AbstractBounds<Token> bounds, long current)
    {
        current++;
        while (!bounds.contains(partitioner.getToken(metadata.partitionKeyAsClusteringComparator().make(String.valueOf(current)).serializeAsPartitionKey())))
            current++;

        return current;
    }
}
