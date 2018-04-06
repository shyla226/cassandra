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

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class MemtableSubrangeTest
{
    private final TableMetadataRef metadata = TableMetadataRef.forOfflineTools(TableMetadata.minimal("keyspace", "table"));

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testEmptyRange()
    {
        MemtableSubrange range = new MemtableSubrange(metadata.get(), Mockito.mock(MemtableAllocator.class));
        assertEquals(0, range.size());
        assertTrue(range.isEmpty());
    }

    @Test
    public void testPutGetOnHeap()
    {
        testPutGet(true);
    }

    @Test
    public void testPutGetOffHeap()
    {
        testPutGet(false);
    }

    private void testPutGet(boolean onHeap)
    {
        MemtableAllocator allocator = Mockito.mock(MemtableAllocator.class);
        when(allocator.onHeapOnly()).thenReturn(onHeap);

        MemtableSubrange range = new MemtableSubrange(metadata.get(), allocator);

        final DecoratedKey key = Util.dk("key1");
        final AtomicBTreePartition partition = new AtomicBTreePartition(metadata, key);

        range.put(key, partition);
        assertEquals(1, range.size());
        assertFalse(range.isEmpty());

        assertTrue("Partition should not have been copied, UNSAFE data access", partition == range.get(key, MemtableSubrange.DataAccess.UNSAFE));
        if (onHeap)
            assertTrue("Partition should not have been copied, allocator already ON HEAP", partition == range.get(key, MemtableSubrange.DataAccess.ON_HEAP));
        else
            assertFalse("Partition should have been copied, allocator OFF HEAP", partition == range.get(key, MemtableSubrange.DataAccess.ON_HEAP));

        // if a key is not found, there should be no NPE
        assertNull(range.get(Util.dk("key2"), MemtableSubrange.DataAccess.UNSAFE));
        assertNull(range.get(Util.dk("key2"), MemtableSubrange.DataAccess.ON_HEAP));
    }

    @Test
    public void testIteratorsOnHeap()
    {
        testIterators(true);
    }

    @Test
    public void testIteratorsOffHeap()
    {
        testIterators(false);
    }

    private void testIterators(boolean onHeap)
    {
        MemtableAllocator allocator = Mockito.mock(MemtableAllocator.class);
        when(allocator.onHeapOnly()).thenReturn(onHeap);

        MemtableSubrange range = new MemtableSubrange(metadata.get(), allocator);
        NavigableMap<DecoratedKey, AtomicBTreePartition> partitions = new ConcurrentSkipListMap<>();

        final int numPartitions = 100;
        final DecoratedKey from = Util.dk("key30");
        final DecoratedKey to = Util.dk("key60");
        for (int i = 0; i < numPartitions; i++)
        {
            final DecoratedKey key = Util.dk("key" + Integer.toString(i));
            final AtomicBTreePartition partition = new AtomicBTreePartition(metadata, key);

            range.put(key, partition);
            partitions.put(key, partition);
        }

        assertEquals(numPartitions, range.size());
        assertFalse(range.isEmpty());

        checkPartitions(range.iterator(MemtableSubrange.DataAccess.UNSAFE), partitions, onHeap, MemtableSubrange.DataAccess.UNSAFE);
        checkPartitions(range.iterator(MemtableSubrange.DataAccess.ON_HEAP), partitions, onHeap, MemtableSubrange.DataAccess.ON_HEAP);

        checkPartitions(range.tailIterator(from, true, MemtableSubrange.DataAccess.UNSAFE), partitions.tailMap(from, true), onHeap, MemtableSubrange.DataAccess.UNSAFE);
        checkPartitions(range.tailIterator(from, true, MemtableSubrange.DataAccess.ON_HEAP), partitions.tailMap(from, true), onHeap, MemtableSubrange.DataAccess.ON_HEAP);
        checkPartitions(range.tailIterator(from, false, MemtableSubrange.DataAccess.UNSAFE), partitions.tailMap(from, false), onHeap, MemtableSubrange.DataAccess.UNSAFE);
        checkPartitions(range.tailIterator(from, false, MemtableSubrange.DataAccess.ON_HEAP), partitions.tailMap(from, false), onHeap, MemtableSubrange.DataAccess.ON_HEAP);

        checkPartitions(range.headIterator(to, true, MemtableSubrange.DataAccess.UNSAFE), partitions.headMap(to, true), onHeap, MemtableSubrange.DataAccess.UNSAFE);
        checkPartitions(range.headIterator(to, true, MemtableSubrange.DataAccess.ON_HEAP), partitions.headMap(to, true), onHeap, MemtableSubrange.DataAccess.ON_HEAP);
        checkPartitions(range.headIterator(to, false, MemtableSubrange.DataAccess.UNSAFE), partitions.headMap(to, false), onHeap, MemtableSubrange.DataAccess.UNSAFE);
        checkPartitions(range.headIterator(to, false, MemtableSubrange.DataAccess.ON_HEAP), partitions.headMap(to, false), onHeap, MemtableSubrange.DataAccess.ON_HEAP);

        checkPartitions(range.subIterator(from, true, to, false, MemtableSubrange.DataAccess.UNSAFE), partitions.subMap(from, true, to, false), onHeap, MemtableSubrange.DataAccess.UNSAFE);
        checkPartitions(range.subIterator(from, true, to, false, MemtableSubrange.DataAccess.ON_HEAP), partitions.subMap(from, true, to, false), onHeap, MemtableSubrange.DataAccess.ON_HEAP);
        checkPartitions(range.subIterator(from, false, to, false, MemtableSubrange.DataAccess.UNSAFE), partitions.subMap(from, false, to, false), onHeap, MemtableSubrange.DataAccess.UNSAFE);
        checkPartitions(range.subIterator(from, false, to, false, MemtableSubrange.DataAccess.ON_HEAP), partitions.subMap(from, false, to, false), onHeap, MemtableSubrange.DataAccess.ON_HEAP);
        checkPartitions(range.subIterator(from, true, to, true, MemtableSubrange.DataAccess.UNSAFE), partitions.subMap(from, true, to, true), onHeap, MemtableSubrange.DataAccess.UNSAFE);
        checkPartitions(range.subIterator(from, true, to, true, MemtableSubrange.DataAccess.ON_HEAP), partitions.subMap(from, true, to, true), onHeap, MemtableSubrange.DataAccess.ON_HEAP);
        checkPartitions(range.subIterator(from, false, to, true, MemtableSubrange.DataAccess.UNSAFE), partitions.subMap(from, false, to, true), onHeap, MemtableSubrange.DataAccess.UNSAFE);
        checkPartitions(range.subIterator(from, false, to, true, MemtableSubrange.DataAccess.ON_HEAP), partitions.subMap(from, false, to, true), onHeap, MemtableSubrange.DataAccess.ON_HEAP);

        Pair<Iterator<PartitionPosition>, Iterator<AtomicBTreePartition>> p = range.iterators(from, to, MemtableSubrange.DataAccess.UNSAFE);
        checkKeys(p.left, partitions.subMap(from, true, to, false).keySet());
        checkPartitions(p.right, partitions.subMap(from, true, to, false), onHeap, MemtableSubrange.DataAccess.UNSAFE);

        p = range.iterators(from, to, MemtableSubrange.DataAccess.ON_HEAP);
        checkKeys(p.left, partitions.subMap(from, true, to, false).keySet());
        checkPartitions(p.right, partitions.subMap(from, true, to, false), onHeap, MemtableSubrange.DataAccess.ON_HEAP);
    }

    private void checkPartitions(Iterator<AtomicBTreePartition> it,
                                 Map<DecoratedKey, AtomicBTreePartition> expectedPartitions,
                                 boolean allocatorOnHeap,
                                 MemtableSubrange.DataAccess dataAccess)
    {
        int counted = 0;
        while (it.hasNext())
        {
            AtomicBTreePartition partition = it.next();
            assertNotNull(partition);
            counted++;

            if (dataAccess == MemtableSubrange.DataAccess.ON_HEAP)
            {
                assertTrue("Partition key is not a buffer key even though it should be ON HEAP",
                           partition.partitionKey() instanceof BufferDecoratedKey);

                BufferDecoratedKey key = (BufferDecoratedKey) partition.partitionKey();
                assertTrue("Partition key buffer does not have an array even though it should be ON HEAP",
                           key.getKey().hasArray());
            }

            if (dataAccess == MemtableSubrange.DataAccess.UNSAFE)
                assertTrue("Partition should not have been copied, UNSAFE data access", partition == expectedPartitions.get(partition.partitionKey()));
            else if (allocatorOnHeap)
                assertTrue("Partition should not have been copied, allocator already ON HEAP", partition == expectedPartitions.get(partition.partitionKey()));
            else
                assertFalse("Partition should have been copied, allocator OFF HEAP", partition == expectedPartitions.get(partition.partitionKey()));
        }
        assertEquals(expectedPartitions.size(), counted);
    }

    private void checkKeys(Iterator<PartitionPosition> keys, Set<DecoratedKey> expectedKeys)
    {
        int counted = 0;
        while (keys.hasNext())
        {
            PartitionPosition key = keys.next();
            assertTrue(expectedKeys.contains(key));
            counted++;
        }

        assertEquals(counted, expectedKeys.size());
    }

    @Test
    public void testPartitionUpdateOnHeap()
    {
        testPartitionUpdate(true);
    }

    @Test
    public void testPartitionUpdateOffHeap()
    {
        testPartitionUpdate(false);
    }

    private void testPartitionUpdate(boolean onHeap)
    {
        MemtableAllocator allocator = Mockito.mock(MemtableAllocator.class);
        when(allocator.onHeapOnly()).thenReturn(onHeap);

        MemtableSubrange range = new MemtableSubrange(metadata.get(), allocator);

        final DecoratedKey key = Util.dk("key1");
        final AtomicBTreePartition partition = new AtomicBTreePartition(metadata, key);
        final PartitionUpdate update = new PartitionUpdate(metadata.get(), key, metadata.get().regularAndStaticColumns(), 1);
        final int dataSize = 100;

        range.put(key, partition);
        range.update(update, dataSize);

        assertEquals(dataSize, range.liveDataSize());
        assertEquals(update.stats().minTimestamp, range.minTimestamp());
        assertEquals(update.stats().minLocalDeletionTime, range.minLocalDeletionTime());
        assertEquals(update.operationCount(), range.currentOperations());
    }
}
