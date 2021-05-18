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
package org.apache.cassandra.db.memtable;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.InsertOnlyOrderedMap;
import org.apache.cassandra.concurrent.NonBlockingHashOrderedMap;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.BTreePartitionUpdater;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemtableAllocator;

public class HashOrderedMapMemtable extends AbstractAllocatorMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(HashOrderedMapMemtable.class);

    public static final Factory FACTORY = HashOrderedMapMemtable::new;

    // We index the memtable by PartitionPosition only for the purpose of being able
    // to select key range using Token.KeyBound. However put() ensures that we
    // actually only store DecoratedKey.
    private final NonBlockingHashOrderedMap<PartitionPosition, AtomicBTreePartition> partitions;

    private final AtomicLong liveDataSize = new AtomicLong(0);

    HashOrderedMapMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner)
    {
        super(commitLogLowerBound, metadataRef, owner);
        assert metadataRef.get().partitioner.sortsByHashCode() : "HashOrderedMapMemtable cannot be used with non-hashing partitioners.";
        Set<Range<Token>> localRanges = owner.localRanges();
        partitions = new NonBlockingHashOrderedMap<>(localRanges);
    }

    protected Factory factory()
    {
        return FACTORY;
    }

    @Override
    public void addMemoryUsageTo(MemoryUsage stats)
    {
        super.addMemoryUsageTo(stats);
    }

    public boolean isClean()
    {
        return partitions.size() == 0;
    }

    /**
     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
     * OpOrdering.
     *
     * commitLogSegmentPosition should only be null if this is a secondary index, in which case it is *expected* to be null
     */
    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        AtomicBTreePartition previous = partitions.get(update.partitionKey());

        long initialSize = 0;
        if (previous == null)
        {
            final DecoratedKey cloneKey = allocator.clone(update.partitionKey(), opGroup);
            AtomicBTreePartition empty = new AtomicBTreePartition(metadata, cloneKey, allocator);
            // We'll add the columns later. This avoids wasting works if we get beaten in the putIfAbsent
            previous = partitions.putIfAbsent(cloneKey, empty);
            if (previous == null)
            {
                previous = empty;
                // allocate the row overhead after the fact; this saves over allocating and having to free after, but
                // means we can overshoot our declared limit.
                int overhead = (int) (cloneKey.getToken().getHeapSize() + cloneKey.unsharedHeapSize() + NonBlockingHashOrderedMap.ITEM_HEAP_OVERHEAD);
                allocator.onHeap().allocate(overhead, opGroup);
                initialSize = 8;
            }
        }

        BTreePartitionUpdater updater = previous.addAll(update, opGroup, indexer);
        updateMin(minTimestamp, previous.stats().minTimestamp);
        liveDataSize.addAndGet(initialSize + updater.dataSize);
        columnsCollector.update(update.columns());
        statsCollector.update(update.stats());
        currentOperations.addAndGet(update.operationCount());
        return updater.colUpdateTimeDelta;
    }

    public long partitionCount()
    {
        return partitions.size();
    }

    public MemtableUnfilteredPartitionIterator makePartitionIterator(final ColumnFilter columnFilter,
                                                                     final DataRange dataRange)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        PartitionPosition left = keyRange.left;
        PartitionPosition right = keyRange.right;

        boolean isBound = keyRange instanceof Bounds;
        boolean includeLeft = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeRight = isBound || keyRange instanceof Range;
        Iterable<Map.Entry<PartitionPosition, AtomicBTreePartition>> subMap = getPartitionsSubMap(left,
                                                                                                  includeLeft,
                                                                                                  right,
                                                                                                  includeRight);

        return new MemtableUnfilteredPartitionIterator(metadata.get(), subMap, columnFilter, dataRange);
    }

    private Iterable<Map.Entry<PartitionPosition, AtomicBTreePartition>> getPartitionsSubMap(PartitionPosition left,
                                                                                             boolean includeLeft,
                                                                                             PartitionPosition right,
                                                                                             boolean includeRight)
    {
        if (left != null && left.isMinimum())
            left = null;
        if (right != null && right.isMinimum())
            right = null;

        try
        {
            return partitions.range(left, includeLeft, right, includeRight);
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Invalid range requested {} - {}", left, right);
            throw e;
        }
    }

    public Partition getPartition(DecoratedKey key)
    {
        return partitions.get(key);
    }

    private static int estimateRowOverhead(final int count)
    {
        // calculate row overhead
        try (final OpOrder.Group group = new OpOrder().start())
        {
            int rowOverhead;
            MemtableAllocator allocator = MEMORY_POOL.newAllocator();
            ConcurrentNavigableMap<PartitionPosition, Object> partitions = new ConcurrentSkipListMap<>();
            final Object val = new Object();
            for (int i = 0 ; i < count ; i++)
                partitions.put(allocator.clone(new BufferDecoratedKey(new LongToken(i), ByteBufferUtil.EMPTY_BYTE_BUFFER), group), val);
            double avgSize = ObjectSizes.measureDeep(partitions) / (double) count;
            rowOverhead = (int) ((avgSize - Math.floor(avgSize)) < 0.05 ? Math.floor(avgSize) : Math.ceil(avgSize));
            rowOverhead -= ObjectSizes.measureDeep(new LongToken(0));
            rowOverhead += AtomicBTreePartition.EMPTY_SIZE;
            rowOverhead += BTreePartitionData.UNSHARED_HEAP_SIZE;
            allocator.setDiscarding();
            allocator.setDiscarded();
            return rowOverhead;
        }
    }

    public FlushCollection<?> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        Iterable<Map.Entry<PartitionPosition, AtomicBTreePartition>> toFlush = getPartitionsSubMap(from, true, to, false);
        long keySize = 0;
        long keyCount = 0;

        boolean trackContention = logger.isTraceEnabled();
        int heavilyContendedRowCount = 0;
        for (Map.Entry<PartitionPosition, AtomicBTreePartition> en : toFlush)
        {
            ++keyCount;
            PartitionPosition key = en.getKey();
            //  make sure we don't write non-sensical keys
            assert key instanceof DecoratedKey;
            keySize += ((DecoratedKey) key).getKey().remaining();

            if (trackContention && en.getValue().useLock())
                heavilyContendedRowCount++;
        }
        if (heavilyContendedRowCount > 0)
            logger.trace("High update contention in {}/{} partitions of {} ",
                         heavilyContendedRowCount,
                         keyCount,
                         HashOrderedMapMemtable.this);
        final long partitionKeySize = keySize;
        final long partitionCount = keyCount;

        return new AbstractFlushCollection<AtomicBTreePartition>()
        {
            public Memtable memtable()
            {
                return HashOrderedMapMemtable.this;
            }

            public PartitionPosition from()
            {
                return from;
            }

            public PartitionPosition to()
            {
                return to;
            }

            public long partitionCount()
            {
                return partitionCount;
            }

            public Iterator<AtomicBTreePartition> iterator()
            {
                return Iterators.transform(toFlush.iterator(), Map.Entry::getValue);
            }

            public long partitionKeySize()
            {
                return partitionKeySize;
            }
        };
    }


    public static class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator implements Memtable.MemtableUnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final Iterator<Map.Entry<PartitionPosition, AtomicBTreePartition>> iter;
        private final Iterable<Map.Entry<PartitionPosition, AtomicBTreePartition>> source;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        public MemtableUnfilteredPartitionIterator(TableMetadata metadata,
                                                   Iterable<Map.Entry<PartitionPosition, AtomicBTreePartition>> map,
                                                   ColumnFilter columnFilter,
                                                   DataRange dataRange)
        {
            this.metadata = metadata;
            this.source = map;
            this.iter = map.iterator();
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        public int getMinLocalDeletionTime()
        {
            int minLocalDeletionTime = Integer.MAX_VALUE;
            for (Map.Entry<PartitionPosition, AtomicBTreePartition> en : source)
                minLocalDeletionTime = Math.min(minLocalDeletionTime, en.getValue().stats().minLocalDeletionTime);

            return minLocalDeletionTime;
        }

        public TableMetadata metadata()
        {
            return metadata;
        }

        public boolean hasNext()
        {
            return iter.hasNext();
        }

        public UnfilteredRowIterator next()
        {
            Map.Entry<PartitionPosition, AtomicBTreePartition> entry = iter.next();
            // Actual stored key should be true DecoratedKey
            assert entry.getKey() instanceof DecoratedKey;
            DecoratedKey key = (DecoratedKey)entry.getKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);

            return filter.getUnfilteredRowIterator(columnFilter, entry.getValue());
        }
    }

    public long getLiveDataSize()
    {
        return liveDataSize.get();
    }

    /**
     * For testing only. Give this memtable too big a size to make it always fail flushing.
     */
    @VisibleForTesting
    public void makeUnflushable()
    {
        liveDataSize.addAndGet(1024L * 1024 * 1024 * 1024 * 1024);
    }
}
