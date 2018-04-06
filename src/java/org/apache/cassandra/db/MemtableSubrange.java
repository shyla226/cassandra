/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.memory.EnsureOnHeap;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * A Memtable sub range of data.
 *
 * Sub ranges are core-local. Individual subrange (including metrics), whether it
 * is for read or for write, has to be accessed from the corresponding core/thread.
 * Core can be determined using {@code #getCoreFor(DecoratedKey dk)}.
 *
 * We index the memtable by PartitionPosition only for the purpose of being able
 * to select key range using Token.KeyBound. However put() ensures that we
 * actually only store DecoratedKey.
 *
 * In order to access the contents of the memtable subrange, a {@link DataAccess} has to be specified.
 * Data can be accessed unsafely as long as the caller is sure that the memtable will not be discarded
 * whilst the data is still in use, other callers need to specify data access on heap.
 */
class MemtableSubrange
{
    // The following fields are volatile as we have to make sure that when we
    // collect results from all sub ranges, the thread accessing the value
    // is guaranteed to see the changes to the values.

    // The smallest timestamp for all partitions stored in this subrange
    private volatile long minTimestamp = Long.MAX_VALUE;

    // The smallest min local deletion time for all partitions in this subrange
    private volatile int minLocalDeletionTime = Integer.MAX_VALUE;

    private volatile long liveDataSize = 0;

    private volatile long currentOperations = 0;

    // This map is used in a single-producer, multi-consumer fashion: only one thread will insert items but
    // several threads may read from it and iterate over it. Iterators are created when a the first item of
    // a flow is requested for example, and then used asynchronously when sub-sequent items are requested.
    // Therefore, iterators should not throw ConcurrentModificationExceptions if the underlying map is modified
    // during iteration, they should provide a weakly consistent view of the map instead.
    // Also, this data is backed by memtable memory, when accessing it callers must specify if it can be accessed
    // unsafely, meaning that the memtable will not be discarded as long as the data is used, or whether the data
    // should be copied on heap for off-heap allocators.
    private final NavigableMap<PartitionPosition, AtomicBTreePartition> data;

    private final ColumnsCollector columnsCollector;

    private final StatsCollector statsCollector;

    private final MemtableAllocator allocator;

    MemtableSubrange(int coreId, TableMetadata metadata)
    {
        this(metadata, Memtable.MEMORY_POOL.newAllocator(coreId));
    }

    @VisibleForTesting
    MemtableSubrange(TableMetadata metadata, MemtableAllocator allocator)
    {
        this.data = new ConcurrentSkipListMap<>();
        this.columnsCollector = new ColumnsCollector(metadata.regularAndStaticColumns());
        this.statsCollector = new StatsCollector();
        this.allocator = allocator;
    }

    /**
     * @return true if the data needs to be copied on heap for safe access
     */
    private boolean copyRequired(DataAccess dataAccess)
    {
        return dataAccess == DataAccess.ON_HEAP && !allocator().onHeapOnly();
    }

    /**
     * Return the partition for a given key, or null if no partition is found.
     *
     * @param key the partition key
     * @param dataAccess how the data should be accessed, whether it should be copied on heap if required or not
     *
     * @return the {@link AtomicBTreePartition} stored in the sub-range if onHeap is false or the allocator is only on heap, otherwise
     * a copy of the partition which is on heap, this will be of type {@link AtomicBTreePartition.AtomicBTreePartitionOnHeap}.
     */
    public AtomicBTreePartition get(PartitionPosition key, DataAccess dataAccess)
    {
        AtomicBTreePartition ret = data.get(key);
        return copyRequired(dataAccess) && ret != null
               ? ret.ensureOnHeap(allocator)
               : ret;
    }

    private Iterator<AtomicBTreePartition> copyOnHeap(Iterator<AtomicBTreePartition> it)
    {
        return new AbstractIterator<AtomicBTreePartition>()
        {
            protected AtomicBTreePartition computeNext()
            {
                if (it.hasNext())
                    return it.next().ensureOnHeap(allocator);
                else
                    return endOfData();
            }
        };
    }

    /**
     * Returns an iterator over the partitions whose positions are included in the two endpoints.
     *
     * @param fromPosition low endpoint of the partition positions in the returned iterator
     * @param fromInclusive {@code true} if the low endpoint is to be included in the returned iterator
     * @param toPosition high endpoint of the partition positions in the returned iterator
     * @param toInclusive {@code true} if the high endpoint is to be included in the returned iterator
     * @param dataAccess how the data should be accessed, whether it should be copied on heap if required or not
     * @return an iterator backed by the memtable partitions, with the partitions optionally copied on heap
     */
    Iterator<AtomicBTreePartition> subIterator(PartitionPosition fromPosition,
                                               boolean fromInclusive,
                                               PartitionPosition toPosition,
                                               boolean toInclusive,
                                               DataAccess dataAccess)
    {
        Iterator<AtomicBTreePartition> it = data.subMap(fromPosition, fromInclusive, toPosition, toInclusive).values().iterator();
        return copyRequired(dataAccess) ? copyOnHeap(it) : it;
    }

    /**
     * Returns an iterator over the partitions whose positions are less than (or
     * equal to, if {@code inclusive} is true) {@code toPosition}.
     *
     * @param toPosition high endpoint of the partition positions in the returned iterator
     * @param inclusive {@code true} if the high endpoint is to be included in the returned iterator
     * @param dataAccess how the data should be accessed, whether it should be copied on heap if required or not
     * @return an iterator backed by the memtable partitions, with the partitions optionally copied on heap
     */
    Iterator<AtomicBTreePartition> headIterator(PartitionPosition toPosition,
                                                boolean inclusive,
                                                DataAccess dataAccess)
    {
        Iterator<AtomicBTreePartition> it = data.headMap(toPosition, inclusive).values().iterator();
        return copyRequired(dataAccess) ? copyOnHeap(it) : it;
    }

    /**
     * Return an iterator over the partitions whose positions are greater than (or
     * equal to, if {@code inclusive} is true) {@code fromPosition}.
     *
     * @param fromPosition low endpoint of the partition positions in the returned iterator
     * @param inclusive {@code true} if the low endpoint is to be included in the returned iterator
     * @param dataAccess how the data should be accessed, whether it should be copied on heap if required or not
     * @return an iterator backed by the memtable partitions, with the partitions optionally copied on heap
     */
    Iterator<AtomicBTreePartition> tailIterator(PartitionPosition fromPosition,
                                                boolean inclusive,
                                                DataAccess dataAccess)
    {
        Iterator<AtomicBTreePartition> it = data.tailMap(fromPosition, inclusive).values().iterator();
        return copyRequired(dataAccess) ? copyOnHeap(it) : it;
    }

    /**
     * Return an iterator over the partitions.
     *
     * @param dataAccess how the data should be accessed, whether it should be copied on heap if required or not
     * @return an iterator backed by the memtable partitions, with the partitions optionally copied on heap
     */
    public Iterator<AtomicBTreePartition> iterator(DataAccess dataAccess)
    {
        Iterator<AtomicBTreePartition> it = data.values().iterator();
        return copyRequired(dataAccess) ? copyOnHeap(it) : it;
    }

    /**
     * Returns two separate iterators, one for the keys and one for the partitions,
     * over the partitions whose positions are included in the two endpoints, inclusive
     * of the first endpoint but exclusive of the second endpoint. The endpoints can be null,
     * in which case all partitioner will be returned. Either both endpoints are null, or both are not null.
     *
     * @param from low endpoint of the partition positions in the returned iterator
     * @param to high endpoint of the partition positions in the returned iterator
     * @param dataAccess how the data should be accessed, whether it should be copied on heap if required or not
     * @return two iterators backed by the memtable partitions, with the partitions and keysp optionally copied on heap
     */
    public Pair<Iterator<PartitionPosition>, Iterator<AtomicBTreePartition>> iterators(PartitionPosition from,
                                                                                       PartitionPosition to,
                                                                                       DataAccess dataAccess)
    {

        assert (from == null && to == null) || (from != null && to != null) : "from and to must either both be null or both not null";

        SortedMap<PartitionPosition, AtomicBTreePartition> map = from == null
                                                                 ? data
                                                                 : data.subMap(from, true, to, false);


        if (!copyRequired(dataAccess))
            return Pair.create(map.keySet().iterator(), map.values().iterator());

        return Pair.create(new AbstractIterator<PartitionPosition>()
        {
            Iterator<PartitionPosition> keys = map.keySet().iterator();
            EnsureOnHeap onHeap = new EnsureOnHeap();

            protected PartitionPosition computeNext()
            {
                if (keys.hasNext())
                    return onHeap.applyToPartitionKey((DecoratedKey) keys.next());
                else
                    return endOfData();
            }
        }, new AbstractIterator<AtomicBTreePartition>()
        {
            Iterator<AtomicBTreePartition> values = map.values().iterator();
            protected AtomicBTreePartition computeNext()
            {
                if (values.hasNext())
                    return values.next().ensureOnHeap(allocator);
                else
                    return endOfData();
            }
        });
    }

    /**
     * Add a partition to the map, note that the partition could be empty, it will
     * be updated later on when {@link #update(PartitionUpdate, long)} is called.
     *
     * @param key the partition key
     * @param partition the partition
     */
    public void put(DecoratedKey key, AtomicBTreePartition partition)
    {
        data.put(key, partition);
    }

    /**
     * This is called when a partition in the map has been updated, to ensure
     * that we keep track of some statistics.
     *
     * @param partitionUpdate the details of the partition update
     * @param dataSize the size of the update
     */
    public void update(PartitionUpdate partitionUpdate, long dataSize)
    {
        updateTimestamp(partitionUpdate.stats().minTimestamp);
        updateLocalDeletionTime(partitionUpdate.stats().minLocalDeletionTime);
        updateLiveDataSize(dataSize);
        updateCurrentOperations(partitionUpdate.operationCount());

        // TODO: check if stats are further optimisable
        columnsCollector.update(partitionUpdate.columns());
        statsCollector.update(partitionUpdate.stats());
    }

    public boolean isEmpty()
    {
        return data.isEmpty();
    }

    public int size()
    {
        return data.size();
    }

    private void updateTimestamp(long timestamp)
    {
        if (timestamp < minTimestamp)
            minTimestamp = timestamp;
    }

    void updateLiveDataSize(long size)
    {
        liveDataSize = liveDataSize + size;
    }

    private void updateCurrentOperations(long op)
    {
        currentOperations = currentOperations + op;
    }

    private void updateLocalDeletionTime(int localDeletionTime)
    {
        if (localDeletionTime < minLocalDeletionTime)
            minLocalDeletionTime = localDeletionTime;
    }

    ColumnsCollector columnsCollector()
    {
        return columnsCollector;
    }

    EncodingStats encodingStats()
    {
        return statsCollector.stats;
    }

    long minTimestamp()
    {
        return minTimestamp;
    }

    int minLocalDeletionTime()
    {
        return minLocalDeletionTime;
    }

    long liveDataSize()
    {
        return liveDataSize;
    }

    /**
     * For testing only. Give this sub range too big a size to make it always fail flushing.
     */
    @VisibleForTesting
    void makeUnflushable()
    {
        liveDataSize += (1024L * 1024 * 1024 * 1024 * 1024);
    }

    long currentOperations()
    {
        return currentOperations;
    }

    MemtableAllocator allocator()
    {
        return allocator;
    }

    /**
     * How data should be accessed. Select UNSAFE if you are
     * sure the memtable backing memory will still be available as
     * this will avoid a copy, otherwise select ON_HEAP knowning that
     * for off-heap allocators this will copy the data.
     */
    enum DataAccess
    {
        /**
         * The data can be backed by the memtable off-heap memory, this can only be used
         * when we are sure that the memtable won't be discarded for the entire life duration
         * of the data being accessed.
         */
        UNSAFE,
        /**
         * The data will be copied on heap if required. This means the data can outlive the
         * memtable. It is the safest option but it may incur a copy on the heap.
         */
        ON_HEAP
    }

    static class ColumnsCollector
    {
        private final Set<ColumnMetadata> columns = new HashSet<>();

        ColumnsCollector(RegularAndStaticColumns columns)
        {
            columns.statics.apply(this.columns::add);
            columns.regulars.apply(this.columns::add);
        }

        public void update(RegularAndStaticColumns columns)
        {
            columns.statics.apply(this.columns::add);
            columns.regulars.apply(this.columns::add);
        }

        public RegularAndStaticColumns get()
        {
            return RegularAndStaticColumns.builder().addAll(columns).build();
        }

        public void merge(ColumnsCollector other)
        {
            columns.addAll(other.columns);
        }
    }

    static class StatsCollector
    {
        private EncodingStats stats = EncodingStats.NO_STATS;

        public void update(EncodingStats newStats)
        {
            stats = stats.mergeWith(newStats);
        }

        public EncodingStats get()
        {
            return stats;
        }
    }
}
