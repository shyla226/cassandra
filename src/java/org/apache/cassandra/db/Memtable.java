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

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.utils.flow.CsFlow;
import io.reactivex.Single;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.FSDiskFullWriteError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.OpOrderSimple;
import org.apache.cassandra.utils.flow.Threads;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.NativePool;
import org.apache.cassandra.utils.memory.SlabPool;

public class Memtable implements Comparable<Memtable>
{
    private static final Logger logger = LoggerFactory.getLogger(Memtable.class);

    public static final MemtablePool MEMORY_POOL = createMemtableAllocatorPool();

    private static MemtablePool createMemtableAllocatorPool()
    {
        long heapLimit = DatabaseDescriptor.getMemtableHeapSpaceInMb() << 20;
        long offHeapLimit = DatabaseDescriptor.getMemtableOffheapSpaceInMb() << 20;
        switch (DatabaseDescriptor.getMemtableAllocationType())
        {
            case unslabbed_heap_buffers:
                return new HeapPool(heapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            case heap_buffers:
                return new SlabPool(heapLimit, 0, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            case offheap_buffers:
                if (!FileUtils.isCleanerAvailable)
                {
                    throw new IllegalStateException("Could not free direct byte buffer: offheap_buffers is not a safe memtable_allocation_type without this ability, please adjust your config. This feature is only guaranteed to work on an Oracle JVM. Refusing to start.");
                }
                return new SlabPool(heapLimit, offHeapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            case offheap_objects:
                return new NativePool(heapLimit, offHeapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            default:
                throw new AssertionError();
        }
    }

    private static final int ROW_OVERHEAD_HEAP_SIZE = estimateRowOverhead(Integer.parseInt(System.getProperty("cassandra.memtable_row_overhead_computation_step", "100000")));

    private final MemtableAllocator allocator;
    private final AtomicLong liveDataSize = new AtomicLong(0);
    private final AtomicLong currentOperations = new AtomicLong(0);

    // the write barrier for directing writes to this memtable during a switch
    private volatile OpOrder.Barrier writeBarrier;
    // the precise upper bound of CommitLogPosition owned by this memtable
    private volatile AtomicReference<CommitLogPosition> commitLogUpperBound;
    // the precise lower bound of CommitLogPosition owned by this memtable; equal to its predecessor's commitLogUpperBound
    private AtomicReference<CommitLogPosition> commitLogLowerBound;

    // The approximate lower bound by this memtable; must be <= commitLogLowerBound once our predecessor
    // has been finalised, and this is enforced in the ColumnFamilyStore.setCommitLogUpperBound
    private final CommitLogPosition approximateCommitLogLowerBound = CommitLog.instance.getCurrentPosition();

    public int compareTo(Memtable that)
    {
        return this.approximateCommitLogLowerBound.compareTo(that.approximateCommitLogLowerBound);
    }

    public static final class LastCommitLogPosition extends CommitLogPosition
    {
        public LastCommitLogPosition(CommitLogPosition copy)
        {
            super(copy.segmentId, copy.position);
        }
    }

    // We index the memtable by PartitionPosition only for the purpose of being able
    // to select key range using Token.KeyBound. However put() ensures that we
    // actually only store DecoratedKey.
    private final List<TreeMap<PartitionPosition, AtomicBTreePartition>> partitions;
    public final ColumnFamilyStore cfs;
    private final long creationNano = System.nanoTime();
    private final boolean hasSplits;

    // The smallest timestamp for all partitions stored in this memtable
    private AtomicLong minTimestamp = new AtomicLong(Long.MAX_VALUE);

    // The smallest min local deletion time for all partitions in this memtable
    // This is actually an integer but to avoid duplicating the code to set the minimum
    // it is stored as a long, see updateIfMin
    private AtomicLong minLocalDeletionTime = new AtomicLong(Integer.MAX_VALUE);

    // Record the comparator of the CFS at the creation of the memtable. This
    // is only used when a user update the CF comparator, to know if the
    // memtable was created with the new or old comparator.
    public final ClusteringComparator initialComparator;

    private final ColumnsCollector columnsCollector;
    private final StatsCollector statsCollector = new StatsCollector();

    // only to be used by init(), to setup the very first memtable for the cfs
    public Memtable(AtomicReference<CommitLogPosition> commitLogLowerBound, ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.commitLogLowerBound = commitLogLowerBound;
        this.allocator = MEMORY_POOL.newAllocator();
        this.initialComparator = cfs.metadata().comparator;
        this.cfs.scheduleFlush();
        this.columnsCollector = new ColumnsCollector(cfs.metadata().regularAndStaticColumns());
        this.hasSplits = !cfs.hasSpecialHandlingForTPC;
        this.partitions = generatePartitionMaps();
    }

    // ONLY to be used for testing, to create a mock Memtable
    @VisibleForTesting
    public Memtable(TableMetadata metadata)
    {
        this.initialComparator = metadata.comparator;
        this.cfs = null;
        this.allocator = null;
        this.columnsCollector = new ColumnsCollector(metadata.regularAndStaticColumns());
        this.hasSplits = false;
        this.partitions = generatePartitionMaps();
    }

    private List<TreeMap<PartitionPosition, AtomicBTreePartition>> generatePartitionMaps()
    {
        if (!hasSplits)
            return Collections.singletonList(new TreeMap<>());

        int capacity = TPC.getNumCores();
        ArrayList<TreeMap<PartitionPosition, AtomicBTreePartition>> partitionMapContainer = new ArrayList<>(capacity);
        for (int i = 0; i < capacity; i++)
            partitionMapContainer.add(new TreeMap<>());
        return partitionMapContainer;
    }

    private Map<PartitionPosition, AtomicBTreePartition> getPartitionMapFor(DecoratedKey key)
    {
        if (!hasSplits)
            return partitions.get(0);

        int coreId = TPC.getCoreForKey(cfs.keyspace, key);
        assert coreId >= 0 && coreId < partitions.size() : "Received invalid core id: " + Integer.toString(coreId);

        return partitions.get(coreId);
    }

    public MemtableAllocator getAllocator()
    {
        return allocator;
    }

    public long getLiveDataSize()
    {
        return liveDataSize.get();
    }

    public long getOperations()
    {
        return currentOperations.get();
    }

    @VisibleForTesting
    public void setDiscarding(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        assert this.writeBarrier == null;
        this.commitLogUpperBound = commitLogUpperBound;
        this.writeBarrier = writeBarrier;
        allocator.setDiscarding();
    }

    void setDiscarded()
    {
        allocator.setDiscarded();
    }

    // decide if this memtable should take the write, or if it should go to the next memtable
    public boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition)
    {
        // if the barrier hasn't been set yet, then this memtable is still taking ALL writes
        OpOrder.Barrier barrier = this.writeBarrier;
        if (barrier == null)
            return true;
        // if the barrier has been set, but is in the past, we are definitely destined for a future memtable
        if (!barrier.isAfter(opGroup))
            return false;
        // if we aren't durable we are directed only by the barrier
        if (commitLogPosition == null)
            return true;
        while (true)
        {
            // otherwise we check if we are in the past/future wrt the CL boundary;
            // if the boundary hasn't been finalised yet, we simply update it to the max of
            // its current value and ours; if it HAS been finalised, we simply accept its judgement
            // this permits us to coordinate a safe boundary, as the boundary choice is made
            // atomically wrt our max() maintenance, so an operation cannot sneak into the past
            CommitLogPosition currentLast = commitLogUpperBound.get();
            if (currentLast instanceof LastCommitLogPosition)
                return currentLast.compareTo(commitLogPosition) >= 0;
            if (currentLast != null && currentLast.compareTo(commitLogPosition) >= 0)
                return true;
            if (commitLogUpperBound.compareAndSet(currentLast, commitLogPosition))
                return true;
        }
    }

    public CommitLogPosition getCommitLogLowerBound()
    {
        return commitLogLowerBound.get();
    }

    public CommitLogPosition getCommitLogUpperBound()
    {
        return commitLogUpperBound.get();
    }

    public boolean isLive()
    {
        return allocator.isLive();
    }

    // IMPORTANT: this method is not thread safe and should only be called when flushing, after the write barrier has
    // been issued and all writes to the memtable have completed
    public boolean isClean()
    {
        for (Map<PartitionPosition, AtomicBTreePartition> memtableSubrange : partitions)
        {
            if (!memtableSubrange.isEmpty())
                return false;
        }
        return true;
    }

    public boolean mayContainDataBefore(CommitLogPosition position)
    {
        return approximateCommitLogLowerBound.compareTo(position) < 0;
    }

    /**
     * @return true if this memtable is expired. Expiration time is determined by CF's memtable_flush_period_in_ms.
     */
    public boolean isExpired()
    {
        int period = cfs.metadata().params.memtableFlushPeriodInMs;
        return period > 0 && (System.nanoTime() - creationNano >= TimeUnit.MILLISECONDS.toNanos(period));
    }

    /**
     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
     * OpOrdering.
     *
     * commitLogSegmentPosition should only be null if this is a secondary index, in which case it is *expected* to be null
     */
    Single<Long> put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        final DecoratedKey key = update.partitionKey();
        final AtomicLong initialSize = new AtomicLong(0);

        return Single.fromCallable(() -> {
                        Map<PartitionPosition, AtomicBTreePartition> partitionMap = getPartitionMapFor(key);
                        AtomicBTreePartition previous = partitionMap.get(key);

                        assert writeBarrier == null || writeBarrier.isAfter(opGroup)
                            : String.format("Put called after write barrier\n%s", FBUtilities.Debug.getStackTrace());

                        if (previous == null)
                        {
                           final DecoratedKey cloneKey = allocator.clone(key, opGroup);
                           AtomicBTreePartition empty = new AtomicBTreePartition(cfs.metadata, cloneKey, allocator);
                           int overhead = (int) (cloneKey.getToken().getHeapSize() + ROW_OVERHEAD_HEAP_SIZE);
                           allocator.onHeap().allocate(overhead, opGroup);
                           initialSize.set(8);

                           // We'll add the columns later.
                           partitionMap.put(cloneKey, empty);
                           previous = empty;
                        }
                        return previous;
                   }).flatMap(previous -> previous.addAllWithSizeDelta(update, opGroup, indexer))
                     .map(p -> {
                       updateIfMin(minTimestamp, p.right.stats().minTimestamp);
                       updateIfMin(minLocalDeletionTime, p.right.stats().minLocalDeletionTime);
                       liveDataSize.addAndGet(initialSize.get() + p.left[0]);
                       columnsCollector.update(update.columns());
                       statsCollector.update(update.stats());
                       currentOperations.addAndGet(update.operationCount());

                       return p.left[1];
                   }).subscribeOn(TPC.getForKey(cfs.keyspace, key));
    }

    private void updateIfMin(AtomicLong val, long newVal)
    {
        long current = val.get();
        while(newVal < current)
        {
            if (val.compareAndSet(current, newVal))
                break;

            current = val.get();
        }
    }

    public int partitionCount()
    {
        return partitions.size();
    }

    public List<FlushRunnable> flushRunnables(LifecycleTransaction txn)
    {
        List<Range<Token>> localRanges = Range.sort(StorageService.instance.getLocalRanges(cfs.keyspace.getName()));

        if (!cfs.getPartitioner().splitter().isPresent() || localRanges.isEmpty())
            return Collections.singletonList(new FlushRunnable(txn));

        return createFlushRunnables(localRanges, txn);
    }

    private List<FlushRunnable> createFlushRunnables(List<Range<Token>> localRanges, LifecycleTransaction txn)
    {
        assert cfs.getPartitioner().splitter().isPresent();

        Directories.DataDirectory[] locations = cfs.getDirectories().getWriteableLocations();
        List<PartitionPosition> boundaries = StorageService.getDiskBoundaries(localRanges, cfs.getPartitioner(), locations);
        List<FlushRunnable> runnables = new ArrayList<>(boundaries.size());
        PartitionPosition rangeStart = cfs.getPartitioner().getMinimumToken().minKeyBound();
        try
        {
            for (int i = 0; i < boundaries.size(); i++)
            {
                PartitionPosition t = boundaries.get(i);
                runnables.add(new FlushRunnable(rangeStart, t, locations[i], txn));
                rangeStart = t;
            }
            return runnables;
        }
        catch (Throwable e)
        {
            throw Throwables.propagate(abortRunnables(runnables, e));
        }
    }

    public Throwable abortRunnables(List<FlushRunnable> runnables, Throwable t)
    {
        if (runnables != null)
            for (FlushRunnable runnable : runnables)
                t = runnable.writer.abort(t);
        return t;
    }

    public String toString()
    {
        return String.format("Memtable-%s@%s(%s serialized bytes, %s ops, %.0f%%/%.0f%% of on/off-heap limit)",
                             cfs.name, hashCode(), FBUtilities.prettyPrintMemory(liveDataSize.get()), currentOperations,
                             100 * allocator.onHeap().ownershipRatio(), 100 * allocator.offHeap().ownershipRatio());
    }

    public CsFlow<FlowableUnfilteredPartition> makePartitionIterator(final ColumnFilter columnFilter, final DataRange dataRange)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        boolean startIsMin = keyRange.left.isMinimum();
        boolean stopIsMin = keyRange.right.isMinimum();

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;

        ArrayList<Pair<Integer, Callable<List<UnfilteredRowIterator>>>> all = new ArrayList<>(partitions.size());

        for (int i = 0; i < partitions.size(); i++)
        {
            final int coreId = i;

            all.add(Pair.create(coreId, () -> {
                TreeMap<PartitionPosition, AtomicBTreePartition> memtableSubrange = partitions.get(coreId);
                SortedMap<PartitionPosition, AtomicBTreePartition> trimmedMemtableSubrange;

                if (startIsMin)
                   trimmedMemtableSubrange = stopIsMin ? memtableSubrange : memtableSubrange.headMap(keyRange.right, includeStop);
                else
                   trimmedMemtableSubrange = stopIsMin
                                             ? memtableSubrange.tailMap(keyRange.left, includeStart)
                                             : memtableSubrange.subMap(keyRange.left, includeStart, keyRange.right, includeStop);

                List<UnfilteredRowIterator> ret = new ArrayList<>(trimmedMemtableSubrange.size());
                for (Map.Entry<PartitionPosition, AtomicBTreePartition> entry : trimmedMemtableSubrange.entrySet())
                {
                    PartitionPosition position = entry.getKey();
                    assert position instanceof DecoratedKey;
                    DecoratedKey key = (DecoratedKey)position;
                    ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);
                    ret.add(filter.getUnfilteredRowIterator(columnFilter, entry.getValue()));
                }
                return ret;
            }));

            // For system tables we just use the first core
            if (!hasSplits)
                break;
        }

        return CsFlow.fromIterable(all)
                     .flatMap(pair -> Threads.evaluateOnCore(pair.right, pair.left))
                     .flatMap(list -> CsFlow.fromIterable(list))
                     .map(iterator -> FlowablePartitions.fromIterator(iterator, TPC.getForKey(cfs.keyspace, iterator.partitionKey())));
    }

    // IMPORTANT: this method is not thread safe and should only be called when flushing, after the write barrier has
    // been issued and all writes to the memtable have completed
    private List<SortedMap<PartitionPosition, AtomicBTreePartition>> getAllSortedPartitions()
    {
        List<SortedMap<PartitionPosition, AtomicBTreePartition>> ret = new ArrayList<>(partitions.size());
        for (TreeMap<PartitionPosition, AtomicBTreePartition> partitionSubrange : partitions)
            ret.add(partitionSubrange);

        return ret;
    }

    // IMPORTANT: this method is not thread safe and should only be called when flushing, after the write barrier has
    // been issued and all writes to the memtable have completed
    private List<SortedMap<PartitionPosition, AtomicBTreePartition>> getSortedSubrange(PartitionPosition from, PartitionPosition to)
    {
        List<SortedMap<PartitionPosition, AtomicBTreePartition>> ret = new ArrayList<>(partitions.size());

        for (TreeMap<PartitionPosition, AtomicBTreePartition> partitionSubrange : partitions)
        {
            SortedMap<PartitionPosition, AtomicBTreePartition> submap = partitionSubrange.subMap(from, to);
            // TreeMap returns these in normal sorted order
            if (!submap.isEmpty())
                ret.add(submap);
        }
        return ret;
    }

    public Partition getPartition(DecoratedKey key)
    {
        return getPartitionMapFor(key).get(key);
    }

    public long getMinTimestamp()
    {
        return minTimestamp.get();
    }

    public int getMinLocalDeletionTime()
    {
        return Math.toIntExact(minLocalDeletionTime.get());
    }

    /**
     * For testing only. Give this memtable too big a size to make it always fail flushing.
     */
    @VisibleForTesting
    public void makeUnflushable()
    {
        liveDataSize.addAndGet((long) 1024 * 1024 * 1024 * 1024 * 1024);
    }

    class FlushRunnable implements Callable<SSTableMultiWriter>
    {
        private final long estimatedSize;
        private final List<SortedMap<PartitionPosition, AtomicBTreePartition>> toFlush;

        private final boolean isBatchLogTable;
        private final SSTableMultiWriter writer;

        // keeping these to be able to log what we are actually flushing
        private final PartitionPosition from;
        private final PartitionPosition to;

        FlushRunnable(PartitionPosition from, PartitionPosition to, Directories.DataDirectory flushLocation, LifecycleTransaction txn)
        {
            this(getSortedSubrange(from, to), flushLocation, from, to, txn);
        }

        FlushRunnable(LifecycleTransaction txn)
        {
            this(getAllSortedPartitions(), null, null, null, txn);
        }

        private FlushRunnable(List<SortedMap<PartitionPosition, AtomicBTreePartition>> toFlush,
                              Directories.DataDirectory flushLocation, PartitionPosition from, PartitionPosition to, LifecycleTransaction txn)
        {
            this.toFlush = toFlush;
            this.from = from;
            this.to = to;
            long keySize = 0;

            for (Set<PartitionPosition> keySet : keysToFlush())
            {
                for (PartitionPosition key : keySet)
                {
                    // make sure we don't write nonsensical keys
                    assert key instanceof DecoratedKey;
                    keySize += ((DecoratedKey) key).getKey().remaining();
                }
            }

            estimatedSize = (long) ((keySize // index entries
                                    + keySize // keys in data file
                                    + liveDataSize.get()) // data
                                    * 1.2); // bloom filter and row index overhead

            this.isBatchLogTable = cfs.name.equals(SystemKeyspace.BATCHES) && cfs.keyspace.getName().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME);

            if (flushLocation == null)
            {
                writer = createFlushWriter(txn, cfs.newSSTableDescriptor(getDirectories().getWriteableLocationAsFile(estimatedSize)), columnsCollector.get(), statsCollector.get());
            }
            else
            {
                // Don't write to blacklisted disks
                File flushTableDir = getDirectories().getLocationForDisk(flushLocation);
                if (BlacklistedDirectories.isUnwritable(flushTableDir))
                    throw new FSWriteError(new IOException("SSTable flush dir has been blacklisted"), flushTableDir.getAbsolutePath());

                // // exclude directory if its total writeSize does not fit to data directory
                if (flushLocation.getAvailableSpace() < estimatedSize)
                    throw new FSDiskFullWriteError(new IOException("Insufficient disk space to write " + estimatedSize + " bytes"), "");

                writer = createFlushWriter(txn, cfs.newSSTableDescriptor(flushTableDir), columnsCollector.get(), statsCollector.get());
            }
        }

        private List<Set<PartitionPosition>> keysToFlush()
        {
            return toFlush.stream().map(m -> m.keySet()).collect(Collectors.toList());
        }

        private List<Iterator<AtomicBTreePartition>> partitionsToFlush()
        {
            return toFlush.stream().map(m -> m.values().iterator()).collect(Collectors.toList());
        }

        protected Directories getDirectories()
        {
            return cfs.getDirectories();
        }

        private void writeSortedContents()
        {
            logger.debug("Writing {}, flushed range = ({}, {}]", Memtable.this.toString(), from, to);

            List<Iterator<AtomicBTreePartition>> partitions = partitionsToFlush();

            // TPC: For OrderPreservingPartitioners we must merge across the memtable ranges
            // To ensure the partitions are written in the correct order.
            // As the ranges in TPC are based on long token and not the ordered token
            if (cfs.metadata().partitioner.preservesOrder() && partitions.size() > 1)
            {
                partitions = Collections.singletonList(MergeIterator.get(partitions, Comparator.comparing(AtomicBTreePartition::partitionKey), new Reducer<AtomicBTreePartition, AtomicBTreePartition>()
                {
                    AtomicBTreePartition reduced = null;

                    @Override
                    public boolean trivialReduceIsTrivial()
                    {
                        return true;
                    }

                    public void reduce(int idx, AtomicBTreePartition current)
                    {
                        reduced = current;
                    }

                    public AtomicBTreePartition getReduced()
                    {
                        return reduced;
                    }
                }));
            }

            // (we can't clear out the map as-we-go to free up memory,
            //  since the memtable is being used for queries in the "pending flush" category)
            for (Iterator<AtomicBTreePartition> partitionSet : partitions)
            {
                logger.debug("Processing sub-range");

                while (partitionSet.hasNext())
                {
                    AtomicBTreePartition partition  = partitionSet.next();

                    //logger.debug("Processing partition {}", partition.partitionKey());

                    // Each batchlog partition is a separate entry in the log. And for an entry, we only do 2
                    // operations: 1) we insert the entry and 2) we delete it. Further, BL data is strictly local,
                    // we don't need to preserve tombstones for repair. So if both operation are in this
                    // memtable (which will almost always be the case if there is no ongoing failure), we can
                    // just skip the entry (CASSANDRA-4667).
                    if (isBatchLogTable && !partition.partitionLevelDeletion().isLive() && partition.hasRows())
                        continue;

                    if (!partition.isEmpty())
                    {
                        try (UnfilteredRowIterator iter = partition.unfilteredIterator())
                        {
                            writer.append(iter);
                        }
                    }
                }
            }

            long bytesFlushed = writer.getFilePointer();
            logger.debug("Completed flushing {} ({}) for commitlog position {}",
                                                                              writer.getFilename(),
                                                                              FBUtilities.prettyPrintMemory(bytesFlushed),
                                                                              commitLogUpperBound);
            // Update the metrics
            cfs.metric.bytesFlushed.inc(bytesFlushed);
        }

        public SSTableMultiWriter createFlushWriter(LifecycleTransaction txn,
                                                    Descriptor descriptor,
                                                    RegularAndStaticColumns columns,
                                                    EncodingStats stats)
        {
            MetadataCollector sstableMetadataCollector = new MetadataCollector(cfs.metadata().comparator)
                    .commitLogIntervals(new IntervalSet<>(commitLogLowerBound.get(), commitLogUpperBound.get()));

            return cfs.createSSTableMultiWriter(descriptor,
                                                toFlush.size(),
                                                ActiveRepairService.UNREPAIRED_SSTABLE,
                                                ActiveRepairService.NO_PENDING_REPAIR,
                                                sstableMetadataCollector,
                                                new SerializationHeader(true, cfs.metadata(), columns, stats), txn);
        }

        @Override
        public SSTableMultiWriter call()
        {
            writeSortedContents();
            return writer;
        }
    }

    private static int estimateRowOverhead(final int count)
    {
        // calculate row overhead
        try (final OpOrder.Group group = new OpOrderSimple().start())
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
            allocator.setDiscarding();
            allocator.setDiscarded();
            return rowOverhead;
        }
    }

    private static class ColumnsCollector
    {
        private final HashMap<ColumnMetadata, AtomicBoolean> predefined = new HashMap<>();
        private final ConcurrentSkipListSet<ColumnMetadata> extra = new ConcurrentSkipListSet<>();
        ColumnsCollector(RegularAndStaticColumns columns)
        {
            for (ColumnMetadata def : columns.statics)
                predefined.put(def, new AtomicBoolean());
            for (ColumnMetadata def : columns.regulars)
                predefined.put(def, new AtomicBoolean());
        }

        public void update(RegularAndStaticColumns columns)
        {
            for (ColumnMetadata s : columns.statics)
                update(s);
            for (ColumnMetadata r : columns.regulars)
                update(r);
        }

        private void update(ColumnMetadata definition)
        {
            AtomicBoolean present = predefined.get(definition);
            if (present != null)
            {
                if (!present.get())
                    present.set(true);
            }
            else
            {
                extra.add(definition);
            }
        }

        public RegularAndStaticColumns get()
        {
            RegularAndStaticColumns.Builder builder = RegularAndStaticColumns.builder();
            for (Map.Entry<ColumnMetadata, AtomicBoolean> e : predefined.entrySet())
                if (e.getValue().get())
                    builder.add(e.getKey());
            return builder.addAll(extra).build();
        }
    }

    private static class StatsCollector
    {
        private final AtomicReference<EncodingStats> stats = new AtomicReference<>(EncodingStats.NO_STATS);

        public void update(EncodingStats newStats)
        {
            while (true)
            {
                EncodingStats current = stats.get();
                EncodingStats updated = current.mergeWith(newStats);
                if (stats.compareAndSet(current, updated))
                    return;
            }
        }

        public EncodingStats get()
        {
            return stats.get();
        }
    }
}
