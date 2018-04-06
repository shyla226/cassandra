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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCBoundaries;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.flow.Flow;
import io.reactivex.Single;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.flow.FlowSource;
import org.apache.cassandra.utils.flow.FlowSubscriber;
import org.apache.cassandra.utils.flow.FlowSubscriptionRecipient;
import org.apache.cassandra.utils.flow.Threads;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.NativePool;
import org.apache.cassandra.utils.memory.SlabPool;
import org.apache.cassandra.utils.units.SizeUnit;

import static org.apache.cassandra.utils.Throwables.maybeFail;

public class Memtable implements Comparable<Memtable>
{
    private static final Logger logger = LoggerFactory.getLogger(Memtable.class);

    // Upon inserting a new update, if we haven't checked the memtable size in the last interval, then check to see if it has reached the maximum size at which it needs flushing
    public static final long FLUSH_CHECK_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(Integer.parseInt(System.getProperty("dse.memtable_flush_check_interval_millis",
                                                                                                                             "100")));

    public static final MemtablePool MEMORY_POOL = createMemtableAllocatorPool();

    private static MemtablePool createMemtableAllocatorPool()
    {
        long heapLimit = DatabaseDescriptor.getMemtableHeapSpaceInMb() << 20;
        long offHeapLimit = DatabaseDescriptor.getMemtableOffheapSpaceInMb() << 20;
        switch (DatabaseDescriptor.getMemtableAllocationType())
        {
            case unslabbed_heap_buffers:
                logger.debug("Memtables allocating with on heap buffers");
                return new HeapPool(heapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            case heap_buffers:
                logger.debug("Memtables allocating with on heap slabs");
                return new SlabPool(heapLimit, 0, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            case offheap_buffers:
                if (!FileUtils.isCleanerAvailable)
                {
                    throw new IllegalStateException("Could not free direct byte buffer: offheap_buffers is not a safe memtable_allocation_type without this ability, please adjust your config. This feature is only guaranteed to work on an Oracle JVM. Refusing to start.");
                }
                logger.debug("Memtables allocating with off heap buffers");
                return new SlabPool(heapLimit, offHeapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            case offheap_objects:
                logger.debug("Memtables allocating with off heap objects");
                return new NativePool(heapLimit, offHeapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
            default:
                throw new AssertionError();
        }
    }

    private static final int ROW_OVERHEAD_HEAP_SIZE = estimateRowOverhead(Integer.parseInt(System.getProperty("cassandra.memtable_row_overhead_computation_step", "100000")));

    private static final long MAX_HEAP_SIZE = DatabaseDescriptor.getMemtableMaxSingleHeapSizeInMb() <= 0
                                              ? Long.MAX_VALUE
                                              : SizeUnit.MEGABYTES.toBytes(DatabaseDescriptor.getMemtableMaxSingleHeapSizeInMb());
    private static final long MAX_OFFHEAP_SIZE = DatabaseDescriptor.getMemtableMaxSingleOffHeapSizeInMb() <= 0
                                                 ? Long.MAX_VALUE :
                                                 SizeUnit.MEGABYTES.toBytes(DatabaseDescriptor.getMemtableMaxSingleOffHeapSizeInMb());


    // the write barrier for directing writes to this memtable during a switch
    private volatile OpOrder.Barrier writeBarrier;
    // the precise upper bound of CommitLogPosition owned by this memtable
    private volatile AtomicReference<CommitLogPosition> commitLogUpperBound;
    // the precise lower bound of CommitLogPosition owned by this memtable; equal to its predecessor's commitLogUpperBound
    private AtomicReference<CommitLogPosition> commitLogLowerBound;

    // The approximate lower bound by this memtable; must be <= commitLogLowerBound once our predecessor
    // has been finalised, and this is enforced in the ColumnFamilyStore.setCommitLogUpperBound
    private final CommitLogPosition approximateCommitLogLowerBound = CommitLog.instance.getCurrentPosition();

    private final AtomicLong lastInsertSizeCheckTimeNanos;
    private final AtomicBoolean flushRequested = new AtomicBoolean(false);

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

    // The boundaries for the keyspace as they were calculated when the memtable is created.
    // The boundaries will be NONE for system keyspaces or if StorageService is not yet initialized.
    // The fact this is fixed for the duration of the memtable lifetime, guarantees we'll always pick the same core
    // for the a given key, even if we race with the StorageService initialization or with topology changes.
    private final TPCBoundaries boundaries;

    private final MemtableSubrange[] subranges;

    public final ColumnFamilyStore cfs;
    public final TableMetadata metadata;
    private final long creationNano = System.nanoTime();

    // Record the comparator of the CFS at the creation of the memtable. This
    // is only used when a user update the CF comparator, to know if the
    // memtable was created with the new or old comparator.
    public final ClusteringComparator initialComparator;

    // only to be used by init(), to setup the very first memtable for the cfs
    public Memtable(AtomicReference<CommitLogPosition> commitLogLowerBound, ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.commitLogLowerBound = commitLogLowerBound;
        this.initialComparator = cfs.metadata().comparator;
        this.metadata = cfs.metadata();
        this.cfs.scheduleFlush();
        this.boundaries = cfs.keyspace.getTPCBoundaries();
        this.subranges = generatePartitionSubranges(boundaries.supportedCores());
        this.lastInsertSizeCheckTimeNanos = new AtomicLong(ApproximateTime.nanoTime());
    }

    // ONLY to be used for testing, to create a mock Memtable
    @VisibleForTesting
    public Memtable(TableMetadata metadata)
    {
        this.initialComparator = metadata.comparator;
        this.metadata = metadata;
        this.cfs = null;
        this.boundaries = TPCBoundaries.NONE;
        this.subranges = generatePartitionSubranges(boundaries.supportedCores());
        this.lastInsertSizeCheckTimeNanos = new AtomicLong(ApproximateTime.nanoTime());
    }

    /**
     * Calculates a core for the given key. Boundaries are calculated on the memtable creation, therefore are
     * independent from topology changes, so this function will stably yield the correct core for the key over
     * the lifetime of the memtable.
     */
    private int getCoreFor(DecoratedKey key)
    {
        int coreId = TPC.getCoreForKey(boundaries, key);
        assert coreId >= 0 && coreId < subranges.length : "Received invalid core id : " + coreId;
        return coreId;
    }

    @VisibleForTesting
    TPCBoundaries getBoundaries()
    {
        return boundaries;
    }

    public void allocateExtraOnHeap(long additionalSpace)
    {
        subranges[0].allocator().onHeap().allocated(additionalSpace);
    }

    public void allocateExtraOffHeap(long additionalSpace)
    {
        subranges[0].allocator().offHeap().allocated(additionalSpace);
    }

    static public class MemoryUsage
    {
        public float ownershipRatioOnHeap = 0.0f;
        public float ownershipRatioOffHeap = 0.0f;
        public long ownsOnHeap = 0;
        public long ownsOffHeap = 0;
    }

    public MemoryUsage getMemoryUsage()
    {
        MemoryUsage stats = new MemoryUsage();
        addMemoryUsage(stats);
        return stats;
    }

    public void addMemoryUsage(MemoryUsage stats)
    {
        for (MemtableSubrange s : subranges)
        {
            stats.ownershipRatioOnHeap += s.allocator().onHeap().ownershipRatio();
            stats.ownershipRatioOffHeap += s.allocator().offHeap().ownershipRatio();
            stats.ownsOnHeap += s.allocator().onHeap().owns();
            stats.ownsOffHeap += s.allocator().offHeap().owns();
        }
    }

    @VisibleForTesting
    public void setDiscarding(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        assert this.writeBarrier == null;
        this.commitLogUpperBound = commitLogUpperBound;
        this.writeBarrier = writeBarrier;
        for (MemtableSubrange sr : subranges)
            sr.allocator().setDiscarding();
    }

    void setDiscarded()
    {
        for (MemtableSubrange sr : subranges)
            sr.allocator().setDiscarded();
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
        return subranges[0].allocator().isLive();
    }

    // IMPORTANT: this method is not thread safe and should only be called when flushing, after the write barrier has
    // been issued and all writes to the memtable have completed
    public boolean isClean()
    {
        // Force memtable switch if boundaries have changed (DB-939)
        return isEmpty() && boundaries.equals(cfs.keyspace.getTPCBoundaries());
    }

    public boolean isEmpty()
    {
        for (MemtableSubrange memtableSubrange : subranges)
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
        int period = metadata.params.memtableFlushPeriodInMs;
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

        final int coreId = getCoreFor(key);
        final MemtableSubrange partitionMap = subranges[coreId];
        Callable<Single<Long>> write = () ->
        {
            AtomicBTreePartition previous = partitionMap.get(key, MemtableSubrange.DataAccess.UNSAFE);
            assert TPC.getCoreId() == coreId;
            //if (logger.isTraceEnabled())
            //    logger.trace("Adding key {} to memtable", key);

            assert writeBarrier == null || writeBarrier.isAfter(opGroup)
            : String.format("Put called after write barrier\n%s", FBUtilities.Debug.getStackTrace());

            MemtableAllocator allocator = partitionMap.allocator();

            if (previous == null)
            {
                final DecoratedKey cloneKey = allocator.clone(key);
                AtomicBTreePartition empty = new AtomicBTreePartition(cfs.metadata, cloneKey);
                int overhead = (int) (cloneKey.getToken().getHeapSize() + ROW_OVERHEAD_HEAP_SIZE);
                allocator.onHeap().allocated(overhead);
                partitionMap.updateLiveDataSize(8);

                // We'll add the columns later.
                partitionMap.put(cloneKey, empty);
                previous = empty;
            }

            return previous.addAllWithSizeDelta(update, indexer, allocator)
                           .map(p ->
                                {
                                    partitionMap.update(p.update, p.dataSize);
                                    maybeFlushIfMaxSizeReached();
                                    return p.colUpdateTimeDelta;
                                });
        };

        // We should block the write if there is no space in the memtable memory to let flushes catch up. For
        // efficiency's sake we can't know the exact size this mutation requires; instead we let the limits slip a
        // little and only block _after_ the used space is above the limit.
        return partitionMap.allocator().whenBelowLimits(write, opGroup, TPC.getForCore(coreId), TPCTaskType.WRITE_MEMTABLE);
    }

    /**
     * Technically we should scatter gather on all the core threads because the size in following calls are not
     * using volatile variables, but for metrics purpose this should be good enough.
     */
    public long getLiveDataSize()
    {
        long total = 0L;
        for (MemtableSubrange subrange : subranges)
            total += subrange.liveDataSize();
        return total;
    }

    public long getOperations()
    {
        long total = 0L;
        for (MemtableSubrange subrange : subranges)
            total += subrange.currentOperations();
        return total;
    }

    public int partitionCount()
    {
        int total = 0;
        for (MemtableSubrange subrange : subranges)
            total += subrange.size(); // TODO: treemap.size?
        return total;
    }

    public long getMinTimestamp()
    {
        long min = Long.MAX_VALUE;
        for (MemtableSubrange subrange : subranges)
            min =  Long.min(min, subrange.minTimestamp());
        return min;
    }

    public int getMinLocalDeletionTime()
    {
        int min = Integer.MAX_VALUE;
        for (MemtableSubrange subrange : subranges)
            min = Integer.min(min, subrange.minLocalDeletionTime());
        return min;
    }

    public String toString()
    {
        MemoryUsage usage = getMemoryUsage();
        return String.format("Memtable-%s@%s(%s serialized bytes, %s ops, %.0f%%/%.0f%% of on/off-heap limit)",
                             cfs.name, hashCode(), FBUtilities.prettyPrintMemory(getLiveDataSize()), getOperations(),
                             100 * usage.ownershipRatioOnHeap, 100 * usage.ownershipRatioOffHeap);
    }

    /**
     * Used for re-ordering partitions when the memtable partitioner is not the same as the TPC partitioner.
     * This is a trivial reducer that expects only one partition at a time, since partitions are unique
     * in the memtable.
     */
    private static class MergeReducer<P> extends Reducer<P, P>
    {
        P reduced;

        public boolean trivialReduceIsTrivial()
        {
            return true;
        }

        public void onKeyChange()
        {
            reduced = null;
        }

        public void reduce(int idx, P current)
        {
            assert reduced == null : "partitions are unique so this should have been called only once";
            reduced = current;
        }

        public P getReduced()
        {
            return reduced;
        }
    }

    /**
     * Return the partitions on a specific core.
     *
     * @param coreId - we query the partitions for this core only
     * @param filterStart exclude partitions before the start (the left bound), only true for the first range unless startIsMin
     * @param filterStop exclude partitions after the end (the right bound), only true for the last range unless stopIsMin
     */
    private Flow<FlowableUnfilteredPartition> getCorePartitions(int coreId, ColumnFilter columnFilter, DataRange dataRange, boolean filterStart, boolean filterStop)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;

        return new FlowSource<FlowableUnfilteredPartition>()
        {
            private Iterator<AtomicBTreePartition> currentPartitions;

            private Iterator<AtomicBTreePartition> getTrimmedSubRange()
            {
                MemtableSubrange memtableSubrange = subranges[coreId];

                if (filterStart && filterStop) // this can happen when there is only one range
                {   // return the data between start and stop
                    return memtableSubrange.subIterator(keyRange.left, includeStart, keyRange.right, includeStop, MemtableSubrange.DataAccess.ON_HEAP);
                }
                else if (filterStart)
                { // return the data from the start onwards
                    return memtableSubrange.tailIterator(keyRange.left, includeStart, MemtableSubrange.DataAccess.ON_HEAP);
                }
                else if (filterStop)
                { // return the data up to the stop
                    return memtableSubrange.headIterator(keyRange.right, includeStop, MemtableSubrange.DataAccess.ON_HEAP);
                }
                else
                {
                    // return all the data
                    return memtableSubrange.iterator(MemtableSubrange.DataAccess.ON_HEAP);
                }
            }

            public void requestFirst(FlowSubscriber<FlowableUnfilteredPartition> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
            {
                subscribe(subscriber, subscriptionRecipient);
                currentPartitions = getTrimmedSubRange();

                if (!currentPartitions.hasNext())
                    subscriber.onComplete();
                else
                    requestNext();
            }

            public void requestNext()
            {
                // if we get here we know that currentPartitions has at least one item because of the check in requestFirst() and
                // at the end of this method, when there are no more items we either call onComplete or onFinal respectively
                AtomicBTreePartition partition = currentPartitions.next();
                DecoratedKey key = partition.partitionKey();
                ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);
                FlowableUnfilteredPartition fup = filter.getFlowableUnfilteredPartition(columnFilter.withPartitionColumnsVerified(partition.columns()), partition);

                if (currentPartitions.hasNext())
                    subscriber.onNext(fup);
                else
                    subscriber.onFinal(fup);
            }

            public void close() throws Exception
            {
                // nothing to close
            }
        };
    }

    /**
     *  Return the partitions in the given data range. Gather all partitions from all threads and merge them. This method should be
     *  called only when the partitions in the memtable are not in the same order as the order according to which they are split
     *  into sub-ranges by the default (TPC) partitioner.
     *
     *  This is a very expensive operation.
     */
    private Flow<FlowableUnfilteredPartition> getMergedPartitions(final ColumnFilter columnFilter, final DataRange dataRange)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        boolean startIsMin = keyRange.left.isMinimum();
        boolean stopIsMin = keyRange.right.isMinimum();

        List<Flow<FlowableUnfilteredPartition>> partitionFlows = new ArrayList<>(subranges.length);
        for (int i = 0; i < subranges.length; i++)
            partitionFlows.add(getCorePartitions(i, columnFilter, dataRange, !startIsMin, !stopIsMin));

        return Flow.merge(partitionFlows,
                          Comparator.comparing(FlowableUnfilteredPartition::partitionKey),
                          new MergeReducer<>());
    }

    /**
     * Return the partitions in the given data range assuming that they are already ordered, which is the case when the
     * memtable partitioner is the same as the global (TPC) partitioner. We only query the sub-ranges within the query bounds,
     * so as to avoid jumping on all core threads, unlike what's done in {@link #getMergedPartitions(ColumnFilter, DataRange)}.
     */
    private Flow<FlowableUnfilteredPartition> getSequentialPartitions(final ColumnFilter columnFilter, final DataRange dataRange)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        boolean startIsMin = keyRange.left.isMinimum();
        boolean stopIsMin = keyRange.right.isMinimum();

        int start = startIsMin ? 0 : TPC.getCoreForBound(boundaries, keyRange.left); // the first core to query
        int end = stopIsMin ? subranges.length - 1 :  TPC.getCoreForBound(boundaries, keyRange.right); // the last core to query

        if (start == end) // avoid the concat if we are only querying one core
            return getCorePartitions(start, columnFilter, dataRange, !startIsMin, !stopIsMin);
        else
            return Flow.concat(() -> new AbstractIterator<Flow<FlowableUnfilteredPartition>>()
            {
                int current = start;
                protected Flow<FlowableUnfilteredPartition> computeNext()
                {
                    int i = current++;
                    return i <= end
                           ? getCorePartitions(i, columnFilter, dataRange, i == start && !startIsMin, i == end && !stopIsMin)
                           : endOfData();
                }
            });
    }

    /**
     * Return a flow of partitions in the memtable, that match the given data range and with the column filter applied
     * to each partitioner. Depending on the memtable partitioner, this method dispatches to either
     * {@link #getMergedPartitions(ColumnFilter, DataRange)} or {@link #getSequentialPartitions(ColumnFilter, DataRange)}.
     *
     * If the memtable partitioner is not the same as the global (TPC) partitioner, then this operation becomes
     * very expensive, especially on machines with many cores. This is the case for index tables or system tables,
     * we shouldn't have too many range queries that fall into this category.
     */
    public Flow<FlowableUnfilteredPartition> makePartitionFlow(final ColumnFilter columnFilter, final DataRange dataRange)
    {
        if (!metadata.partitioner.equals(DatabaseDescriptor.getPartitioner()))
            return getMergedPartitions(columnFilter, dataRange);
        else
            return getSequentialPartitions(columnFilter, dataRange);
    }

    List<FlushRunnable> createFlushRunnables(LifecycleTransaction txn)
    {
        return createFlushRunnables(txn, cfs.getDiskBoundaries());
    }

    @VisibleForTesting
    List<FlushRunnable> createFlushRunnables(LifecycleTransaction txn, DiskBoundaries diskBoundaries)
    {
        List<PartitionPosition> boundaries = diskBoundaries.positions;
        List<Directories.DataDirectory> locations = diskBoundaries.directories;
        if (boundaries == null)
            return Collections.singletonList(new FlushRunnable(this.subranges, null, null, null, txn));

        List<FlushRunnable> runnables = new ArrayList<>(boundaries.size());
        PartitionPosition rangeStart = cfs.getPartitioner().getMinimumToken().minKeyBound();
        try
        {
            for (int i = 0; i < boundaries.size(); i++)
            {
                PartitionPosition t = boundaries.get(i);
                runnables.add(new FlushRunnable(this.subranges, locations.get(i), rangeStart, t, txn));
                rangeStart = t;
            }
            return runnables;
        }
        catch (Throwable e)
        {
            for (Memtable.FlushRunnable runnable : runnables)
                e = runnable.abort(e);

            throw Throwables.propagate(e);
        }
    }

    public Flow<Partition> getPartition(DecoratedKey key)
    {
        int coreId = getCoreFor(key);
        return Flow.just(subranges[coreId].get(key, MemtableSubrange.DataAccess.ON_HEAP));
    }

    /**
     * For testing only. Give this memtable too big a size to make it always fail flushing.
     */
    @VisibleForTesting
    public void makeUnflushable() throws Exception
    {
        for (int i = 0; i < subranges.length; i++)
        {
            final MemtableSubrange subrange = subranges[i];
            Threads.evaluateOnCore(() -> {
                subrange.makeUnflushable();
                return null;
            }, i, TPCTaskType.UNKNOWN).reduceBlocking(null, (a, b) -> null);
        }
    }

    /**
     * The valid states for {@link FlushRunnable} writers. The thread writing the contents
     * will transition from IDLE -> RUNNING and back to IDLE when finished using the writer
     * or from ABORTING -> ABORTED if another thread has transitioned from RUNNING -> ABORTING.
     * We can also transition directly from IDLE -> ABORTED. Whichever threads transitions
     * to ABORTED is responsible to abort the writer.
     */
    @VisibleForTesting
    enum FlushRunnableWriterState
    {
        IDLE, // the runnable is idle, either not yet started or completed but with the writer waiting to be committed
        RUNNING, // the runnable is executing, therefore the writer cannot be aborted or else a SEGV may ensue
        ABORTING, // an abort request has been issued, this only happens if abort() is called whilst RUNNING
        ABORTED  // the writer has been aborted, no resources will be leaked
    }

    class FlushRunnable implements Callable<SSTableMultiWriter>
    {
        private final long estimatedSize;
        private final List<Iterator<AtomicBTreePartition>> toFlush;

        private final boolean isBatchLogTable;
        private final SSTableMultiWriter writer;

        // keeping these to be able to log what we are actually flushing
        private final PartitionPosition from;
        private final PartitionPosition to;

        private final int keyCount;

        private final AtomicReference<FlushRunnableWriterState> state;

        private FlushRunnable(MemtableSubrange[] subranges,
                              Directories.DataDirectory flushLocation,
                              PartitionPosition from,
                              PartitionPosition to,
                              LifecycleTransaction txn)
        {
            this.toFlush = new ArrayList<>(subranges.length);
            this.from = from;
            this.to = to;
            long keySize = 0;
            int keyCount = 0;
            long liveDataSize = 0;

            MemtableSubrange.ColumnsCollector columnsCollector = new MemtableSubrange.ColumnsCollector(metadata.regularAndStaticColumns());
            EncodingStats stats = EncodingStats.NO_STATS;

            for (MemtableSubrange partitionSubrange : subranges)
            {
                Pair<Iterator<PartitionPosition>, Iterator<AtomicBTreePartition>> p = partitionSubrange.iterators(from, to, MemtableSubrange.DataAccess.UNSAFE);
                Iterator<PartitionPosition> keyIt = p.left;
                Iterator<AtomicBTreePartition> partitionIt = p.right;

                if (!keyIt.hasNext())
                    continue;

                while(keyIt.hasNext())
                {
                    PartitionPosition key = keyIt.next();
                    // make sure we don't write nonsensical keys
                    assert key instanceof DecoratedKey;
                    keySize += ((DecoratedKey) key).getKey().remaining();
                    ++keyCount;
                }

                this.toFlush.add(partitionIt);
                // this is not correct because it doesn't take into account the keys between from and to but at the moment
                // the partition sub range only stores a global live data size counter. It is also better than it was before
                // where each FlushRunnable was using the entire memtable live data size for all sub ranges, regardless of
                // which sub ranges were flushed
                liveDataSize += partitionSubrange.liveDataSize();

                columnsCollector.merge(partitionSubrange.columnsCollector());
                stats = stats.mergeWith(partitionSubrange.encodingStats());
            }

            this.keyCount = keyCount;
            estimatedSize = (long) ((keyCount * 8 // partition index
                                    + keySize // keys in data file
                                    + liveDataSize) // data
                                    * 1.2); // bloom filter and row index overhead

            this.isBatchLogTable = cfs.name.equals(SystemKeyspace.BATCHES) && cfs.keyspace.getName().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME);

            if (flushLocation == null)
            {
                writer = createFlushWriter(txn, cfs.newSSTableDescriptor(getDirectories().getWriteableLocationAsFile(estimatedSize)), columnsCollector.get(), stats);
            }
            else
            {
                // Don't write to blacklisted disks
                File flushTableDir = getDirectories().getLocationForDisk(flushLocation);
                if (BlacklistedDirectories.isUnwritable(flushTableDir))
                    throw new FSWriteError(new IOException("SSTable flush dir has been blacklisted"), flushTableDir.getAbsolutePath());

                // exclude directory if its total writeSize does not fit to data directory
                if (flushLocation.getAvailableSpace() < estimatedSize)
                    throw new FSDiskFullWriteError(new IOException("Insufficient disk space to write " + estimatedSize + " bytes"));

                writer = createFlushWriter(txn, cfs.newSSTableDescriptor(flushTableDir), columnsCollector.get(), stats);
            }

            state = new AtomicReference<>(FlushRunnableWriterState.IDLE);
        }

        protected Directories getDirectories()
        {
            return cfs.getDirectories();
        }

        private void writeSortedContents()
        {
            if (!state.compareAndSet(FlushRunnableWriterState.IDLE, FlushRunnableWriterState.RUNNING))
            {
                logger.debug("Failed to write {}, flushed range = ({}, {}], state: {}",
                             Memtable.this.toString(), from, to, state);
                return;
            }

            logger.debug("Writing {}, flushed range = ({}, {}], state: {}, partitioner {}",
                         Memtable.this.toString(), from, to, state, metadata.partitioner);

            try
            {
                List<Iterator<AtomicBTreePartition>> partitions = toFlush;

                // TPC: If the partitioner is not the same as the one used for TPC (e.g. indexes)  we must merge
                // across the memtable ranges to ensure that the subranges are written in the correct order.
                if (!metadata.partitioner.equals(DatabaseDescriptor.getPartitioner()) && partitions.size() > 1)
                {
                    partitions = Collections.singletonList(MergeIterator.get(partitions,
                                                                             Comparator.comparing(Partition::partitionKey),
                                                                             new MergeReducer<>()));
                }

                // (we can't clear out the map as-we-go to free up memory,
                //  since the memtable is being used for queries in the "pending flush" category)
                for (Iterator<AtomicBTreePartition> partitionSet : partitions)
                {
                    if (state.get() == FlushRunnableWriterState.ABORTING)
                        break;

                    while (partitionSet.hasNext())
                    {
                        if (state.get() == FlushRunnableWriterState.ABORTING)
                            break;

                        AtomicBTreePartition partition = partitionSet.next();

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
                            catch (Throwable t)
                            {
                                logger.debug("Error when flushing: {}/{}", t.getClass().getName(), t.getMessage());
                                Throwables.propagate(t);
                            }
                        }
                    }
                }
            }
            finally
            {
                while (true)
                {
                    if (state.compareAndSet(FlushRunnableWriterState.RUNNING, FlushRunnableWriterState.IDLE))
                    {
                        long bytesFlushed = writer.getFilePointer();
                        logger.debug("Completed flushing {} ({}) for commitlog position {}",
                                     writer.getFilename(),
                                     FBUtilities.prettyPrintMemory(bytesFlushed),
                                     commitLogUpperBound);
                        // Update the metrics
                        cfs.metric.bytesFlushed.inc(bytesFlushed);
                        break;
                    }
                    else if (state.compareAndSet(FlushRunnableWriterState.ABORTING, FlushRunnableWriterState.ABORTED))
                    {
                        logger.debug("Flushing of {} aborted", writer.getFilename());
                        maybeFail(writer.abort(null));
                        break;
                    }
                }
            }
        }

        public Throwable abort(Throwable throwable)
        {
            while (true)
            {
                if (state.compareAndSet(FlushRunnableWriterState.IDLE, FlushRunnableWriterState.ABORTED))
                {
                    logger.debug("Flushing of {} aborted", writer.getFilename());
                    return writer.abort(throwable);
                }
                else if (state.compareAndSet(FlushRunnableWriterState.RUNNING, FlushRunnableWriterState.ABORTING))
                {
                    // thread currently executing writeSortedContents() will take care of aborting and throw any exceptions
                    return throwable;
                }
            }
        }

        @VisibleForTesting
        FlushRunnableWriterState state()
        {
            return state.get();
        }

        public SSTableMultiWriter createFlushWriter(LifecycleTransaction txn,
                                                    Descriptor descriptor,
                                                    RegularAndStaticColumns columns,
                                                    EncodingStats stats)
        {
            MetadataCollector sstableMetadataCollector = new MetadataCollector(metadata.comparator)
                    .commitLogIntervals(new IntervalSet<>(commitLogLowerBound.get(), commitLogUpperBound.get()));

            return cfs.createSSTableMultiWriter(descriptor,
                                                keyCount,
                                                ActiveRepairService.UNREPAIRED_SSTABLE,
                                                ActiveRepairService.NO_PENDING_REPAIR,
                                                sstableMetadataCollector,
                                                new SerializationHeader(true, metadata, columns, stats), txn);
        }

        @Override
        public SSTableMultiWriter call()
        {
            writeSortedContents();
            return writer;
        }
    }

    private void maybeFlushIfMaxSizeReached()
    {
        if (MAX_HEAP_SIZE == Long.MAX_VALUE && MAX_OFFHEAP_SIZE == Long.MAX_VALUE)
            return;

        long lastCheckedTime = lastInsertSizeCheckTimeNanos.get();
        long now = ApproximateTime.nanoTime();
        if (now - lastCheckedTime < Memtable.FLUSH_CHECK_INTERVAL_NANOS)
            return;

        if (!lastInsertSizeCheckTimeNanos.compareAndSet(lastCheckedTime, now))
            return;

        long sizeUsedHeap = 0;
        long sizeUsedOffheap = 0;
        for (int i = 0; i < subranges.length; i++)
        {
            sizeUsedHeap += subranges[i].allocator().onHeap().owns();
            sizeUsedOffheap += subranges[i].allocator().offHeap().owns();
        }

        if (sizeUsedHeap < MAX_HEAP_SIZE && sizeUsedOffheap < MAX_OFFHEAP_SIZE)
            return;

        if (flushRequested.compareAndSet(false, true))
        {
            logger.debug("Flushing {} due to max size reached: heap {} >= {} OR off-heap {} >= {}",
                         cfs,
                         FBUtilities.prettyPrintMemory(sizeUsedHeap), FBUtilities.prettyPrintMemory(MAX_HEAP_SIZE),
                         FBUtilities.prettyPrintMemory(sizeUsedOffheap), FBUtilities.prettyPrintMemory(MAX_OFFHEAP_SIZE));
            TPC.ioScheduler().execute(() -> cfs.switchMemtableIfCurrent(Memtable.this), TPCTaskType.FLUSH_MEMTABLE);
        }
    }

    private MemtableSubrange[] generatePartitionSubranges(int splits)
    {
        if (splits == 1)
            return new MemtableSubrange[] { new MemtableSubrange(0, metadata) };

        MemtableSubrange[] partitionMapContainer = new MemtableSubrange[splits];
        for (int i = 0; i < splits; i++)
            partitionMapContainer[i] = new MemtableSubrange(i, cfs.metadata());

        return partitionMapContainer;
    }

    private static int estimateRowOverhead(final int count)
    {
        // calculate row overhead
        int rowOverhead;
        MemtableAllocator allocator = MEMORY_POOL.newAllocator(-1);
        ConcurrentNavigableMap<PartitionPosition, Object> partitions = new ConcurrentSkipListMap<>();
        final Object val = new Object();
        for (int i = 0 ; i < count ; i++)
            partitions.put(allocator.clone(new BufferDecoratedKey(new LongToken(i), ByteBufferUtil.EMPTY_BYTE_BUFFER)), val);
        double avgSize = ObjectSizes.measureDeep(partitions) / (double) count;
        rowOverhead = (int) ((avgSize - Math.floor(avgSize)) < 0.05 ? Math.floor(avgSize) : Math.ceil(avgSize));
        rowOverhead -= ObjectSizes.measureDeep(new LongToken(0));
        rowOverhead += AtomicBTreePartition.EMPTY_SIZE;
        allocator.setDiscarding();
        allocator.setDiscarded();
        return rowOverhead;
    }
}
