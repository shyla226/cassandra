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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtableCleaner;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.NativePool;
import org.apache.cassandra.utils.memory.SlabPool;

public abstract class AbstractMemtable implements Memtable
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractMemtable.class);

    public static final MemtablePool MEMORY_POOL = AbstractMemtable.createMemtableAllocatorPool();

    protected final Owner owner;
    protected final MemtableAllocator allocator;
    protected final AtomicLong liveDataSize = new AtomicLong(0);
    protected final AtomicLong currentOperations = new AtomicLong(0);
    private final long creationNano = System.nanoTime();
    // The smallest timestamp for all partitions stored in this memtable
    protected final AtomicLong minTimestamp = new AtomicLong(Long.MAX_VALUE);

    // the write barrier for directing writes to this memtable or the next during a switch
    private volatile OpOrder.Barrier writeBarrier;
    // the precise upper bound of CommitLogPosition owned by this memtable
    private volatile AtomicReference<CommitLogPosition> commitLogUpperBound;
    // the precise lower bound of CommitLogPosition owned by this memtable; equal to its predecessor's commitLogUpperBound
    private AtomicReference<CommitLogPosition> commitLogLowerBound;

    // The approximate lower bound by this memtable; must be <= commitLogLowerBound once our predecessor
    // has been finalised, and this is enforced in the ColumnFamilyStore.setCommitLogUpperBound
    private final CommitLogPosition approximateCommitLogLowerBound = CommitLog.instance.getCurrentPosition();


    // Record the comparator of the CFS at the creation of the memtable. This
    // is only used when a user update the CF comparator, to know if the
    // memtable was created with the new or old comparator.
    public final ClusteringComparator initialComparator;

    protected TableMetadataRef metadata;

    protected final ColumnsCollector columnsCollector;
    protected final StatsCollector statsCollector = new StatsCollector();

    private static MemtablePool createMemtableAllocatorPool()
    {
        long heapLimit = DatabaseDescriptor.getMemtableHeapSpaceInMb() << 20;
        long offHeapLimit = DatabaseDescriptor.getMemtableOffheapSpaceInMb() << 20;
        float memtableCleanupThreshold = DatabaseDescriptor.getMemtableCleanupThreshold();
        MemtableCleaner cleaner = ColumnFamilyStore::flushLargestMemtable;
        switch (DatabaseDescriptor.getMemtableAllocationType())
        {
        case unslabbed_heap_buffers:
            logger.debug("Memtables allocating with on-heap buffers");
            return new HeapPool(heapLimit, memtableCleanupThreshold, cleaner);
        case heap_buffers:
            logger.debug("Memtables allocating with on-heap slabs");
            return new SlabPool(heapLimit, 0, memtableCleanupThreshold, cleaner);
        case offheap_buffers:
            logger.debug("Memtables allocating with off-heap buffers");
            return new SlabPool(heapLimit, offHeapLimit, memtableCleanupThreshold, cleaner);
        case offheap_objects:
            logger.debug("Memtables allocating with off-heap objects");
            return new NativePool(heapLimit, offHeapLimit, memtableCleanupThreshold, cleaner);
        default:
            throw new AssertionError();
        }
    }

    // only to be used by init(), to setup the very first memtable for the cfs
    public AbstractMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound,
                            TableMetadataRef metadataRef,
                            Owner owner)
    {
        this.metadata = metadataRef;
        this.commitLogLowerBound = commitLogLowerBound;
        this.owner = owner;
        this.allocator = MEMORY_POOL.newAllocator();
        this.initialComparator = metadata.get().comparator;
        this.columnsCollector = new ColumnsCollector(metadata.get().regularAndStaticColumns());
        scheduleFlush();
    }

    public CommitLogPosition getApproximateCommitLogLowerBound()
    {
        return approximateCommitLogLowerBound;
    }

    public MemtableAllocator getAllocator()
    {
        return allocator;
    }

    public TableMetadata metadata()
    {
        return metadata.get();
    }

    public boolean updateMetadata()
    {
        scheduleFlush();
        // If the CF comparator has changed, we need to change the memtable,
        // because the old one still aliases the previous comparator.
        return initialComparator == metadata().comparator;
    }

    protected abstract Factory factory();

    public long getLiveDataSize()
    {
        return liveDataSize.get();
    }

    public long getOperations()
    {
        return currentOperations.get();
    }

    public void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        assert this.writeBarrier == null;
        this.commitLogUpperBound = commitLogUpperBound;
        this.writeBarrier = writeBarrier;
        allocator.setDiscarding();
    }

    public void discard()
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

    public boolean mayContainDataBefore(CommitLogPosition position)
    {
        return approximateCommitLogLowerBound.compareTo(position) < 0;
    }

    public String toString()
    {
        return String.format("Memtable-%s@%s(%s serialized bytes, %s ops, %.0f%%/%.0f%% of on/off-heap limit)",
                             metadata.get().name,
                             hashCode(),
                             FBUtilities.prettyPrintMemory(liveDataSize.get()),
                             currentOperations,
                             100 * allocator.onHeap().ownershipRatio(),
                             100 * allocator.offHeap().ownershipRatio());
    }

    public long getMinTimestamp()
    {
        return minTimestamp.get();
    }

    protected void updateMin(AtomicLong minTracker, long newValue)
    {
        while (true)
        {
            long memtableMinTimestamp = minTracker.get();
            if (memtableMinTimestamp <= newValue)
                break;
            if (minTracker.compareAndSet(memtableMinTimestamp, newValue))
                break;
        }
    }

    /**
     * For testing only. Give this memtable too big a size to make it always fail flushing.
     */
    @VisibleForTesting
    public void makeUnflushable()
    {
        liveDataSize.addAndGet(1024L * 1024 * 1024 * 1024 * 1024);
    }

    public MemoryUsage getMemoryUsage()
    {
        MemoryUsage stats = new MemoryUsage(MEMORY_POOL);
        addMemoryUsageTo(stats);
        return stats;
    }

    public void addMemoryUsageTo(MemoryUsage stats)
    {
        stats.ownershipRatioOnHeap += getAllocator().onHeap().ownershipRatio();
        stats.ownershipRatioOffHeap += getAllocator().offHeap().ownershipRatio();
        stats.ownsOnHeap += getAllocator().onHeap().owns();
        stats.ownsOffHeap += getAllocator().offHeap().owns();
    }

    public void allocateExtraOnHeap(long additionalSpace, OpOrder.Group opGroup)
    {
        getAllocator().onHeap().allocate(additionalSpace, opGroup);
    }

    public void allocateExtraOffHeap(long additionalSpace, OpOrder.Group opGroup)
    {
        getAllocator().offHeap().allocate(additionalSpace, opGroup);
    }

    void scheduleFlush()
    {
        int period = metadata().params.memtableFlushPeriodInMs;
        if (period > 0)
        {
            logger.trace("scheduling flush in {} ms", period);
            WrappedRunnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow()
                {
                    if (isClean())
                    {
                        // if we're still clean, instead of swapping just reschedule a flush for later
                        scheduleFlush();
                    }
                    else
                    {
                        // we'll be rescheduled by the constructor of the Memtable.
                        owner.signalFlushRequired(AbstractMemtable.this);
                    }
                }
            };
            ScheduledExecutors.scheduledTasks.schedule(runnable, period, TimeUnit.MILLISECONDS);
        }
    }

    RegularAndStaticColumns columns()
    {
        return columnsCollector.get();
    }

    EncodingStats encodingStats()
    {
        return statsCollector.get();
    }

    protected static class ColumnsCollector
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

    protected static class StatsCollector
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

    protected abstract class AbstractFlushCollection<P extends Partition> implements FlushCollection<P>
    {
        public long dataSize()
        {
            return getLiveDataSize();
        }

        public CommitLogPosition commitLogLowerBound()
        {
            return AbstractMemtable.this.getCommitLogLowerBound();
        }

        public CommitLogPosition commitLogUpperBound()
        {
            return AbstractMemtable.this.getCommitLogUpperBound();
        }

        public EncodingStats encodingStats()
        {
            return AbstractMemtable.this.encodingStats();
        }

        public RegularAndStaticColumns columns()
        {
            return AbstractMemtable.this.columns();
        }
    }
}
