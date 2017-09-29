/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.mos;


import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionStrategyManager;
import org.apache.cassandra.db.compaction.MemoryOnlyStrategy;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.memory.MemoryUtil;

public class MemoryOnlyStatus implements MemoryOnlyStatusMXBean
{
    private static final Logger logger = LoggerFactory.getLogger(MemoryOnlyStatus.class);

    //Singleton
    public static final MemoryOnlyStatus instance = new MemoryOnlyStatus();

    /**
     * The maximum amount of memory that can be locked.
     */
    private final long maxAvailableBytes;

    /**
     * The total amount of memory currently locked, for all tables
     */
    private final AtomicLong totalLockedBytes = new AtomicLong(0);

    /**
     * The total amount of memory that should have been locked but couldn't for whatever reason.
     */
    private final AtomicLong totalFailedBytes = new AtomicLong(0);

    private MemoryOnlyStatus()
    {
        this(DatabaseDescriptor.getMaxMemoryToLockBytes());
    }

    @VisibleForTesting
    MemoryOnlyStatus(long maxAvailableBytes)
    {
        this.maxAvailableBytes = maxAvailableBytes;

        logger.debug("Max available memory to lock in RAM: {} kbytes", maxAvailableBytes >> 10); // divide by 1024
    }

    public long getMaxAvailableBytes()
    {
        return maxAvailableBytes;
    }

    public MemoryLockedBuffer lock(MappedByteBuffer buffer)
    {
        long address = MemoryUtil.getAddress(buffer);

        long length = buffer.capacity();
        long lengthRoundedTo4k = FBUtilities.align(length, 4096);
        long newSize = totalLockedBytes.addAndGet(lengthRoundedTo4k);
        if (newSize <= maxAvailableBytes)
        {
            logger.debug("Lock buffer address: {} length: {}", address, length);
            if (NativeLibrary.tryMlock(address, length))
            {
                // DSP-14169/APOLLO-1052: checking buffer.isLoaded() returns true and judging by the API docs, it's merely a hint
                // and not a guarantee. Therefore we just call buffer.load() directly to make sure a byte is read and brought into memory.
                buffer.load();
                return MemoryLockedBuffer.succeeded(address, lengthRoundedTo4k);
            }
        }
        else
        {
            logger.warn("Buffer address: {} length: {} could not be locked.  Size" +
                        "limit ({}) reached. After locking size would be: {}",
                        address,
                        length,
                        maxAvailableBytes,
                        newSize);
        }

        totalLockedBytes.addAndGet(-lengthRoundedTo4k);
        totalFailedBytes.addAndGet(lengthRoundedTo4k);
        return MemoryLockedBuffer.failed(address, lengthRoundedTo4k);
    }

    public void unlock(MemoryLockedBuffer buffer)
    {
        // A/342 CODE-REVIEW:: in the original DSE implementation, we don't unlock the memory explicitly,
        // we just update the totals. However, when this method gets called, the sstable is most likely
        // still referenced, see LogTransaction.obsoleted() and Tracker.notifyDeleting(). Therefore,
        // the buffers in MmappedRegions haven't been released yet (FileUtils.clean) and therefore we
        // may lock more memory than the hard limit specified by the user in the yaml config

        if (buffer.succeeded)
            totalLockedBytes.addAndGet(-buffer.amount);
        else
            totalFailedBytes.addAndGet(-buffer.amount);
    }

    /**
     * This method is called by the file handle if it receives a request to lock memory without any
     * memory mapped regions. In the initial MemoryOnlyStrategy implementation ported from DSE,
     * the quantity of memory failed to lock is incremented if the user selects MemoryOnlyStrategy
     * for a table but then sets disk access mode to standard.
     *
     * @param sizeBytes - typically the length on disk
     */
    public void reportFailedAttemptedLocking(long sizeBytes)
    {
        totalFailedBytes.addAndGet(sizeBytes);
    }

    /**
     * See comment in {@link MemoryOnlyStatus#reportFailedAttemptedLocking}.
     *
     *  @param sizeBytes - typically the length on disk, must match what passed to reportFailedAttemptedLocking()
     */
    public void clearFailedAttemptedLocking(long sizeBytes)
    {
        totalFailedBytes.addAndGet(-sizeBytes);
    }

    private static boolean isMemoryOnly(ColumnFamilyStore cfs)
    {
        List<List<AbstractCompactionStrategy>> strategies = cfs.getCompactionStrategyManager().getStrategies();
        return strategies.size() > 0 && strategies.get(0).size() > 0 && strategies.get(0).get(0) instanceof MemoryOnlyStrategy;
    }

    @Nullable
    private TableInfo maybeCreateTableInfo(ColumnFamilyStore cfs)
    {
        if (!isMemoryOnly(cfs))
            return null; // existing behavior ported from DSE

        // unless there is a race, the tables returned from the compaction strategies should match
        // the live sstables. However I'm not sure we can just look at the live sstables because
        // we need to make sure that the sstables have been locked, which is currently done by the strategy
        List<MemoryLockedBuffer> lockedBuffers = new ArrayList<>(cfs.getLiveSSTables().size());

        CompactionStrategyManager csm = cfs.getCompactionStrategyManager();
        for (List<AbstractCompactionStrategy> strategyList : csm.getStrategies())
        {
            for (AbstractCompactionStrategy strategy : strategyList)
            {
                if (strategy instanceof MemoryOnlyStrategy)
                {
                    // important - do not call SSTableReader.getLockedMemory() unless you are sure
                    // Memory locking was requested for this CFS, else it might incorrectly report
                    // that memory could not be locked if the table files are not memory mapped
                    for (SSTableReader ssTableReader : ((MemoryOnlyStrategy)strategy).getSSTables())
                        Iterables.addAll(lockedBuffers, ssTableReader.getLockedMemory());
                }
            }
        }

        return createTableInfo(cfs.keyspace.getName(), cfs.getTableName(), lockedBuffers, maxAvailableBytes);
    }

    // Pull out some information about any tables which aren't affected by the metered flusher
    public List<TableInfo> getMemoryOnlyTableInformation()
    {
        List<TableInfo> tableInfos = new ArrayList<>();

        // there is currently a problem in that when tables are dropped we may fail to
        // capture them here but their locked memory is still accounted for in the totals
        // until the sstable notifications have been received by the strategy
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            TableInfo ti = maybeCreateTableInfo(cfs);
            if (ti != null)
                tableInfos.add(ti);
        }

        return tableInfos;
    }

    // For one table only
    public TableInfo getMemoryOnlyTableInformation(String ks, String cf)
    {
        Keyspace keyspace = Schema.instance.getKeyspaceInstance(ks);
        if (keyspace == null)
            throw new IllegalArgumentException(String.format("Keyspace %s does not exist.", ks));

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cf);
        TableInfo ti = maybeCreateTableInfo(cfs);
        if (ti == null)
            throw new IllegalArgumentException(String.format("Keyspace %s Table %s is not using MemoryOnlyStrategy.", ks, cf));

        return ti;
    }

    public TotalInfo getMemoryOnlyTotals()
    {
        return new TotalInfo(totalLockedBytes.get(),
                             totalFailedBytes.get(),
                             maxAvailableBytes);
    }

    public double getMemoryOnlyPercentUsed()
    {
        if (maxAvailableBytes > 0)
        {
            return ((double) totalLockedBytes.get()) / maxAvailableBytes;
        }
        else
        {
            return 0;
        }
    }

    private static TableInfo createTableInfo(String ks, String cf, List<MemoryLockedBuffer> buffers, long maxMemoryToLock)
    {
        return new TableInfo(ks, cf, buffers.stream().map(MemoryLockedBuffer::locked).reduce(0L, Long::sum),
                             buffers.stream().map(MemoryLockedBuffer::notLocked).reduce(0L, Long::sum),
                             maxMemoryToLock);
    }
}
