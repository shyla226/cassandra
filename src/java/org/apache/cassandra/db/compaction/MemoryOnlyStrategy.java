/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.compaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.mos.MemoryOnlyStatus;
import org.apache.cassandra.db.mos.MemoryOnlyStrategyOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static com.google.common.collect.Iterables.filter;

public class MemoryOnlyStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(MemoryOnlyStrategy.class);

    // The sstables that have been locked, or attempted to lock
    private final Set<SSTableReader> sstables = Collections.newSetFromMap(new ConcurrentHashMap<>());

    protected MemoryOnlyStrategyOptions options;

    public MemoryOnlyStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);

        // Any caching is waste of valuable memory when using MemoryOnlyStrategy.
        // Avoid duplicate warnings for 2i tables.
        if (!cfs.isIndex() && cfs.metadata.get().params.caching.cacheKeys())
            logger.error("Table {}.{} uses MemoryOnlyStrategy and should have `caching` set to {'keys':'NONE', 'rows_per_partition':'NONE'} (got {})",
                         cfs.keyspace.getName(),
                         cfs.getTableName(),
                         cfs.metadata.get().params.caching);

        this.options = new MemoryOnlyStrategyOptions(options);
    }

    // TLDR: we probably want to compact the biggest sstables
    //
    // The number one metric we care about for MemoryOnlyStrategy is latency and especially the worst case
    // latency.  We want the tables to be fast.
    //
    // In the worst case (select *) Cassandra must read every column of every sstable for a given partition
    // and our average latency will be proportional to the total size of the partition distributed across the
    // tables (in other words, the most important metric is the total size of the data, not the number of sstables.
    // More memory consumed is also more likely to result disk IO, which is Very Bad for latency).
    //
    // So a reasonable metric to optimise is the compaction efficiency: the amount of memory we expect
    // to save versus the amount of time the compaction will take (this isn't perfect, because we may end up
    // in a 'worse' state than before, but the problem is already messy enough computationally).  If we
    // have a potential compaction of tables with size S and expected resulting size C, our efficiency is
    // therefore (S - C) / S.
    //
    // What we would like to do is compute all possible subsets of our sstables, run
    // SSTableReader.estimateCompactionGain on them, and pick the ones with the highest efficiency.  Unfortunately
    // there are 2^n possible subsets.  I looked for a dynamic programming algorithm there without much success,
    // but perhaps there is something I am missing.  However, compacting the biggest sstables is a reasonable
    // metric if we assume the keys are randomly distributed.
    //
    // Imagine we have a known working set size W and wish to add another table of size X to our potential
    // compaction {S -> C}.  The new size C will be:
    // C + X - (CX/W), with the final term being the savings of the compaction where both keys are present,
    // that is the overlap between the sstables containing the randomly distributed keys.
    //
    // Recalling our initial efficiency formula of (S - C) / S, and replacing S with S+X and C with C + X - (CX/W),
    // this means the new efficiency will be: (S+X - (C+X-(CX/W)))/(S+X) = (S - C + CX/W)/(S+X)
    //
    // Differentiating this with respect to X with Wolfram Alpha
    // http://www.wolframalpha.com/input/?i=differentiate+%28S+-+C+%2B+CX%2FW%29%2F%28S%2BX%29
    // gives us a nice polynomial, and the important part is the numerator (the denominator is always positive):
    // CW - CX + WX  This is trivially greater than zero (the workload size W will be greater than the previous
    // expected compaction size C) so the efficiency monotonically increasing with respect to the size of X.
    //
    // APOLLO-342: see this comment for a further discussion on this reasoning:
    // https://github.com/riptano/apollo/pull/153#discussion_r110808864
    private static final Comparator<SSTableReader> maxSizeComparator = new Comparator<SSTableReader>()
    {
        public int compare(SSTableReader o1, SSTableReader o2)
        {
            // flipped, so we sort in descending order
            return Long.compare(o2.onDiskLength(), o1.onDiskLength());
        }
    };

    /**
     * The current status of compactions in progress for
     * a given table.
     */
    private final static class CompactionProgress
    {
        /**
         * The minimum number of bytes not yet scanned amongst all the compactions
         * currently in progress for this table.
         */
        long minRemainingBytes = Long.MAX_VALUE;

        /**
         * The number of active compactions currently in progress for this table.
         */
        int numActiveCompactions = 0;


        private CompactionProgress(TableMetadata metadata)
        {
            for (CompactionInfo.Holder compaction : CompactionMetrics.getCompactions())
            {
                CompactionInfo info = compaction.getCompactionInfo();

                if (null != info.getTableMetadata() && info.getTableMetadata().id.equals(metadata.id))
                {
                    numActiveCompactions += 1;
                    minRemainingBytes = Math.min(info.getTotal() - info.getCompleted(), minRemainingBytes);
                }
            }
        }
    }

    private List<SSTableReader> getNextBackgroundSSTables(final int gcBefore)
    {
        if (Iterables.isEmpty(cfs.getSSTables(SSTableSet.LIVE)))
            return Collections.emptyList();

        // If we have multiple compactions running, we can run into two threads competing for the largest sstables:
        // Thread A will continually find new tables and compact them into a big table, and so will Thread B,
        // but A will never be able to compact B's tables because B will always be compacting them, and vice
        // versa.  So we end up with twice the data size (bad).  Therefore, we should not start any compactions that
        // will finish after any currently running compaction for this table by limiting the max compaction size below.
        CompactionProgress progress = new CompactionProgress(cfs.metadata());

        if (progress.numActiveCompactions >= options.maxActiveCompactions)
            return Collections.emptyList();

        // The multiplication by 15/16 is providing a bit of slack room so that the second compaction finishes first
        // to avoid the problem mentioned in the comment above when multiple compactions are running.
        // Even if this is not the case, the point is that as the currently running compaction(s) approach completion,
        // it will be impossible to start new ones, and eventually there will come a point where no compactions are running.
        // Then the system can start a new Great Compaction with all the tables.
        long maxCompactionSize = (progress.minRemainingBytes / 16) * 15;

        Iterator<SSTableReader> it = filter(cfs.getUncompactingSSTables(), (sstable) -> sstables.contains(sstable) && !sstable.isMarkedSuspect()).iterator();
        if (!it.hasNext())
            return Collections.emptyList();

        List<SSTableReader> cands = Lists.newArrayList(it);
        if (cands.size() < options.minCompactionThreshold)
            return Collections.emptyList();

        Collections.sort(cands, maxSizeComparator);

        long totalSize = 0;
        int candIndex = 0, maxCandIndex = Math.min(options.maxCompactionThreshold, cands.size());

        // note: this loop works because subList takes an exclusive index
        for (; candIndex < maxCandIndex; candIndex++)
        {
            if ((totalSize += cands.get(candIndex).onDiskLength()) > maxCompactionSize)
                break;
        }

        return cands.subList(0, candIndex);
    }

    @SuppressWarnings("resource")
    public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        while (true)
        {
            List<SSTableReader> toCompact = getNextBackgroundSSTables(gcBefore);

            if (toCompact.isEmpty())
                return null;

            LifecycleTransaction transaction = cfs.getTracker().tryModify(toCompact, OperationType.COMPACTION);
            if (transaction != null)
                return new CompactionTask(cfs, transaction, gcBefore);
        }
    }

    @SuppressWarnings("resource")
    public Collection<AbstractCompactionTask> getMaximalTask(final int gcBefore, boolean splitOutput)
    {
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables);
        if (Iterables.isEmpty(filteredSSTables))
            return null;
        LifecycleTransaction txn = cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
        if (txn == null)
            return null;
        return Arrays.<AbstractCompactionTask>asList(new CompactionTask(cfs, txn, gcBefore));
    }

    @SuppressWarnings("resource")
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        LifecycleTransaction transaction = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (transaction == null)
        {
            logger.debug("Unable to mark {} for compaction; probably a background compaction got to it first.  You can" +
                         " disable background compactions temporarily if this is a problem",
                         sstables);
            return null;
        }

        return new CompactionTask(cfs, transaction, gcBefore).setUserDefined(true);
    }

    public int getEstimatedRemainingTasks()
    {
        int theSize = sstables.size();
        if (theSize >= cfs.getMinimumCompactionThreshold())
            return (theSize / cfs.getMaximumCompactionThreshold()) + 1;
        return 0;
    }

    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = MemoryOnlyStrategyOptions.validateOptions(options, uncheckedOptions);
        return uncheckedOptions;
    }

    @Override
    public boolean shouldDefragment()
    {
        return false;
    }

    @Override
    public void addSSTable(SSTableReader added)
    {
        if (sstables.contains(added))
            return;

        added.lock(MemoryOnlyStatus.instance);
        sstables.add(added);
    }

    @Override
    public void removeSSTable(SSTableReader removed)
    {
        if (!sstables.contains(removed))
            return;

        removed.unlock(MemoryOnlyStatus.instance);
        sstables.remove(removed);
    }

    /**
     * Returns the sstables managed by this strategy instance
     */
    public Set<SSTableReader> getSSTables()
    {
        return ImmutableSet.copyOf(sstables);
    }

    public String toString()
    {
        return String.format("MemoryOnlyStrategy[%s/%s/%s]",
                             cfs.getMinimumCompactionThreshold(),
                             cfs.getMaximumCompactionThreshold(),
                             MemoryOnlyStatus.instance.getMaxAvailableBytes());
    }
}
