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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.Throwables.*;

/**
 * The unified compaction strategy is described in this design document:
 *
 * TODO: link to design doc or SEP
 */
public class UnifiedCompactionStrategy extends AbstractCompactionStrategy.WithAggregates
{
    private static final Logger logger = LoggerFactory.getLogger(UnifiedCompactionStrategy.class);

    private final Set<SSTableReader> sstables;

    /** The controller can be changed at any time to change the strategy behavior */
    private Controller controller;

    public UnifiedCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        this(cfs, options, Controller.fromOptions(cfs, options));
    }

    public UnifiedCompactionStrategy(ColumnFamilyStore cfs, Controller controller)
    {
        this(cfs, new HashMap<>(), controller);
    }

    public UnifiedCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options, Controller controller)
    {
        super(cfs, options);

        this.sstables = ConcurrentHashMap.newKeySet();
        this.controller = controller;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        return Controller.validateOptions(AbstractCompactionStrategy.validateOptions(options));
    }

    @Override
    public void startup()
    {
        perform(() -> super.startup(),
                           () -> controller.startup(this, ScheduledExecutors.scheduledTasks));
    }

    @Override
    public void shutdown()
    {
        perform(() -> super.shutdown(),
                           controller::shutdown);
    }

    @Override
    protected CompactionAggregate getNextBackgroundAggregate(int gcBefore)
    {
        controller.onStrategyBackgroundTaskRequest();

        Collection<CompactionAggregate> pending = getAggregates();
        backgroundCompactions.setPending(pending);

        return pending.isEmpty() ? null : pending.stream().filter(aggr -> !aggr.isEmpty()).findFirst().orElse(null);
    }

    @Override
    protected AbstractCompactionTask createCompactionTask(final int gcBefore, LifecycleTransaction txn, CompactionAggregate compaction)
    {
        return CompactionTask.forCompaction(this, txn, gcBefore);
    }

    private Collection<CompactionAggregate> getAggregates()
    {
        Collection<Bucket> buckets = getBuckets();
        List<CompactionAggregate> aggregates = new ArrayList<>(buckets.size());
        for (Bucket bucket : buckets)
            aggregates.add(bucket.getCompactionAggregate());

        return aggregates;
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public void replaceSSTables(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        for (SSTableReader remove : removed)
            sstables.remove(remove);

        sstables.addAll(added);
    }

    @Override
    public void addSSTable(SSTableReader sstable)
    {
        sstables.add(sstable);
    }

    @Override
    public void addSSTables(Iterable<SSTableReader> added)
    {
        for (SSTableReader sstable : added)
            sstables.add(sstable);
    }

    @Override
    void removeDeadSSTables()
    {
        removeDeadSSTables(sstables);
    }

    @Override
    public void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }

    @Override
    public Set<SSTableReader> getSSTables()
    {
        return sstables;
    }

    @VisibleForTesting
    public int getW(int index)
    {
        return controller.getW(index);
    }

    @VisibleForTesting
    public Controller getController()
    {
        return controller;
    }

    @VisibleForTesting
    List<Bucket> getBuckets()
    {
        // Copy and sort the eligible sstables by size from smallest to largest
        List<SSTableReader> candidates = new ArrayList<>();
        Iterables.addAll(candidates, nonSuspectAndNotIn(sstables, dataTracker.getCompacting()));
        candidates.sort(SSTableReader.sizeComparator);

        List<Bucket> buckets = new ArrayList<>(4);
        int index = 0;
        Bucket bucket = new Bucket(controller, index, 0);
        for (SSTableReader candidate : candidates)
        {
            if (candidate.onDiskLength() < bucket.max)
            {
                bucket.add(candidate);
                continue;
            }

            bucket.sort();
            buckets.add(bucket); // add even if empty

            while (true)
            {
                bucket = new Bucket(controller, ++index, bucket.max);
                if (candidate.onDiskLength() < bucket.max)
                {
                    bucket.add(candidate);
                    break;
                }
                else
                {
                    buckets.add(bucket); // add the empty bucket
                }
            }
        }

        if (!bucket.sstables.isEmpty())
        {
            bucket.sort();
            buckets.add(bucket);
        }

        logger.debug("Found {} buckets for {}.{}", buckets.size(), cfs.getKeyspaceName(), cfs.getTableName());
        return buckets;
    }

    @Override
    public String toString()
    {
        return String.format("Unified strategy %s", getMetadata());
    }

    /**
     * A bucket: index, sstables and some properties.
     */
    static class Bucket
    {
        final List<SSTableReader> sstables;
        final int index;
        final double survivalFactor;
        final int W; // scaling factor used to calculate F and T
        final int F; // fanout factor between buckets
        final int T; // num. sorted runs that trigger a compaction
        final long min; // min size of sstables for this bucket
        final long max; //max size of sstables for this bucket

        Bucket(Controller controller, int index, long minSize)
        {
            this.sstables = new ArrayList<>(1);
            this.index = index;
            this.survivalFactor = controller.getSurvivalFactor();
            this.W = controller.getW(index);
            this.F = W < 0 ? 2 - W : 2 + W; // see formula in design doc
            this.T = W < 0 ? 2 : F; // see formula in design doc
            this.min = minSize;
            this.max = (long) Math.floor((minSize == 0 ? controller.getMinSSTableSizeBytes() : minSize) * F * controller.getSurvivalFactor());
        }

        void add(SSTableReader sstable)
        {
            this.sstables.add(sstable);
        }

        void sort()
        {
            if (W >= 0)
                sstables.sort(Comparator.comparing(SSTableReader::getMaxTimestamp));
            else
                sstables.sort(Comparator.comparing(SSTableReader::onDiskLength).reversed());

            logger.debug("Bucket: {}", this);
        }

        /**
         * Return the compaction aggregate
         */
        CompactionAggregate getCompactionAggregate()
        {
            if (sstables.size() < T)
                return CompactionAggregate.createUnified(sstables, CompactionPick.EMPTY, ImmutableList.of(), this);

            CompactionPick selected = null;
            List<CompactionPick> pending = new ArrayList<>();

            // The maximum number of sstables to compact together. When W is < 0 then T is 2 and normally each
            // bucket should not have more than 2 sstables unless we've just switched into this configuration or
            // compactions are falling behind, in this case it makes sense to compact all the sstables together
            // up to F at most, beyond F we risk skipping the next bucket.
            int max = W >= 0 ? T : Math.max(T, Math.min(F, sstables.size()));

            int i = 0;
            while ((sstables.size() - i) >= max)
            {
                if (selected == null)
                    selected = CompactionPick.create(index, sstables.subList(i, i + max));
                else
                    pending.add(CompactionPick.create(index, sstables.subList(i, i + max)));

                i += T;
            }

            return CompactionAggregate.createUnified(sstables, selected, pending, this);
        }

        int WA()
        {
            return W >= 0 ? 1 : F; // W >= 0 => tiered compaction, <0 => leveled compaction
        }

        int RA()
        {
            return W >= 0 ? T : 1; // W >= 0 => tiered compaction, <0 => leveled compaction
        }

        @Override
        public String toString()
        {
            return String.format("W: %d, T: %d, F: %d, index: %d, min: %s, max %s, %d sstables",
                                 W, T, F, index, FBUtilities.prettyPrintMemory(min), FBUtilities.prettyPrintMemory(max), sstables.size());
        }
    }
}
