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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.ShardedMultiWriter;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.Throwables.*;

/**
 * The unified compaction strategy is described in this design document:
 *
 * TODO: link to design doc or SEP
 */
public class UnifiedCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(UnifiedCompactionStrategy.class);

    /**
     * An equivalence class is a function that takes an sstable and returns a property of this
     * sstable that defines the equivalence class. For example, the repair status or disk
     * index may define equivalence classes. See the concrete equivalence classes below.
     *
     * @param <T> The type of the property that defines the equivalence class
     */
    private interface EquivalenceClass<T> extends Comparator<SSTableReader> {

        @Override
        int compare(SSTableReader a, SSTableReader b);

        /** Return a name that describes the equivalence class */
        String name(SSTableReader ssTableReader);
    };

    /**
     * Group sstables by their repair state: repaired, unrepaired, pending repair with a specific UUID (one group per pending repair).
     */
    private static final class RepairEquivClass implements EquivalenceClass<Boolean>
    {
        @Override
        public int compare(SSTableReader a, SSTableReader b)
        {
            // This is the same as name(apply(a)).compareTo(name(apply(b)))
            int af = a.isRepaired() ? 1 : !a.isPendingRepair() ? 2 : 0;
            int bf = b.isRepaired() ? 1 : !b.isPendingRepair() ? 2 : 0;
            if (af != 0 || bf != 0)
                return Integer.compare(af, bf);
            return a.pendingRepair().compareTo(b.pendingRepair());
        }

        @Override
        public String name(SSTableReader ssTableReader)
        {
            if (ssTableReader.isRepaired())
                return "repaired";
            else if (!ssTableReader.isPendingRepair())
                return "unrepaired";
            else
                return "pending_repair_" + ssTableReader.pendingRepair();
        }
    }

    /**
     * Group sstables by their disk index.
     */
    private static final class DiskIndexEquivClass implements EquivalenceClass<Integer>
    {
        private final DiskBoundaries boundaries;

        DiskIndexEquivClass(DiskBoundaries boundaries)
        {
            this.boundaries = boundaries;
        }

        @Override
        public int compare(SSTableReader a, SSTableReader b)
        {
            return Integer.compare(boundaries.getDiskIndexFromKey(a), boundaries.getDiskIndexFromKey(b));
        }

        @Override
        public String name(SSTableReader ssTableReader)
        {
            return "disk_" + boundaries.getDiskIndexFromKey(ssTableReader);
        }
    }

    /**
     * Group sstables by their shard. If the data set size is larger than the shared size in the compaction options,
     * then we create an equivalence class based by shard. Each sstable ends up in a shard based on their first
     * key. Each shard is calculated by splitting the local token ranges into a number of shards, where the number
     * of shards is calculated as ceil(data_size / shard size);
     */
    private static final class ShardEquivClass implements EquivalenceClass<Integer>
    {
        private final List<PartitionPosition> boundaries;

        ShardEquivClass(List<PartitionPosition> boundaries)
        {
            this.boundaries = boundaries;
        }

        private int getPositionIndex(DecoratedKey key)
        {
            int pos = Collections.binarySearch(boundaries, key);
            assert pos < 0; // boundaries are .minkeybound and .maxkeybound so they should never be equal
            return -pos - 1;
        }

        @Override
        public int compare(SSTableReader a, SSTableReader b)
        {
            return Integer.compare(getPositionIndex(a.getFirst()), getPositionIndex(b.getFirst()));
        }

        @Override
        public String name(SSTableReader ssTableReader)
        {
            return "shard_" + getPositionIndex(ssTableReader.getFirst());
        }
    }

    // TODO - missing equivalence classes:

    // - by time window to emulate TWCS, in this case only the latest shard will use size based buckets, the older
    //   shards will get major compactions

    /** The controller can be changed at any time to change the strategy behavior */
    private Controller controller;

    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, Map<String, String> options)
    {
        this(factory, options, Controller.fromOptions(factory.getCfs(), options));
    }

    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, Controller controller)
    {
        this(factory, new HashMap<>(), controller);
    }

    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, Map<String, String> options, Controller controller)
    {
        super(factory, options);
        this.controller = controller;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        return Controller.validateOptions(CompactionStrategyOptions.validateOptions(options));
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
    public synchronized Collection<AbstractCompactionTask> getNextBackgroundTasks(int gcBefore)
    {
        // TODO - we should perhaps consider executing this code less frequently than legacy strategies
        // since it's more expensive, and we should therefore prevent a second concurrent thread from executing at all

        controller.onStrategyBackgroundTaskRequest();

        Collection<CompactionAggregate> aggregates = getNextAggregates();
        Collection<AbstractCompactionTask> tasks = new ArrayList<>(aggregates.size());

        for (CompactionAggregate aggregate : aggregates)
        {
            LifecycleTransaction transaction = dataTracker.tryModify(aggregate.getSelected().sstables, OperationType.COMPACTION);
            if (transaction != null)
            {
                backgroundCompactions.setSubmitted(transaction.opId(), aggregate);
                tasks.add(createCompactionTask(transaction, gcBefore));
            }
            else
            {
                // Because this code is synchronized it should never be the case that another thread is marking the same sstables as compacting
                logger.error("Failed to submit compaction {} because a transaction could not be created, this is not expected and should be reported", aggregate);
            }
        }

        return tasks;
    }

    /**
     * Create the sstable writer used for flushing.
     *
     * @return either a normal sstable writer, if there are no shards, or a sharded sstable writer that will
     *         create multiple sstables if a shard has a sufficiently large sstable.
     */
    @Override
    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                       long keyCount,
                                                       long repairedAt,
                                                       UUID pendingRepair,
                                                       boolean isTransient,
                                                       MetadataCollector meta,
                                                       SerializationHeader header,
                                                       Collection<Index.Group> indexGroups,
                                                       LifecycleNewTracker lifecycleNewTracker)
    {
        if (controller.getNumShards() <= 1)
            return super.createSSTableMultiWriter(descriptor,
                                                  keyCount,
                                                  repairedAt,
                                                  pendingRepair,
                                                  isTransient,
                                                  meta,
                                                  header,
                                                  indexGroups,
                                                  lifecycleNewTracker);

        return new ShardedMultiWriter(cfs,
                                      descriptor,
                                      keyCount,
                                      repairedAt,
                                      pendingRepair,
                                      isTransient,
                                      meta,
                                      header,
                                      indexGroups,
                                      lifecycleNewTracker,
                                      controller.getMinSstableSizeBytes(),
                                      getBoundaries());
    }

    /**
     * Create the task that in turns creates the sstable writer used for compaction.
     *
     * @return either a normal compaction task, if there are no shards, or a sharded compaction task that in turn will
     * create a sharded compaction writer.
     */
    private CompactionTask createCompactionTask(LifecycleTransaction transaction, int gcBefore)
    {
        if (controller.getNumShards() <= 1)
            return new CompactionTask(cfs, transaction, gcBefore, false, this);

        return new UnifiedCompactionTask(cfs, this, transaction, gcBefore, controller.getMinSstableSizeBytes(), getBoundaries());
    }

    @VisibleForTesting
    List<PartitionPosition> getBoundaries()
    {
        return cfs.getLocalRanges().split(controller.getNumShards());
    }

    private Collection<CompactionAggregate> getNextAggregates()
    {
        controller.onStrategyBackgroundTaskRequest();

        Map<Shard, List<Bucket>> shards = getShardsWithBuckets();
        List<CompactionAggregate> pending = new ArrayList<>(shards.size() * 4); // assumes 4 buckets per shard
        List<CompactionAggregate> toSubmit = new ArrayList<>(shards.size());

        for (Map.Entry<Shard, List<Bucket>> entry : shards.entrySet())
        {
            boolean submitted = false;
            for (Bucket bucket : entry.getValue())
            {
                CompactionAggregate aggregate = bucket.getCompactionAggregate(entry.getKey());
                pending.add(aggregate);

                if (!submitted && !aggregate.isEmpty())
                {
                    toSubmit.add(aggregate);
                    submitted = true;
                }
            }
        }

        // all aggregates are set as pending
        backgroundCompactions.setPending(pending);

        // but only the first non empty bucket of each shard will be submitted
        return toSubmit;
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return backgroundCompactions.getEstimatedRemainingTasks();
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public Set<SSTableReader> getSSTables()
    {
        return dataTracker.getLiveSSTables();
    }

    @Override
    public boolean supportsEarlyOpen()
    {
        // Hopefully we won't need to support early open since shards will keep sstables in the order of GBs,
        // we should probably test though
        return false;
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

    /**
     * @return the list of equivalence classes according to which we should group sstables before applying the buckets.
     */
    private Collection<EquivalenceClass<?>> makeEquivalenceClasses()
    {
        ImmutableList.Builder<EquivalenceClass<?>> ret = ImmutableList.builderWithExpectedSize(3);

        ret.add(new RepairEquivClass());

        DiskBoundaries diskBoundaries = cfs.getDiskBoundaries();
        if (diskBoundaries.getNumBoundaries() > 1)
            ret.add(new DiskIndexEquivClass(diskBoundaries));

        if (controller.getNumShards() > 1)
            ret.add(new ShardEquivClass(getBoundaries()));

        return ret.build();
    }

    /**
     * Group candidate sstables (non suspect and not already compacting) into one or more compaction shards. Each
     * compaction shard is obtained by comparing using a compound comparator for the equivalence classes.
     *
     * @return a list of shards, where each shard contains sstables that are eligible for being compacted together
     */
    @VisibleForTesting
    Collection<Shard> getCompactionShards()
    {
        // add all non suspect and non compacting sstables to the candidates, no-early open so all live sstables
        // should be canonical but review what happens when we switch over from a legacy strategy that supports early open

        final Collection<EquivalenceClass<?>> equivalenceClasses = makeEquivalenceClasses();
        final Ordering<SSTableReader> comparator = Ordering.compound(equivalenceClasses);
        Map<SSTableReader, Shard> tables = new TreeMap<>(comparator);
        for (SSTableReader table : nonSuspectAndNotIn(dataTracker.getLiveSSTables(), dataTracker.getCompacting()))
            tables.computeIfAbsent(table, t -> new Shard(equivalenceClasses, comparator))
                  .add(table);

        return tables.values();
    }

    @VisibleForTesting
    Map<Shard, List<Bucket>> getShardsWithBuckets()
    {
        Collection<Shard> shards = getCompactionShards();
        Map<Shard, List<Bucket>> ret = new LinkedHashMap<>(); // should preserve the order of shards

        for (Shard shard : shards)
        {
            List<Bucket> buckets = new ArrayList<>(4);
            shard.sstables.sort(SSTableReader.sizeComparator);

            int index = 0;
            Bucket bucket = new Bucket(controller, index, 0);
            for (SSTableReader candidate : shard.sstables)
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

            if (!buckets.isEmpty())
                ret.put(shard, buckets);

            logger.debug("Shard {} has {} buckets", shard, buckets.size());
        }

        logger.debug("Found {} shards with buckets for {}.{}", ret.size(), cfs.getKeyspaceName(), cfs.getTableName());
        return ret;
    }

    public TableMetadata getMetadata()
    {
        return cfs.metadata();
    }

    /**
     * A compaction shard contains the list of sstables that belong to this shard and the list
     * of equivalence classes that were applied in order to compose this shard, as well as the value
     * of the group of the shard and the previous ones.
     */
    @VisibleForTesting
    final static class Shard implements Comparable<Shard>
    {
        final List<SSTableReader> sstables;
        final List<EquivalenceClass<?>> equivalenceClasses;
        final Comparator<SSTableReader> comparator;

        Shard(Iterable<EquivalenceClass<?>> equivalenceClasses, Comparator<SSTableReader> comparator)
        {
            this.sstables = new ArrayList<>();
            this.equivalenceClasses = ImmutableList.copyOf(equivalenceClasses);
            this.comparator = comparator;
        }

        void add(SSTableReader ssTableReader)
        {
            sstables.add(ssTableReader);
        }

        public String name()
        {
            SSTableReader t = sstables.get(0);
            return String.join("-", Iterables.transform(equivalenceClasses, e -> e.name(t)));
        }

        @Override
        public int compareTo(Shard o)
        {
            return comparator.compare(this.sstables.get(0), o.sstables.get(0));
        }

        @Override
        public String toString()
        {
            return String.format("%s, %d sstables", name(), sstables.size());
        }
    }

    @Override
    public String toString()
    {
        return String.format("Unified strategy %s", getMetadata());
    }

    /**
     * A bucket: index, sstables and some properties.
     */
    @VisibleForTesting
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
            this.max = (long) Math.floor((minSize == 0 ? controller.getMinSstableSizeBytes() : minSize) * F * controller.getSurvivalFactor());
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
        CompactionAggregate getCompactionAggregate(Shard shard)
        {
            if (sstables.size() < T)
                return CompactionAggregate.createUnified(sstables, CompactionPick.EMPTY, ImmutableList.of(), shard, this);

            // if we have at least T sstables, let's try to compact them in one go to reduce WA, which is typical when we
            // switch from a high W to a negative W, e.g. after a write ramp-up followed by a read WL. This may skip levels, any negative consequences?
            return CompactionAggregate.createUnified(sstables, CompactionPick.create(index, sstables), ImmutableList.of(), shard, this);
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
