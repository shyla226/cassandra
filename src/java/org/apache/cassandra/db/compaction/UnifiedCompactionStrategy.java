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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SortedLocalRanges;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.ShardedMultiWriter;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
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

    /** The controller can be changed at any time to change the strategy behavior */
    private Controller controller;

    private volatile ArenaSelector arenaSelector;

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
        perform(super::startup,
                () -> controller.startup(this, ScheduledExecutors.scheduledTasks));
    }

    @Override
    public void shutdown()
    {
        perform(super::shutdown,
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
                                      getShardBoundaries());
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

        return new UnifiedCompactionTask(cfs, this, transaction, gcBefore, controller.getMinSstableSizeBytes(), getShardBoundaries());
    }

    private void maybeUpdateSelector()
    {
        if (arenaSelector != null && !arenaSelector.diskBoundaries.isOutOfDate())
            return; // the disk boundaries (and thus the local ranges too) have not changed since the last time we calculated

        synchronized (this)
        {
            if (arenaSelector != null && !arenaSelector.diskBoundaries.isOutOfDate())
                return; // another thread beat us to the update

            DiskBoundaries currentBoundaries = cfs.getDiskBoundaries();
            List<PartitionPosition> shardBoundaries = computeShardBoundaries(currentBoundaries.getLocalRanges(),
                                                                             currentBoundaries.getPositions(),
                                                                             controller.getNumShards(),
                                                                             cfs.getPartitioner().splitter());
            arenaSelector = new ArenaSelector(currentBoundaries, shardBoundaries);
            // Note: this can just as well be done without the synchronization (races would be benign, just doing some
            // redundant work). For the current usages of this blocking is fine and expected to perform no worse.
        }
    }

    /**
     * We want to split the local token range in shards, aiming for close to equal share for each shard.
     * If there are no disk boundaries, we just split the token space equally, but multiple disks have been defined
     * (each with its own share of the local range), we can't have shards spanning disk boundaries. This means that
     * shards need to be selected within the disk's portion of the local ranges.
     *
     * As an example of what this means, consider a 3-disk node and 10 shards. The range is split equally between
     * disks, but we can only split shards within a disk range, thus we end up with 6 shards taking 1/3*1/3=1/9 of the
     * token range, and 4 smaller shards taking 1/3*1/4=1/12 of the token range.
     */
    @VisibleForTesting
    static List<PartitionPosition> computeShardBoundaries(SortedLocalRanges localRanges, List<PartitionPosition> diskBoundaries, int numShards, Optional<Splitter> splitter)
    {
        if (!splitter.isPresent())
            return diskBoundaries;
        if (diskBoundaries == null || diskBoundaries.size() <= 1)
            return localRanges.split(numShards);

        if (numShards <= diskBoundaries.size())
            return diskBoundaries;

        return splitPerDiskRanges(localRanges,
                                  diskBoundaries,
                                  getRangesTotalSize(localRanges.getRanges()),
                                  numShards,
                                  splitter.get());
    }

    /**
     * Split the per-disk ranges and generate the required number of shard boundaries.
     * This works by accumulating the size after each disk's share, multiplying by shardNum/totalSize and rounding to
     * produce an integer number of total shards needed by the disk boundary, which in turns defines how many need to be
     * added for this disk.
     *
     * For example, for a total size of 1, 2 disks (each of 0.5 share) and 3 shards, this will:
     * -process disk 1:
     * -- calculate 1/2 as the accumulated size
     * -- map this to 3/2 and round to 2 shards
     * -- split the disk's ranges into two equally-sized shards
     * -process disk 2:
     * -- calculate 1 as the accumulated size
     * -- map it to 3 and round to 3 shards
     * -- assign the disk's ranges to one shard
     *
     * The resulting shards will not be of equal size and works best if the disk shares are distributed evenly (which
     * the current code always ensures).
     */
    private static List<PartitionPosition> splitPerDiskRanges(SortedLocalRanges localRanges,
                                                              List<PartitionPosition> diskBoundaries,
                                                              double totalSize,
                                                              int numShards,
                                                              Splitter splitter)
    {
        double perShard = totalSize / numShards;
        List<PartitionPosition> shardBoundaries = new ArrayList<>(numShards);
        double processedSize = 0;
        Token left = diskBoundaries.get(0).getToken().getPartitioner().getMinimumToken();
        for (PartitionPosition boundary : diskBoundaries)
        {
            Token right = boundary.getToken();
            List<Splitter.WeightedRange> disk = localRanges.subrange(new Range<>(left, right));

            processedSize += getRangesTotalSize(disk);
            int targetCount = (int) Math.round(processedSize / perShard);
            List<Token> splits = splitter.splitOwnedRanges(Math.max(targetCount - shardBoundaries.size(), 1), disk, Splitter.SplitType.ALWAYS_SPLIT).boundaries;
            shardBoundaries.addAll(Collections2.transform(splits, Token::maxKeyBound));
            // The splitting always results in maxToken as the last boundary. Replace it with the disk's upper bound.
            shardBoundaries.set(shardBoundaries.size() - 1, boundary);

            left = right;
        }
        assert shardBoundaries.size() == numShards;
        return shardBoundaries;
    }

    private static double getRangesTotalSize(List<Splitter.WeightedRange> ranges)
    {
        double totalSize = 0;
        for (Splitter.WeightedRange range : ranges)
            totalSize += range.left().size(range.right());
        return totalSize;
    }

    @VisibleForTesting
    List<PartitionPosition> getShardBoundaries()
    {
        maybeUpdateSelector();
        return arenaSelector.shardBoundaries;
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

        final ArenaSelector arenaSelector = this.arenaSelector;
        Map<SSTableReader, Shard> tables = new TreeMap<>(arenaSelector);
        for (SSTableReader table : nonSuspectAndNotIn(dataTracker.getLiveSSTables(), dataTracker.getCompacting()))
            tables.computeIfAbsent(table, t -> new Shard(arenaSelector))
                  .add(table);

        return tables.values();
    }

    @VisibleForTesting
    Map<Shard, List<Bucket>> getShardsWithBuckets()
    {
        maybeUpdateSelector();
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
        final ArenaSelector selector;

        Shard(ArenaSelector selector)
        {
            this.sstables = new ArrayList<>();
            this.selector = selector;
        }

        void add(SSTableReader ssTableReader)
        {
            sstables.add(ssTableReader);
        }

        public String name()
        {
            SSTableReader t = sstables.get(0);
            return selector.name(t);
        }

        @Override
        public int compareTo(Shard o)
        {
            return selector.compare(this.sstables.get(0), o.sstables.get(0));
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
