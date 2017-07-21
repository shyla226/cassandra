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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataTracker;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.AlwaysPresentFilter;

import org.apache.cassandra.utils.concurrent.Refs;

/**
 * Manage compaction options.
 */
public class CompactionController implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionController.class);
    static final boolean NEVER_PURGE_TOMBSTONES = Boolean.getBoolean("cassandra.never_purge_tombstones");

    public final ColumnFamilyStore cfs;

    // note that overlappingTree and overlappingSSTables will be null if NEVER_PURGE_TOMBSTONES is set - this is a
    // good thing so that noone starts using them and thinks that if overlappingSSTables is empty, there
    // is no overlap.
    private DataTracker.SSTableIntervalTree overlappingTree;
    private Refs<SSTableReader> overlappingSSTables;
    private final Iterable<SSTableReader> compacting;
    private final boolean ignoreOverlaps;
    public final int gcBefore;

    protected CompactionController(ColumnFamilyStore cfs, int maxValue)
    {
        this(cfs, null, maxValue, false);
    }

    public CompactionController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, int gcBefore)
    {
        this(cfs, compacting, gcBefore, false);
    }

    public CompactionController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, int gcBefore, boolean ignoreOverlaps)
    {
        assert cfs != null;
        this.cfs = cfs;
        this.gcBefore = gcBefore;
        this.compacting = compacting;
        this.ignoreOverlaps = ignoreOverlaps;
        refreshOverlaps();
        if (NEVER_PURGE_TOMBSTONES)
            logger.warn("You are running with -Dcassandra.never_purge_tombstones=true, this is dangerous!");
    }

    void maybeRefreshOverlaps()
    {
        if (NEVER_PURGE_TOMBSTONES)
        {
            logger.debug("not refreshing overlaps - running with -Dcassandra.never_purge_tombstones=true");
            return;
        }

        if (ignoreOverlaps())
            return;

        for (SSTableReader reader : overlappingSSTables)
        {
            if (reader.isMarkedCompacted())
            {
                refreshOverlaps();
                return;
            }
        }
    }

    private void refreshOverlaps()
    {
        if (NEVER_PURGE_TOMBSTONES || ignoreOverlaps())
            return;

        if (this.overlappingSSTables != null)
            overlappingSSTables.release();

        if (compacting == null)
            overlappingSSTables = Refs.tryRef(Collections.<SSTableReader>emptyList());
        else
            overlappingSSTables = cfs.getAndReferenceOverlappingSSTables(compacting);
        this.overlappingTree = DataTracker.buildIntervalTree(overlappingSSTables);
    }

    public Set<SSTableReader> getFullyExpiredSSTables()
    {
        return getFullyExpiredSSTables(cfs, compacting, overlappingSSTables, gcBefore, ignoreOverlaps());
    }

    public static Set<SSTableReader> getFullyExpiredSSTables(ColumnFamilyStore cfStore, Iterable<SSTableReader> compacting, Iterable<SSTableReader> overlapping, int gcBefore)
    {
        return getFullyExpiredSSTables(cfStore, compacting, overlapping, gcBefore, false);
    }

    /**
     * Finds expired sstables
     *
     * works something like this;
     * 1. find "global" minTimestamp of overlapping sstables and compacting sstables containing any non-expired data
     * 2. build a list of fully expired candidates
     * 3. check if the candidates to be dropped actually can be dropped (maxTimestamp < global minTimestamp)
     *    - if not droppable, remove from candidates
     * 4. return candidates.
     *
     * @param cfStore current cf store
     * @param compacting we take the drop-candidates from this set, it is usually the sstables included in the compaction
     * @param overlappingByKey the sstables that overlap the ones in compacting in terms of key ranges.
     * @param gcBefore time period to consider sstables fully expired
     * @param ignoreOverlaps whether or not to ignore the overlaps (both time and key range)
     * @return fully expired sstables
     */
    public static Set<SSTableReader> getFullyExpiredSSTables(ColumnFamilyStore cfStore, Iterable<SSTableReader> compacting, Iterable<SSTableReader> overlappingByKey, int gcBefore, boolean ignoreOverlaps)
    {
        logger.debug("Checking droppable sstables in {}", cfStore);

        if (compacting == null || NEVER_PURGE_TOMBSTONES)
            return Collections.<SSTableReader>emptySet();

        Set<SSTableReader> candidates = new HashSet<>();
        long minTimestamp = Long.MAX_VALUE;

        for (SSTableReader candidate : compacting)
        {
            if (candidate.getSSTableMetadata().maxLocalDeletionTime < gcBefore)
                candidates.add(candidate);
            else
                minTimestamp = Math.min(minTimestamp, candidate.getMinTimestamp());
        }

        // When ignoring overlaps, we judge solely based on the fact whether or not the sstable has to be expired
        if (ignoreOverlaps)
            return candidates;

        for (SSTableReader sstable : overlappingByKey)
        {
            // Overlapping might include fully expired sstables. What we care about here is
            // the min timestamp of the overlapping sstables that actually contain live data.
            if (sstable.getSSTableMetadata().maxLocalDeletionTime >= gcBefore)
                minTimestamp = Math.min(minTimestamp, sstable.getMinTimestamp());
        }

        // At this point, minTimestamp denotes the lowest timestamp of any relevant
        // SSTable that contains a constructive value. candidates contains all the
        // candidates with no constructive values. The ones out of these that have
        // (getMaxTimestamp() < minTimestamp) serve no purpose anymore.

        Iterator<SSTableReader> iterator = candidates.iterator();
        while (iterator.hasNext())
        {
            SSTableReader candidate = iterator.next();
            if (candidate.getMaxTimestamp() >= minTimestamp)
            {
                iterator.remove();
            }
            else
            {
                logger.debug("Dropping expired SSTable {} (maxLocalDeletionTime={}, gcBefore={})",
                        candidate, candidate.getSSTableMetadata().maxLocalDeletionTime, gcBefore);
            }
        }
        return candidates;
    }

    public String getKeyspace()
    {
        return cfs.keyspace.getName();
    }

    public String getColumnFamily()
    {
        return cfs.name;
    }

    /**
     * @return the largest timestamp before which it's okay to drop tombstones for the given partition;
     * i.e., after the maxPurgeableTimestamp there may exist newer data that still needs to be suppressed
     * in other sstables.  This returns the minimum timestamp for any SSTable that contains this partition and is not
     * participating in this compaction, or LONG.MAX_VALUE if no such SSTable exists.
     */
    public long maxPurgeableTimestamp(DecoratedKey key)
    {
        if (NEVER_PURGE_TOMBSTONES)
            return Long.MIN_VALUE;

        List<SSTableReader> filteredSSTables = overlappingTree.search(key);
        long min = Long.MAX_VALUE;
        for (SSTableReader sstable : filteredSSTables)
        {
            // if we don't have bloom filter(bf_fp_chance=1.0 or filter file is missing),
            // we check index file instead.
            if (sstable.getBloomFilter() instanceof AlwaysPresentFilter && sstable.getPosition(key, SSTableReader.Operator.EQ, false) != null)
                min = Math.min(min, sstable.getMinTimestamp());
            else if (sstable.getBloomFilter().isPresent(key.getKey()))
                min = Math.min(min, sstable.getMinTimestamp());
        }
        return min;
    }

    public void close()
    {
        if (overlappingSSTables != null)
            overlappingSSTables.release();
    }

    protected boolean ignoreOverlaps()
    {
        return ignoreOverlaps;
    }
}
