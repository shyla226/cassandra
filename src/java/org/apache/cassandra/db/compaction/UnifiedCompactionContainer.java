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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.repair.consistent.admin.CleanupSummary;
import org.apache.cassandra.schema.CompactionParams;

public class UnifiedCompactionContainer implements CompactionStrategyContainer
{
    private final ColumnFamilyStore cfs;
    private final CompactionParams params;
    private final UnifiedCompactionStrategy strategy;

    AtomicBoolean enabled;

    UnifiedCompactionContainer(CompactionStrategyFactory factory, boolean enabled)
    {
        cfs = factory.getCfs();
        params = cfs.metadata().params.compaction;
        strategy = new UnifiedCompactionStrategy(factory, params.options());
        this.enabled = new AtomicBoolean(enabled);

        factory.getCompactionLogger().strategyCreated(strategy);

        if (strategy.getOptions().isLogAll())
            factory.getCompactionLogger().enable();
        else
            factory.getCompactionLogger().disable();

        startup();
    }

    @Override
    public void enable()
    {
        this.enabled.set(true);
    }

    @Override
    public void disable()
    {
        this.enabled.set(false);
    }

    @Override
    public boolean isEnabled()
    {
        return enabled.get() && strategy.isActive;
    }

    @Override
    public boolean isActive()
    {
        return strategy.isActive;
    }

    @Override
    public void reload(CompactionStrategyContainer previous, CompactionParams compactionParams, ReloadReason reason)
    {
        throw new UnsupportedOperationException("Not supported, create a new container");
    }

    @Override
    public CompactionParams getCompactionParams()
    {
        return params;
    }

    @Override
    public int mutateRepaired(Collection<SSTableReader> sstables, long repairedAt, UUID pendingRepair, boolean isTransient) throws IOException
    {
        // TODO - support repair
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Set<UUID> pendingRepairs()
    {
        return Collections.emptySet();
    }

    @Override
    public boolean hasDataForPendingRepair(UUID sessionID)
    {
        return false;
    }

    @Override
    public List<List<CompactionStrategy>> getStrategies()
    {
        return ImmutableList.of(ImmutableList.of(strategy));
    }

    @Override
    public List<AbstractStrategyHolder.GroupedSSTableContainer> groupSSTables(Iterable<SSTableReader> sstables)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public AbstractCompactionTask findUpgradeSSTableTask()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getUnleveledSSTables()
    {
        return 0;
    }

    @Override
    public CleanupSummary releaseRepairData(Collection<UUID> sessions)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public CompactionLogger getCompactionLogger()
    {
        return strategy.compactionLogger;
    }

    @Override
    public void pause()
    {
        strategy.pause();
    }

    @Override
    public void resume()
    {
        strategy.resume();
    }

    @Override
    public void startup()
    {
        strategy.startup();
    }

    @Override
    public void shutdown()
    {
        strategy.shutdown();
    }

    @Override
    public Collection<AbstractCompactionTask> getNextBackgroundTasks(int gcBefore)
    {
        return strategy.getNextBackgroundTasks(gcBefore);
    }

    @Override
    public CompactionTasks getMaximalTasks(int gcBefore, boolean splitOutput)
    {
        return strategy.getMaximalTasks(gcBefore, splitOutput);
    }

    @Override
    public CompactionTasks getUserDefinedTasks(Collection<SSTableReader> sstables, int gcBefore)
    {
        return strategy.getUserDefinedTasks(sstables, gcBefore);
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return strategy.getEstimatedRemainingTasks();
    }

    @Override
    public AbstractCompactionTask createCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes)
    {
        return strategy.createCompactionTask(txn, gcBefore, maxSSTableBytes);
    }

    @Override
    public int getTotalCompactions()
    {
        return strategy.getTotalCompactions();
    }

    @Override
    public List<CompactionStrategyStatistics> getStatistics()
    {
        return strategy.getStatistics();
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return strategy.getMaxSSTableBytes();
    }

    @Override
    public int[] getSSTableCountPerLevel()
    {
        return strategy.getSSTableCountPerLevel();
    }

    @Override
    public int getLevelFanoutSize()
    {
        return strategy.getLevelFanoutSize();
    }

    @Override
    public ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
    {
        return strategy.getScanners(sstables, ranges);
    }

    @Override
    public String getName()
    {
        return strategy.getName();
    }

    @Override
    public Set<SSTableReader> getSSTables()
    {
        return strategy.getSSTables();
    }

    @Override
    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        return strategy.groupSSTablesForAntiCompaction(sstablesToGroup);
    }

    @Override
    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                       long keyCount,
                                                       long repairedAt,
                                                       UUID pendingRepair,
                                                       boolean isTransient,
                                                       MetadataCollector collector,
                                                       SerializationHeader header,
                                                       Collection<Index.Group> indexGroups,
                                                       LifecycleNewTracker lifecycleNewTracker)
    {
        return strategy.createSSTableMultiWriter(descriptor,
                                                 keyCount,
                                                 repairedAt,
                                                 pendingRepair,
                                                 isTransient,
                                                 collector,
                                                 header,
                                                 indexGroups,
                                                 lifecycleNewTracker);
    }

    @Override
    public boolean supportsEarlyOpen()
    {
        return strategy.supportsEarlyOpen();
    }

    @Override
    public void onInProgress(CompactionProgress progress)
    {
        strategy.onInProgress(progress);
    }

    @Override
    public void onCompleted(UUID id)
    {
        strategy.onCompleted(id);
    }

    @Override
    public void handleNotification(INotification notification, Object sender)
    {
        // TODO - this is a no-op because the strategy is stateless but we could detect here
        // sstables that are added either because of streaming or because of nodetool refresh
    }
}
