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
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.repair.consistent.admin.CleanupSummary;
import org.apache.cassandra.schema.CompactionParams;

public interface CompactionStrategyContainer extends CompactionStrategy, INotificationConsumer
{
    /**
     * Enable compaction.
     */
    void enable();

    /**
     * Disable compaction.
     */
    void disable();

    /**
     * @return {@code true} if compaction is enabled and running; e.g. if autocompaction has been disabled via nodetool
     *         or JMX, this should return {@code false}, even if the underlying compaction strategy hasn't been paused.
     */
    boolean isEnabled();

    /**
     * @return {@code true} if compaction is running, i.e. if the underlying compaction strategy is not currently
     *         paused or being shut down.
     */
    boolean isActive();

    /**
     * The reason for reloading
     */
    enum ReloadReason
    {
        /** A new strategy container has been created.  */
        FULL,

        /** A new strategy container has been reloaded due to table metadata changes, e.g. a schema change. */
        METADATA_CHANGE,

        /** A request over JMX to update the compaction parameters only locally, without changing the schema permanently. */
        JMX_REQUEST,

        /** The disk boundaries were updated, in this case the strategies may need to be recreated even if the params haven't changed */
        DISK_BOUNDARIES_UPDATED
    };

    /**
     * Reload the strategy container taking into account the state of the previous strategy container instance
     * ({@code this}, in case we're not reloading after switching between containers), the new compaction parameters,
     * and the reason for reloading.
     * <p/>
     * Depending on the reason, different actions are taken, for example the schema parameters are not updated over
     * JMX and the decision on whether to enable or disable compaction depends only on the parameters over JMX, but
     * also on the previous JMX directive in case of a full reload. Also, the disk boundaries are not updated over JMX.
     * <p/>
     * See the implementations of this method for more details.
     *
     * @param previous the strategy container instance which state needs to be inherited/taken into account, in many
     *                 cases the same as {@code this}, but never {@code null}.
     * @param compactionParams the new compaction parameters
     * @param reason the reason for reloading
     */
    void reload(@Nonnull CompactionStrategyContainer previous, CompactionParams compactionParams, ReloadReason reason);

    /**
     * Return the compaction parameters. These are not necessarily the same as the ones specified in the schema, they
     * may have been overwritten over JMX.
     *
     * @return the compaction params currently active
     */
    CompactionParams getCompactionParams();

    /**
     * Mutates sstable repairedAt times and notifies listeners of the change with the writeLock held. Prevents races
     * with other processes between when the metadata is changed and when sstables are moved between strategies.
     */
    int mutateRepaired(Collection<SSTableReader> sstables, long repairedAt, UUID pendingRepair, boolean isTransient) throws IOException;

    /**
     * @return any pending repairs
     */
    Set<UUID> pendingRepairs();

    /**
     * @return true if there are any pending repair tasks for the given session.
     */
    boolean hasDataForPendingRepair(UUID sessionID);

    /**
     * This method is to keep compatibility with legacy strategies where there are multiple inner strategies
     * handling sstables by repair status.
     *
     * @return all inner compaction strategies
     */
    List<List<CompactionStrategy>> getStrategies();

    /**
     * Split sstables into a list of grouped sstable containers, the list index an sstable
     *
     * lives in matches the list index of the holder that's responsible for it
     */
    List<AbstractStrategyHolder.GroupedSSTableContainer> groupSSTables(Iterable<SSTableReader> sstables);

    /**
     * finds the oldest (by modification date) non-latest-version sstable on disk and creates an upgrade task for it
     * @return
     */
    AbstractCompactionTask findUpgradeSSTableTask();

    int getUnleveledSSTables();

    CleanupSummary releaseRepairData(Collection<UUID> sessions);
}