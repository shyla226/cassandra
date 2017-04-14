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
package org.apache.cassandra.db.partitions;

import java.util.function.Predicate;

import io.reactivex.functions.Function;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;

public abstract class PurgeFunction extends Transformation<UnfilteredRowIterator> implements Function<FlowableUnfilteredPartition, FlowableUnfilteredPartition>
{
    private final DeletionPurger purger;
    private final int nowInSec;

    public PurgeFunction(int nowInSec, int gcBefore, int oldestUnrepairedTombstone, boolean onlyPurgeRepairedTombstones)
    {
        this.nowInSec = nowInSec;
        this.purger = (timestamp, localDeletionTime) ->
                      !(onlyPurgeRepairedTombstones && localDeletionTime >= oldestUnrepairedTombstone)
                      && localDeletionTime < gcBefore
                      && getPurgeEvaluator().test(timestamp);
    }

    protected abstract Predicate<Long> getPurgeEvaluator();

    // Called at the beginning of each new partition
    protected void onNewPartition(DecoratedKey partitionKey)
    {
    }

    // Called for each partition that had only purged infos and are empty post-purge.
    protected void onEmptyPartitionPostPurge(DecoratedKey partitionKey)
    {
    }

    // Called for every unfiltered. Meant for CompactionIterator to update progress
    protected void updateProgress()
    {
    }

    @Override
    protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
    {
        onNewPartition(partition.partitionKey());

        UnfilteredRowIterator purged = Transformation.apply(partition, this);
        if (purged.isEmpty())
        {
            onEmptyPartitionPostPurge(purged.partitionKey());
            purged.close();
            return null;
        }

        return purged;
    }

    @Override
    public FlowableUnfilteredPartition applyToPartition(FlowableUnfilteredPartition partition)
    {
        onNewPartition(partition.header.partitionKey);

        FlowableUnfilteredPartition ret = Transformation.apply(partition, this);
        if (ret.isEmpty())
        {
            onEmptyPartitionPostPurge(partition.header.partitionKey);
            return null;
        }

        return ret;
    }

    @Override
    public FlowableUnfilteredPartition apply(FlowableUnfilteredPartition partition)
    {
        return applyToPartition(partition);
    }

    @Override
    protected DeletionTime applyToDeletion(DeletionTime deletionTime)
    {
        return purger.shouldPurge(deletionTime) ? DeletionTime.LIVE : deletionTime;
    }

    @Override
    public Row applyToStatic(Row row)
    {
        updateProgress();
        return row.purge(purger, nowInSec);
    }

    @Override
    protected Row applyToRow(Row row)
    {
        updateProgress();
        return row.purge(purger, nowInSec);
    }

    @Override
    protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
    {
        updateProgress();
        return marker.purge(purger, nowInSec);
    }
}
