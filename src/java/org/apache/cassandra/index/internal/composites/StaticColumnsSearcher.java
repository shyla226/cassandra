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

package org.apache.cassandra.index.internal.composites;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.flow.Flow;


public class StaticColumnsSearcher extends CassandraIndexSearcher
{
    public StaticColumnsSearcher(ReadCommand command,
                                 RowFilter.Expression expression,
                                 CassandraIndex index)
    {
        super(command, expression, index);
        assert index.getIndexedColumn().isStatic();
    }

    protected Flow<FlowableUnfilteredPartition> queryDataFromIndex(final DecoratedKey indexKey,
                                                                   final FlowablePartition indexHits,
                                                                   final ReadCommand command,
                                                                   final ReadExecutionController executionController)
    {
        assert indexHits.staticRow == Rows.EMPTY_STATIC_ROW;

        return indexHits.content.flatMap(hit ->
        {
            IndexEntry nextEntry = index.decodeEntry(indexKey, hit);

            SinglePartitionReadCommand dataCmd;
            DecoratedKey partitionKey = index.baseCfs.decorateKey(nextEntry.indexedKey);

            if (!command.selectsKey(partitionKey) || !command.selectsClustering(partitionKey, nextEntry.indexedEntryClustering))
                return Flow.<FlowableUnfilteredPartition>empty();  // CASSANDRA-13277

            // If the index is on a static column, we just need to do a full read on the partition.
            // Note that we want to re-use the command.columnFilter() in case of future change.
            dataCmd = SinglePartitionReadCommand.create(index.baseCfs.metadata(),
                                                        command.nowInSec(),
                                                        command.columnFilter(),
                                                        RowFilter.NONE,
                                                        DataLimits.NONE,
                                                        partitionKey,
                                                        new ClusteringIndexSliceFilter(Slices.ALL, false));

            Flow<FlowableUnfilteredPartition> partition = dataCmd.queryStorage(index.baseCfs, executionController); // one or less
            return partition.skippingMap(p -> filterStaleEntry(p,
                                                               indexKey.getKey(),
                                                               nextEntry,
                                                               executionController.writeOpOrderGroup(),
                                                               command.nowInSec())
            );
        });
    }

    // We assume all rows in dataIter belong to the same partition.
    private FlowableUnfilteredPartition filterStaleEntry(FlowableUnfilteredPartition dataIter,
                                                         final ByteBuffer indexValue,
                                                         final IndexEntry entry,
                                                         final OpOrder.Group writeOp,
                                                         final int nowInSec)
    throws Exception
    {
        boolean stale = false;
        // if there is a partition level delete in the base table, we need to filter
        // any index entries which would be shadowed by it
        if (!dataIter.header.partitionLevelDeletion.isLive())
        {
            DeletionTime deletion = dataIter.header.partitionLevelDeletion;
            if (deletion.deletes(entry.timestamp))
                stale = true;
        }

        if (stale || index.isStale(dataIter.staticRow, indexValue, nowInSec))
        {
            index.deleteStaleEntry(entry.indexValue,
                                   entry.indexClustering,
                                   new DeletionTime(entry.timestamp, nowInSec),
                                   writeOp).subscribe();    // We don't need to wait for completion.
            dataIter.unused();
            return null;  // tpc TODO was empty partition. why?
        }

        return dataIter;
    }
}
