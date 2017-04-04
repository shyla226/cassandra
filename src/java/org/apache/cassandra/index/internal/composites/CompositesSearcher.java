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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.utils.FlowableUtils;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.concurrent.OpOrder;


public class CompositesSearcher extends CassandraIndexSearcher
{
    public CompositesSearcher(ReadCommand command,
                              RowFilter.Expression expression,
                              CassandraIndex index)
    {
        super(command, expression, index);
        assert !index.getIndexedColumn().isStatic();
    }

    private IndexEntry decodeMatchingEntry(DecoratedKey indexKey, Row hit, ReadCommand command)
    {
        IndexEntry entry = index.decodeEntry(indexKey, hit);
        DecoratedKey partitionKey = command.metadata().partitioner.decorateKey(entry.indexedKey); // tpc TODO Not very happy with redoing this
        if (command.selectsKey(partitionKey) && command.selectsClustering(partitionKey, entry.indexedEntryClustering))
            return entry;
        else
            return null;
    }

    protected Flowable<FlowableUnfilteredPartition> queryDataFromIndex(final DecoratedKey indexKey,
                                                                       final FlowablePartition indexHits,
                                                                       final ReadCommand command,
                                                                       final ReadExecutionController executionController)
    {
        assert indexHits.staticRow == Rows.EMPTY_STATIC_ROW;

        class Collector implements FlowableUtils.GroupOp<IndexEntry, Flowable<FlowableUnfilteredPartition>>
        {
            public boolean inSameGroup(IndexEntry l, IndexEntry r)
            {
                return l.indexedKey.equals(r.indexedKey);
            }

            public Flowable<FlowableUnfilteredPartition> map(List<IndexEntry> entries)
            {
                DecoratedKey partitionKey = index.baseCfs.decorateKey(entries.get(0).indexedKey);
                BTreeSet.Builder clusterings = BTreeSet.builder(index.baseCfs.getComparator());
                for (IndexEntry e : entries)
                    clusterings.add(e.indexedEntryClustering);

                // Query the gathered index hits. We still need to filter stale hits from the resulting query.
                ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings.build(), false);
                SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.create(index.baseCfs.metadata(),
                                                                                       command.nowInSec(),
                                                                                       command.columnFilter(),
                                                                                       command.rowFilter(),
                                                                                       DataLimits.NONE,
                                                                                       partitionKey,
                                                                                       filter);
                Flowable<FlowableUnfilteredPartition> partition = dataCmd.queryStorage(index.baseCfs, executionController); // one or less

                return partition.map(p -> filterStaleEntries(p,
                                                             indexKey.getKey(), entries,
                                                             executionController.writeOpOrderGroup(),
                                                             command.nowInSec()));
            }
        }

        return indexHits.content.lift(FlowableUtils.skippingMap(hit -> decodeMatchingEntry(indexKey, hit, command)))
                                .lift(FlowableUtils.group(new Collector()))
                                .lift(FlowableUtils.concatLazy());
    }

    // We assume all rows in dataIter belong to the same partition.
    @SuppressWarnings("resource") // closed by the callers of ReadCommand.executeLocally()
    private FlowableUnfilteredPartition filterStaleEntries(FlowableUnfilteredPartition dataIter,
                                    final ByteBuffer indexValue,
                                    final List<IndexEntry> entries,
                                    final OpOrder.Group writeOp,
                                    final int nowInSec)
    {
        // collect stale index entries and delete them when we close this iterator
        final List<IndexEntry> staleEntries = new ArrayList<>();

        // if there is a partition level delete in the base table, we need to filter
        // any index entries which would be shadowed by it
        if (!dataIter.header.partitionLevelDeletion.isLive())
        {
            DeletionTime deletion = dataIter.header.partitionLevelDeletion;
            entries.forEach(e -> {
                if (deletion.deletes(e.timestamp))
                    staleEntries.add(e);
            });
        }

        ClusteringComparator comparator = dataIter.header.metadata.comparator;
        class Transform extends FlowableUtils.OnceCloseable implements FlowableUtils.SkippingOp<Unfiltered, Unfiltered>
        {
            private int entriesIdx;

            @Override
            public Unfiltered apply(Unfiltered unfiltered)
            {
                if (!unfiltered.isRow())
                    return unfiltered;

                Row row = (Row) unfiltered;
                IndexEntry entry = findEntry(row.clustering());
                if (!index.isStale(row, indexValue, nowInSec))
                    return row;

                staleEntries.add(entry);
                return null;
            }

            private IndexEntry findEntry(Clustering clustering)
            {
                assert entriesIdx < entries.size();
                while (entriesIdx < entries.size())
                {
                    IndexEntry entry = entries.get(entriesIdx++);
                    // The entries are in clustering order. So that the requested entry should be the
                    // next entry, the one at 'entriesIdx'. However, we can have stale entries, entries
                    // that have no corresponding row in the base table typically because of a range
                    // tombstone or partition level deletion. Delete such stale entries.
                    // For static column, we only need to compare the partition key, otherwise we compare
                    // the whole clustering.
                    int cmp = comparator.compare(entry.indexedEntryClustering, clustering);
                    assert cmp <= 0; // this would means entries are not in clustering order, which shouldn't happen
                    if (cmp == 0)
                        return entry;
                    else
                        staleEntries.add(entry);
                }
                // entries correspond to the rows we've queried, so we shouldn't have a row that has no corresponding entry.
                throw new AssertionError();
            }

            @Override
            public void onClose()
            {
                //This is purely a optimization
                // if it fails we don't really care
                deleteAllEntries(staleEntries, writeOp, nowInSec).subscribe();
            }
        }

        Flowable<Unfiltered> content = FlowableUtils.skippingMap(dataIter.content, new Transform());
        return new FlowableUnfilteredPartition(dataIter.header,
                                               dataIter.staticRow,
                                               content);
    }

    private Completable deleteAllEntries(final List<IndexEntry> entries, final OpOrder.Group writeOp, final int nowInSec)
    {
        return Completable.merge(entries.stream()
                                        .map(entry -> index.deleteStaleEntry(entry.indexValue,
                                                                             entry.indexClustering,
                                                                             new DeletionTime(entry.timestamp, nowInSec),
                                                                             writeOp))
                                        .collect(Collectors.toList()));
    }
}
