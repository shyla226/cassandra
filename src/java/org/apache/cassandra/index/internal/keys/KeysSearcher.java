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
package org.apache.cassandra.index.internal.keys;

import java.nio.ByteBuffer;

import io.reactivex.Single;
import org.apache.cassandra.concurrent.TPCOpOrder;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.apache.cassandra.schema.TableMetadata;

public class KeysSearcher extends CassandraIndexSearcher
{
    public KeysSearcher(ReadCommand command,
                        RowFilter.Expression expression,
                        CassandraIndex indexer)
    {
        super(command, expression, indexer);
    }

    protected Single<UnfilteredPartitionIterator> queryDataFromIndex(final DecoratedKey indexKey,
                                                                     final RowIterator indexHits,
                                                                     final ReadCommand command,
                                                                     final ReadExecutionController executionController)
    {
        assert indexHits.staticRow() == Rows.EMPTY_STATIC_ROW;

        UnfilteredPartitionIterator iter = new UnfilteredPartitionIterator()
        {
            private UnfilteredRowIterator next;

            public TableMetadata metadata()
            {
                return command.metadata();
            }

            public boolean hasNext()
            {
                return prepareNext();
            }

            public UnfilteredRowIterator next()
            {
                if (next == null)
                    prepareNext();

                UnfilteredRowIterator toReturn = next;
                next = null;
                return toReturn;
            }

            private boolean prepareNext()
            {
                while (next == null && indexHits.hasNext())
                {
                    Row hit = indexHits.next();
                    DecoratedKey key = index.baseCfs.decorateKey(hit.clustering().get(0));
                    if (!command.selectsKey(key))
                        continue;

                    ColumnFilter extendedFilter = getExtendedFilter(command.columnFilter());
                    SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.create(index.baseCfs.metadata(),
                                                                                           command.nowInSec(),
                                                                                           extendedFilter,
                                                                                           command.rowFilter(),
                                                                                           DataLimits.NONE,
                                                                                           key,
                                                                                           command.clusteringIndexFilter(key));

                    @SuppressWarnings("resource") // filterIfStale closes it's iterator if either it materialize it or if it returns null.
                    // Otherwise, we close right away if empty, and if it's assigned to next it will be called either
                    // by the next caller of next, or through closing this iterator is this come before.
                    UnfilteredRowIterator dataIter = filterIfStale(FlowablePartitions.toIterator(dataCmd.queryStorage(index.baseCfs, executionController).blockingSingle()),
                                                                                hit,
                                                                                indexKey.getKey(),
                                                                                executionController.writeOpOrderGroup(),
                                                                                command.nowInSec());

                    if (dataIter != null)
                        next = dataIter;
                }

                return next != null;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void close()
            {
                indexHits.close();
            }
        };

        return Single.just(iter).doOnError(t -> iter.close());
    }

    private ColumnFilter getExtendedFilter(ColumnFilter initialFilter)
    {
        if (command.columnFilter().fetches(index.getIndexedColumn()))
            return initialFilter;

        ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
        builder.addAll(initialFilter.fetchedColumns());
        builder.add(index.getIndexedColumn());
        return builder.build();
    }

    private UnfilteredRowIterator filterIfStale(UnfilteredRowIterator iterator,
                                                Row indexHit,
                                                ByteBuffer indexedValue,
                                                TPCOpOrder.Group writeOp,
                                                int nowInSec)
    {
        assert iterator.metadata().isCompactTable();
        Row data = iterator.staticRow();
        if (index.isStale(data, indexedValue, nowInSec))
        {
            // Index is stale, remove the index entry and ignore
            index.deleteStaleEntry(index.getIndexCfs().decorateKey(indexedValue),
                    makeIndexClustering(iterator.partitionKey().getKey(), Clustering.EMPTY),
                    new DeletionTime(indexHit.primaryKeyLivenessInfo().timestamp(), nowInSec),
                    writeOp).blockingAwait();
            iterator.close();
            return null;
        }
        else
        {
            return iterator;
        }
    }
}
