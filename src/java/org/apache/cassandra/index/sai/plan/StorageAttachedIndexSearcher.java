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
package org.apache.cassandra.index.sai.plan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.Token;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;


public class StorageAttachedIndexSearcher implements Index.Searcher
{
    private final QueryController controller;
    private final QueryContext queryContext;

    public StorageAttachedIndexSearcher(ColumnFamilyStore cfs,
                                        TableQueryMetrics tableQueryMetrics,
                                        ReadCommand command,
                                        List<RowFilter.Expression> expressions,
                                        long executionQuotaMs)
    {
        this.queryContext = new QueryContext(executionQuotaMs);
        this.controller = new QueryController(cfs, command, expressions, queryContext, tableQueryMetrics);
    }

    @Override
    public UnfilteredPartitionIterator search(ReadExecutionController executionController) throws RequestTimeoutException
    {
        return  new ResultRetriever(analyze(), controller, executionController, queryContext);
    }

    /**
     * Converts expressions into filter tree and reference {@link SSTableIndex}s used for query.
     */
    private Operation analyze()
    {
        // complete() will perform blocking IO via SimpleChunkReader when building range iterator, it must not run on TPC thread.
        // But SASI is doing blocking io via {@link MappedBuffer}, so it can run on TPC thread.
        return Operation.initTreeBuilder(controller).complete();
    }

    private static class ResultRetriever extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        private final AbstractBounds<PartitionPosition> keyRange;

        private final Operation operation;
        private final QueryController controller;
        private final ReadExecutionController executionController;
        private final QueryContext queryContext;

        private Iterator<DecoratedKey> currentKeys = null;
        private DecoratedKey lastKey;

        private ResultRetriever(Operation operation, QueryController controller,
                ReadExecutionController executionController, QueryContext queryContext)
        {
            this.keyRange = controller.dataRange().keyRange();

            this.operation = operation;
            this.controller = controller;
            this.executionController = executionController;
            this.queryContext = queryContext;
        }

        @Override
        public UnfilteredRowIterator computeNext()
        {
            if (operation == null)
                return endOfData();

            operation.skipTo((Long) keyRange.left.getToken().getTokenValue());

            // IMPORTANT: The correctness of the entire query pipeline relies on the fact that we consume a token
            // and materialize its keys before moving on to the next token in the flow. This sequence must not be broken
            // with toList() or similar. (Both the union and intersection flow constructs, to avoid excessive object
            // allocation, reuse their token mergers as they process individual positions on the ring.)
            for (;;)
            {
                if (currentKeys == null || !currentKeys.hasNext())
                {
                    if (!operation.hasNext())
                        return endOfData();

                    Token token = operation.next();
                    currentKeys = token.keys();
                }

                while (currentKeys.hasNext())
                {
                    DecoratedKey key = currentKeys.next();

                    if (!keyRange.right.isMinimum() && keyRange.right.compareTo(key) < 0)
                        return endOfData();

                    if (!keyRange.contains(key))
                        continue;

                    UnfilteredRowIterator partition = apply(key);
                    if (partition != null)
                        return partition;
                }
            }
        }

        public UnfilteredRowIterator apply(DecoratedKey key)
        {
            // Key reads are lazy, delayed all the way to this point. Skip if we've already seen this one:
            if (key.equals(lastKey))
                return null;

            lastKey = key;

            // SPRC should only return UnfilteredRowIterator, but it returns UnfilteredPartitionIterator due to Flow.
            try (UnfilteredRowIterator partition = controller.getPartition(key, executionController))
            {
                queryContext.partitionsRead++;

                return applyIndexFilter(partition, operation.filterTree, queryContext);
            }
        }

        private static UnfilteredRowIterator applyIndexFilter(UnfilteredRowIterator partition, FilterTree tree, QueryContext queryContext)
        {
            Row staticRow = partition.staticRow();
            List<Unfiltered> clusters = new ArrayList<>();

            while (partition.hasNext())
            {
                Unfiltered row = partition.next();

                queryContext.rowsFiltered++;
                if (tree.satisfiedBy(partition.partitionKey(), row, staticRow))
                    clusters.add(row);
            }

            if (clusters.isEmpty())
            {
                queryContext.rowsFiltered++;
                if (tree.satisfiedBy(partition.partitionKey(), staticRow, staticRow))
                    clusters.add(staticRow);
            }

            /*
             * If {@code clusters} is empty, which means either all clustering row and static row pairs failed,
             *       or static row and static row pair failed. In both cases, we should not return any partition.
             * If {@code clusters} is not empty, which means either there are some clustering row and static row pairs match the filters,
             *       or static row and static row pair matches the filters. In both cases, we should return a partition with static row,
             *       and remove the static row marker from the {@code clusters} for the latter case.
             */
            if (clusters.isEmpty())
                return null;

            return new PartitionIterator(partition, staticRow, Iterators.filter(clusters.iterator(), u -> !((Row)u).isStatic()));
        }

        private static class PartitionIterator extends AbstractUnfilteredRowIterator
        {
            private final Iterator<Unfiltered> rows;

            public PartitionIterator(UnfilteredRowIterator partition, Row staticRow, Iterator<Unfiltered> content)
            {
                super(partition.metadata(),
                      partition.partitionKey(),
                      partition.partitionLevelDeletion(),
                      partition.columns(),
                      staticRow,
                      partition.isReverseOrder(),
                      partition.stats());

                rows = content;
            }

            @Override
            protected Unfiltered computeNext()
            {
                return rows.hasNext() ? rows.next() : endOfData();
            }
        }

        @Override
        public TableMetadata metadata()
        {
            return controller.metadata();
        }

        public void close()
        {
            FileUtils.closeQuietly(operation);
            controller.finish();
        }
    }
}
