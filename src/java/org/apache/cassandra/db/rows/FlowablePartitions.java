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
package org.apache.cassandra.db.rows;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.reactivex.Scheduler;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.flow.CsFlow;
import org.apache.cassandra.utils.flow.Threads;

/**
 * Partition manipulation functions.
 *
 */
public class FlowablePartitions
{
    static class BaseRowIterator<T> implements PartitionTrait
    {
        final PartitionTrait source;
        final CloseableIterator<T> iter;

        BaseRowIterator(PartitionTrait source, CloseableIterator<T> iter)
        {
            this.source = source;
            this.iter = iter;
        }

        public TableMetadata metadata()
        {
            return source.metadata();
        }

        public boolean isReverseOrder()
        {
            return source.isReverseOrder();
        }

        public RegularAndStaticColumns columns()
        {
            return source.columns();
        }

        public DecoratedKey partitionKey()
        {
            return source.partitionKey();
        }

        public Row staticRow()
        {
            return source.staticRow();
        }

        public DeletionTime partitionLevelDeletion()
        {
            return source.partitionLevelDeletion();
        }

        public EncodingStats stats()
        {
            return source.stats();
        }

        public void close()
        {
            iter.close();
        }

        public boolean hasNext()
        {
            return iter.hasNext();
        }

        public T next()
        {
            return iter.next();
        }
    }

    @SuppressWarnings("resource") // caller to close
    public static UnfilteredRowIterator toIterator(FlowableUnfilteredPartition partition)
    {
        try
        {
            CloseableIterator<Unfiltered> iterator = CsFlow.toIterator(partition.content);

            class URI extends BaseRowIterator<Unfiltered> implements UnfilteredRowIterator
            {
                URI()
                {
                    super(partition, iterator);
                }
            }

            return new URI();
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }

    @SuppressWarnings("resource") // caller to close
    public static RowIterator toIteratorFiltered(FlowablePartition partition)
    {
        try
        {
            CloseableIterator<Row> iterator = CsFlow.toIterator(partition.content);

            class RI extends BaseRowIterator<Row> implements RowIterator
            {
                RI()
                {
                    super(partition, iterator);
                }
            }

            return new RI();
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }


    public static class EmptyFlowableUnfilteredPartition extends FlowableUnfilteredPartition
    {
        public EmptyFlowableUnfilteredPartition(PartitionHeader header)
        {
            super(header, Rows.EMPTY_STATIC_ROW, CsFlow.empty());
        }
    }

    public static FlowableUnfilteredPartition fromIterator(UnfilteredRowIterator iter, Scheduler callOn)
    {
        CsFlow<Unfiltered> data = CsFlow.fromIterable(() -> iter);
        if (callOn != null)
            data = data.lift(Threads.requestOn(callOn));
        Row staticRow = iter.staticRow();
        return new FlowableUnfilteredPartition(new PartitionHeader(iter.metadata(), iter.partitionKey(), iter.partitionLevelDeletion(), iter.columns(), iter.isReverseOrder(), iter.stats()),
                                               staticRow,
                                               data);
    }

    public static FlowableUnfilteredPartition empty(TableMetadata metadata, DecoratedKey partitionKey, boolean reversed)
    {
        return new EmptyFlowableUnfilteredPartition(new PartitionHeader(metadata, partitionKey, DeletionTime.LIVE, RegularAndStaticColumns.NONE, reversed, EncodingStats.NO_STATS));
    }

    public static FlowableUnfilteredPartition merge(List<FlowableUnfilteredPartition> flowables, int nowInSec)
    {
        // Note: we can't trust flowables to not change after we are called. Defensive copying needed for anything
        // that is passed on to asynchronous processing.
        assert !flowables.isEmpty();
        FlowableUnfilteredPartition first = flowables.get(0);
        if (flowables.size() == 1)
            return first;

        PartitionHeader header = first.header.mergeWith(flowables.stream().skip(1).map(x -> x.header).iterator());
        MergeReducer reducer = new MergeReducer(flowables.size(), nowInSec, header, null);

        Row staticRow;
        if (!header.columns.statics.isEmpty())
            staticRow = mergeStaticRows(flowables, header.partitionLevelDeletion, header.columns.statics, nowInSec);
        else
            staticRow = Rows.EMPTY_STATIC_ROW;

        Comparator<Clusterable> comparator = header.metadata.comparator;
        if (header.isReverseOrder)
            comparator = comparator.reversed();

        return new FlowableUnfilteredPartition(
                header,
                staticRow,
                CsFlow.merge(ImmutableList.copyOf(Lists.transform(flowables, x -> x.content)),
                             comparator,
                             reducer));
    }

    public static Row mergeStaticRows(List<FlowableUnfilteredPartition> sources, DeletionTime activeDeletion, Columns columns, int nowInSec)
    {
        Row.Merger rowMerger = new Row.Merger(sources.size(), nowInSec, columns.size(), columns.hasComplex());
        int i = 0;
        for (FlowableUnfilteredPartition source : sources)
            rowMerger.add(i++, source.staticRow);
        Row merged = rowMerger.merge(activeDeletion);

        if (merged == null)
            return Rows.EMPTY_STATIC_ROW;
        else
            return merged;
    }

    private static final Comparator<FlowableUnfilteredPartition> flowablePartitionComparator = (x, y) -> x.header.partitionKey.compareTo(y.header.partitionKey);

    public static CsFlow<FlowableUnfilteredPartition> mergePartitions(final List<CsFlow<FlowableUnfilteredPartition>> sources, final int nowInSec)
    {
        return CsFlow.merge(sources, flowablePartitionComparator, new Reducer<FlowableUnfilteredPartition, FlowableUnfilteredPartition>()
        {
            private final List<FlowableUnfilteredPartition> toMerge = new ArrayList<>(sources.size());

            public void reduce(int idx, FlowableUnfilteredPartition current)
            {
                toMerge.add(current);
            }

            public FlowableUnfilteredPartition getReduced()
            {
                return FlowablePartitions.merge(toMerge, nowInSec);
            }

            public void onKeyChange()
            {
                toMerge.clear();
            }

            public boolean trivialReduceIsTrivial()
            {
                return true;
            }
        });
    }

    public static CsFlow<FlowableUnfilteredPartition> fromPartitions(UnfilteredPartitionIterator iter, Scheduler scheduler)
    {
        CsFlow<FlowableUnfilteredPartition> flow = CsFlow.fromIterable(() -> iter)
                                                         .map(i -> fromIterator(i, scheduler));
        if (scheduler != null)
            flow = flow.lift(Threads.requestOn(scheduler));
        return flow;
    }

    @SuppressWarnings("resource") // caller to close
    public static UnfilteredPartitionIterator toPartitions(CsFlow<FlowableUnfilteredPartition> partitions, TableMetadata metadata)
    {
        try
        {
            CloseableIterator<FlowableUnfilteredPartition> iterator = CsFlow.toIterator(partitions);

            return new UnfilteredPartitionIterator()
            {
                public TableMetadata metadata()
                {
                    return metadata;
                }

                public void close()
                {
                    iterator.close();
                }

                public boolean hasNext()
                {
                    return iterator.hasNext();
                }

                public UnfilteredRowIterator next()
                {
                    return toIterator(iterator.next());
                }
            };
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }

    @SuppressWarnings("resource") // caller to close
    public static PartitionIterator toPartitionsFiltered(CsFlow<FlowablePartition> partitions)
    {
        try
        {
            CloseableIterator<FlowablePartition> iterator = CsFlow.toIterator(partitions);

            return new PartitionIterator()
            {
                public void close()
                {
                    iterator.close();
                }

                public boolean hasNext()
                {
                    return iterator.hasNext();
                }

                public RowIterator next()
                {
                    return toIteratorFiltered(iterator.next());
                }
            };
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }

    public static FlowablePartition filter(FlowableUnfilteredPartition data, int nowInSec)
    {
        Row staticRow = data.staticRow.purge(DeletionPurger.PURGE_ALL, nowInSec);
        CsFlow<Row> content = filteredContent(data, nowInSec);

        return new FlowablePartition(data.header,
                                     staticRow,
                                     content);
    }

    public static CsFlow<FlowablePartition> filterAndSkipEmpty(FlowableUnfilteredPartition data, int nowInSec)
    {
        Row staticRow = data.staticRow.purge(DeletionPurger.PURGE_ALL, nowInSec);
        CsFlow<Row> content = filteredContent(data, nowInSec);

        if (staticRow != null && !staticRow.isEmpty())
            return CsFlow.just(new FlowablePartition(data.header,
                                                     staticRow,
                                                     content));
        else
            return content.skipMapEmpty(c -> new FlowablePartition(data.header,
                                                                   Rows.EMPTY_STATIC_ROW,
                                                                   c));
    }

    private static CsFlow<Row> filteredContent(FlowableUnfilteredPartition data, int nowInSec)
    {
        return data.content.skippingMap(unfiltered -> unfiltered.isRow()
                                                      ? ((Row) unfiltered).purge(DeletionPurger.PURGE_ALL, nowInSec)
                                                      : null);
    }

    /**
     * Filters the given partition stream, removing all partitions that become empty after filtering.
     */
    public static CsFlow<FlowablePartition> filterAndSkipEmpty(CsFlow<FlowableUnfilteredPartition> data, int nowInSec)
    {
        return data.flatMap(p -> filterAndSkipEmpty(p, nowInSec));
    }

    /**
     * Skips empty partitions. This is not terribly efficient as it has to cache the first row of the partition and
     * reconstruct the FlowableUnfilteredPartition object.
     */
    public static CsFlow<FlowableUnfilteredPartition> skipEmptyPartitions(CsFlow<FlowableUnfilteredPartition> partitions)
    {
        return partitions.flatMap(partition ->
                partition.staticRow().isEmpty() && partition.partitionLevelDeletion().isLive() ?
                        partition.content.skipMapEmpty(content -> new FlowableUnfilteredPartition(partition.header, partition.staticRow, content)) :
                        CsFlow.just(partition));
    }
}
