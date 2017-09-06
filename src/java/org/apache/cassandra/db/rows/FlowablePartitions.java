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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Scheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
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
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.Threads;

/**
 * Partition manipulation functions.
 */
public class FlowablePartitions
{
    private final static Logger logger = LoggerFactory.getLogger(FlowablePartitions.class);

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
            CloseableIterator<Unfiltered> iterator = Flow.toIterator(partition.content);

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
            CloseableIterator<Row> iterator = Flow.toIterator(partition.content);

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

    public static FlowableUnfilteredPartition fromIterator(UnfilteredRowIterator iter, Scheduler callOn)
    {
        Flow<Unfiltered> data = Flow.fromIterable(() -> iter);
        if (callOn != null)
            data = data.lift(Threads.requestOn(callOn, TPCTaskType.READ_FROM_ITERATOR));
        Row staticRow = iter.staticRow();
        return new FlowableUnfilteredPartition(new PartitionHeader(iter.metadata(), iter.partitionKey(), iter.partitionLevelDeletion(), iter.columns(), iter.isReverseOrder(), iter.stats()),
                                               staticRow,
                                               data);
    }

    public static FlowablePartition fromIterator(RowIterator iter, Scheduler callOn)
    {
        Flow<Row> data = Flow.fromIterable(() -> iter);
        if (callOn != null)
            data = data.lift(Threads.requestOn(callOn, TPCTaskType.READ_FROM_ITERATOR));

        Row staticRow = iter.staticRow();
        return new FlowablePartition(new PartitionHeader(iter.metadata(), iter.partitionKey(), DeletionTime.LIVE, iter.columns(), iter.isReverseOrder(), EncodingStats.NO_STATS),
                                     staticRow,
                                     data);
    }

    public static FlowableUnfilteredPartition empty(TableMetadata metadata, DecoratedKey partitionKey, boolean reversed)
    {
        return new FlowableUnfilteredPartition(PartitionHeader.empty(metadata, partitionKey, reversed),
                                               Rows.EMPTY_STATIC_ROW,
                                               Flow.empty());
    }

    public static FlowableUnfilteredPartition merge(List<FlowableUnfilteredPartition> flowables, int nowInSec, UnfilteredRowIterators.MergeListener listener)
    {
        // Note: we can't trust flowables to not change after we are called. Defensive copying needed for anything
        // that is passed on to asynchronous processing.
        assert !flowables.isEmpty();
        FlowableUnfilteredPartition first = flowables.get(0);
        if (flowables.size() == 1 && listener == null)
            return first;

        List<PartitionHeader> headers = new ArrayList<>(flowables.size());
        List<Flow<Unfiltered>> contents = new ArrayList<>(flowables.size());
        for (FlowableUnfilteredPartition flowable : flowables)
        {
            headers.add(flowable.header);
            contents.add(flowable.content);
        }
        PartitionHeader header = PartitionHeader.merge(headers, listener);
        MergeReducer reducer = new MergeReducer(flowables.size(), nowInSec, header, listener);

        Row staticRow;
        if (!header.columns.statics.isEmpty())
            staticRow = mergeStaticRows(flowables, header, nowInSec, listener);
        else
            staticRow = Rows.EMPTY_STATIC_ROW;

        Comparator<Clusterable> comparator = header.metadata.comparator;
        if (header.isReverseOrder)
            comparator = comparator.reversed();


        Flow<Unfiltered> content = Flow.merge(contents,
                                              comparator,
                                              reducer);
        if (listener != null)
            content = content.doOnClose(listener::close);

        return new FlowableUnfilteredPartition(header,
                                               staticRow,
                                               content);
    }

    public static Row mergeStaticRows(List<FlowableUnfilteredPartition> sources,
                                      PartitionHeader header,
                                      int nowInSec,
                                      UnfilteredRowIterators.MergeListener listener)
    {
        Columns columns = header.columns.statics;
        if (columns.isEmpty())
            return Rows.EMPTY_STATIC_ROW;

        boolean hasStatic = false;
        for (FlowableUnfilteredPartition source : sources)
            hasStatic |= !source.staticRow.isEmpty();
        if (!hasStatic)
            return Rows.EMPTY_STATIC_ROW;

        Row.Merger merger = new Row.Merger(sources.size(), nowInSec, columns.size(), columns.hasComplex());
        for (int i = 0; i < sources.size(); i++)
            merger.add(i, sources.get(i).staticRow);

        Row merged = merger.merge(header.partitionLevelDeletion);

        if (merged == null)
            merged = Rows.EMPTY_STATIC_ROW;

        if (listener != null)
            listener.onMergedRows(merged, merger.mergedRows());

        return merged;
    }

    private static final Comparator<FlowableUnfilteredPartition> flowablePartitionComparator = Comparator.comparing(x -> x.header.partitionKey);

    public static Flow<FlowableUnfilteredPartition> mergePartitions(final List<Flow<FlowableUnfilteredPartition>> sources,
                                                                    final int nowInSec,
                                                                    final MergeListener listener)
    {
        assert !sources.isEmpty();
        if (sources.size() == 1 && listener == null)
            return sources.get(0);

        Flow<FlowableUnfilteredPartition> merge = Flow.merge(sources, flowablePartitionComparator, new Reducer<FlowableUnfilteredPartition, FlowableUnfilteredPartition>()
        {
            private final FlowableUnfilteredPartition[] toMerge = new FlowableUnfilteredPartition[sources.size()];
            private PartitionHeader header;

            public void reduce(int idx, FlowableUnfilteredPartition current)
            {
                header = current.header;
                toMerge[idx] = current;
            }

            public FlowableUnfilteredPartition getReduced()
            {
                UnfilteredRowIterators.MergeListener rowListener = listener == null ? null : listener.getRowMergeListener(header.partitionKey, toMerge);

                FlowableUnfilteredPartition nonEmptyPartition = null;
                int nonEmptyPartitions = 0;

                for (int i = 0, length = toMerge.length; i < length; i++)
                {
                    FlowableUnfilteredPartition element = toMerge[i];
                    if (element == null)
                    {
                        toMerge[i] = FlowablePartitions.empty(header.metadata, header.partitionKey, header.isReverseOrder);
                    }
                    else
                    {
                        nonEmptyPartitions++;
                        nonEmptyPartition = element;
                    }
                }

                return nonEmptyPartitions == 1 && rowListener == null
                       ? nonEmptyPartition
                       : FlowablePartitions.merge(Arrays.asList(toMerge), nowInSec, rowListener);
            }

            public void onKeyChange()
            {
                Arrays.fill(toMerge, null);
            }

            public boolean trivialReduceIsTrivial()
            {
                return listener == null;
            }
        });

        if (listener != null)
            merge = merge.doOnClose(listener::close);
        return merge;
    }

    /**
     * An interface to implement to be notified of merge events.
     */
    public interface MergeListener
    {
        public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, FlowableUnfilteredPartition[] versions);

        public default void close() { }

        public static final MergeListener NONE = new MergeListener()
        {
            public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, FlowableUnfilteredPartition[] versions)
            {
                return null;
            }
        };
    }

    public static Flow<FlowableUnfilteredPartition> fromPartitions(UnfilteredPartitionIterator iter, Scheduler scheduler)
    {
        Flow<FlowableUnfilteredPartition> flow = Flow.fromIterable(() -> iter)
                                                     .map(i -> fromIterator(i, scheduler));
        if (scheduler != null)
            flow = flow.lift(Threads.requestOn(scheduler, TPCTaskType.READ_FROM_ITERATOR));
        return flow;
    }

    public static Flow<FlowablePartition> fromPartitions(PartitionIterator iter, Scheduler scheduler)
    {
        Flow<FlowablePartition> flow = Flow.fromIterable(() -> iter)
                                           .map(i -> fromIterator(i, scheduler));
        if (scheduler != null)
            flow = flow.lift(Threads.requestOn(scheduler, TPCTaskType.READ_FROM_ITERATOR));
        return flow;
    }

    @SuppressWarnings("resource") // caller to close
    public static UnfilteredPartitionIterator toPartitions(Flow<FlowableUnfilteredPartition> partitions, TableMetadata metadata)
    {
        try
        {
            CloseableIterator<FlowableUnfilteredPartition> iterator = Flow.toIterator(partitions);

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
    public static PartitionIterator toPartitionsFiltered(Flow<FlowablePartition> partitions)
    {
        try
        {
            CloseableIterator<FlowablePartition> iterator = Flow.toIterator(partitions);

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

    private static Row filterStaticRow(Row row, int nowInSec, boolean enforStrictLiveness)
    {
        if (row == null || row.isEmpty())
            return Rows.EMPTY_STATIC_ROW;

        row = row.purge(DeletionPurger.PURGE_ALL, nowInSec, enforStrictLiveness);
        return row == null ? Rows.EMPTY_STATIC_ROW : row;
    }

    public static FlowablePartition filter(FlowableUnfilteredPartition data, int nowInSec)
    {
        boolean enforStrictLiveness = data.metadata().enforceStrictLiveness();
        Row staticRow = data.staticRow.purge(DeletionPurger.PURGE_ALL, nowInSec, enforStrictLiveness);
        Flow<Row> content = filteredContent(data, nowInSec);

        return new FlowablePartition(data.header,
                                     filterStaticRow(staticRow, nowInSec, enforStrictLiveness),
                                     content);
    }

    public static Flow<FlowablePartition> filterAndSkipEmpty(FlowableUnfilteredPartition data, int nowInSec)
    {
        Row staticRow = filterStaticRow(data.staticRow, nowInSec, data.metadata().enforceStrictLiveness());
        Flow<Row> content = filteredContent(data, nowInSec);

        if (!staticRow.isEmpty())
            return Flow.just(new FlowablePartition(data.header,
                                                   staticRow,
                                                   content));
        else
            return content.skipMapEmpty(c -> new FlowablePartition(data.header,
                                                                   Rows.EMPTY_STATIC_ROW,
                                                                   c));
    }

    private static Flow<Row> filteredContent(FlowableUnfilteredPartition data, int nowInSec)
    {
        return data.content.skippingMap(unfiltered -> unfiltered.isRow()
                ? ((Row) unfiltered).purge(DeletionPurger.PURGE_ALL, nowInSec, data.metadata().enforceStrictLiveness())
                : null);
    }

    /**
     * Filters the given partition stream, removing all partitions that become empty after filtering.
     */
    public static Flow<FlowablePartition> filterAndSkipEmpty(Flow<FlowableUnfilteredPartition> data, int nowInSec)
    {
        return data.flatMap(p -> filterAndSkipEmpty(p, nowInSec));
    }

    /**
     * Skips empty partitions. This is not terribly efficient as it has to cache the first row of the partition and
     * reconstruct the FlowableUnfilteredPartition object.
     */
    public static Flow<FlowableUnfilteredPartition> skipEmptyPartitions(Flow<FlowableUnfilteredPartition> partitions)
    {
        return partitions.flatMap(partition ->
                partition.staticRow().isEmpty() && partition.partitionLevelDeletion().isLive() ?
                partition.content.skipMapEmpty(content -> new FlowableUnfilteredPartition(partition.header, partition.staticRow, content)) :
                Flow.just(partition));
    }
    
    public static Flow<FlowablePartition> mergeAndFilter(List<Flow<FlowableUnfilteredPartition>> results,
                                                         int nowInSec,
                                                         MergeListener listener)
    {
        return filterAndSkipEmpty(mergePartitions(results, nowInSec, listener), nowInSec);
    }

    public static Flow<Row> allRows(Flow<FlowablePartition> data)
    {
        return data.flatMap(partition -> partition.content);
    }
}
