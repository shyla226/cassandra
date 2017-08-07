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

import org.apache.cassandra.concurrent.StagedScheduler;
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
import org.apache.cassandra.utils.flow.FlowSubscriber;
import org.apache.cassandra.utils.flow.FlowSubscriptionRecipient;
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
            CloseableIterator<Unfiltered> iterator = Flow.toIterator(partition.content());

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
            CloseableIterator<Row> iterator = Flow.toIterator(partition.content());

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

    public static FlowableUnfilteredPartition fromIterator(UnfilteredRowIterator iter)
    {
        return new FromUnfilteredRowIterator(iter);
    }

    private static class FromUnfilteredRowIterator extends FlowableUnfilteredPartition.FlowSource
    {
        private final UnfilteredRowIterator iter;

        public FromUnfilteredRowIterator(UnfilteredRowIterator iter)
        {
            super(new PartitionHeader(iter.metadata(),
                                      iter.partitionKey(),
                                      iter.partitionLevelDeletion(),
                                      iter.columns(),
                                      iter.isReverseOrder(),
                                      iter.stats()), iter.staticRow());
            this.iter = iter;
        }

        @Override
        public void requestFirst(FlowSubscriber<Unfiltered> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
        {
            super.subscribe(subscriber, subscriptionRecipient);

            if (iter.hasNext())
                requestNext();
            else
                subscriber.onComplete();
        }

        public void requestNext()
        {
            // This is only called when we know there is a next element
            Unfiltered next = iter.next();
            if (iter.hasNext())
                subscriber.onNext(next);
            else
                subscriber.onFinal(next);
        }

        public void unused() throws Exception
        {
            close();
        }

        public void close() throws Exception
        {
            iter.close();
        }
    }

    public static FlowablePartition fromIterator(RowIterator iter, StagedScheduler callOn)
    {
        return new FromRowIterator(iter, callOn);
    }

    private static class FromRowIterator extends FlowablePartition.FlowSource implements Runnable
    {
        private final RowIterator iter;
        private final StagedScheduler callOn;

        public FromRowIterator(RowIterator iter, StagedScheduler callOn)
        {
            super(new PartitionHeader(iter.metadata(),
                                      iter.partitionKey(),
                                      DeletionTime.LIVE,
                                      iter.columns(),
                                      iter.isReverseOrder(),
                                      EncodingStats.NO_STATS), iter.staticRow());
            this.iter = iter;
            this.callOn = callOn;
        }

        @Override
        public void requestFirst(FlowSubscriber<Row> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
        {
            super.subscribe(subscriber, subscriptionRecipient);

            if (iter.hasNext())
                requestNext();
            else
                subscriber.onComplete();
        }

        public void requestNext()
        {
            if (callOn == null || callOn.isOnScheduler(Thread.currentThread()))
                run();
            else
                callOn.scheduleDirect(this, TPCTaskType.READ_FROM_ITERATOR, 0, null);
        }

        public void run()
        {
            // This is only called when we know there is a next element
            Row next = iter.next();
            if (iter.hasNext())
                subscriber.onNext(next);
            else
                subscriber.onFinal(next);
        }

        public void unused() throws Exception
        {
            close();
        }

        public void close() throws Exception
        {
            iter.close();
        }
    }

    public static FlowableUnfilteredPartition empty(TableMetadata metadata, DecoratedKey partitionKey, boolean reversed)
    {
        return FlowableUnfilteredPartition.create(PartitionHeader.empty(metadata, partitionKey, reversed),
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
            headers.add(flowable.header());
            contents.add(flowable.content());
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

        return FlowableUnfilteredPartition.create(header,
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
            hasStatic |= !source.staticRow().isEmpty();
        if (!hasStatic)
            return Rows.EMPTY_STATIC_ROW;

        Row.Merger merger = new Row.Merger(sources.size(), nowInSec, columns.size(), columns.hasComplex());
        for (int i = 0; i < sources.size(); i++)
            merger.add(i, sources.get(i).staticRow());

        Row merged = merger.merge(header.partitionLevelDeletion);

        if (merged == null)
            merged = Rows.EMPTY_STATIC_ROW;

        if (listener != null)
            listener.onMergedRows(merged, merger.mergedRows());

        return merged;
    }

    private static final Comparator<FlowableUnfilteredPartition> flowablePartitionComparator =
            Comparator.comparing(x -> x.header().partitionKey);

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
                header = current.header();
                toMerge[idx] = current;
            }

            /**
             * Merge the partitions, depending on whether there is a row merge listener we call the appropriate method,
             * see {@link #mergeAllPartitions(UnfilteredRowIterators.MergeListener)} or {@link #mergeNonEmptyPartitions()}.
             *
             * @return a merged partition
             */
            public FlowableUnfilteredPartition getReduced()
            {
                UnfilteredRowIterators.MergeListener rowListener = listener == null
                                                                   ? null
                                                                   : listener.getRowMergeListener(header.partitionKey, toMerge);
                return rowListener == null ? mergeNonEmptyPartitions() : mergeAllPartitions(rowListener);
            }

            /**
             * Merge all the partitions, creating empty partitions if some source did not produce a partition. This
             * is required because the listener expects to receive one partition per source.
             *
             * @param rowListener - the merge listener
             *
             * @return a merged partition
             */
            private FlowableUnfilteredPartition mergeAllPartitions(UnfilteredRowIterators.MergeListener rowListener)
            {
                FlowableUnfilteredPartition empty = null;

                for (int i = 0, length = toMerge.length; i < length; i++)
                {
                    FlowableUnfilteredPartition element = toMerge[i];
                    if (element == null)
                    {
                        if (empty == null)
                            empty = FlowablePartitions.empty(header.metadata, header.partitionKey, header.isReverseOrder);
                        toMerge[i] = empty;
                    }
                }

                return FlowablePartitions.merge(Arrays.asList(toMerge), nowInSec, rowListener);
            }

            /**
             * Merge non-empty partitions. If no partitions are found, then return an empty partition, otherwise only
             * merge the partitions that are available, discarding those that are null. We can only do this optimization
             * when there is no listener, see {@link #getReduced()}.
             *
             * @return a merged partition
             */
            private FlowableUnfilteredPartition mergeNonEmptyPartitions()
            {
                List<FlowableUnfilteredPartition> nonEmptyPartitions = new ArrayList<>(toMerge.length);

                for (int i = 0, length = toMerge.length; i < length; i++)
                {
                    FlowableUnfilteredPartition element = toMerge[i];
                    if (element != null)
                        nonEmptyPartitions.add(element);
                }

                if (nonEmptyPartitions.isEmpty())
                    return FlowablePartitions.empty(header.metadata, header.partitionKey, header.isReverseOrder);
                else if (nonEmptyPartitions.size() == 1)
                    return nonEmptyPartitions.get(0);
                else
                    return FlowablePartitions.merge(nonEmptyPartitions, nowInSec, null);
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

    public static Flow<FlowableUnfilteredPartition> fromPartitions(UnfilteredPartitionIterator iter)
    {
        return Flow.fromIterable(() -> iter)
                   .map(i -> fromIterator(i));
    }

    public static Flow<FlowablePartition> fromPartitions(PartitionIterator iter, StagedScheduler scheduler)
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

    private static Row filterStaticRow(Row row, int nowInSec, boolean enforceStrictLiveness)
    {
        if (row == null || row.isEmpty())
            return Rows.EMPTY_STATIC_ROW;

        row = row.purge(DeletionPurger.PURGE_ALL, nowInSec, enforceStrictLiveness);
        return row == null ? Rows.EMPTY_STATIC_ROW : row;
    }

    public static FlowablePartition filter(FlowableUnfilteredPartition data, int nowInSec)
    {
        boolean enforceStrictLiveness = data.metadata().enforceStrictLiveness();
        return FlowablePartition.create(data.header(),
                                        filterStaticRow(data.staticRow(), nowInSec, enforceStrictLiveness),
                                        filteredContent(data, nowInSec));
    }

    public static Flow<FlowablePartition> filterAndSkipEmpty(FlowableUnfilteredPartition data, int nowInSec)
    {
        Row staticRow = filterStaticRow(data.staticRow(), nowInSec, data.metadata().enforceStrictLiveness());
        Flow<Row> content = filteredContent(data, nowInSec);

        if (!staticRow.isEmpty())
            return Flow.just(FlowablePartition.create(data.header(),
                                                      staticRow,
                                                      content));
        else
            return content.skipMapEmpty(c -> FlowablePartition.create(data.header(),
                                                                      Rows.EMPTY_STATIC_ROW,
                                                                      c));
    }

    public static Flow<FlowablePartition> skipEmpty(FlowablePartition data)
    {
        return data.staticRow().isEmpty()
               ? data.content().skipMapEmpty(c -> FlowablePartition.create(data.header(), Rows.EMPTY_STATIC_ROW, c))
               : Flow.just(data);
    }

    private static Flow<Row> filteredContent(FlowableUnfilteredPartition data, int nowInSec)
    {
        return data.content().skippingMap(unfiltered -> unfiltered.isRow()
                ? ((Row) unfiltered).purge(DeletionPurger.PURGE_ALL, nowInSec, data.metadata().enforceStrictLiveness())
                : null);
    }

    /**
     * Filters the given partition stream, keeping all partitions that become empty after filtering.
     */
    public static Flow<FlowablePartition> filter(Flow<FlowableUnfilteredPartition> data, int nowInSec)
    {
        return data.map(p -> filter(p, nowInSec));
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
    public static Flow<FlowableUnfilteredPartition> skipEmptyUnfilteredPartitions(Flow<FlowableUnfilteredPartition> partitions)
    {
        return partitions.flatMap(partition ->
                partition.staticRow().isEmpty() && partition.partitionLevelDeletion().isLive() ?
                partition.content()
                         .skipMapEmpty(content -> FlowableUnfilteredPartition.create(partition.header(),
                                                                                     partition.staticRow(),
                                                                                     content)) :
                Flow.just(partition));
    }

    public static Flow<FlowablePartition> skipEmptyPartitions(Flow<FlowablePartition> partitions)
    {
        return partitions.flatMap(FlowablePartitions::skipEmpty);
    }

    public static Flow<FlowablePartition> mergeAndFilter(List<Flow<FlowableUnfilteredPartition>> results,
                                                         int nowInSec,
                                                         MergeListener listener)
    {
        return filterAndSkipEmpty(mergePartitions(results, nowInSec, listener), nowInSec);
    }

    public static Flow<Row> allRows(Flow<FlowablePartition> data)
    {
        return data.flatMap(partition -> partition.content());
    }
}
