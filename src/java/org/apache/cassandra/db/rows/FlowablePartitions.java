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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FlowableUtils;
import org.apache.cassandra.utils.MergeFlowable;
import org.apache.cassandra.utils.Reducer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Partition manipulation functions.
 *
 */
public class FlowablePartitions
{
    public static UnfilteredRowIterator toIterator(FlowableUnfilteredPartition source)
    {
        IteratorSubscription subscr = new IteratorSubscription(source.header, source.staticRow.blockingGet());
        source.content.subscribe(subscr);
        return subscr;
    }

    static class IteratorSubscription extends AbstractUnfilteredRowIterator implements Subscriber<Unfiltered>
    {
        static final Unfiltered POISON_PILL = Rows.EMPTY_STATIC_ROW;

        Subscription subscription = null;
        BlockingQueue<Unfiltered> queue = new ArrayBlockingQueue<>(3);  // onComplete comes with next() sometimes, leave one more for onError
        Throwable error = null;

        public IteratorSubscription(PartitionHeader header, Row staticRow)
        {
            super(header.metadata, header.partitionKey, header.partitionLevelDeletion, header.columns, staticRow, header.isReverseOrder, header.stats);
        }

        public void close()
        {
            subscription.cancel();
        }

        @Override
        public void onComplete()
        {
            Uninterruptibles.putUninterruptibly(queue, POISON_PILL);
        }

        @Override
        public void onError(Throwable arg0)
        {
            error = arg0;
            Uninterruptibles.putUninterruptibly(queue, POISON_PILL);
        }

        @Override
        public void onNext(Unfiltered arg0)
        {
            Uninterruptibles.putUninterruptibly(queue, arg0);
        }

        @Override
        public void onSubscribe(Subscription arg0)
        {
            assert subscription == null;
            subscription = arg0;
        }

        protected Unfiltered computeNext()
        {
            Unfiltered next = queue.poll();
            if (error != null)
                Throwables.propagate(error);
            if (next != null)
                return next == POISON_PILL ? endOfData() : next;

            subscription.request(1);

            next = Uninterruptibles.takeUninterruptibly(queue);
            if (error != null)
                Throwables.propagate(error);
            return next == POISON_PILL ? endOfData() : next;
        }
    }

    public static FlowableUnfilteredPartition fromIterator(UnfilteredRowIterator iter, Scheduler callOn)
    {
        Flowable<Unfiltered> data = FlowableUtils.fromCloseableIterator(iter);
        if (callOn != null)
            data = data.subscribeOn(callOn);
        Row staticRow = iter.staticRow();
        return new FlowableUnfilteredPartition(new PartitionHeader(iter.metadata(), iter.partitionKey(), iter.partitionLevelDeletion(), iter.columns(), iter.isReverseOrder(), iter.stats()),
                                               staticRow.isEmpty() ? Rows.EMPTY_STATIC_ROW_SINGLE : Single.just(staticRow),
                                               data);
    }

    public static FlowableUnfilteredPartition empty(TableMetadata metadata, DecoratedKey partitionKey, boolean reversed)
    {
        return new FlowableUnfilteredPartition(new PartitionHeader(metadata, partitionKey, DeletionTime.LIVE, RegularAndStaticColumns.NONE, reversed, EncodingStats.NO_STATS),
                                               Rows.EMPTY_STATIC_ROW_SINGLE,
                                               Flowable.empty());
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

        Single<Row> staticRow;
        if (!header.columns.statics.isEmpty())
            staticRow = Single.zip(ImmutableList.copyOf(Lists.transform(flowables, x -> x.staticRow)),
                                   list -> mergeStaticRows(list, header.partitionLevelDeletion, header.columns.statics, nowInSec));
        else
            staticRow = Rows.EMPTY_STATIC_ROW_SINGLE;

        Comparator<Clusterable> comparator = header.metadata.comparator;
        if (header.isReverseOrder)
            comparator = comparator.reversed();

        return new FlowableUnfilteredPartition(
                header,
                staticRow,
                new MergeFlowable<>(ImmutableList.copyOf(Lists.transform(flowables, x -> x.content)),
                                    comparator,
                                    reducer));
    }

    public static Row mergeStaticRows(Object[] sources, DeletionTime activeDeletion, Columns columns, int nowInSec)
    {
        Row.Merger rowMerger = new Row.Merger(sources.length, nowInSec, columns.size(), columns.hasComplex());
        int i = 0;
        for (Object source : sources)
            rowMerger.add(i++, (Row) source);
        Row merged = rowMerger.merge(activeDeletion);

        if (merged == null)
            return Rows.EMPTY_STATIC_ROW;
        else
            return merged;
    }

    private static final Comparator<FlowableUnfilteredPartition> flowablePartitionComparator = (x, y) -> x.header.partitionKey.compareTo(y.header.partitionKey);

    public static Flowable<FlowableUnfilteredPartition> mergePartitions(final List<? extends Flowable<FlowableUnfilteredPartition>> sources, final int nowInSec)
    {
        return MergeFlowable.get(sources, flowablePartitionComparator, new Reducer<FlowableUnfilteredPartition, FlowableUnfilteredPartition>()
        {
            private final List<FlowableUnfilteredPartition> toMerge = new ArrayList<>(sources.size());

            public void reduce(int idx, FlowableUnfilteredPartition current)
            {
                toMerge.add(current);
            }

            protected FlowableUnfilteredPartition getReduced()
            {
                return FlowablePartitions.merge(toMerge, nowInSec);
            }

            protected void onKeyChange()
            {
                toMerge.clear();
            }
        });
    }

    public static Flowable<FlowableUnfilteredPartition> fromPartitions(UnfilteredPartitionIterator iter, Scheduler scheduler)
    {
        Flowable<FlowableUnfilteredPartition> flowable = FlowableUtils.fromCloseableIterator(iter)
                                                                      .map(i -> fromIterator(i, scheduler));
        if (scheduler != null)
            flowable.subscribeOn(scheduler);
        return flowable;
    }

    public static UnfilteredPartitionIterator toPartitions(Flowable<FlowableUnfilteredPartition> source, TableMetadata metadata)
    {
        UPartitionsSubscription subscr = new UPartitionsSubscription(metadata);
        source.subscribe(subscr);
        return subscr;
    }

    static class UPartitionsSubscription extends AbstractUnfilteredPartitionIterator implements Subscriber<FlowableUnfilteredPartition>
    {
        static final FlowableUnfilteredPartition POISON_PILL = empty(null, null, false);

        Subscription subscription = null;
        BlockingQueue<FlowableUnfilteredPartition> queue = new ArrayBlockingQueue<>(3);  // onComplete comes with next() sometimes, leave one more for onError
        Throwable error = null;
        FlowableUnfilteredPartition next = null;
        TableMetadata metadata = null;

        UPartitionsSubscription(TableMetadata metadata)
        {
            this.metadata = metadata;
        }

        public void close()
        {
            subscription.cancel();
        }

        @Override
        public void onComplete()
        {
            Uninterruptibles.putUninterruptibly(queue, POISON_PILL);
        }

        @Override
        public void onError(Throwable arg0)
        {
            error = arg0;
            Uninterruptibles.putUninterruptibly(queue, POISON_PILL);
        }

        @Override
        public void onNext(FlowableUnfilteredPartition arg0)
        {
            Uninterruptibles.putUninterruptibly(queue, arg0);
        }

        @Override
        public void onSubscribe(Subscription arg0)
        {
            assert subscription == null;
            subscription = arg0;
        }

        protected FlowableUnfilteredPartition computeNext()
        {
            if (next != null)
                return next;

            next = queue.poll();
            if (error != null)
                Throwables.propagate(error);
            if (next != null)
                return next;

            subscription.request(1);

            next = Uninterruptibles.takeUninterruptibly(queue);
            if (error != null)
                Throwables.propagate(error);
            return next;
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean hasNext()
        {
            return computeNext() != POISON_PILL;
        }

        @Override
        public UnfilteredRowIterator next()
        {
            boolean has = hasNext();
            assert has;
            FlowableUnfilteredPartition toReturn = next;
            next = null;
            return toIterator(toReturn);
        }
    }

    public static FlowablePartition filter(FlowableUnfilteredPartition data, int nowInSec)
    {
        return new FlowablePartition(data.header,
                                     data.staticRow,
                                     data.content.ofType(Row.class)
                                                 .map(r -> r.purge(DeletionPurger.PURGE_ALL, nowInSec))
                                                 .filter(x -> x != null));
    }

    public static Flowable<FlowablePartition> filter(Flowable<FlowableUnfilteredPartition> data, int nowInSec)
    {
        return data.map(p -> filter(p, nowInSec)); // TODO: can we filter empty ones?
    }
}
