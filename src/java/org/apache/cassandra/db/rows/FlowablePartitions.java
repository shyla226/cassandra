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
import java.util.stream.Collectors;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Scheduler;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.flow.CsFlow;
import org.apache.cassandra.utils.flow.CsSubscriber;
import org.apache.cassandra.utils.flow.CsSubscription;
import org.apache.cassandra.utils.flow.Threads;

/**
 * Partition manipulation functions.
 *
 */
public class FlowablePartitions
{
    private final static Logger logger = LoggerFactory.getLogger(FlowablePartitions.class);

    public static UnfilteredRowIterator toIterator(FlowableUnfilteredPartition source)
    {
        try
        {
            return new IteratorSubscription(source.header, source.staticRow, source.content);
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }

    static class IteratorSubscription extends AbstractUnfilteredRowIterator implements CsSubscriber<Unfiltered>
    {
        static final Unfiltered POISON_PILL = Rows.EMPTY_STATIC_ROW;

        final CsSubscription subscription;
        BlockingQueue<Unfiltered> queue = new ArrayBlockingQueue<>(1);
        Throwable error = null;

        public IteratorSubscription(PartitionHeader header, Row staticRow, CsFlow<Unfiltered> source) throws Exception
        {
            super(header.metadata, header.partitionKey, header.partitionLevelDeletion, header.columns, staticRow, header.isReverseOrder, header.stats);
            subscription = source.subscribe(this);
        }

        public void close()
        {
            try
            {
                subscription.close();
            }
            catch (Exception e)
            {
                throw Throwables.propagate(e);
            }
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

        protected Unfiltered computeNext()
        {
            assert queue.isEmpty();
            subscription.request();

            Unfiltered next = Uninterruptibles.takeUninterruptibly(queue);
            if (error != null)
                throw Throwables.propagate(error);
            return next == POISON_PILL ? endOfData() : next;
        }
    }

    public static class EmptyFlowableUnfilteredPartition extends FlowableUnfilteredPartition
    {
        public EmptyFlowableUnfilteredPartition(PartitionHeader header)
        {
            super(header, Rows.EMPTY_STATIC_ROW, CsFlow.empty(), false);
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

    private static final Comparator<FlowableUnfilteredPartition> flowablePartitionComparator = Comparator.comparing(x -> x.header.partitionKey);

    public static CsFlow<FlowableUnfilteredPartition> mergePartitions(final List<CsFlow<FlowableUnfilteredPartition>> sources, final int nowInSec)
    {
        assert !sources.isEmpty();
        if (sources.size() == 1)
            return sources.get(0);

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

    public static UnfilteredPartitionIterator toPartitions(CsFlow<FlowableUnfilteredPartition> source, TableMetadata metadata)
    {
        try
        {
            return new UPartitionsSubscription(metadata, source);
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }

    static class UPartitionsSubscription extends AbstractUnfilteredPartitionIterator implements CsSubscriber<FlowableUnfilteredPartition>
    {
        static final FlowableUnfilteredPartition POISON_PILL = empty(null, null, false);

        final CsSubscription subscription;
        BlockingQueue<FlowableUnfilteredPartition> queue = new ArrayBlockingQueue<>(1);
        Throwable error = null;
        FlowableUnfilteredPartition next = null;
        TableMetadata metadata = null;
        StackTraceElement[] stackTrace;

        UPartitionsSubscription(TableMetadata metadata, CsFlow<FlowableUnfilteredPartition> source) throws Exception
        {
            this.metadata = metadata;
            subscription = source.subscribe(this);
            this.stackTrace = CsFlow.maybeGetStackTrace();
        }

        public void close()
        {
            try
            {
                subscription.close();
            }
            catch (Exception e)
            {
                throw Throwables.propagate(e);
            }
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

        protected FlowableUnfilteredPartition computeNext()
        {
            if (next != null)
                return next;

            assert queue.isEmpty();
            subscription.request();

            next = Uninterruptibles.takeUninterruptibly(queue);
            if (error != null)
                throw Throwables.propagate(error);
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

        public String toString()
        {
            return "toPartitions with metadata " + metadata().toString() + CsFlow.stackTraceString(stackTrace);
        }
    }

    public static FlowablePartition filter(FlowableUnfilteredPartition data, int nowInSec)
    {
        return new FlowablePartition(data.header,
                                     data.staticRow,
                                     data.content.skippingMap(unfiltered -> unfiltered.isRow()
                                                                            ? ((Row) unfiltered).purge(DeletionPurger.PURGE_ALL, nowInSec)
                                                                            : null));
    }

    public static CsFlow<FlowablePartition> filter(CsFlow<FlowableUnfilteredPartition> data, int nowInSec)
    {
        return data.map(p -> filter(p, nowInSec)); // tpc TODO: can we filter empty ones?
    }
}
