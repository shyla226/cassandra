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
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;

import io.reactivex.Flowable;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.rows.UnfilteredRowIterators.MergeListener;
import org.apache.cassandra.db.rows.UnfilteredRowIterators.MergeReducer;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeFlowable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Mutable row container, holding partition-level information plus a Flowable<Unfiltered>
 *
 */
public class UnfilteredRowContainer
{
    public Flowable<Unfiltered> content;

    /**
     * The metadata for the table this iterator on.
     */
    public final CFMetaData metadata;

    /**
     * Whether or not the rows returned by this iterator are in reversed
     * clustering order.
     */
    public final boolean isReverseOrder;

    /**
     * A subset of the columns for the (static and regular) rows returned by this iterator.
     * Every row returned by this iterator must guarantee that it has only those columns.
     */
    public PartitionColumns columns;

    /**
     * The partition key of the partition this in an iterator over.
     */
    public final DecoratedKey partitionKey;

    /**
     * The static part corresponding to this partition (this can be an empty
     * row but cannot be {@code null}).
     */
    public Row staticRow;

    /**
     * The partition level deletion for the partition this iterate over.
     */
    public DeletionTime partitionLevelDeletion;

    /**
     * Return "statistics" about what is returned by this iterator. Those are used for
     * performance reasons (for delta-encoding for instance) and code should not
     * expect those to be exact.
     */
    public EncodingStats stats;

    public UnfilteredRowContainer(CFMetaData metadata, PartitionColumns columns, DecoratedKey partitionKey,
            boolean isReverseOrder, Row staticRow, Flowable<Unfiltered> content, DeletionTime partitionLevelDeletion,
            EncodingStats stats)
    {
        super();
        this.metadata = metadata;
        this.columns = columns;
        this.partitionKey = partitionKey;
        this.isReverseOrder = isReverseOrder;
        this.staticRow = staticRow;
        this.content = content;
        this.partitionLevelDeletion = partitionLevelDeletion;
        this.stats = stats;
    }

    public void mergeWith(List<UnfilteredRowContainer> sources, int nowInSec, MergeListener listener)
    {
        if (sources.isEmpty())
            return;

        List<Flowable<Unfiltered>> flowables = new ArrayList<>(sources.size() + 1);
        flowables.add(content);

        Row.Merger rowMerger = null;
        EncodingStats.Merger statsMerger = null;
        DeletionTime delTime = partitionLevelDeletion;
        Columns statics = columns.statics;
        Columns regulars = columns.regulars;
        int i = 1;

        for (UnfilteredRowContainer source : sources)
        {
            flowables.add(source.content);

            DeletionTime iterDeletion = source.partitionLevelDeletion;
            if (!delTime.supersedes(iterDeletion))
                delTime = iterDeletion;

            statics = statics.mergeTo(source.columns.statics);
            regulars = regulars.mergeTo(source.columns.regulars);

            if (!statics.isEmpty() && !source.staticRow.isEmpty())
            {
                if (rowMerger == null)
                {
                    rowMerger = new Row.Merger(sources.size() + 1, nowInSec, statics.size(), statics.hasComplex());
                    rowMerger.add(0, staticRow);
                }
                rowMerger.add(i, source.staticRow);
            }

            EncodingStats stats = source.stats;
            if (!stats.equals(EncodingStats.NO_STATS))
            {
                if (statsMerger == null)
                    statsMerger = new EncodingStats.Merger(this.stats);

                statsMerger.mergeWith(stats);
            }
            ++i;
        }

        if (!partitionLevelDeletion.supersedes(delTime))
        {
            if (listener != null)
            {
                DeletionTime[] versions = listener == null ? null : new DeletionTime[i];
                versions[0] = partitionLevelDeletion;

                i = 1;
                for (UnfilteredRowContainer source : sources)
                    versions[i] = source.partitionLevelDeletion;

                listener.onMergedPartitionLevelDeletion(delTime, versions);
            }
            partitionLevelDeletion = delTime;
        }

        if (statics != columns.statics || regulars != columns.regulars)
            columns = new PartitionColumns(statics, regulars);

        if (rowMerger != null)
        {
            Row merged = rowMerger.merge(partitionLevelDeletion);
            if (merged == null)
                merged = Rows.EMPTY_STATIC_ROW;
            if (listener != null)
                listener.onMergedRows(merged, rowMerger.mergedRows());
        }

        if (statsMerger != null)
            stats = statsMerger.get();

        this.content = new MergeFlowable<Unfiltered, Unfiltered>(flowables, isReverseOrder ? metadata.comparator.reversed() : metadata.comparator,
                new MergeReducer(i, isReverseOrder, delTime, regulars, nowInSec, listener));
    }

    public UnfilteredRowIterator toIterator()
    {
        Iter iter = new Iter(metadata, partitionKey, partitionLevelDeletion, columns, staticRow, isReverseOrder, stats);
        content.subscribe(iter);
        return iter;
    }

    // We have our own flowable-to-iter implementation to take care of canceling on close
    static class Iter extends AbstractUnfilteredRowIterator implements Subscriber<Unfiltered>
    {
        static final Unfiltered POISON_PILL = Rows.EMPTY_STATIC_ROW;

        Subscription subscription = null;
        BlockingQueue<Unfiltered> queue = new ArrayBlockingQueue<>(1);
        Throwable error = null;

        public Iter(CFMetaData metadata, DecoratedKey partitionKey, DeletionTime partitionLevelDeletion,
                PartitionColumns columns, Row staticRow, boolean isReverseOrder, EncodingStats stats)
        {
            super(metadata, partitionKey, partitionLevelDeletion, columns, staticRow, isReverseOrder, stats);
        }

        public void close()
        {
            subscription.cancel();
        }

        @Override
        public void onComplete()
        {
            queue.offer(POISON_PILL);
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

        @Override
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

    public static UnfilteredRowContainer fromIterator(UnfilteredRowIterator it)
    {
        return new UnfilteredRowContainer(it.metadata(), it.columns(), it.partitionKey(), it.isReverseOrder(), it.staticRow(), new FlowableFromIter<Unfiltered>(it), it.partitionLevelDeletion(), it.stats());
    }
    
    // We have our own iter-to-flowable implementation to take care of closing on cancel and to ensure single use
    static class FlowableFromIter<T> extends Flowable<T>
    {
        CloseableIterator<T> iter;

        public FlowableFromIter(CloseableIterator<T> iter)
        {
            this.iter = iter;
        }

        @Override
        protected void subscribeActual(Subscriber<? super T> subscriber)
        {
            assert iter != null;        // make sure only subscribed once
            subscriber.onSubscribe(new IterSubscription<T>(subscriber, iter));
            iter = null;
        }
    }

    static class IterSubscription<T> implements Subscription
    {
        final Subscriber<? super T> subscriber;
        final CloseableIterator<T> iter;

        public IterSubscription(Subscriber<? super T> subscriber, CloseableIterator<T> iter)
        {
            super();
            this.subscriber = subscriber;
            this.iter = iter;
        }

        @Override
        public void request(long arg0)
        {
            if (iter.hasNext())
                subscriber.onNext(iter.next());
            else
                subscriber.onComplete();
            // synchronous so exceptions will be propagated
        }

        @Override
        public void cancel()
        {
            iter.close();
        }
    }
    
    public static UnfilteredRowContainer empty(CFMetaData metadata, DecoratedKey partitionKey, boolean reversed)
    {
        return new UnfilteredRowContainer(metadata, PartitionColumns.NONE, partitionKey, reversed, Rows.EMPTY_STATIC_ROW, Flowable.empty(), DeletionTime.LIVE, EncodingStats.NO_STATS);
    }
    
    public static UnfilteredRowContainer merge(List<UnfilteredRowContainer> sources, int nowInSec)
    {
        assert !sources.isEmpty();
        UnfilteredRowContainer first = sources.get(0);
        first.mergeWith(sources.subList(1, sources.size()), nowInSec, null);
        return first;
    }
}
