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

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;

import io.reactivex.Flowable;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeFlowable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Mutable row container, holding partition-level information plus a Flowable<Unfiltered>
 *
 */
public class FlowableUnfilteredRows
{
    public static UnfilteredRowIterator toIterator(Flowable<Unfiltered> source)
    {
        IteratorSubscription subscr = new IteratorSubscription();
        source.subscribe(subscr);
        return subscr.iterator();
    }
    
    static class IteratorSubscription implements Subscriber<Unfiltered>
    {
        static final Unfiltered POISON_PILL = Rows.EMPTY_STATIC_ROW;

        Subscription subscription = null;
        BlockingQueue<Unfiltered> queue = new ArrayBlockingQueue<>(3);  // onComplete comes with next() sometimes, leave one more for onError
        Throwable error = null;

        public void close()
        {
            subscription.cancel();
        }

        public UnfilteredRowIterator iterator()
        {
            PartitionHeader header = (PartitionHeader) getNext();
            assert header != null;
            Row staticRow = (Row) getNext();
            return new Iter(header, staticRow, this);
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

        protected Unfiltered getNext()
        {
            Unfiltered next = queue.poll();
            if (error != null)
                Throwables.propagate(error);
            if (next != null)
                return next == POISON_PILL ? null : next;

            subscription.request(1);

            next = Uninterruptibles.takeUninterruptibly(queue);
            if (error != null)
                Throwables.propagate(error);
            return next == POISON_PILL ? null : next;
        }
    }

    // We have our own flowable-to-iter implementation to take care of canceling on close
    static class Iter extends AbstractUnfilteredRowIterator
    {
        final IteratorSubscription subscr;
        public Iter(PartitionHeader header, Row staticRow,
                IteratorSubscription subscr)
        {
            super(header.metadata, header.partitionKey, header.partitionLevelDeletion, header.columns, staticRow, header.isReverseOrder, header.stats);
            this.subscr = subscr;
        }

        @Override
        protected Unfiltered computeNext()
        {
            Unfiltered v = subscr.getNext();
            return v != null ? v : endOfData();
        }

    }

    public static Flowable<Unfiltered> fromIterator(UnfilteredRowIterator it)
    {
        return new FlowableFromIter<Unfiltered>(it, new PartitionHeader(it.metadata(), it.partitionKey(), it.partitionLevelDeletion(), it.columns(), it.isReverseOrder(), it.stats()), it.staticRow());
    }

    // We have our own iter-to-flowable implementation to take care of closing on cancel and to ensure single use
    static class FlowableFromIter<T> extends Flowable<T>
    {
        CloseableIterator<T> iter;
        T[] prepend;

        @SafeVarargs
        public FlowableFromIter(CloseableIterator<T> iter, T... prepend)
        {
            this.iter = iter;
            this.prepend = prepend;
        }

        @Override
        protected void subscribeActual(Subscriber<? super T> subscriber)
        {
            assert iter != null;        // make sure only subscribed once
            subscriber.onSubscribe(new IterSubscription<T>(subscriber, iter, prepend));
            iter = null;
        }
    }

    static class IterSubscription<T> implements Subscription
    {
        final Subscriber<? super T> subscriber;
        final CloseableIterator<T> iter;
        final T[] prepend;
        int pos = 0;

        public IterSubscription(Subscriber<? super T> subscriber, CloseableIterator<T> iter, T[] prepend)
        {
            super();
            this.subscriber = subscriber;
            this.iter = iter;
            this.prepend = prepend;
        }

        @Override
        public void request(long count)
        {
            while (--count >= 0)
            {
                if (pos < prepend.length)
                    subscriber.onNext(prepend[pos++]);
                else if (iter.hasNext())
                    subscriber.onNext(iter.next());
                else
                    subscriber.onComplete();
                // synchronous so exceptions will be propagated
            }
        }

        @Override
        public void cancel()
        {
            iter.close();
        }
    }
    
    public static Flowable<Unfiltered> empty(CFMetaData metadata, DecoratedKey partitionKey, boolean reversed)
    {
        return Flowable.just(new PartitionHeader(metadata, partitionKey, DeletionTime.LIVE, PartitionColumns.NONE, reversed, EncodingStats.NO_STATS),
                             Rows.EMPTY_STATIC_ROW);
    }
    
    public static Flowable<Unfiltered> merge(List<Flowable<Unfiltered>> flowables, int nowInSec)
    {
        assert !flowables.isEmpty();
//        if (flowables.size() == 1)
//            return flowables.get(0);

        MergeReducer reducer = new MergeReducer(flowables.size(), nowInSec, null);
        return new MergeFlowable<Unfiltered, Unfiltered>(flowables,
                reducer,
                reducer);
    }
}
