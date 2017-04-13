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

package org.apache.cassandra.utils;

import io.reactivex.Flowable;
import io.reactivex.FlowableOperator;
import io.reactivex.functions.Function;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Lazy version of Flowable.concat. The main difference is that it never requests more data before consumer has
 * requested.
 *
 * @param <O> the value type of the output, also referred to as the downstream
 * @param <I> the value type of the input, also referred to as the upstream
 */
public class FlowableConcatLazy<I, O> implements FlowableOperator<O, I>
{
    //private static final Logger logger = LoggerFactory.getLogger(FlowableConcatLazy.class);

    /** A direct implementation of cancat lazy, that is without any mapping */
    private static final FlowableConcatLazy<?, ?> direct = new FlowableConcatLazy<Flowable<Object>, Object>(x -> x);

    public static <T> FlowableConcatLazy<Flowable<T>, T> getDirect()
    {
        return (FlowableConcatLazy<Flowable<T>, T>) direct;
    }

    private final Function<I, Flowable<O>> mapper;

    FlowableConcatLazy(Function<I, Flowable<O>> mapper)
    {
        this.mapper = mapper;
    }

    public Subscriber<I> apply(Subscriber<? super O> subscriber)
    {
        return new Concat<I, O>(subscriber, mapper);
    }

    static class Concat<I, O> implements Subscription, Subscriber<I>
    {
        /** The downstream subscriber, also known as observer, it will receive the output items by calling
         * the onXXXX() methods. */
        private final Subscriber<? super O> subscriber;

        /** The mapper converts each input (upstream) item into a flowable of output (downstream) items */
        private final Function<I, Flowable<O>> mapper;

        /** A subscription to the upstream publisher, it will emit the publishers to be
         * concatenated one by one when we call request.
         */
        Subscription source;

        /** The number of items the downstream has actually requested */
        volatile long requests;

        /** The number of items that have been requested to the sources, current or otherwise */
        volatile long requested;

        /** Set to true when the downstream cancels or when the upstream reports completed. */
        volatile boolean cancelled;

        /** Set to true when doRequests() is executing to prevent deadlocks due to onXXXX methods
         * calling doRequests recursively from a request call
         */
        volatile boolean requesting;

        /** Set to true when the upstream reports completed (no more sources) */
        volatile boolean complete;

        /** Set to false when we request a publisher to the upstream and to false when we receive it*/
        volatile boolean subscribed;

        /** Set to false when an item is requested and to true when it is received, ensures we request some more
         * if a publisher completes without items (empty).
         */
        volatile boolean received = true;

        /** The subscriber to the current source, wrapped in utility class. */
        volatile ConcatItem current;

        Concat(Subscriber<? super O> subscriber, Function<I, Flowable<O>> mapper)
        {
            this.subscriber = subscriber;
            this.mapper = mapper;
        }

        public void request(long l)
        {
            requests += l;
            if (requests < l)   // overflow, some downstream callers may invoke request(Long.MAX_VALUE)
                requests = Long.MAX_VALUE;
            doRequests();
        }

        public void cancel()
        {
            cancelled = true;
            source.cancel();
            if (current != null)
                current.source.cancel();
        }

        private void doRequests()
        {
            synchronized (this)
            {
                if (requesting || cancelled || requested == requests || source == null)
                    return;

                requesting = true;
            }

            loop:
            while (true)
            {
                ConcatItem cur = current;
                if (cur == null)
                { // no current source, request one
                    if (complete)
                    {
                        subscriber.onComplete();
                        cancelled = true;
                        return;
                    }
                    else
                    {
                        subscribed = false;
                        source.request(1);
                        synchronized (this)
                        {
                            if (!subscribed || cancelled || requested == requests)
                            {
                                requesting = false;
                                return;
                            }
                        }
                    }
                }
                else
                { // request the next item to the current source and increment the number of items requested
                    ++requested;
                    received = false;
                    // We may receive onComplete from child between getting cur and being here, or even while we
                    // are calling request, so we can't really not do requests after we have received completion.
                    // That's ok; we only need to make sure our flowables are OK with being requested after (or while)
                    // they have sent onComplete.
                    cur.source.request(1);
                    synchronized (this)
                    {
                        // Check if current completed while we were performing the request.
                        cur = current;
                        if (cur == null && !received)
                        {
                            // If so, and the request did not result in an onNext or onComplete (possibly because
                            // onComplete was racing with us getting cur and making decision based on it), we need to
                            // get another item. However, it could call onComplete later or in a race with this.
                            received = true;
                            ++requests;
                        }
                        else if (!received || cancelled || requested == requests)
                        {
                            requesting = false;
                            return;
                        }
                    }
                }
            }
        }

        public void onSubscribe(Subscription subscription)
        {
            source = subscription;
            subscriber.onSubscribe(this);
        }

        public void onNext(I next)
        {
            Flowable<O> child;
            try
            {
                child = mapper.apply(next);
            }
            catch (Throwable t)
            {
                onError(t);
                return;
            }

            ConcatItem item = new ConcatItem();
            child.subscribe(item);
            // onSubscribe will call doRequests
        }

        public void onError(Throwable throwable)
        {
            assert current == null;
            subscriber.onError(throwable);
        }

        public void onComplete()
        {
            complete = true;
            if (!subscribed)
            {
                // This was in response to a request that did not get onNext. We need to pass on the onComplete.
                subscribed = true;
                doRequests();
            }
        }

        class ConcatItem implements Subscriber<O>
        {
            Subscription source;

            public void onSubscribe(Subscription subscription)
            {
                assert current == null;
                source = subscription;
                current = this;
                subscribed = true;
                doRequests();
            }

            public void onNext(O next)
            {
                received = true;
                subscriber.onNext(next);
                doRequests();
            }

            public void onError(Throwable throwable)
            {
                Concat.this.source.cancel();
                subscriber.onError(throwable);
            }

            public void onComplete()
            {
                boolean needsRequest = false;
                synchronized (Concat.this)  // synchronized to prevent issuing a request twice here and in doRequests
                {
                    current = null;
                    if (!received)
                    {
                        received = true;
                        needsRequest = true;
                    }
                }
                if (needsRequest)
                    request(1);
            }
        }
    }

}
