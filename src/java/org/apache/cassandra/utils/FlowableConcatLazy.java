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
 */
public class FlowableConcatLazy<I, O> implements FlowableOperator<O, I>
{
    private static FlowableConcatLazy<?, ?> direct = new FlowableConcatLazy<Flowable<Object>, Object>(x -> x);

    public static <T> FlowableConcatLazy<Flowable<T>, T> getDirect()
    {
        return (FlowableConcatLazy<Flowable<T>, T>) direct;
    }


    final Function<I, Flowable<O>> mapper;

    public FlowableConcatLazy(Function<I, Flowable<O>> mapper)
    {
        this.mapper = mapper;
    }

    public Subscriber<I> apply(Subscriber<? super O> subscriber)
    {
        return new Concat<I, O>(subscriber, mapper);
    }

    static class Concat<I, O> implements Subscription, Subscriber<I>
    {
        private final Subscriber<? super O> subscriber;
        private final Function<I, Flowable<O>> mapper;

        Subscription source;
        long requests;
        long requested;
        boolean cancelled;
        boolean requesting;
        boolean complete;
        boolean subscribed;

        ConcatItem current;

        public Concat(Subscriber<? super O> subscriber, Function<I, Flowable<O>> mapper)
        {
            this.subscriber = subscriber;
            this.mapper = mapper;
        }

        public void request(long l)
        {
            requests = FBUtilities.add(requested, l);
            doRequests();
        }

        public void cancel()
        {
            cancelled = true;
            if (current != null)
                current.source.cancel();
        }

        // TODO: Remove synchronization:
        // -- need to make sure we can't leave loop after onXXX/request call has rejected entering loop due to 'requesting'.
        private synchronized void doRequests()
        {
            if (cancelled || requesting || requested == requests || source == null)
                return;

            requesting = true;

            loop:
            while (!cancelled && requested < requests)
            {
                if (current == null)
                {
                    if (complete)
                    {
                        subscriber.onComplete();
                        cancelled = true;
                    }
                    else
                    {
                        subscribed = false;
                        source.request(1);
                        if (!subscribed)
                            break loop;
                    }
                }
                else
                {
                    ++requested;
                    current.source.request(1);
                }
            }
            requesting = false;
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

            assert current == null;
            current = new ConcatItem();
            child.subscribe(current);
            subscribed = true;
            doRequests();
        }

        public void onError(Throwable throwable)
        {
            assert current == null;
            subscriber.onError(throwable);
        }

        public void onComplete()
        {
            complete = true;
            subscribed = true;
            doRequests();
        }

        class ConcatItem implements Subscriber<O>
        {
            Subscription source;

            public void onSubscribe(Subscription subscription)
            {
                source = subscription;
            }

            public void onNext(O next)
            {
                subscriber.onNext(next);
                doRequests();
            }

            public void onError(Throwable throwable)
            {
                subscriber.onError(throwable);
            }

            public void onComplete()
            {
                current = null;
                doRequests();
            }
        }
    }

}
