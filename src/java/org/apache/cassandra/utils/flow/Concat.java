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

package org.apache.cassandra.utils.flow;

import java.util.Arrays;
import java.util.function.Supplier;

import org.apache.cassandra.utils.Throwables;

/**
 * Implementation of methods relating to the concatenation of flows.
 */
class Concat
{
    static <T> Flow<T> concat(Flow<Flow<T>> source)
    {
        return source.flatMap(x -> x);
    }

    static <T> Flow<T> concat(Iterable<Flow<T>> sources)
    {
        return concat(Flow.fromIterable(sources));
    }

    static <O> Flow<O> concat(Flow<O>[] sources)
    {
        return concat(Arrays.asList(sources));
    }

    static <T> Flow<T> concatWith(Flow<T> source, Supplier<Flow<T>> supplier)
    {
        // Implement directly rather than creating an iterable and calling concat, because this is used by short reads,
        // where most of the time the supplier will return null. This direct implementation should
        // be a little be lighter than having to create an iterable and a subsequent flat-map. It also
        // helps a bit in reading stack traces in JFR or flame graphs, since flatMap is used ubiquitously.

        return new ConcatWithFlow<>(source, supplier);
    }

    private static class ConcatWithFlow<T> extends Flow.RequestLoopFlow<T> implements FlowSubscription, FlowSubscriber<T>
    {
        private final Supplier<Flow<T>> supplier;
        private FlowSubscriber<T> subscriber;
        private FlowSubscription subscription;

        ConcatWithFlow(Flow<T> source, Supplier<Flow<T>> supplier)
        {
            this.supplier = supplier;
            this.subscription = source.subscribe(this);
        }

        public FlowSubscription subscribe(FlowSubscriber<T> subscriber)
        {
            assert this.subscriber == null : "Flow are single-use.";
            this.subscriber = subscriber;
            return this;
        }

        public void request()
        {
            // here subscription should never be null, we could assert it but for perf. reasons we don't
            subscription.request();
        }

        public void close() throws Exception
        {
            // subscription could be null if we already closed it in OnComplete
            if (subscription != null)
                subscription.close();
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            // subscription could be null if we already closed it in OnComplete
            return subscription == null ? throwable : subscription.addSubscriberChainFromSource(throwable);
        }

        public String toString()
        {
            return formatTrace("concat-with", subscriber);
        }

        public void onNext(T item)
        {
            subscriber.onNext(item);
        }

        public void onComplete()
        {
            Throwable err = Throwables.perform((Throwable)null, subscription::close);
            if (err != null)
            {
                onError(err);
                return;
            }

            // set to null after calling onError so we don't lose the subscriber's stack trace but before
            // subscribing to the next one so we don't call close twice
            subscription = null;

            Flow<T> next = supplier.get();
            if (next == null)
            {
                subscriber.onComplete();
                return;
            }

            err = Throwables.perform((Throwable)null, () -> subscription = next.subscribe(this));
            if (err != null)
            {
                onError(err);
                return;
            }

            // Request another child in a request loop to avoid stack overflow.
            // 'subscription' may change during the loop, so do the loop on 'this' instead.
            requestInLoop(this);
        }

        public void onError(Throwable t)
        {
            subscriber.onError(addSubscriberChainFromSource(t));
        }
    }

}
