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

import static org.apache.cassandra.utils.flow.CsFlow.formatTrace;

/**
 * Implementation of methods relating to the concatenation of flows.
 */
class Concat
{
    static <T> CsFlow<T> concat(CsFlow<CsFlow<T>> source)
    {
        return source.flatMap(x -> x);
    }

    static <T> CsFlow<T> concat(Iterable<CsFlow<T>> sources)
    {
        return concat(CsFlow.fromIterable(sources));
    }

    static <O> CsFlow<O> concat(CsFlow<O>[] sources)
    {
        return concat(Arrays.asList(sources));
    }

    static <T> CsFlow<T> concatWith(CsFlow<T> source, Supplier<CsFlow<T>> supplier)
    {
        // Implement directly rather than creating an iterable and calling concat, because this is used by short reads,
        // where most of the time the supplier will return null. This direct implementation should
        // be a little be lighter than having to create an iterable and a subsequent flat-map. It also
        // helps a bit in reading stack traces in JFR or flame graphs, since flatMap is used ubiquitously.

        return new CsFlow<T>()
        {
            public CsSubscription subscribe(CsSubscriber<T> subscriber) throws Exception
            {
                return new ConcatWithSubscription<>(source, supplier, subscriber);
            }
        };
    }

    private static class ConcatWithSubscription<T> extends CsFlow.RequestLoop implements CsSubscription, CsSubscriber<T>
    {
        private final Supplier<CsFlow<T>> supplier;
        private final CsSubscriber<T> subscriber;
        private CsSubscription subscription;

        ConcatWithSubscription(CsFlow<T> source, Supplier<CsFlow<T>> supplier, CsSubscriber<T> subscriber) throws Exception
        {
            this.supplier = supplier;
            this.subscriber = subscriber;
            this.subscription = source.subscribe(this);
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

            CsFlow<T> next = supplier.get();
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
