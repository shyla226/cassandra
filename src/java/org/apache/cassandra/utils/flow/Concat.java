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
import java.util.concurrent.Callable;

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

    static <T> Flow<T> concatWith(Flow<T> source, Callable<Flow<T>> supplier)
    {
        // Implement directly rather than creating an iterable and calling concat, because this is used by short reads,
        // where most of the time the supplier will return null. This direct implementation should
        // be a little be lighter than having to create an iterable and a subsequent flat-map. It also
        // helps a bit in reading stack traces in JFR or flame graphs, since flatMap is used ubiquitously.

        return new ConcatWithFlow<>(source, supplier);
    }

    private static class ConcatWithFlow<T> extends FlowTransform<T, T>
    {
        private final Callable<Flow<T>> supplier;
        boolean completeOnRequest = false;

        ConcatWithFlow(Flow<T> source, Callable<Flow<T>> supplier)
        {
            super(source);
            this.supplier = supplier;
        }

        public void requestNext()
        {
            // here subscription should never be null, we could assert it but for perf. reasons we don't
            if (!completeOnRequest)
                source.requestNext();
            else
            {
                completeOnRequest = false;
                onComplete();
            }
        }

        public void close() throws Exception
        {
            // subscription could be null if we already closed it in OnComplete
            if (source != null)
                source.close();
        }

        public String toString()
        {
            return formatTrace("concat-with", supplier, sourceFlow);
        }

        public void onNext(T item)
        {
            subscriber.onNext(item);
        }

        public void onFinal(T item)
        {
            completeOnRequest = true;
            subscriber.onNext(item);
            // We could expand this to request the next from the supplier, but this brings a set of complications
            // (error to be signalled at next request, next supplier's requestFirst is to be called
            // the first time etc.). It's easier to just convert a final to next + complete.
        }

        public void onComplete()
        {
            try
            {
                source.close();
            }
            catch (Throwable t)
            {
                onError(t);
                return;
            }

            // set to null after calling onError so we don't lose the subscriber's stack trace but before
            // subscribing to the next one so we don't call close twice
            source = null;

            Flow<T> next;
            try
            {
                next = supplier.call();
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }
            if (next == null)
            {
                subscriber.onComplete();
                return;
            }

            // Request another item in a request loop to avoid stack overflow.
            sourceFlow = next;
            requestInLoop(requestFirstInLoop);
        }

        // A somewhat hacky method of applying the RequestLoop machinery to do requestFirst.
        // Warning: requestInLoop cannot be used to do anything else in this class.
        private final FlowSubscription requestFirstInLoop = new FlowSubscription()
        {
            public void requestNext()
            {
                ConcatWithFlow<T> us = ConcatWithFlow.this;
                sourceFlow.requestFirst(us, us);
            }

            public void close() throws Exception
            {
                // Nothing to do. This is not a real subscription.
            }
        };
    }
}
