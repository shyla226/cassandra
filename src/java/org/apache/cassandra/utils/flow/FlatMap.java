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

import javax.swing.text.html.CSS;

import io.reactivex.functions.Function;
import org.apache.cassandra.utils.Throwables;

/**
 * Implementation of {@link CsFlow#flatMap(Function)}, which applies a method to a flow and concatenates the results.
 * <p>
 * This is done in depth-first fashion, i.e. one item is requested from the flow, and the result of the conversion
 * is issued to the downstream subscriber completely before requesting the next item.
 */
class FlatMap<I, O> implements CsSubscription, CsSubscriber<I>
{
    public static <I, O> CsFlow<O> flatMap(CsFlow<I> source, Function<I, CsFlow<O>> op)
    {
        class FlatMapFlow extends CsFlow<O>
        {
            public CsSubscription subscribe(CsSubscriber<O> subscriber) throws Exception
            {
                return new FlatMap<I, O>(subscriber, op, source);
            }
        }
        return new FlatMapFlow();
    }

    /**
     * The downstream subscriber which will receive the flow using the onXXXX() methods.
     */
    private final CsSubscriber<O> subscriber;

    /**
     * The mapper converts each input (upstream) item into a CsFlow of output (downstream) items
     */
    private final Function<I, CsFlow<O>> mapper;

    /**
     * Upstream subscription which will be requested to supply source items.
     */
    private final CsSubscription source;

    /**
     * If an item is active, this holds our subscription to the resulting flow.
     */
    volatile FlatMapChild current;

    FlatMap(CsSubscriber<O> subscriber, Function<I, CsFlow<O>> mapper, CsFlow<I> source) throws Exception
    {
        this.subscriber = subscriber;
        this.mapper = mapper;
        this.source = source.subscribe(this);
    }

    public void request()
    {
        if (current == null)
            source.request();
        else
            current.source.request();
    }

    public void close() throws Exception
    {
        try
        {
            if (current != null)
                current.source.close();
        }
        finally
        {
            source.close();
        }
    }

    public void onNext(I next)
    {
        if (!verify(current == null, null))
            return;

        try
        {
            CsFlow<O> child = mapper.apply(next);
            current = new FlatMapChild(child);
        }
        catch (Throwable t)
        {
            onError(t);
            return;
        }

        current.source.request();
    }

    public void onError(Throwable throwable)
    {
        if (!verify(current == null, throwable))
            return;

        subscriber.onError(throwable);
    }

    public void onComplete()
    {
        if (!verify(current == null, null))
            return;

        subscriber.onComplete();
    }

    boolean verify(boolean test, Throwable existingFail)
    {
        if (!test)
            subscriber.onError(Throwables.merge(existingFail, new AssertionError("FlatMap unexpected state\n\t" + this)));
        return test;
    }

    public String toString()
    {
        return "flatmap(" + mapper.toString() + ")\n\tsubscriber " + subscriber;
    }

    class FlatMapChild implements CsSubscriber<O>
    {
        final CsSubscription source;

        FlatMapChild(CsFlow<O> source) throws Exception
        {
            this.source = source.subscribe(this);
        }

        public void onNext(O next)
        {
            if (!verify(current == this, null))
                return;

            subscriber.onNext(next);
        }

        public void onError(Throwable throwable)
        {
            if (!verify(current == this, throwable))
                return;

            subscriber.onError(throwable);
        }

        public void onComplete()
        {
            if (!verify(current == this, null))
                return;

            try
            {
                current = null;
                source.close();
            }
            catch (Exception e)
            {
                subscriber.onError(e);
            }

            FlatMap.this.request();
            // Recursion by the above call could cause stack overflow on a long sequence of empty mappings.
            // This is not really expected to happen, but if it ends up being a concern a loop such as the one in
            // GroupOp.request() will have to be implemented.
        }

        public String toString()
        {
            return FlatMap.this.toString();
        }
    }
}
