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

import io.reactivex.functions.Function;
import org.apache.cassandra.utils.Throwables;

/**
 * Implementation of {@link Flow#flatMap(FlatMapper)}, which applies a method to a flow and concatenates the results.
 * <p>
 * This is done in depth-first fashion, i.e. one item is requested from the flow, and the result of the conversion
 * is issued to the downstream subscriber completely before requesting the next item.
 */
public class FlatMap<I, O> extends Flow.RequestLoop implements FlowSubscription, FlowSubscriber<I>
{
    public static <I, O> Flow<O> flatMap(Flow<I> source, FlatMapper<I, O> op)
    {
        class FlatMapFlow extends Flow<O>
        {
            public FlowSubscription subscribe(FlowSubscriber<O> subscriber) throws Exception
            {
                return new FlatMap<I, O>(subscriber, op, source);
            }
        }
        return new FlatMapFlow();
    }

    /**
     * The downstream subscriber which will receive the flow using the onXXXX() methods.
     */
    private final FlowSubscriber<O> subscriber;


    /**
     * The mapper converts each input (upstream) item into a Flow of output (downstream) items
     */
    private final FlatMapper<I, O> mapper;

    /**
     * Upstream subscription which will be requested to supply source items.
     */
    private final FlowSubscription source;

    /**
     * If an item is active, this holds our subscription to the resulting flow.
     */
    volatile FlatMapChild current;

    FlatMap(FlowSubscriber<O> subscriber, FlatMapper<I, O> mapper, Flow<I> source) throws Exception
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

    public Throwable addSubscriberChainFromSource(Throwable throwable)
    {
        return source.addSubscriberChainFromSource(throwable);
    }

    public void onNext(I next)
    {
        if (!verify(current == null, null))
            return;

        try
        {
            Flow<O> child = mapper.apply(next);
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
        return Flow.formatTrace("flatMap", mapper, subscriber);
    }

    class FlatMapChild implements FlowSubscriber<O>
    {
        final FlowSubscription source;

        FlatMapChild(Flow<O> source) throws Exception
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

            // We have to request another child; since there is a risk of multiple empty children being returned
            // and causing stack exhaustion, perform this request in a loop.
            requestInLoop(FlatMap.this);
        }

        public String toString()
        {
            return FlatMap.this.toString();
        }
    }

    public interface FlatMapper<I, O> extends Function<I, Flow<O>>
    {
    }
}
