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
public class FlatMap<I, O> extends Flow.RequestLoopFlow<O> implements FlowSubscription, FlowSubscriber<I>
{
    public static <I, O> Flow<O> flatMap(Flow<I> source, FlatMapper<I, O> op)
    {
        return new FlatMap(op, source);
    }

    /**
     * The downstream subscriber which will receive the flow using the onXXXX() methods.
     */
    private FlowSubscriber<O> subscriber;


    /**
     * The mapper converts each input (upstream) item into a Flow of output (downstream) items
     */
    private final FlatMapper<I, O> mapper;

    /**
     * Upstream subscription which will be requested to supply source items.
     */
    FlowSubscription source;
    private final Flow<I> sourceFlow;

    /**
     * If an item is active, this holds our subscription to the resulting flow.
     */
    private final FlatMapChild child = new FlatMapChild();

    FlatMap(FlatMapper<I, O> mapper, Flow<I> source)
    {
        this.mapper = mapper;
        this.sourceFlow = source;
    }

    public void requestFirst(FlowSubscriber<O> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
    {
        assert this.subscriber == null : "Flow are single-use.";
        this.subscriber = subscriber;
        subscriptionRecipient.onSubscribe(this);

        sourceFlow.requestFirst(this, this);
    }

    public void onSubscribe(FlowSubscription source)
    {
        this.source = source;
    }

    public void requestNext()
    {
        if (child.isActive())
            child.requestNext();
        else
            source.requestNext();
    }

    public void close() throws Exception
    {
        try
        {
            if (child.isActive())
                child.source.close();
        }
        finally
        {
            source.close();
        }
    }

    public void onNext(I next)
    {
        if (!verify(!child.isActive(), null))
            return;

        Flow<O> flow;
        try
        {
            flow = mapper.apply(next);
        }
        catch (Throwable t)
        {
            onError(t);
            return;
        }

        child.requestFirst(flow);
    }

    public void onError(Throwable throwable)
    {
        if (!verify(!child.isActive(), throwable))
            return;

        subscriber.onError(throwable);
    }

    public void onComplete()
    {
        if (!verify(!child.isActive(), null))
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
        return Flow.formatTrace("flatMap", mapper, sourceFlow);
    }

    class FlatMapChild implements FlowSubscriber<O>
    {
        FlowSubscription source;

        FlatMapChild()
        {
            source = null;
        }

        boolean isActive()
        {
            return source != null;
        }

        void requestFirst(Flow<O> source)
        {
            source.requestFirst(this, this);
        }

        void requestNext()
        {
            source.requestNext();
        }

        public void onSubscribe(FlowSubscription source)
        {
            if (!verify(this.source == null, null))
                return;

            this.source = source;
        }

        public void onNext(O next)
        {
            if (!verify(source != null, null))
                return;

            subscriber.onNext(next);
        }

        public void onError(Throwable throwable)
        {
            if (!verify(source != null, throwable))
                return;

            subscriber.onError(throwable);
        }

        public void onComplete()
        {
            if (!verify(source != null, null))
                return;

            try
            {
                source.close();
            }
            catch (Exception e)
            {
                subscriber.onError(e);
            }
            source = null;

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
