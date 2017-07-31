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

/**
 * Base class for flow transformations that only intercept both the onNext path (e.g. map)
 *
 * Descendants need to implement onNext().
 */
public abstract class FlowTransformNext<I, O> extends Flow.RequestLoopFlow<O> implements FlowSubscriber<I>
{
    protected FlowSubscriber<O> subscriber;
    protected final FlowSubscription source;

    protected FlowTransformNext(Flow<I> source)
    {
        this.source = source.subscribe(this);
    }

    @Override
    public FlowSubscription subscribe(FlowSubscriber<O> subscriber)
    {
        assert this.subscriber == null : "Flow are single-use.";
        this.subscriber = subscriber;
        return source;  // We are not modifying requests, so let the downstream directly query our source
    }

    @Override
    public void onError(Throwable t)
    {
        subscriber.onError(t);
    }

    @Override
    public void onComplete()
    {
        subscriber.onComplete();
    }

    @Override
    public String toString()
    {
        return formatTrace(getClass().getSimpleName(), subscriber);
    }

    // at least onNext must be overridden by subclass
}
