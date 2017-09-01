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
 * Base class for flow transformations that only intercepts the onNext path (e.g. map)
 *
 * Descendants need to implement onNext().
 */
public abstract class FlowTransformNext<I, O> extends FlowTransformBase<I, O>
{
    protected FlowSubscriptionRecipient subscriptionRecipient;

    protected FlowTransformNext(Flow<I> source)
    {
        super(source);
    }

    @Override
    public void requestFirst(FlowSubscriber<O> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
    {
        assert this.subscriber == null : "Flow are single-use.";
        this.subscriber = subscriber;
        this.subscriptionRecipient = subscriptionRecipient;
        sourceFlow.requestFirst(this, this);
    }

    @Override
    public void onSubscribe(FlowSubscription source)
    {
        this.source = source;
        subscriptionRecipient.onSubscribe(source);
    }

    // at least onNext and onFinal must be overridden by subclass
}
