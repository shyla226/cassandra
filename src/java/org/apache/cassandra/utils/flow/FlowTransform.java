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
 * Base class for flow transformations that need to intercept both the request and onNext paths (e.g. group)
 *
 * Descendants need to implement onNext() and most probably requestFirst() and requestNext().
 */
public abstract class FlowTransform<I, O> extends FlowTransformBase<I, O> implements FlowSubscription
{
    protected FlowTransform(Flow<I> source)
    {
        super(source);
    }

    @Override
    public void requestFirst(FlowSubscriber<O> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
    {
        assert this.subscriber == null : "Flow are single-use.";
        this.subscriber = subscriber;
        subscriptionRecipient.onSubscribe(this);
        sourceFlow.requestFirst(this, this);
    }

    @Override
    public void onSubscribe(FlowSubscription source)
    {
        this.source = source;
    }

    @Override
    public void requestNext()
    {
        source.requestNext();
    }

    @Override
    public void close() throws Exception
    {
        source.close();
    }
}
