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
 * Base class for flow sources that combine Flow and subscription object in the same class.
 *
 * Descendants need to implement requestNext() and close(). If the first request is special, they will also override
 * requestFirst() using subscribe().
 */
public abstract class FlowSource<T> extends Flow<T> implements FlowSubscription
{
    protected FlowSubscriber<T> subscriber;

    public void subscribe(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
    {
        assert this.subscriber == null : "Flow are single-use.";
        this.subscriber = subscriber;
        subscriptionRecipient.onSubscribe(this);
    }

    public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
    {
        subscribe(subscriber, subscriptionRecipient);
        requestNext();
    }

    @Override
    public String toString()
    {
        return formatTrace(getClass().getSimpleName());
    }
}
