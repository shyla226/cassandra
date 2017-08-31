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
 * Base class for FlowTransformX.
 */
public abstract class FlowTransformBase<I, O> extends Flow.RequestLoopFlow<O> implements FlowSubscriber<I>
{
    protected Flow<I> sourceFlow;
    protected FlowSubscriber<O> subscriber;
    protected FlowSubscription source;

    public FlowTransformBase(Flow<I> source)
    {
        this.sourceFlow = source;
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
        return formatTrace(getClass().getSimpleName(), sourceFlow);
    }
}
