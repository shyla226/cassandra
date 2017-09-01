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

/**
 * Functionality for skipping over empty flows.
 * For usage example, see
 * {@link org.apache.cassandra.db.filter.RowFilter.CQLFilter#filter(Flow, org.apache.cassandra.schema.TableMetadata, int)}
 */
public class SkipEmpty
{
    public static <T> Flow<Flow<T>> skipEmpty(Flow<T> flow)
    {
        return new SkipEmptyContent<>(flow, x -> x);
    }

    public static <T, U> Flow<U> skipMapEmpty(Flow<T> flow, Function<Flow<T>, U> mapper)
    {
        return new SkipEmptyContent<>(flow, mapper);
    }

    /**
     * This is both flow and subscription. Done this way as we can only subscribe to these implementations once
     * and thus it doesn't make much sense to create subscription-specific instances.
     */
    static class SkipEmptyContent<T, U> extends Flow<U>
    {
        final Function<Flow<T>, U> mapper;
        FlowSubscriber<U> subscriber;
        final SkipEmptyContentSubscriber<T> child;

        public SkipEmptyContent(Flow<T> content, Function<Flow<T>, U> mapper)
        {
            this.mapper = mapper;
            child = new SkipEmptyContentSubscriber(content, this);
        }

        public void requestFirst(FlowSubscriber<U> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
        {
            assert this.subscriber == null : "Flow are single-use.";
            this.subscriber = subscriber;

            subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
            child.start();
        }

        void onContent(Flow<T> child)
        {
            U result;
            try
            {
                result = mapper.apply(child);
            }
            catch (Exception e)
            {
                onError(e);
                return;
            }

            subscriber.onFinal(result);
        }

        void onEmpty()
        {
            subscriber.onComplete();
        }

        void onError(Throwable e)
        {
            subscriber.onError(e);
        }

        public String toString()
        {
            return Flow.formatTrace("skipEmpty", mapper, child.sourceFlow);
        }
    }

    /**
     * This is both flow and subscription. Done this way as we can only subscribe to these implementations once
     * and thus it doesn't make much sense to create subscription-specific instances.
     */
    private static class SkipEmptyContentSubscriber<T> extends FlowTransformNext<T, T>
    {
        final SkipEmptyContent parent;
        T first = null;
        boolean firstIsFinal = false;

        public SkipEmptyContentSubscriber(Flow<T> content,
                                          SkipEmptyContent parent)
        {
            super(content);
            this.parent = parent;
        }

        void start()
        {
            sourceFlow.requestFirst(this, this);
        }

        public void onSubscribe(FlowSubscription source)
        {
            this.source = source;
        }

        public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
        {
            assert first != null;
            assert source != null;
            assert this.subscriber == null : "Flow are single-use.";
            this.subscriber = subscriber;
            subscriptionRecipient.onSubscribe(source);  // subscribe direct, we only modify firstvalue
            T toReturn = first;
            first = null;
            if (firstIsFinal)
                subscriber.onFinal(toReturn);
            else
                subscriber.onNext(toReturn);
        }

        public void onNext(T item)
        {
            if (subscriber != null)
                subscriber.onNext(item);
            else
            {
                if (first != null)
                    parent.onError(new AssertionError("Got onNext twice with " + first + " and then " + item + " in " + parent
                                                                                                                        .toString()));
                first = item;
                parent.onContent(this);
            }
        }

        public void onFinal(T item)
        {
            if (subscriber != null)
                subscriber.onFinal(item);
            else
            {
                if (first != null)
                    parent.onError(new AssertionError("Got onNext twice with " + first + " and then " + item + " in " + parent
                                                                                                                        .toString()));
                first = item;
                firstIsFinal = true;
                parent.onContent(this);
            }
        }

        public void onComplete()
        {
            if (subscriber != null)
                subscriber.onComplete();
            else
            {
                // Empty flow. No one will subscribe to us now, so make sure our subscription is closed.
                try
                {
                    source.close();
                }
                catch (Exception e)
                {
                    parent.onError(e);
                    return;
                }
                parent.onEmpty();
            }
        }

        public void onError(Throwable t)
        {
            if (subscriber != null)
                subscriber.onError(t);
            else
            {
                // Error before first element. No one will subscribe to us now, so make sure our subscription is closed.
                try
                {
                    source.close();
                }
                catch (Throwable tt)
                {
                    t.addSuppressed(tt);
                }
                parent.onError(t);
            }
        }

        public String toString()
        {
            return parent.toString();
        }
    }
}
