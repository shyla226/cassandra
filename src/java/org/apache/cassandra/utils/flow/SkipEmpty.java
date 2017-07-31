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
    static class SkipEmptyContent<T, U> extends Flow<U> implements FlowSubscription
    {
        final Function<Flow<T>, U> mapper;
        FlowSubscriber<U> subscriber;
        final SkipEmptyContentSubscriber<T> child;

        private enum State
        {
            UNSUBSCRIBED,
            SUBSCRIBED,
            REQUESTED,
            SUPPLIED,
            COMPLETED,
            CLOSED
        }
        State state = State.UNSUBSCRIBED;

        public SkipEmptyContent(Flow<T> content, Function<Flow<T>, U> mapper)
        {
            this.mapper = mapper;
            child = new SkipEmptyContentSubscriber(content, this);
        }

        public FlowSubscription subscribe(FlowSubscriber<U> subscriber)
        {
            if (state != State.UNSUBSCRIBED)
                throw new AssertionError("skipEmpty partitions can only be subscribed to once. State was " + state);

            this.subscriber = subscriber;
            state = State.SUBSCRIBED;
            return this;
        }

        public void request()
        {
            if (tryTransition(State.SUPPLIED, State.COMPLETED))
            {
                subscriber.onComplete();
                return;
            }
            if (!verifyTransition(State.SUBSCRIBED, State.REQUESTED))
                return;

            child.source.request();
        }

        public void close() throws Exception
        {
            switch (state)
            {
            case SUBSCRIBED:
            case SUPPLIED:
            case COMPLETED:
                state = State.CLOSED;
                break;
            default:
                throw new AssertionError("Unexpected state " + state + " while closing " + this);
            }
        }

        void onContent(Flow<T> child)
        {
            if (!verifyTransition(State.REQUESTED, State.SUPPLIED))
                return;

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

            subscriber.onNext(result);
        }

        void onEmpty()
        {
            if (!verifyTransition(State.REQUESTED, State.COMPLETED))
                return;

            subscriber.onComplete();
        }

        void onError(Throwable e)
        {
            state = state.COMPLETED;
            subscriber.onError(e);
        }

        boolean tryTransition(State from, State to)
        {
            if (state != from)
                return false;

            state = to;
            return true;
        }

        boolean verifyTransition(State from, State to)
        {
            if (tryTransition(from, to))
                return true;

            onError(new AssertionError("Incorrect state " + from + " to transition to " + to + " in " + this));
            return false;
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return child.addSubscriberChainFromSource(throwable);
        }

        public String toString()
        {
            return Flow.formatTrace("skipEmpty", mapper, subscriber);
        }
    }

    /**
     * This is both flow and subscription. Done this way as we can only subscribe to these implementations once
     * and thus it doesn't make much sense to create subscription-specific instances.
     */
    private static class SkipEmptyContentSubscriber<T> extends FlowTransform<T, T>
    {
        final SkipEmptyContent parent;
        T first = null;

        public SkipEmptyContentSubscriber(Flow<T> content,
                                          SkipEmptyContent parent)
        {
            super(content);
            this.parent = parent;
        }

        public FlowSubscription subscribe(FlowSubscriber<T> subscriber)
        {
            assert first != null;
            return super.subscribe(subscriber);
        }

        public void request()
        {
            if (first != null)
            {
                T toReturn = first;
                first = null;
                subscriber.onNext(toReturn);
            }
            else
                source.request();
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
                    parent.onEmpty();
                }
                catch (Exception e)
                {
                    parent.onError(e);
                }
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
            return Flow.formatTrace("skipEmpty", parent.mapper, subscriber);
        }
    }
}
