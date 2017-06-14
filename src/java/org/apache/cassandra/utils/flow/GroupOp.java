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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Operator for grouping elements of a CsFlow. Used with {@link CsFlow#group(GroupOp)}.
 * <p>
 * Stream is broken up in selections of consecutive elements where {@link #inSameGroup} returns true, passing each
 * collection through {@link #map(List)}.
 * <p>
 * Warning: not safe to use if the items in the stream rely on holding on to resources, since it keeps a list of active
 * items.
 */
public interface GroupOp<I, O>
{
    /**
     * Should return true if l and r are to be grouped together.
     */
    boolean inSameGroup(I l, I r);

    /**
     * Transform the group. May return null, meaning skip.
     */
    O map(List<I> inputs);

    public static <I, O> CsFlow<O> group(CsFlow<I> source, GroupOp<I, O> op)
    {
        class GroupFlow extends CsFlow<O>
        {
            public CsSubscription subscribe(CsSubscriber<O> subscriber) throws Exception
            {
                return new Subscription<>(subscriber, op, source);
            }
        }
        return new GroupFlow();
    }

    static class Subscription<I, O> extends CsFlow.RequestLoop
    implements CsSubscriber<I>, CsSubscription
    {
        final CsSubscriber<O> subscriber;
        final GroupOp<I, O> mapper;
        final CsSubscription source;
        volatile boolean completeOnNextRequest;
        I first;
        List<I> entries;

        public Subscription(CsSubscriber<O> subscriber, GroupOp<I, O> mapper, CsFlow<I> source) throws Exception
        {
            this.subscriber = subscriber;
            this.mapper = mapper;
            this.source = source.subscribe(this);
        }

        public void onNext(I entry)
        {
            O out = null;
            if (first == null || !mapper.inSameGroup(first, entry))
            {
                if (first != null)
                    out = mapper.map(entries);

                entries = new ArrayList<>();
                first = entry;
            }

            entries.add(entry);
            if (out != null)
                subscriber.onNext(out);
            else
                requestInLoop(this);
        }

        public void onError(Throwable throwable)
        {
            subscriber.onError(throwable);
        }

        public void onComplete()
        {
            O out = null;

            if (first != null)
            {
                out = mapper.map(entries);
                first = null;   // don't hold on to references
                entries = null;
            }

            if (out != null)
            {
                completeOnNextRequest = true;
                subscriber.onNext(out);
            }
            else
                subscriber.onComplete();
        }

        public void request()
        {
            if (!completeOnNextRequest)
                source.request();
            else
                subscriber.onComplete();
        }

        public void close() throws Exception
        {
            source.close();
        }

        public String toString()
        {
            return "group(" + mapper + ")\n\tsubscriber " + subscriber;
        }
    }
}
