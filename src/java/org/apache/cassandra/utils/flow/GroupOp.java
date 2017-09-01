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

/**
 * Operator for grouping elements of a Flow. Used with {@link Flow#group(GroupOp)}.
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
    boolean inSameGroup(I l, I r) throws Exception;

    /**
     * Transform the group. May return null, meaning skip.
     */
    O map(List<I> inputs) throws Exception;

    public static <I, O> Flow<O> group(Flow<I> source, GroupOp<I, O> op)
    {
        return new GroupFlow<>(op, source);
    }

    static class GroupFlow<I, O> extends FlowTransform<I, O>
    {
        final GroupOp<I, O> mapper;
        volatile boolean completeOnNextRequest = false;
        I first;
        List<I> entries;

        public GroupFlow(GroupOp<I, O> mapper, Flow<I> source)
        {
            super(source);
            this.mapper = mapper;
        }

        public void onFinal(I entry)
        {
            O out = null;
            try
            {
                if (first == null || !mapper.inSameGroup(first, entry))
                {
                    if (first != null)
                        out = mapper.map(entries);

                    entries = new ArrayList<>();
                    first = entry;
                }
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            entries.add(entry);
            if (out != null)
            {
                completeOnNextRequest = true;
                subscriber.onNext(out);
            }
            else
                onComplete();
        }

        public void onNext(I entry)
        {
            O out = null;
            try
            {
                if (first == null || !mapper.inSameGroup(first, entry))
                {
                    if (first != null)
                        out = mapper.map(entries);

                    entries = new ArrayList<>();
                    first = entry;
                }
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            entries.add(entry);
            if (out != null)
                subscriber.onNext(out);
            else
                requestInLoop(source);
        }

        public void onComplete()
        {
            O out = null;

            try
            {
                if (first != null)
                {
                    out = mapper.map(entries);
                    first = null;   // don't hold on to references
                    entries = null;
                }
            } catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (out != null)
                subscriber.onFinal(out);
            else
                subscriber.onComplete();
        }

        public void requestNext()
        {
            if (!completeOnNextRequest)
                source.requestNext();
            else
                onComplete();
        }

        public String toString()
        {
            return Flow.formatTrace("group", mapper, sourceFlow);
        }
    }
}
