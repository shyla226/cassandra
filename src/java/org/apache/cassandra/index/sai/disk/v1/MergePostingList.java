/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sai.disk.v1;


import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.MoreObjects;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.io.util.FileUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Merges multiple {@link PostingList} which individually contain unique items into a single list.
 */
@NotThreadSafe
// TODO can be simplified as it's now synchronous
public class MergePostingList implements PostingList
{
    private static final Comparator<PeekingPostingList> ROW_ID_COMPARATOR = Comparator.comparingInt(o -> o.currentRowID);

    /**
     * Merging children.
     */
    private final PeekingPostingList[] peekingPostingLists;
    /**
     * Tmp array to collect all leading children that will consume their top element to yield next id.
     */
    private final PeekingPostingList[] leading;
    /**
     * Holds children which have to load their top value before next merging step can happen.
     */
    private final PeekingPostingList[] dirty;
    /**
     * Next empty slot in the dirty array above.
     */
    private int dirtyIdx = 0;
    /**
     * Priority heap that holds children with fetched but unconsumed values. Enables constant time access to the min
     * element.
     */
    private final PriorityQueue<PeekingPostingList> sorted;

    private final Closeable onClose;
    private final int size; // upper bound
    private int lastRowId = -1;

    private MergePostingList(List<PostingList> postingLists, Closeable onClose)
    {
        this.onClose = onClose;
        this.peekingPostingLists = new PeekingPostingList[postingLists.size()];
        this.leading = new PeekingPostingList[postingLists.size()];
        this.dirty = new PeekingPostingList[postingLists.size()];
        this.sorted = new PriorityQueue<>(postingLists.size(), ROW_ID_COMPARATOR);

        int size = 0;
        for (int i = 0; i < postingLists.size(); ++i)
        {
            final PeekingPostingList list = new PeekingPostingList(postingLists.get(i));
            peekingPostingLists[i] = list;
            dirty[dirtyIdx++] = list;
            size += list.size();
        }
        this.size = size;
    }

    public static PostingList merge(List<PostingList> postings, Closeable onClose)
    {
        checkArgument(!postings.isEmpty());
        return postings.size() > 1 ?
               new MergePostingList(postings, onClose) : postings.get(0);
    }

    public static PostingList merge(List<PostingList> postings)
    {
        return merge(postings, () -> FileUtils.close(postings));
    }

    @Override
    public void close() throws IOException
    {
        onClose.close();
    }

    @Override
    public int size()
    {
        return size;
    }

    @SuppressWarnings("resource")
    @Override
    public final int nextPosting() throws IOException
    {
        for (int i = dirtyIdx; i > 0; i--)
        {
            final PeekingPostingList list = dirty[i - 1];
            int peek = list.peek();
            // we're removing duplicates
            if (peek <= lastRowId)
            {
                list.consume();
                i++;
                continue;
            }
            else if (peek != PostingList.END_OF_STREAM)
            {
                // only add back to the heap if there's anything left in the list
                sorted.add(list);
            }
            dirtyIdx--;
        }

        final PeekingPostingList min = sorted.poll();
        if (min == null)
        {
            return PostingList.END_OF_STREAM;
        }

        int leadingIdx = 0;
        leading[leadingIdx++] = min;

        // Record the other list with the same current ID...
        while (!sorted.isEmpty() && sorted.peek().currentRowID == min.currentRowID)
        {
            leading[leadingIdx++] = sorted.poll();
        }

        for (int idx = 0; idx < leadingIdx; ++idx)
        {
            PeekingPostingList list = leading[idx];
            // just consumes buffered top value
            list.consume();
            dirty[dirtyIdx++] = list;
        }

        return lastRowId = min.currentRowID;
    }

    @Override
    public int advance(int targetRowID) throws IOException
    {
        int nextMin = PostingList.END_OF_STREAM;
        int leadingIdx = 0;
        for (PeekingPostingList list : peekingPostingLists)
        {
            final int peek = list.advanceWithoutConsuming(targetRowID);
            if (peek != PostingList.END_OF_STREAM)
            {
                if (peek == nextMin)
                {
                    leading[leadingIdx++] = list;
                }
                else if (peek < nextMin)
                {
                    leadingIdx = 0;
                    leading[leadingIdx++] = list;
                    nextMin = peek;
                }
            }
        }

        for (int idx = 0; idx < leadingIdx; ++idx)
        {
            leading[idx].consume();
        }

        // all entries on the heap must be updated
        while (!sorted.isEmpty())
        {
            dirty[dirtyIdx++] = sorted.poll();
        }

        return lastRowId = nextMin;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("lastRowId", lastRowId)
                          .add("childrenCount", peekingPostingLists.length)
                          .add("children", Arrays.toString(peekingPostingLists))
                          .toString();
    }

    private static class PeekingPostingList implements PostingList
    {
        private final PostingList delegate;
        private int currentRowID;
        private State state = State.READY;

        PeekingPostingList(PostingList delegate)
        {
            this.delegate = delegate;
        }

        int advanceWithoutConsuming(int targetRowID) throws IOException
        {
            // If we advanced at some point, but haven't consumed the last rowID yet, we must check if we can advance over it or not.
            switch (state)
            {
                case EXHAUSTED:
                    return PostingList.END_OF_STREAM;
                case UNCONSUMED:
                    if (currentRowID >= targetRowID)
                    {
                        return currentRowID;
                    }
                default:
                    break;
            }

            state = State.ADVANCING;
            final int id = advance(targetRowID);
            state = id == PostingList.END_OF_STREAM ? State.EXHAUSTED : State.UNCONSUMED;
            return id;
        }

        int consume()
        {
            checkState(isUnconsumed());
            state = State.READY;
            return currentRowID;
        }

        int peek() throws IOException
        {
            if (isExhausted())
            {
                return PostingList.END_OF_STREAM;
            }
            else if (isUnconsumed())
            {
                return currentRowID;
            }
            checkState(state == State.READY);
            final int id = nextPosting();
            state = id == PostingList.END_OF_STREAM ? State.EXHAUSTED : State.UNCONSUMED;
            return id;
        }

        @Override
        public void close() throws IOException { delegate.close();}

        @Override
        public int nextPosting() throws IOException
        {
            if (isExhausted())
            {
                return PostingList.END_OF_STREAM;
            }
            else if (isUnconsumed())
            {
                return consume();
            }
            checkState(state == State.READY);
            return currentRowID = delegate.nextPosting();
        }

        @Override
        public int size()
        {
            return delegate.size();
        }

        @Override
        public int advance(int targetRowID) throws IOException
        {
            checkState(state == State.ADVANCING);
            return currentRowID = delegate.advance(targetRowID);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                              .add("state", state)
                              .add("currentRowID", currentRowID)
                              .add("delegate", delegate)
                              .toString();
        }

        private boolean isUnconsumed()
        {
            return state == State.UNCONSUMED;
        }

        private boolean isExhausted()
        {
            return state == State.EXHAUSTED;
        }

        private enum State
        {
            /**
             * When the next element needs to be fetched from the delegate posting list.
             */
            READY,
            /**
             * When delegate posting list is in the middle of advancing.
             */
            ADVANCING,
            /**
             * When delegate list has been advanced, but we hold its top element, because it hasn't been consumed yet.
             */
            UNCONSUMED,
            /**
             * When delegate posting list is empty.
             */
            EXHAUSTED
        }
    }
}
