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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.Throwables;

/**
 * Merges sorted input flows which individually contain unique items.
 */
public class Merge
{
    public static <In, Out> Flow<Out> get(
                                           List<? extends Flow<In>> sources,
                                           Comparator<? super In> comparator,
                                           Reducer<In, Out> reducer)
    {
        if (sources.size() == 1)
        {
            if (!reducer.trivialReduceIsTrivial())
                return sources.get(0).map(next ->
                                          {
                                              reducer.onKeyChange();
                                              reducer.reduce(0, next);
                                              return reducer.getReduced();
                                          });

            @SuppressWarnings("unchecked")
            Flow<Out> converted = (Flow<Out>) sources.get(0);
            return converted;
        }
        return new ManyToOne<>(reducer, sources, comparator);
    }

    /**
     * A MergeIterator that consumes multiple input values per output value.
     *
     * The most straightforward way to implement this is to use a {@code PriorityQueue} of iterators, {@code poll} it to
     * find the next item to consume, then {@code add} the iterator back after advancing. This is not very efficient as
     * {@code poll} and {@code add} in all cases require at least {@code log(size)} comparisons (usually more than
     * {@code 2*log(size)}) per consumed item, even if the input is suitable for fast iteration.
     *
     * The implementation below makes use of the fact that replacing the top element in a binary heap can be done much
     * more efficiently than separately removing it and placing it back, especially in the cases where the top iterator
     * is to be used again very soon (e.g. when there are large sections of the output where only a limited number of
     * input iterators overlap, which is normally the case in many practically useful situations, e.g. levelled
     * compaction). To further improve this particular scenario, we also use a short sorted section at the start of the
     * queue.
     *
     * The heap is laid out as this (for {@code SORTED_SECTION_SIZE == 2}):
     *                 0
     *                 |
     *                 1
     *                 |
     *                 2
     *               /   \
     *              3     4
     *             / \   / \
     *             5 6   7 8
     *            .. .. .. ..
     * Where each line is a <= relationship.
     *
     * In the sorted section we can advance with a single comparison per level, while advancing a level within the heap
     * requires two (so that we can find the lighter element to pop up).
     * The sorted section adds a constant overhead when data is uniformly distributed among the iterators, but may up
     * to halve the iteration time when one iterator is dominant over sections of the merged data (as is the case with
     * non-overlapping iterators).
     *
     * The iterator is further complicated by the need to avoid advancing the input iterators until an output is
     * actually requested. To achieve this {@code consume} walks the heap to find equal items without advancing the
     * iterators, and {@code advance} moves them and restores the heap structure before any items can be consumed.
     *
     * To avoid having to do additional comparisons in consume to identify the equal items, we keep track of equality
     * between children and their parents in the heap. More precisely, the lines in the diagram above define the
     * following relationship:
     *   parent <= child && (parent == child) == child.equalParent
     * We can track, make use of and update the equalParent field without any additional comparisons.
     *
     * For more formal definitions and proof of correctness, see CASSANDRA-8915.
     */
    // the candidates are stashed back onto the heap and closed when close() is called,
    // Eclipse Warnings cannot work it out and complains in several places
    @SuppressWarnings("resource")
    static final class ManyToOne<In, Out> extends Flow.RequestLoopFlow<Out> implements FlowSubscription
    {
        protected Candidate<In>[] heap;
        private final Reducer<In, Out> reducer;
        FlowSubscriber<Out> subscriber;
        AtomicInteger advancing = new AtomicInteger();

        /** Number of non-exhausted iterators. */
        int size;

        /**
         * Position of the deepest, right-most child that needs advancing before we can start consuming.
         * Because advancing changes the values of the items of each iterator, the parent-chain from any position
         * in this range that needs advancing is not in correct order. The trees rooted at any position that does
         * not need advancing, however, retain their prior-held binary heap property.
         */
        int needingAdvance;

        /**
         * The number of elements to keep in order before the binary heap starts, exclusive of the top heap element.
         */
        static final int SORTED_SECTION_SIZE = 4;

        public ManyToOne(Reducer<In, Out> reducer, List<? extends Flow<In>> sources, Comparator<? super In> comparator)
        {
            this.reducer = reducer;

            @SuppressWarnings("unchecked")
            Candidate<In>[] heap = new Candidate[sources.size()];
            this.heap = heap;
            size = 0;

            for (int i = 0; i < sources.size(); i++)
            {
                Candidate<In> candidate = new Candidate<In>(this, i, sources.get(i), comparator);
                heap[size++] = candidate;
            }
            needingAdvance = size;

        }

        public void requestFirst(FlowSubscriber<Out> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
        {
            assert this.subscriber == null : "Flow are single-use.";
            this.subscriber = subscriber;
            subscriptionRecipient.onSubscribe(this);

            advancing.set(size);
            for (int i = 0; i < needingAdvance; i++)
                heap[i].requestFirst();
        }

        @Override
        public void close() throws Exception
        {
            Throwable t = Throwables.close(null, Arrays.asList(heap));
            Throwables.maybeFail(t);
        }

        public String toString()
        {
            return Flow.formatTrace("merge", reducer);
        }

        /**
         * Advance all iterators that need to be advanced and place them into suitable positions in the heap.
         *
         * By walking the iterators backwards we know that everything after the point being processed already forms
         * correctly ordered subheaps, thus we can build a subheap rooted at the current position by only sinking down
         * the newly advanced iterator. Because all parents of a consumed iterator are also consumed there is no way
         * that we can process one consumed iterator but skip over its parent.
         *
         * The procedure is the same as the one used for the initial building of a heap in the heapsort algorithm and
         * has a maximum number of comparisons {@code (2 * log(size) + SORTED_SECTION_SIZE / 2)} multiplied by the
         * number of iterators whose items were consumed at the previous step, but is also at most linear in the size of
         * the heap if the number of consumed elements is high (as it is in the initial heap construction). With non- or
         * lightly-overlapping iterators the procedure finishes after just one (resp. a couple of) comparisons.
         */
        public void requestNext()
        {
            // Set to 1 initially to guard so we don't get called while we are increasing advancing.
            // It must be 0 before we are called.
            int prev = advancing.getAndIncrement();
            if (prev != 0)
            {
                subscriber.onError(new AssertionError("Merge advance called while another has " + prev + " outstanding requests."));
                return;
            }

            for (int i = needingAdvance - 1; i >= 0; --i)
            {
                Candidate<In> candidate = heap[i];
                /*
                 *  needingAdvance runs to the maximum index (and deepest-right node) that may need advancing;
                 *  since the equal items that were consumed at-once may occur in sub-heap "veins" of equality,
                 *  not all items above this deepest-right position may have been consumed; these already form
                 *  valid sub-heaps and can be skipped-over entirely
                 */
                if (candidate.needsRequest())
                {
                    advancing.incrementAndGet();
                    candidate.request();        // this can be jumping to/spawning another thread, or calling onAdvance()
                }
            }
            onAdvance();
        }

        /**
         * Called when a child has returned an item in response to a request from the method above.
         * This can come concurrently since children can be performing requests in separate threads.
         */
        void onAdvance()
        {
            // Count how many we have left to receive and just exit if we don't have all.
            if (advancing.decrementAndGet() > 0)
                return;

            // Only one thread can reach this point.
            for (int i = needingAdvance - 1; i >= 0; --i)
            {
                Candidate<In> candidate = heap[i];

                // The test below should now be true for every item that hit needsRequest() in the loop above.
                if (candidate.justAdvanced())
                    replaceAndSink(heap[i], i);
            }
            needingAdvance = 0;
            consume();
        }

        /**
         * Consume all items that sort like the current top of the heap. As we cannot advance the iterators to let
         * equivalent items pop up, we walk the heap to find them and mark them as needing advance.
         *
         * This relies on the equalParent flag to avoid doing any comparisons.
         */
        private void consume()
        {
            if (size == 0)
            {
                subscriber.onComplete();
                return;
            }

            try
            {
                reducer.onKeyChange();

                heap[0].consume(reducer);
                final int size = this.size;
                final int sortedSectionSize = Math.min(size, SORTED_SECTION_SIZE);
                int i;
                consume:
                {
                    for (i = 1; i < sortedSectionSize; ++i)
                    {
                        if (!heap[i].equalParent)
                            break consume;
                        heap[i].consume(reducer);
                    }
                    i = Math.max(i, consumeHeap(i) + 1);
                }
                needingAdvance = i;
            }
            catch (Throwable t)
            {
                onError(t);
                return;
            }

            Throwable error = reducer.getErrors();
            if (error != null)
                onError(error);
            else
            {
                Out item = reducer.getReduced();
                if (item != null)
                    subscriber.onNext(item);    // usually requests
                else
                    requestInLoop(this); // reducer rejected its input; get another set
            }
        }

        void onError(Throwable error)
        {
            subscriber.onError(error);
        }

        /**
         * Recursively consume all items equal to equalItem in the binary subheap rooted at position idx.
         *
         * @return the largest equal index found in this search.
         */
        private int consumeHeap(int idx)
        {
            if (idx >= size || !heap[idx].equalParent)
                return -1;

            heap[idx].consume(reducer);
            int nextIdx = (idx << 1) - (SORTED_SECTION_SIZE - 1);
            return Math.max(idx, Math.max(consumeHeap(nextIdx), consumeHeap(nextIdx + 1)));
        }

        /**
         * Replace an iterator in the heap with the given position and move it down the heap until it finds its proper
         * position, pulling lighter elements up the heap.
         *
         * Whenever an equality is found between two elements that form a new parent-child relationship, the child's
         * equalParent flag is set to true if the elements are equal.
         */
        private void replaceAndSink(Candidate<In> candidate, int currIdx)
        {
            if (candidate.item == null && candidate.error == null)
            {
                // Drop iterator by replacing it with the last one in the heap.
                Candidate<In> toDrop = candidate;
                candidate = heap[--size];
                heap[size] = toDrop; // stash iterator at end so it can be closed
            }
            // The new element will be top of its heap, at this point there is no parent to be equal to.
            candidate.equalParent = false;

            final int size = this.size;
            final int sortedSectionSize = Math.min(size - 1, SORTED_SECTION_SIZE);

            int nextIdx;

            // Advance within the sorted section, pulling up items lighter than candidate.
            while ((nextIdx = currIdx + 1) <= sortedSectionSize)
            {
                if (!heap[nextIdx].equalParent) // if we were greater then an (or were the) equal parent, we are >= the child
                {
                    int cmp = candidate.compareTo(heap[nextIdx]);
                    if (cmp <= 0)
                    {
                        heap[nextIdx].equalParent = cmp == 0;
                        heap[currIdx] = candidate;
                        return;
                    }
                }

                heap[currIdx] = heap[nextIdx];
                currIdx = nextIdx;
            }
            // If size <= SORTED_SECTION_SIZE, nextIdx below will be no less than size,
            // because currIdx == sortedSectionSize == size - 1 and nextIdx becomes
            // (size - 1) * 2) - (size - 1 - 1) == size.

            // Advance in the binary heap, pulling up the lighter element from the two at each level.
            while ((nextIdx = (currIdx * 2) - (sortedSectionSize - 1)) + 1 < size)
            {
                if (!heap[nextIdx].equalParent)
                {
                    if (!heap[nextIdx + 1].equalParent)
                    {
                        // pick the smallest of the two children
                        int siblingCmp = heap[nextIdx + 1].compareTo(heap[nextIdx]);
                        if (siblingCmp < 0)
                            ++nextIdx;

                        // if we're smaller than this, we are done, and must only restore the heap and equalParent properties
                        int cmp = candidate.compareTo(heap[nextIdx]);
                        if (cmp <= 0)
                        {
                            if (cmp == 0)
                            {
                                heap[nextIdx].equalParent = true;
                                if (siblingCmp == 0) // siblingCmp == 0 => nextIdx is the left child
                                    heap[nextIdx + 1].equalParent = true;
                            }

                            heap[currIdx] = candidate;
                            return;
                        }

                        if (siblingCmp == 0)
                        {
                            // siblingCmp == 0 => nextIdx is still the left child
                            // if the two siblings were equal, and we are inserting something greater, we will
                            // pull up the left one; this means the right gets an equalParent
                            heap[nextIdx + 1].equalParent = true;
                        }
                    }
                    else
                        ++nextIdx;  // descend down the path where we found the equal child
                }

                heap[currIdx] = heap[nextIdx];
                currIdx = nextIdx;
            }

            // our loop guard ensures there are always two siblings to process; typically when we exit the loop we will
            // be well past the end of the heap and this next condition will match...
            if (nextIdx >= size)
            {
                heap[currIdx] = candidate;
                return;
            }

            // ... but sometimes we will have one last child to compare against, that has no siblings
            if (!heap[nextIdx].equalParent)
            {
                int cmp = candidate.compareTo(heap[nextIdx]);
                if (cmp <= 0)
                {
                    heap[nextIdx].equalParent = cmp == 0;
                    heap[currIdx] = candidate;
                    return;
                }
            }

            heap[currIdx] = heap[nextIdx];
            heap[nextIdx] = candidate;
        }
    }

    // Holds and is comparable by the head item of an flow it has subscribed to
    protected final static class Candidate<In> implements Comparable<Candidate<In>>, FlowSubscriber<In>, AutoCloseable
    {
        private final ManyToOne<In, ?> merger;
        private final Flow<In> sourceFlow;
        private FlowSubscription source;
        private final Comparator<? super In> comp;
        private final int idx;
        private In item;
        private boolean completeOnNextRequest = false;
        Throwable error = null;

        enum State {
            NEEDS_REQUEST,
            AWAITING_ADVANCE,
            ADVANCED,
            PROCESSED
            // eventually SMALLER_SIBLING, EQUAL_PARENT (imply PROCESSED)
        };
        private final AtomicReference<State> state = new AtomicReference<>(State.NEEDS_REQUEST);

        boolean equalParent;

        public Candidate(final ManyToOne<In, ?> merger, int idx, Flow<In> source, Comparator<? super In> comp)
        {
            this.merger = merger;
            this.comp = comp;
            this.idx = idx;
            this.sourceFlow = source;
        }

        void requestFirst()
        {
            if (!verifyStateChange(State.NEEDS_REQUEST, State.AWAITING_ADVANCE, true))
                return;
            sourceFlow.requestFirst(this, this);
        }

        public void onSubscribe(FlowSubscription source)
        {
            this.source = source;
        }

        public boolean needsRequest()
        {
            return state.get() == State.NEEDS_REQUEST;
        }

        private boolean verifyStateChange(State from, State to, boolean itemShouldBeNull)
        {
            State prev = state.getAndSet(to);
            if (prev == from && (!itemShouldBeNull || item == null))
                return true;

            onOurError(new AssertionError("Invalid state " + prev +
                                          (item == null ? "/" : "/non-") + "null item to transition " +
                                          from + (itemShouldBeNull ? "/null item" : "") + "->" + to));
            return false;
        }

        private AssertionError onOurError(AssertionError e)
        {
            // We are in a bad state. We can't expect to pass this error through the merge, so pass on to subscriber
            // directly and abort everything we can.
            merger.onError(Flow.wrapException(e, this));
            return e;
        }

        protected void request()
        {
            if (!verifyStateChange(State.NEEDS_REQUEST, State.AWAITING_ADVANCE, true))
                return;

            if (completeOnNextRequest)
                onComplete();
            else
                source.requestNext();
        }

        @Override
        public void onComplete()
        {
            onAdvance(null);
        }

        @Override
        public void onError(Throwable error)
        {
            this.error = Flow.wrapException(error, this);
            onAdvance(null);
        }

        @Override
        public void onNext(In next)
        {
            if (next != null)
                onAdvance(next);
            else
                onError(new AssertionError("null item in onNext"));
        }

        @Override
        public void onFinal(In next)
        {
            completeOnNextRequest = true;
            onNext(next);
        }

        private void onAdvance(In next)
        {
            if (!verifyStateChange(State.AWAITING_ADVANCE, State.ADVANCED, true))
                return;

            item = next;
            merger.onAdvance();
        }

        public boolean justAdvanced()
        {
            if (state.get() == State.PROCESSED)
                return false;

            verifyStateChange(State.ADVANCED, State.PROCESSED, false);

            return true;
        }

        public int compareTo(Candidate<In> that)
        {
            if (this.state.get() != State.PROCESSED || that.state.get() != State.PROCESSED)
            {
                Candidate<In> invalid = this.state.get() != State.PROCESSED ? this : that;
                throw onOurError(new AssertionError("Comparing unprocessed item " + invalid + " in state " + invalid.state.get()));
            }

            if (this.error != null || that.error != null)
                return ((this.error != null) ? -1 : 0) - ((that.error != null) ? -1 : 0);

            assert this.item != null && that.item != null;
            int ret = comp.compare(this.item, that.item);
            return ret;
        }

        public void consume(Reducer<In, ?> reducer)
        {
            if (!verifyStateChange(State.PROCESSED, State.NEEDS_REQUEST, false))
                return;

            if (error != null)
                reducer.error(error);
            else
                reducer.reduce(idx, item);
            item = null;
            error = null;
        }

        public void close() throws Exception
        {
            source.close();
        }

        public String toString()
        {
            return Flow.formatTrace("merge child", sourceFlow);
        }
    }
}