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
package org.apache.cassandra.index.sai.utils;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.Token;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;

/**
 * Modified from {@link org.apache.cassandra.index.sasi.utils.RangeIntersectionIterator} to support:
 * 1. no generic type to reduce allocation
 * 2. support selective intersection to reduce disk io
 * 3. make sure iterators are closed when intersection ends because of lazy key fetching
 */
@SuppressWarnings("resource")
public class RangeIntersectionIterator
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // The cassandra.sai.intersection.clause.limit (default: 2) controls the maximum number of range iterator that
    // will be used in the final intersection of a query operation.
    private static final int INTERSECTION_CLAUSE_LIMIT = Integer.getInteger("cassandra.sai.intersection.clause.limit", 2);

    static
    {
        logger.info(String.format("Storage attached index intersection clause limit is %d", INTERSECTION_CLAUSE_LIMIT));
    }

    public enum Strategy
    {
        BOUNCE, LOOKUP, ADAPTIVE
    }

    public static Builder builder()
    {
        return builder(Strategy.ADAPTIVE);
    }

    public static Builder selectiveBuilder()
    {
        return selectiveBuilder(INTERSECTION_CLAUSE_LIMIT);
    }

    public static Builder selectiveBuilder(int limit)
    {
        return new Builder(limit, Strategy.ADAPTIVE);
    }

    @VisibleForTesting
    protected static Builder builder(Strategy strategy)
    {
        return new Builder(strategy);
    }

    public static class Builder extends RangeIterator.Builder
    {
        private final Strategy strategy;
        private final int limit;

        public Builder(Strategy strategy)
        {
            super(IteratorType.INTERSECTION);
            this.limit = Integer.MAX_VALUE;
            this.strategy = strategy;
        }

        public Builder(int limit, Strategy strategy)
        {
            super(IteratorType.INTERSECTION);
            this.limit = limit;
            this.strategy = strategy;
        }

        protected RangeIterator buildIterator()
        {
            // all ranges will be included
            if (limit >= ranges.size() || limit <= 0)
                return buildIterator(statistics, ranges, strategy);

            // Apply most selective iterators during intersection, because larger number of iterators will result lots of disk seek.
            List<RangeIterator> selectiveIterator = new ArrayList<>(ranges);
            selectiveIterator.sort(Comparator.comparingLong(RangeIterator::getCount));

            RangeIterator.Builder.Statistics selectiveStatistics = new RangeIterator.Builder.Statistics(IteratorType.INTERSECTION);
            for (int i = selectiveIterator.size() - 1; i >= 0 && i >= limit; i--)
                FileUtils.closeQuietly(selectiveIterator.remove(i));

            for (RangeIterator iterator : selectiveIterator)
                selectiveStatistics.update(iterator);

            if (Tracing.isTracing())
                Tracing.trace("Selecting {} {} of {} out of {} indexes",
                              selectiveIterator.size(),
                              selectiveIterator.size() > 1 ? "indexes with cardinalities" : "index with cardinality",
                              selectiveIterator.stream().map(RangeIterator::getCount).map(Object::toString).collect(Collectors.joining(", ")),
                              ranges.size());

            PriorityQueue<RangeIterator> selectiveRanges = new PriorityQueue<>(limit, Comparator.comparing(RangeIterator::getCurrent));
            selectiveRanges.addAll(selectiveIterator);

            return buildIterator(selectiveStatistics, selectiveRanges, strategy);
        }

        private static RangeIterator buildIterator(Builder.Statistics statistics, PriorityQueue<RangeIterator> ranges, Strategy strategy)
        {
            // if the range is disjoint or we have an intersection with an empty set,
            // we can simply return an empty iterator, because it's not going to produce any results.
            if (statistics.isDisjoint())
            {
                // release posting lists
                FileUtils.closeQuietly(ranges);
                return RangeIterator.empty();
            }

            if (ranges.size() == 1)
                return ranges.poll();

            switch (strategy)
            {
                case LOOKUP:
                    return new LookupIntersectionIterator(statistics, ranges);

                case BOUNCE:
                    return new BounceIntersectionIterator(statistics, ranges);

                case ADAPTIVE:
                    return statistics.sizeRatio() <= 0.01d
                           ? new LookupIntersectionIterator(statistics, ranges)
                           : new BounceIntersectionIterator(statistics, ranges);

                default:
                    throw new IllegalStateException("Unknown strategy: " + strategy);
            }
        }
    }

    @VisibleForTesting
    public static abstract class AbstractIntersectionIterator extends RangeIterator
    {
        protected final PriorityQueue<RangeIterator> ranges;
        protected final Token.TokenMerger merger;

        private AbstractIntersectionIterator(Builder.Statistics statistics, PriorityQueue<RangeIterator> ranges)
        {
            super(statistics);
            this.ranges = ranges;
            this.merger = new Token.ReusableTokenMerger(ranges.size());
        }

        public void close() throws IOException
        {
            for (RangeIterator range : ranges)
                FileUtils.closeQuietly(range);
        }
    }

    /**
     * Iterator which performs intersection of multiple ranges by using bouncing (merge-join) technique to identify
     * common elements in the given ranges. Aforementioned "bounce" works as follows: range queue is poll'ed for the
     * range with the smallest current token (main loop), that token is used to {@link RangeIterator#skipTo(Long)}
     * other ranges, if token produced by {@link RangeIterator#skipTo(Long)} is equal to current "candidate" token,
     * both get merged together and the same operation is repeated for next range from the queue, if returned token
     * is not equal than candidate, candidate's range gets put back into the queue and the main loop gets repeated until
     * next intersection token is found or at least one iterator runs out of tokens.
     *
     * This technique is every efficient to jump over gaps in the ranges.
     */
    @VisibleForTesting
    protected static class BounceIntersectionIterator extends AbstractIntersectionIterator
    {
        private BounceIntersectionIterator(Builder.Statistics statistics, PriorityQueue<RangeIterator> ranges)
        {
            super(statistics, ranges);
        }

        protected Token computeNext()
        {
            List<RangeIterator> processed = null;

            while (!ranges.isEmpty())
            {
                RangeIterator head = ranges.poll();

                // jump right to the beginning of the intersection or return next element
                if (head.getCurrent().compareTo(getMinimum()) < 0)
                    head.skipTo(getMinimum());

                Token candidate = head.hasNext() ? head.next() : null;
                if (candidate == null || candidate.get() > getMaximum())
                {
                    ranges.add(head);
                    return endOfData();
                }

                if (processed == null)
                    processed = new ArrayList<>();

                merger.reset();
                merger.add(candidate);

                boolean intersectsAll = true, exhausted = false;
                while (!ranges.isEmpty())
                {
                    RangeIterator range = ranges.poll();

                    // found a range which doesn't overlap with one (or possibly more) other range(s)
                    if (!isOverlapping(head, range))
                    {
                        exhausted = true;
                        intersectsAll = false;
                        FileUtils.closeQuietly(range);
                        break;
                    }

                    Token point = range.skipTo(candidate.get());

                    if (point == null) // other range is exhausted
                    {
                        exhausted = true;
                        intersectsAll = false;
                        FileUtils.closeQuietly(range);
                        break;
                    }

                    processed.add(range);

                    if (candidate.get().equals(point.get()))
                    {
                        merger.add(point);
                        // advance skipped range to the next element if any
                        Iterators.getNext(range, null);
                    }
                    else
                    {
                        intersectsAll = false;
                        break;
                    }
                }

                ranges.add(head);

                ranges.addAll(processed);
                processed.clear();

                if (exhausted)
                    return endOfData();

                if (intersectsAll)
                    return merger.merge();
            }

            return endOfData();
        }

        protected void performSkipTo(Long nextToken)
        {
            List<RangeIterator> skipped = new ArrayList<>();

            while (!ranges.isEmpty())
            {
                RangeIterator range = ranges.poll();
                range.skipTo(nextToken);
                skipped.add(range);
            }

            for (RangeIterator range : skipped)
                ranges.add(range);
        }
    }

    /**
     * Iterator which performs a linear scan over a primary range (the smallest of the ranges)
     * and O(log(n)) lookup into secondary ranges using values from the primary iterator.
     * This technique is efficient when one of the intersection ranges is smaller than others
     * e.g. ratio 0.01d (default), in such situation scan + lookup is more efficient comparing
     * to "bounce" merge because "bounce" distance is never going to be big.
     *
     */
    @VisibleForTesting
    protected static class LookupIntersectionIterator extends AbstractIntersectionIterator
    {
        private final RangeIterator smallestIterator;

        private LookupIntersectionIterator(Builder.Statistics statistics, PriorityQueue<RangeIterator> ranges)
        {
            super(statistics, ranges);

            smallestIterator = statistics.minRange;

            if (smallestIterator.getCurrent().compareTo(getMinimum()) < 0)
                smallestIterator.skipTo(getMinimum());
        }

        protected Token computeNext()
        {
            while (smallestIterator.hasNext())
            {
                Token candidate = smallestIterator.next();
                long token = candidate.get();

                merger.reset();
                merger.add(candidate);

                boolean intersectsAll = true;
                for (RangeIterator range : ranges)
                {
                    // avoid checking against self, much cheaper than changing queue comparator
                    // to compare based on the size and re-populating such queue.
                    if (range.equals(smallestIterator))
                        continue;

                    // found a range which doesn't overlap with one (or possibly more) other range(s)
                    if (!isOverlapping(smallestIterator, range))
                        return endOfData();

                    Token point = range.skipTo(token);

                    if (point == null) // one of the iterators is exhausted
                        return endOfData();

                    if (point.get() != token)
                    {
                        intersectsAll = false;
                        break;
                    }

                    merger.add(point);
                }

                if (intersectsAll)
                    return merger.merge();
            }

            return endOfData();
        }

        protected void performSkipTo(Long nextToken)
        {
            smallestIterator.skipTo(nextToken);
        }
    }
}
