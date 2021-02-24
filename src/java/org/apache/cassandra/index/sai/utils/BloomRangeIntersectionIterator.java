/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.index.sai.utils;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.BloomFilterAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.Token;
import org.apache.cassandra.index.sai.disk.PostingListRangeIterator;
import org.apache.cassandra.index.sai.memory.KeyRangeIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;

/**
 * Modified from {@link org.apache.cassandra.index.sasi.utils.RangeIntersectionIterator} to support:
 * 1. no generic type to reduce allocation
 * 2. support selective intersection to reduce disk io
 * 3. make sure iterators are closed when intersection ends because of lazy key fetching
 */
@SuppressWarnings("resource")
public class BloomRangeIntersectionIterator
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // The cassandra.sai.intersection.clause.limit (default: 2) controls the maximum number of range iterator that
    // will be used in the final intersection of a query operation.
    private static final int INTERSECTION_CLAUSE_LIMIT = Integer.getInteger("cassandra.sai.intersection.clause.limit", 2);

    static
    {
        logger.info(String.format("Storage attached index intersection clause limit is %d", INTERSECTION_CLAUSE_LIMIT));
    }

    public static boolean shouldDefer(int numberOfExpressions)
    {
        return (INTERSECTION_CLAUSE_LIMIT <= 0) || (numberOfExpressions <= INTERSECTION_CLAUSE_LIMIT);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder selectiveBuilder()
    {
        return selectiveBuilder(INTERSECTION_CLAUSE_LIMIT);
    }

    public static Builder selectiveBuilder(int limit)
    {
        return new Builder(limit);
    }

    public static class Builder extends RangeIterator.Builder
    {
        private final int limit;

        public Builder()
        {
            super(IteratorType.INTERSECTION);
            this.limit = Integer.MAX_VALUE;
        }

        public Builder(int limit)
        {
            super(IteratorType.INTERSECTION);
            this.limit = limit;
        }

        protected RangeIterator buildIterator()
        {
            // all ranges will be included
            if (limit >= ranges.size() || limit <= 0)
                return buildIterator(statistics, ranges);

            // Apply most selective iterators during intersection, because larger number of iterators will result lots of disk seek.
            List<RangeIterator> selectiveIterator = new ArrayList<>(ranges);
            selectiveIterator.sort(Comparator.comparingLong(RangeIterator::getCount));

            Statistics selectiveStatistics = new Statistics(IteratorType.INTERSECTION);
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

            return buildIterator(selectiveStatistics, selectiveRanges);
        }

        private static RangeIterator buildIterator(Statistics statistics, PriorityQueue<RangeIterator> ranges)
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

            return new LookupIntersectionIterator(statistics, ranges);
        }
    }

    /**
     * Iterator which performs a linear scan over a primary range (the smallest of the ranges)
     * and O(log(n)) lookup into secondary ranges using values from the primary iterator.
     * This technique is efficient when one of the intersection ranges is smaller than others
     * e.g. ratio 0.01d (default), in such situation scan + lookup is more efficient comparing
     * to "bounce" merge because "bounce" distance is never going to be big.
     */
    @VisibleForTesting
    public static class LookupIntersectionIterator extends RangeIterator
    {
        private final RangeIterator smallestIterator;
        private final List<RangeIterator> toRelease;
        private final PriorityQueue<RangeIterator> ranges;
        private final Token.TokenMerger merger;

        public LookupIntersectionIterator(Builder.Statistics statistics, PriorityQueue<RangeIterator> ranges)
        {
            super(statistics);

            this.merger = new Token.ReusableTokenMerger(ranges.size());

            this.ranges = ranges;

            this.toRelease = new ArrayList<>(ranges);

            smallestIterator = statistics.minRange;

            if (smallestIterator.getCurrent().compareTo(getMinimum()) < 0)
                smallestIterator.skipTo(getMinimum());
        }

        @Override
        protected Token computeNext()
        {
            while (smallestIterator.hasNext())
            {
                // TODO: 'Token candidate' needs to have the sstable ids in an array of the matches
                //       with a parallel array of row ids
                final Token candidate = smallestIterator.next();
                final Long token = candidate.get();

                final long[] tokenHashes = BloomFilterAccessor.getHash(token, (obj, sink) -> sink.putLong(obj.longValue()));

                merger.reset();

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

                    final TermIterator termIterator = (TermIterator)range;
                    Token skipToToken = null;
                    if (termIterator.postingRangeIterators.size() == 0
                        && termIterator.memtableRangeIterators.size() == 0)
                    {
                        // this termIterator is exhausted therefore the intersection is finished
                        return endOfData();
                    }

                    final Iterator<KeyRangeIterator> memIterator = termIterator.memtableRangeIterators.iterator();
                    while (memIterator.hasNext())
                    {
                        final KeyRangeIterator memtableRangeIterator = memIterator.next();

                        // TDOO: a bloom filter may be added to the memtable index

                        final Token token2 = memtableRangeIterator.skipTo(token);
                        if (token2 == null)
                        {
                            // iterator is exhausted, remove it from the list
                            memIterator.remove();
                        }
                        else if (token2.get().equals(token))
                        {
                            skipToToken = token2;
                            // once a match is found in any sstable return
                            // as the read command loads from all sstables later
                            break;
                        }
                    }

                    // the memtable index does not have the token
                    // try the sstables
                    if (skipToToken == null)
                    {
                        final Iterator<PostingListRangeIterator> iterator = termIterator.postingRangeIterators.iterator();
                        while (iterator.hasNext())
                        {
                            final PostingListRangeIterator sstableRangeIterator = iterator.next();

                            // TODO: get the sstable id from the posting list range iterator
                            //       use the token candidate sstable parallel row id array
                            //       compare directly by row id
                            if (sstableRangeIterator.maybeContains(tokenHashes))
                            {
                                final Token token2 = range.skipTo(token);
                                if (token2 == null)
                                {
                                    // iterator is exhausted, remove it from the list
                                    iterator.remove();
                                }
                                else if (token2.get().equals(token))
                                {
                                    skipToToken = token2;
                                    // once a match is found in any sstable return
                                    // as the read command loads from all sstables later
                                    break;
                                }
                            }
                        }
                    }

                    if (skipToToken == null)
                    {
                        intersectsAll = false;
                        break;
                    }

                    merger.add(skipToToken);
                }

                if (intersectsAll)
                    return candidate;
            }

            return endOfData();
        }

        @Override
        protected void performSkipTo(Long nextToken)
        {
            smallestIterator.skipTo(nextToken);
        }

        @Override
        public void close() throws IOException
        {
            toRelease.forEach(FileUtils::closeQuietly);
        }
    }
}
