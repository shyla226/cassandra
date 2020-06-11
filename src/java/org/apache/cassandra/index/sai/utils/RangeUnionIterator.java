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
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.cassandra.index.sai.Token;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Range Union Iterator is used to return sorted stream of elements from multiple RangeIterator instances.
 *
 * PriorityQueue is used as a sorting mechanism for the ranges, where each computeNext() operation would poll
 * from the queue (and push when done), which returns range that contains the smallest element, because
 * sorting is done on the moving window of range iteration {@link RangeIterator#getCurrent()}. Once retrieved
 * the smallest element (return candidate) is attempted to be merged with other ranges, because there could
 * be equal elements in adjacent ranges, such ranges are poll'ed only if their {@link RangeIterator#getCurrent()}
 * equals to the return candidate.
 *
 *
 * Modified from {@link org.apache.cassandra.index.sasi.utils.RangeUnionIterator} to support:
 * 1. no generic type to reduce allocation=
 * 2. make sure iterators are closed when intersection ends because of lazy key fetching
 */
@SuppressWarnings("resource")
public class RangeUnionIterator extends RangeIterator
{
    // Due to lazy key fetching, we cannot close iterator immediately
    private final PriorityQueue<RangeIterator> ranges;

    private final Token.TokenMerger merger;
    private final List<RangeIterator> toRelease;

    private RangeUnionIterator(Builder.Statistics statistics, PriorityQueue<RangeIterator> ranges)
    {
        super(statistics);
        this.ranges = ranges;
        this.merger = new Token.ReusableTokenMerger(ranges.size());
        this.toRelease = new ArrayList<>(ranges);
    }

    public Token computeNext()
    {
        RangeIterator head = null;

        while (!ranges.isEmpty())
        {
            head = ranges.poll();
            if (head.hasNext())
                break;
        }

        if (head == null || !head.hasNext())
            return endOfData();

        Token candidate = head.next();

        List<RangeIterator> processedRanges = new ArrayList<>();

        if (head.hasNext())
            processedRanges.add(head);

        merger.reset();
        merger.add(candidate);

        while (!ranges.isEmpty())
        {
            // peek here instead of poll is an optimization
            // so we can re-insert less ranges back if candidate
            // is less than head of the current range.
            RangeIterator range = ranges.peek();

            int cmp = Long.compare(candidate.get(), range.getCurrent());

            assert cmp <= 0;

            if (cmp < 0)
            {
                break; // candidate is smaller than next token, return immediately
            }
            else if (cmp == 0)
            {
                merger.add(range.next()); // consume and merge

                range = ranges.poll();
                // re-prioritize changed range

                if (range.hasNext())
                    processedRanges.add(range);
            }
        }

        ranges.addAll(processedRanges);
        return merger.merge();
    }

    protected void performSkipTo(Long nextToken)
    {
        while (!ranges.isEmpty())
        {
            if (ranges.peek().getCurrent().compareTo(nextToken) >= 0)
                break;

            RangeIterator head = ranges.poll();

            if (head.getMaximum().compareTo(nextToken) >= 0)
            {
                head.skipTo(nextToken);
                if (head.hasNext())
                {
                    ranges.add(head);
                    continue;
                }
            }
        }
    }

    public void close() throws IOException
    {
        // Due to lazy key fetching, we cannot close iterator immediately
        toRelease.forEach(FileUtils::closeQuietly);
        ranges.forEach(FileUtils::closeQuietly);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static RangeIterator build(List<RangeIterator> tokens)
    {
        return new Builder().add(tokens).build();
    }

    public static class Builder extends RangeIterator.Builder
    {
        public Builder()
        {
            super(IteratorType.UNION);
        }

        protected RangeIterator buildIterator()
        {
            switch (rangeCount())
            {
                case 1:
                    return ranges.poll();

                default:
                    return new RangeUnionIterator(statistics, ranges);
            }
        }
    }
}
