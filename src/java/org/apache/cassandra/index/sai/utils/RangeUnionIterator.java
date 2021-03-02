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
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

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
 * Modified from {@link org.apache.cassandra.index.sasi.utils.RangeUnionIterator} to support:
 * 1. no generic type to reduce allocation=
 * 2. make sure iterators are closed when intersection ends because of lazy key fetching
 */
@SuppressWarnings("resource")
public class RangeUnionIterator extends RangeIterator
{
    // Due to lazy key fetching, we cannot close iterator immediately
    private final PriorityQueue<RangeIterator> ranges;

    // If the ranges are deferred then the ranges queue is not
    // necessarily in order so we need to maintain a separate queue
    // of candidate tokens until the ranges queue is ordered correctly
    private final PriorityQueue<PrimaryKey> candidates;

    private final List<RangeIterator> toRelease;

    private RangeUnionIterator(Builder.Statistics statistics, PriorityQueue<RangeIterator> ranges)
    {
        super(statistics);
        this.ranges = ranges;
        this.candidates = new PriorityQueue<>(ranges.size());
        this.toRelease = new ArrayList<>(ranges);
    }

    public PrimaryKey computeNext()
    {
        PrimaryKey candidate;
        List<RangeIterator> processedRanges = new ArrayList<>(ranges.size());

        // Only poll the ranges for a new candidate if the candidates queue is empty.
        // Otherwise, always start with a candidate from the candidates queue until
        // it is empty.
        if (candidates.isEmpty())
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

            candidate = head.next();

            if (head.hasNext())
                processedRanges.add(head);
        }
        else
        {
            candidate = candidates.poll();
            // may have duplicates in the candidates queue so flush them out before continuing
            while (!candidates.isEmpty())
            {
                if (candidate.compareTo(candidates.peek()) < 0)
                    break;
                candidates.poll();
            }
        }

        PrimaryKey minCurrent = ranges.stream().map(RangeIterator::getCurrent).min(Comparator.naturalOrder()).get();

        if (candidate.compareTo(minCurrent) < 0)
        {
            ranges.addAll(processedRanges);
            return candidate;
        }

        while (!ranges.isEmpty())
        {
            RangeIterator range = ranges.poll();

            if (!range.hasNext())
                continue;

            int cmp = candidate.compareTo(range.getCurrent());

            if (cmp > 0)
            {
                candidates.add(candidate);
                candidate = range.next();
            }
            else
            {
                candidates.add(range.next());
            }

            processedRanges.add(range);
        }

        ranges.addAll(processedRanges);
        return candidate;
    }

    protected void performSkipTo(PrimaryKey nextKey)
    {
        while (!candidates.isEmpty())
        {
            PrimaryKey candidate = candidates.peek();
            if (candidate.compareTo(nextKey) >= 0)
                break;
            candidates.poll();
        }
        while (!ranges.isEmpty())
        {
            if (ranges.peek().getCurrent().compareTo(nextKey) >= 0)
                break;

            RangeIterator head = ranges.poll();

            if (head.getMaximum().compareTo(nextKey) >= 0)
            {
                head.skipTo(nextKey);
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
