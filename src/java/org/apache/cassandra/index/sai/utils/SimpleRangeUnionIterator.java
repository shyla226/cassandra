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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.Token;
import org.apache.cassandra.io.util.FileUtils;

public class SimpleRangeUnionIterator extends RangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final List<RangeIterator> ranges;
    private final int size;
    private final Token.TokenMerger merger;
    private final List<RangeIterator> toRelease;
    private final List<RangeIterator> candidates = new ArrayList<>();


    public SimpleRangeUnionIterator(Builder.Statistics statistics, List<RangeIterator> ranges)
    {
        super(statistics);
        this.ranges = ranges;
        this.size = ranges.size();
        this.merger = new Token.ReusableTokenMerger(ranges.size());
        this.toRelease = new ArrayList<>(ranges);
    }

    @Override
    protected void performSkipTo(Long nextToken)
    {
        for (RangeIterator range : ranges)
        {
            if (range.hasNext())
                range.skipTo(nextToken);
        }
    }

    @Override
    public void close() throws IOException
    {
        toRelease.forEach(FileUtils::closeQuietly);
        ranges.forEach(FileUtils::closeQuietly);
    }

    @Override
    protected Token computeNext()
    {
        merger.reset();
        candidates.clear();
        Token candidate = null;
        for (RangeIterator range : ranges)
        {
            if (range.hasNext())
            {
                if (candidate == null)
                {
                    candidate = range.peek();
                    candidates.add(range);
                    merger.add(candidate);
                }
                else
                {
                    int cmp = candidate.compareTo(range.peek());
                    if (cmp == 0)
                    {
                        candidates.add(range);
                        merger.add(range.peek());
                    }
                    else if (cmp > 0)
                    {
                        candidates.clear();
                        merger.reset();
                        candidate = range.peek();
                        merger.add(candidate);
                        candidates.add(range);
                    }
                }
            }
        }
        if (candidates.isEmpty())
            return endOfData();
        candidates.forEach(RangeIterator::next);
        return merger.merge();
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
        protected List<RangeIterator> simpleRanges = new ArrayList<>();

        public Builder()
        {
            super(IteratorType.UNION);
        }

        public RangeIterator.Builder add(RangeIterator range)
        {
            if (range == null)
                return this;

            if (range.getCount() > 0)
            {
                simpleRanges.add(range);
                statistics.update(range);
            }
            else
                FileUtils.closeQuietly(range);

            return this;
        }

        public RangeIterator.Builder add(List<RangeIterator> ranges)
        {
            if (ranges == null || ranges.isEmpty())
                return this;

            ranges.forEach(this::add);
            return this;
        }

        public int rangeCount()
        {
            return simpleRanges.size();
        }

        protected RangeIterator buildIterator()
        {
            switch (rangeCount())
            {
                case 1:
                    return simpleRanges.get(0);

                default:
                    return new SimpleRangeUnionIterator(statistics, simpleRanges);
            }
        }
    }
}
