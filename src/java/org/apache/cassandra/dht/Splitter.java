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

package org.apache.cassandra.dht;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Partition splitter.
 */
public class Splitter
{
    private final IPartitioner partitioner;

    protected Splitter(IPartitioner partitioner)
    {
        this.partitioner = partitioner;
    }

    public List<Token> splitOwnedRanges(int parts, List<Range<Token>> localRanges, boolean dontSplitRanges)
    {
        if (localRanges == null || localRanges.isEmpty() || parts == 1)
            return Collections.singletonList(partitioner.getMaximumToken());

        double totalTokens = 0;
        for (Range<Token> r : localRanges)
        {
            totalTokens += r.left.size(r.right);
        }
        double perPart = totalTokens / parts;
        // the range owned is so tiny we can't split it:
        if (perPart == 0)
            return Collections.singletonList(partitioner.getMaximumToken());

        if (dontSplitRanges)
            return splitOwnedRangesNoPartialRanges(localRanges, perPart, parts);

        List<Token> boundaries = new ArrayList<>();
        double sum = 0;
        for (Range<Token> r : localRanges)
        {
            double currentRangeWidth = r.left.size(r.right);
            Token left = r.left;
            while (sum + currentRangeWidth >= perPart)
            {
                double withinRangeBoundary = perPart - sum;
                left = partitioner.split(left, r.right, withinRangeBoundary / currentRangeWidth);
                boundaries.add(left);
                currentRangeWidth = currentRangeWidth - withinRangeBoundary;
                sum = 0;
            }
            sum += currentRangeWidth;
        }
        if (boundaries.size() < parts)
            boundaries.add(partitioner.getMaximumToken());
        else
            boundaries.set(boundaries.size() - 1, partitioner.getMaximumToken());

        assert boundaries.size() == parts : boundaries.size() +"!="+parts+" "+boundaries+":"+localRanges;
        return boundaries;
    }

    private List<Token> splitOwnedRangesNoPartialRanges(List<Range<Token>> localRanges, double perPart, int parts)
    {
        List<Token> boundaries = new ArrayList<>(parts);
        double sum = 0;
        int i = 0;
        while (boundaries.size() < parts - 1)
        {
            Range<Token> r = localRanges.get(i);
            Range<Token> nextRange = localRanges.get(i + 1);

            double currentRangeWidth = r.left.size(r.right);
            double nextRangeWidth = nextRange.left.size(nextRange.right);
            sum += currentRangeWidth;
            // does this or next range take us beyond the per part limit?
            if (sum + nextRangeWidth > perPart)
            {
                // Either this or the next range will take us beyond the perPart limit. Will stopping now or
                // adding the next range create the smallest difference to perPart?
                double diffCurrent = Math.abs(sum - perPart);
                double diffNext = Math.abs(sum + nextRangeWidth - perPart);
                if (diffNext >= diffCurrent)
                {
                    sum = 0;
                    boundaries.add(r.right);
                }
            }
            i++;
        }
        boundaries.add(partitioner.getMaximumToken());
        return boundaries;
    }
}
