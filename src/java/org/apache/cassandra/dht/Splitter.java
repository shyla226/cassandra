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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import static java.util.stream.Collectors.toSet;

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
                double ratio = withinRangeBoundary / currentRangeWidth;
                left = partitioner.split(left, r.right, Math.min(ratio, 1.0));
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

        assert boundaries.size() == parts : boundaries.size() + "!=" + parts + " " + boundaries + ":" + localRanges;
        return boundaries;
    }

    private List<Token> splitOwnedRangesNoPartialRanges(List<Range<Token>> localRanges, double perPart, int parts)
    {
        List<Token> boundaries = new ArrayList<>(parts);
        double sum = 0;

        int i = 0;
        final int rangesCount = localRanges.size();
        while (boundaries.size() < parts - 1 && i < rangesCount - 1)
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

    /**
     * Splits the specified token ranges in at least {@code parts} subranges.
     * <p>
     * Each returned subrange will be contained in exactly one of the specified ranges.
     *
     * @param ranges a collection of token ranges to be split
     * @param parts the minimum number of returned ranges
     * @return at least {@code minParts} token ranges covering {@code ranges}
     */
    public Set<Range<Token>> split(Collection<Range<Token>> ranges, int parts)
    {
        int numRanges = ranges.size();
        if (numRanges >= parts)
        {
            return Sets.newHashSet(ranges);
        }
        else
        {
            int partsPerRange = (int) Math.ceil((double) parts / numRanges);
            return ranges.stream()
                         .map(range -> split(range, partsPerRange))
                         .flatMap(Collection::stream)
                         .collect(toSet());
        }
    }

    /**
     * Splits the specified token range in {@code parts} subranges, unless the range has not enough tokens in which case
     * the range will be returned without splitting.
     *
     * @param range a token range
     * @param parts the number of subranges
     * @return {@code parts} even subranges of {@code range}, or {@code range} if it is too small to be splitted
     */
    private Set<Range<Token>> split(Range<Token> range, int parts)
    {
        Token left = range.left;
        Set<Range<Token>> subranges = new LinkedHashSet<>(parts);

        for (double i = 1; i < parts; i++)
        {
            Token right = partitioner.split(range.left, range.right, i / parts);
            if (!left.equals(right))
                subranges.add(new Range<>(left, right));
            left = right;
        }
        subranges.add(new Range<>(left, range.right));
        return subranges;
    }

    /**
     * Computes the normalized position of this token relative to this range
     *
     * @return A number between 0.0 and 1.0 representing this token's position
     * in this range or -1.0 if this range doesn't contain this token.
     */
    public double positionInRange(Token token, Range<Token> range)
    {
        // full range case
        if (range.left.equals(range.right))
            return positionInRange(token, new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken()));

        // leftmost token means we are on position 0.0
        if (token.equals(range.left))
            return 0.0;

        // rightmost token means we are on position 1.0
        if (token.equals(range.right))
            return 1.0;

        // Impossible to find position when token is not contained in range
        if (!range.contains(token))
            return -1.0;

        double rangeSize = range.left.size(range.right);
        double positionSize = range.left.size(token);
        return positionSize / rangeSize;
    }
}
