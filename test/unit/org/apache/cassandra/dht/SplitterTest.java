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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

public class SplitterTest
{

    @Test
    public void randomSplitTestNoVNodesRandomPartitioner()
    {
        randomSplitTestNoVNodes(new RandomPartitioner());
    }

    @Test
    public void randomSplitTestNoVNodesMurmur3Partitioner()
    {
        randomSplitTestNoVNodes(new Murmur3Partitioner());
    }

    @Test
    public void randomSplitTestNoVNodesByteOrderedPartitioner()
    {
        randomSplitTestNoVNodes(ByteOrderedPartitioner.instance);
    }

    @Test
    public void randomSplitTestVNodesRandomPartitioner()
    {
        randomSplitTestVNodes(new RandomPartitioner());
    }
    @Test
    public void randomSplitTestVNodesMurmur3Partitioner()
    {
        randomSplitTestVNodes(new Murmur3Partitioner());
    }
    @Test
    public void randomSplitTestVNodesByteOrderedPartitioner()
    {
        randomSplitTestVNodes(ByteOrderedPartitioner.instance);
    }

    @Test
    public void randomSplitTestRandomPartitioner()
    {
        randomSplitTest(new RandomPartitioner());
    }

    @Test
    public void randomSplitTestMurmur3Partitioner()
    {
        randomSplitTest(new Murmur3Partitioner());
    }

    @Test
    public void randomSplitTestByteOrderedPartitioner()
    {
        randomSplitTest(ByteOrderedPartitioner.instance);
    }

    public void randomSplitTestNoVNodes(IPartitioner partitioner)
    {
        Splitter splitter = partitioner.splitter().get();
        Random r = new Random();
        for (int i = 0; i < 10000; i++)
        {
            int parts = r.nextInt(9) + 1;
            boolean includeMin = r.nextBoolean();
            boolean includeMax = r.nextBoolean();

            List<Range<Token>> localRanges = generateLocalRanges(1,
                                                                 r.nextInt(4) + 1,
                                                                 r,
                                                                 partitioner,
                                                                 includeMin,
                                                                 includeMax);
            List<Token> boundaries = splitter.splitOwnedRanges(parts, localRanges, false);
            assertEquals("localRange = " + localRanges, parts, boundaries.size());

            assertTrue("boundaries = " + boundaries + " ranges = " + localRanges,
                       assertRangeSizeEqual(localRanges, boundaries, partitioner, true));
        }
    }

    public void randomSplitTest(IPartitioner partitioner)
    {
        Splitter splitter = partitioner.splitter().get();

        Random r = new Random();
        for (int i = 0; i < 10000; i++)
        {
            int numTokens = 1 + r.nextInt(32);
            int rf = r.nextInt(4) + 1;
            int parts = 1 + r.nextInt(32);
            boolean includeMin = r.nextBoolean();
            boolean includeMax = r.nextBoolean();

            List<Range<Token>> localRanges = generateLocalRanges(numTokens, rf, r, partitioner, includeMin, includeMax);

            List<Token> boundaries = splitter.splitOwnedRanges(parts, localRanges, false);

            assertTrue("numTokens = " + numTokens + " parts = " + parts + " boundaries = " + boundaries.size(),
                       parts == boundaries.size());
            assertTrue("boundaries = " + boundaries + " ranges = " + localRanges,
                       assertRangeSizeEqual(localRanges, boundaries, partitioner, true));
        }
    }

    public void randomSplitTestVNodes(IPartitioner partitioner)
    {
        Splitter splitter = partitioner.splitter().get();
        Random r = new Random();
        for (int i = 0; i < 10000; i++)
        {
            // we need many tokens to be able to split evenly over the disks
            int numTokens = 172 + r.nextInt(128);
            int rf = r.nextInt(4) + 2;
            int parts = r.nextInt(5)+1;
            boolean includeMin = r.nextBoolean();
            boolean includeMax = r.nextBoolean();
            List<Range<Token>> localRanges = generateLocalRanges(numTokens, rf, r, partitioner, includeMin, includeMax);
            List<Token> boundaries = splitter.splitOwnedRanges(parts, localRanges, true);
            if (!assertRangeSizeEqual(localRanges, boundaries, partitioner, false))
                fail(String.format("Could not split %d tokens with rf=%d into %d parts (localRanges=%s, boundaries=%s)", numTokens, rf, parts, localRanges, boundaries));
        }
    }

    private boolean assertRangeSizeEqual(List<Range<Token>> localRanges, List<Token> tokens, IPartitioner partitioner, 
                                         boolean splitIndividualRanges)
    {
        Token start = partitioner.getMinimumToken();
        List<Double> splits = new ArrayList<>();

        for (int i = 0; i < tokens.size(); i++)
        {
            if (i < tokens.size() - 1) // sorted
                assert tokens.get(i).compareTo(tokens.get(i + 1)) < 0;
            Token end = tokens.get(i);
            splits.add(sumOwnedBetween(localRanges, start, end, splitIndividualRanges));
            start = end;
        }

        // For byte-order, the difference could be large. There is no real fixed max for byte-order
        if (partitioner.preservesOrder())
            return true;

        // when we dont need to keep around full ranges, the difference is small between the partitions
        double delta = splitIndividualRanges ? 0.001 : 0.2;
        boolean allBalanced = true;
        for (double b : splits)
        {
            for (double i : splits)
            {
                double q = b / i;
                if (Math.abs(q - 1) > delta)
                    allBalanced = false;
            }
        }
        return allBalanced;
    }

    private double sumOwnedBetween(List<Range<Token>> localRanges, Token start, Token end, boolean splitIndividualRanges)
    {
        double sum = 0;
        for (Range<Token> range : localRanges)
        {
            if (splitIndividualRanges)
            {
                Set<Range<Token>> intersections = new Range<>(start, end).intersectionWith(range);
                for (Range<Token> intersection : intersections)
                    sum += intersection.left.size(intersection.right);
            }
            else
            {
                if (new Range<>(start, end).contains(range.left))
                    sum += range.left.size(range.right);
            }
        }
        return sum;
    }

    private static List<Range<Token>> generateLocalRanges(int numTokens,
                                                          int rf,
                                                          Random r,
                                                          IPartitioner partitioner,
                                                          boolean includeMin,
                                                          boolean includeMax)
    {
        int localTokens = numTokens * rf;
        List<Token> randomTokens = new ArrayList<>();

        int i = 0;
        if (includeMin)
        {
            i++;
            randomTokens.add(partitioner.getMinimumToken());
        }
        if (includeMax)
        {
            i++;
            randomTokens.add(partitioner.getMaximumToken());
        }
        for (; i < localTokens * 2; i++)
        {
            Token t = partitioner.getRandomToken(r);
            randomTokens.add(t);
        }

        Collections.sort(randomTokens);

        List<Range<Token>> localRanges = new ArrayList<>(localTokens);
        for (i = 0; i < randomTokens.size() - 1; i++)
        {
            assert randomTokens.get(i).compareTo(randomTokens.get(i+1)) < 0;
            localRanges.add(new Range<>(randomTokens.get(i), randomTokens.get(i+1)));
            i++;
        }
        return localRanges;
    }
}
