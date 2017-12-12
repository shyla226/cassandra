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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    @Test
    public void testSplitMurmur3Partitioner()
    {
        IPartitioner partitioner = new Murmur3Partitioner();

        Token ini = partitioner.getMaximumToken();
        Token mid = new Murmur3Partitioner.LongToken(0);

        testSplit(partitioner, ini, mid, TokenFactory.LONG);
    }

    @Test
    public void testSplitRandomPartitioner()
    {
        IPartitioner partitioner = new RandomPartitioner();

        Token ini = new RandomPartitioner.BigIntegerToken(BigInteger.ZERO);
        Token mid = new RandomPartitioner.BigIntegerToken(RandomPartitioner.MAXIMUM.divide(BigInteger.valueOf(2)));

        testSplit(partitioner, ini, mid, TokenFactory.BIGINT);
    }

    @Test
    public void testSplitByteOrderedPartitioner()
    {
        IPartitioner partitioner = new ByteOrderedPartitioner();

        Token ini = new ByteOrderedPartitioner.BytesToken(new byte[]{ 0 });
        Token mid = partitioner.midpoint(partitioner.getMinimumToken(), partitioner.getMaximumToken());

        testSplit(partitioner, ini, mid, TokenFactory.BYTES);
    }

    @SuppressWarnings("unchecked")
    private static void testSplit(IPartitioner partitioner, Token ini, Token mid, TokenFactory factory)
    {
        Splitter splitter = getSplitter(partitioner);
        boolean isBytes = partitioner instanceof ByteOrderedPartitioner;

        Token min = partitioner.getMinimumToken();
        Token max = partitioner.getMaximumToken();
        Token q1 = partitioner.midpoint(min, mid); // first quartile
        Token q3 = partitioner.midpoint(mid, max); // third quartile

        // empty ranges
        testSplit(splitter, 1, ranges(), ranges());
        testSplit(splitter, 2, ranges(), ranges());

        // full ring min-min
        Set<Range<Token>> ranges = ranges(range(min, min));
        testSplit(splitter, 1, ranges, ranges);
        testSplit(splitter, 2, ranges, ranges(range(min, mid), range(mid, min)));
        testSplit(splitter, 4, ranges,
                  ranges(range(min, q1), range(q1, mid), range(mid, q3), range(q3, min)));

        // full ring min-max
        ranges = ranges(range(min, max));
        testSplit(splitter, 1, ranges, ranges);
        testSplit(splitter, 2, ranges, ranges(range(min, mid), range(mid, max)));
        testSplit(splitter, 4, ranges, ranges(range(min, q1), range(q1, mid), range(mid, q3), range(q3, max)));

        // full ring any-any
        testSplit(splitter, 1, ranges(range(mid, mid)), ranges(range(mid, mid)));
        testSplit(splitter, 2, ranges(range(mid, mid)), ranges(range(mid, ini), range(ini, mid)));
        testSplit(splitter, 1, ranges(range(q1, q1)), ranges(range(q1, q1)));
        testSplit(splitter, 4,
                  ranges(range(q1, q1)),
                  ranges(range(q1, mid), range(mid, q3), range(q3, ini), range(ini, q1)));

        // regular single range
        testSplit(splitter, 1, ranges(factory.range(1, 100)), ranges(factory.range(1, 100)));
        testSplit(splitter, 2, ranges(factory.range(1, 100)), ranges(factory.range(1, 50), factory.range(50, 100)));
        testSplit(splitter, 4,
                  ranges(factory.range(1, 100)),
                  ranges(factory.range(1, 25), factory.range(25, 50), factory.range(50, 75), factory.range(75, 100)));
        testSplit(splitter, 5,
                  ranges(factory.range(3, 79)),
                  ranges(factory.range(3, 18),
                         factory.range(18, 33),
                         factory.range(33, 48),
                         factory.range(48, 63),
                         factory.range(63, 79)));
        testSplit(splitter, 3,
                  ranges(factory.range(3, 20)),
                  ranges(factory.range(3, 8), factory.range(8, 14), factory.range(14, 20)));
        testSplit(splitter, 4,
                  ranges(factory.range(3, 20)),
                  ranges(factory.range(3, 7), factory.range(7, 11), factory.range(11, 15), factory.range(15, 20)));
        testSplit(splitter, 2, ranges(range(q1, q3)), ranges(range(q1, mid), range(mid, q3)));

        // wrapping single range
        testSplit(splitter, 2, ranges(range(q3, q1)), ranges(range(q3, ini), range(ini, q1)));

        // single range with less tokens than parts, we can't easily do this for ByteOrderedPartitioner
        if (!isBytes)
        {
            testSplit(splitter, 1, ranges(factory.range(1, 2)), ranges(factory.range(1, 2)));
            testSplit(splitter, 2, ranges(factory.range(1, 2)), ranges(factory.range(1, 2)));
            testSplit(splitter, 8, ranges(factory.range(1, 2)), ranges(factory.range(1, 2)));
            testSplit(splitter, 3, ranges(factory.range(1, 3)), ranges(factory.range(1, 2), factory.range(2, 3)));
            testSplit(splitter, 4,
                      ranges(factory.range(1, 4)),
                      ranges(factory.range(1, 2), factory.range(2, 3), factory.range(3, 4)));
            testSplit(splitter, 8,
                      ranges(factory.range(1, 4)),
                      ranges(factory.range(1, 2), factory.range(2, 3), factory.range(3, 4)));
        }

        // multiple ranges
        testSplit(splitter, 1,
                  ranges(factory.range(1, 100), factory.range(200, 300)),
                  ranges(factory.range(1, 100), factory.range(200, 300)));
        testSplit(splitter, 2,
                  ranges(factory.range(1, 100), factory.range(200, 300)),
                  ranges(factory.range(1, 100), factory.range(200, 300)));
        testSplit(splitter, 4,
                  ranges(factory.range(1, 100), factory.range(200, 300), range(q3, q1)),
                  ranges(factory.range(1, 50),
                         factory.range(50, 100),
                         factory.range(200, 250),
                         factory.range(250, 300),
                         range(q3, ini), range(ini, q1)));
    }

    private static void testSplit(Splitter splitter, int parts, Set<Range<Token>> ranges, Set<Range<Token>> expected)
    {
        Set<Range<Token>> splittedRanges = splitter.split(ranges, parts);
        assertEquals(expected, splittedRanges);

        // verify that the ranges are equivalent
        assertEquals(0, ranges.stream().mapToInt(r -> r.subtractAll(splittedRanges).size()).sum());
        assertEquals(0, splittedRanges.stream().mapToInt(r -> r.subtractAll(ranges).size()).sum());
    }

    @Test
    public void testPositionInRangeMurmur3Partitioner()
    {
        testPositionInRange(new Murmur3Partitioner(), TokenFactory.LONG);
    }

    @Test
    public void testPositionInRangeRandomPartitioner()
    {
        testPositionInRange(new RandomPartitioner(), TokenFactory.BIGINT);
    }

    @Test
    public void testPositionInRangeByteOrderedPartitioner()
    {
        testPositionInRange(new ByteOrderedPartitioner(), TokenFactory.BYTES);
    }

    private static void testPositionInRange(IPartitioner partitioner, TokenFactory factory)
    {
        Splitter splitter = getSplitter(partitioner);

        Token min = partitioner.getMinimumToken();
        Token max = partitioner.getMaximumToken();
        Token mid = partitioner.midpoint(min, max);
        Token q1 = partitioner.midpoint(min, mid); // first quartile
        Token q3 = partitioner.midpoint(mid, max); // third quartile

        // Test full ring
        for (Range<Token> range : Arrays.asList(range(min, max), range(min, min)))
        {
            testPositionInRange(splitter, range, 0.0, min);
            testPositionInRange(splitter, range, 0.5, mid);
            testPositionInRange(splitter, range, 1.0, max);
            testPositionInRange(splitter, range, 0.25, q1);
            testPositionInRange(splitter, range, 0.75, q3);
        }

        // Test tiny range
        Range<Token> range = factory.range(0, 3);
        testPositionInRange(splitter, range, 0.0, range.left);
        testPositionInRange(splitter, range, 1.0, range.right);
        testPositionInRange(splitter, range, 0.33, factory.token(1));
        testPositionInRange(splitter, range, 0.66, factory.token(2));
        testPositionInRange(splitter, range, -1, factory.token(10));

        // Test medium range
        range = factory.range(0, 1000);
        testPositionInRange(splitter, range, 0.0, range.left);
        testPositionInRange(splitter, range, 1.0, range.right);
        testPositionInRange(splitter, range, 0.25, factory.token(250));
        testPositionInRange(splitter, range, 0.5, factory.token(500));
        testPositionInRange(splitter, range, 0.75, factory.token(750));
        testPositionInRange(splitter, range, 0.33, factory.token(333));
        testPositionInRange(splitter, range, 0.99, factory.token(999));
        testPositionInRange(splitter, range, -1.0, factory.token(-1));

        // Test large range
        range = range(mid, max);
        testPositionInRange(splitter, range, 0.0, mid);
        testPositionInRange(splitter, range, 0.5, q3);
        testPositionInRange(splitter, range, 1.0, max);
        testPositionInRange(splitter, range, -1., min);
        testPositionInRange(splitter, range, -1., q1);

        // Test wrap-around range
        range = range(q3, q1);
        testPositionInRange(splitter, range, 0.0, q3);
        testPositionInRange(splitter, range, 0.5, max);
        testPositionInRange(splitter, range, 0.5, min);
        testPositionInRange(splitter, range, 1.0, q1);
        testPositionInRange(splitter, range, -1., mid);
    }

    private static void testPositionInRange(Splitter splitter, Range<Token> range, double expected, Token token)
    {
        assertEquals(expected, splitter.positionInRange(token, range), 0.01);
    }

    private static Splitter getSplitter(IPartitioner partitioner)
    {
        return partitioner.splitter()
                          .orElseThrow(() -> new AssertionError(partitioner.getClass() + " must have a splitter"));
    }

    @SuppressWarnings("unchecked")
    private static Set<Range<Token>> ranges(Range<Token>... ranges)
    {
        return newHashSet(ranges);
    }

    private static Range<Token> range(Token left, Token right)
    {
        return new Range<>(left, right);
    }

    /** Class for building tokens and token ranges for different partitioners from simple integers. */
    private interface TokenFactory
    {
        static final TokenFactory LONG = Murmur3Partitioner.LongToken::new;
        static final TokenFactory BIGINT = x -> new RandomPartitioner.BigIntegerToken(BigInteger.valueOf(x));
        static final TokenFactory BYTES = x -> new ByteOrderedPartitioner.BytesToken(Int32Type.instance.decompose(x));

        Token token(int value);

        default Range<Token> range(int left, int right)
        {
            return new Range<>(token(left), token(right));
        }
    }
}
