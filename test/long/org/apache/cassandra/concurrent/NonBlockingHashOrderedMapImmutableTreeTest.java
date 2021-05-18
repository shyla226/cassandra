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
package org.apache.cassandra.concurrent;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.HashComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NonBlockingHashOrderedMapImmutableTreeTest
{
    @Test
    public void testLimitedRanges() throws InterruptedException
    {
        Collection<Range<Token>> ranges = new ArrayList<>(16);
        // Pick 4 ranges out of 16 possible ranges
        long partRange = BigInteger.valueOf(Murmur3Partitioner.MAXIMUM)
                                   .subtract(BigInteger.valueOf(Murmur3Partitioner.MINIMUM.comparableHashCode()))
                                   .divide(BigInteger.valueOf(1024)).longValue();

        int[] parts = ThreadLocalRandom.current().ints(0, 1023).distinct().limit(16).sorted().toArray();
        for (int part : parts)
        {
            long min = Murmur3Partitioner.MINIMUM.comparableHashCode() + part * partRange;
            long max = min + partRange;
            ranges.add(new Range<>(new Murmur3Partitioner.LongToken(min), new Murmur3Partitioner.LongToken(max)));
        }

        doTest(ranges, 32768);
    }

    void doTest(Collection<Range<Token>> acceptedRanges, int count)
    {
        NonBlockingHashOrderedMap<Key, Integer> map = new NonBlockingHashOrderedMap<>(acceptedRanges);
        int round = 1;
        // Insert items to selected ranges only, nothing outside. They should be backed by different indexes
        for (Range<Token> acceptedRange : acceptedRanges)
        {
            long indexInterval = ((acceptedRange.right.comparableHashCode() - acceptedRange.left.comparableHashCode()) / count) - 1;
            for (int i = 0; i < count; i++)
            {
                int value = (round * (count << 2)) + i;
                long hash = acceptedRange.left.comparableHashCode() + (i * indexInterval);
                map.putIfAbsent(new Key(hash, value), 0);
            }
            round++;
        }

        assertTrue(map.valid()); // Check map's hash order

        // Verify that all the indexes are backed by a single linked list (reader's view) and nothing was lost
        int mapCount = 0;
        long previousValue = 0;
        for (Map.Entry<Key, Integer> keyIntegerEntry : map.range(null, null))
        {
            assertTrue(previousValue <= keyIntegerEntry.getKey().value);
            previousValue = keyIntegerEntry.getKey().value;
            mapCount++;
        }
        assertEquals(acceptedRanges.size() * count, mapCount);

        List<NonBlockingHashOrderedMap.RangeIndexNode> indexes = map.getIndexes();
        assertEquals(0, map.getIndexes().get(0).size); // overflowIndex should be empty
        for (int i = 1; i < indexes.size(); i++)
        {
            // This is the amount of items that each indexNode should be indexing
            assertEquals(count, indexes.get(i).size);
        }
    }

    private static class Key implements HashComparable<Key>
    {
        final long value;
        final long hash;

        public Key(long hash, long value)
        {
            this.hash = hash;
            this.value = value;
        }

        @Override
        public long comparableHashCode()
        {
            return hash;
        }

        @Override
        public int compareTo(Key other)
        {
            return Long.compare(this.value, other.value);
        }
    }
}
