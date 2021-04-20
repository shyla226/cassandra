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

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

/**
 * The unified compaction strategy is described in this design document:
 *
 * TODO: link to design doc or SEP
 *
 * It has properties of  both tiered and leveled compactions and it adapts to the workload
 * by switching between strategies or increasing / decreasing the fanout factor.
 *
 * The essential formulae are the calculations of buckets:
 *
 * S = ⌊log_oF(size / m)⌋ = ⌊(ln size - ln m) / (ln F + ln o)⌋
 *
 * where log_oF is the log with oF as the base
 * o is the survival factor, currently fixed to 1
 * F is the fanout factor calculated below
 * m is the minimal size, fixed in the strategy options
 * size is the sorted run size (sum of all the sizes of the sstables in the sorted run)
 *
 * Also, T is the number of sstables that trigger compaction.
 *
 * Give a parameter W, which is fixed in these tests, then T and F are calculated as follows:
 *
 * - W < 0 then T = 2 and F = 2 - W (leveled merge policy)
 * - W > 0 then T = F and F = 2 + W (tiered merge policy)
 * - W = 0 then T = F = 2 (middle ground)
 */
public class UnifiedCompactionStrategyTest extends BaseCompactionStrategyTest
{
    @BeforeClass
    public static void setUpClass()
    {
        BaseCompactionStrategyTest.setUpClass();
    }

    @Before
    public void setUp()
    {
        super.setUp();
    }

    @Test
    public void testNoSSTables()
    {
        Controller controller = Mockito.mock(Controller.class);
        long minimalSizeBytes = 2 << 20;
        when(controller.getMinSSTableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getW(anyInt())).thenReturn(4);
        when(controller.getSurvivalFactor()).thenReturn(1.0);

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        assertNull(strategy.getNextBackgroundTask(FBUtilities.nowInSeconds()));
        assertEquals(0, strategy.getEstimatedRemainingTasks());
    }

    @Test
    public void testGetBucketsSameW()
    {
        final int m = 2; // minimal sorted run size in MB m
        final Map<Integer, Integer> sstables = new TreeMap<>();

        for (int i = 0; i < 20; i++)
        {
            int numSSTables = 2 + random.nextInt(18);
            sstables.put(m * i, numSSTables);
        }

        // W = 3, o = 1 => F = 5, T = 5 => expected T sstables and 2 buckets: 0-10m, 10-50m
        testGetBuckets(sstables, new int[] { 3 }, m, new int[] {5, 5});

        // W = 2, o = 1 => F = 4, T = 4 => expected T sstables and 3 buckets: 0-8m, 8-32m, 32-128m
        testGetBuckets(sstables, new int[] { 2 }, m, new int[] {4, 4, 4});

        // W = 0, o = 1 => F = 2, T = 2 => expected 2 sstables and 5 buckets: 0-4m, 4-8m, 8-16m, 16-32m, 32-64m
        testGetBuckets(sstables, new int[] { 0 }, m, new int[] {2, 2, 2, 2, 2});

        // W = -2, o = 1 => F = 4, T = 2 => expected 2 sstables and 3 buckets: 0-8mb, 8-32m, 32-128m
        testGetBuckets(sstables, new int[] { -2 }, m, new int[] {2, 2, 2});

        // W = -3, o = 1 => F = 5, T = 2 => expected 2 sstables and 2 buckets: 0-10m, 10-50m
        testGetBuckets(sstables, new int[] { -3 }, m, new int[] {2, 2});

        // remove sstables from 4m to 8m to create an empty bucket in the next call
        sstables.remove(4); // 4m
        sstables.remove(6); // 6m
        sstables.remove(8); // 8m

        // W = 0, o = 1 => F = 2, T = 2 => expected 2 sstables and 5 buckets: 0-4m, 4-8m, 8-16m, 16-32m, 32-64m
        testGetBuckets(sstables, new int[] { 0 }, m, new int[] {2, 2, 2, 2, 2});
    }

    @Test
    public void testGetBucketsDifferentWs()
    {
        final int m = 2; // minimal sorted run size in MB m
        final Map<Integer, Integer> sstables = new TreeMap<>();

        for (int i : new int[] { 50, 100, 200, 400, 600, 800, 1000})
        {
            int numSSTables = 2 + random.nextInt(18);
            sstables.put(i, numSSTables);
        }

        // W = [30, 2, -6], o = 1 => F = [32, 4, 8] , T = [32, 4, 2]  => expected 3 buckets: 0-64m, 64-256m 256-2048m
        testGetBuckets(sstables, new int[]{ 30, 2, -6 }, m, new int[] {32, 4, 2});

        // W = [30, 6, -8], o = 1 => F = [32, 8, 10] , T = [32, 8, 2]  => expected 3 buckets: 0-64m, 64-544m 544-5440m
        testGetBuckets(sstables, new int[]{ 30, 6, -8 }, m, new int[] {32, 8, 2});

        // W = [0, 0, 0, -2, -2], o = 1 => F = [2, 2, 2, 4, 4] , T = [2, 2, 2, 2, 2]  => expected 6 buckets: 0-4m, 4-8m, 8-16m, 16-64m, 64-256m, 256-1024m
        testGetBuckets(sstables, new int[]{ 0, 0, 0, -2, -2 }, m, new int[] {2, 2, 2, 2, 2, 2});
    }

    private void testGetBuckets(Map<Integer, Integer> sstables, int[] Ws, int m, int[] expectedTs)
    {
        long minimalSizeBytes = m << 20;

        Controller controller = Mockito.mock(Controller.class);
        when(controller.getMinSSTableSizeBytes()).thenReturn(minimalSizeBytes);

        when(controller.getW(anyInt())).thenAnswer(answer -> {
            int index = answer.getArgument(0);
            return Ws[index < Ws.length ? index : Ws.length - 1];
        });

        when(controller.getSurvivalFactor()).thenReturn(1.0);

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        IPartitioner partitioner = cfs.getPartitioner();
        DecoratedKey first = new BufferDecoratedKey(partitioner.getMinimumToken(), ByteBuffer.allocate(0));
        DecoratedKey last = new BufferDecoratedKey(partitioner.getMinimumToken(), ByteBuffer.allocate(0));

        for(Map.Entry<Integer, Integer> entry : sstables.entrySet())
        {
            for (int i = 0; i < entry.getValue(); i++)
            {
                // we want a number > 0 and < 1 so that the sstable has always some size and never crosses the boundary to the next bucket
                // so we leave a 1% margin, picking a number from 0.01 to 0.99
                double rand = 0.01 + 0.98 * random.nextDouble();
                long sizeOnDiskBytes = (entry.getKey() << 20) + (long) (minimalSizeBytes * rand);
                strategy.addSSTable(mockSSTable(sizeOnDiskBytes, System.currentTimeMillis(), first, last));
            }
        }

        List<UnifiedCompactionStrategy.Bucket> buckets = strategy.getBuckets();
        assertNotNull(buckets);

        assertEquals(expectedTs.length, buckets.size());

        for (int i = 0; i < expectedTs.length; i++)
        {
            UnifiedCompactionStrategy.Bucket bucket = buckets.get(i);
            if (bucket.sstables.size() >= expectedTs[i])
                assertFalse(bucket.getCompactionAggregate().isEmpty());
            else
                assertTrue(bucket.getCompactionAggregate().isEmpty());
        }
    }
}