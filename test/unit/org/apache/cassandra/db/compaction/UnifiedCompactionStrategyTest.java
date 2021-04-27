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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SortedLocalRanges;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
@RunWith(Parameterized.class)
public class UnifiedCompactionStrategyTest extends BaseCompactionStrategyTest
{
    private final static long ONE_MB = 1 << 20;

    // Multiple disks can be used both with and without disk boundaries. We want to test both cases.

    @Parameterized.Parameters
    public static Iterable<Object[]> params()
    {
        return Arrays.asList(new Object[][] { {false}, {true} });
    }

    @Parameterized.Parameter
    public boolean useDiskBoundaries = true;

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
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getW(anyInt())).thenReturn(4);
        when(controller.getSurvivalFactor()).thenReturn(1.0);
        when(controller.getNumShards()).thenReturn(1);

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        assertTrue(strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).isEmpty());
        assertEquals(0, strategy.getEstimatedRemainingTasks());
    }

    @Test
    public void testGetBucketsSameWUniqueArena()
    {
        final int m = 2; // minimal sorted run size in MB m
        final Map<Integer, Integer> sstables = new TreeMap<>();

        for (int i = 0; i < 20; i++)
        {
            int numSSTables = 2 + random.nextInt(18);
            sstables.put(m * i, numSSTables);
        }

        // W = 3, o = 1 => F = 5, T = 5 => expected T sstables and 2 buckets: 0-10m, 10-50m
        testGetBucketsOneArena(sstables, new int[] { 3 }, m, new int[] { 5, 5});

        // W = 2, o = 1 => F = 4, T = 4 => expected T sstables and 3 buckets: 0-8m, 8-32m, 32-128m
        testGetBucketsOneArena(sstables, new int[] { 2 }, m, new int[] { 4, 4, 4});

        // W = 0, o = 1 => F = 2, T = 2 => expected 2 sstables and 5 buckets: 0-4m, 4-8m, 8-16m, 16-32m, 32-64m
        testGetBucketsOneArena(sstables, new int[] { 0 }, m, new int[] { 2, 2, 2, 2, 2});

        // W = -2, o = 1 => F = 4, T = 2 => expected 2 sstables and 3 buckets: 0-8mb, 8-32m, 32-128m
        testGetBucketsOneArena(sstables, new int[] { -2 }, m, new int[] { 2, 2, 2});

        // W = -3, o = 1 => F = 5, T = 2 => expected 2 sstables and 2 buckets: 0-10m, 10-50m
        testGetBucketsOneArena(sstables, new int[] { -3 }, m, new int[] { 2, 2});

        // remove sstables from 4m to 8m to create an empty bucket in the next call
        sstables.remove(4); // 4m
        sstables.remove(6); // 6m
        sstables.remove(8); // 8m

        // W = 0, o = 1 => F = 2, T = 2 => expected 2 sstables and 5 buckets: 0-4m, 4-8m, 8-16m, 16-32m, 32-64m
        testGetBucketsOneArena(sstables, new int[] { 0 }, m, new int[] { 2, 2, 2, 2, 2});
    }

    @Test
    public void testGetBucketsDifferentWsUniqueArena()
    {
        final int m = 2; // minimal sorted run size in MB m
        final Map<Integer, Integer> sstables = new TreeMap<>();

        for (int i : new int[] { 50, 100, 200, 400, 600, 800, 1000})
        {
            int numSSTables = 2 + random.nextInt(18);
            sstables.put(i, numSSTables);
        }

        // W = [30, 2, -6], o = 1 => F = [32, 4, 8] , T = [32, 4, 2]  => expected 3 buckets: 0-64m, 64-256m 256-2048m
        testGetBucketsOneArena(sstables, new int[]{ 30, 2, -6 }, m, new int[] { 32, 4, 2});

        // W = [30, 6, -8], o = 1 => F = [32, 8, 10] , T = [32, 8, 2]  => expected 3 buckets: 0-64m, 64-544m 544-5440m
        testGetBucketsOneArena(sstables, new int[]{ 30, 6, -8 }, m, new int[] { 32, 8, 2});

        // W = [0, 0, 0, -2, -2], o = 1 => F = [2, 2, 2, 4, 4] , T = [2, 2, 2, 2, 2]  => expected 6 buckets: 0-4m, 4-8m, 8-16m, 16-64m, 64-256m, 256-1024m
        testGetBucketsOneArena(sstables, new int[]{ 0, 0, 0, -2, -2 }, m, new int[] { 2, 2, 2, 2, 2, 2});
    }

    private void testGetBucketsOneArena(Map<Integer, Integer> sstableMap, int[] Ws, int m, int[] expectedTs)
    {
        long minimalSizeBytes = m << 20;

        Controller controller = Mockito.mock(Controller.class);
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getNumShards()).thenReturn(1);

        when(controller.getW(anyInt())).thenAnswer(answer -> {
            int index = answer.getArgument(0);
            return Ws[index < Ws.length ? index : Ws.length - 1];
        });

        when(controller.getSurvivalFactor()).thenReturn(1.0);

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        IPartitioner partitioner = cfs.getPartitioner();
        DecoratedKey first = new BufferDecoratedKey(partitioner.getMinimumToken(), ByteBuffer.allocate(0));
        DecoratedKey last = new BufferDecoratedKey(partitioner.getMaximumToken(), ByteBuffer.allocate(0));

        List<SSTableReader> sstables = new ArrayList<>();
        for(Map.Entry<Integer, Integer> entry : sstableMap.entrySet())
        {
            for (int i = 0; i < entry.getValue(); i++)
            {
                // we want a number > 0 and < 1 so that the sstable has always some size and never crosses the boundary to the next bucket
                // so we leave a 1% margin, picking a number from 0.01 to 0.99
                double rand = 0.01 + 0.98 * random.nextDouble();
                long sizeOnDiskBytes = (entry.getKey() << 20) + (long) (minimalSizeBytes * rand);
                sstables.add(mockSSTable(sizeOnDiskBytes, System.currentTimeMillis(), first, last));
            }
        }
        dataTracker.addInitialSSTables(sstables);

        Map<UnifiedCompactionStrategy.Shard, List<UnifiedCompactionStrategy.Bucket>> arenas = strategy.getShardsWithBuckets();
        assertNotNull(arenas);
        assertEquals(1, arenas.size());

        for (Map.Entry<UnifiedCompactionStrategy.Shard, List<UnifiedCompactionStrategy.Bucket>> entry : arenas.entrySet())
        {
            List<UnifiedCompactionStrategy.Bucket> buckets = entry.getValue();
            assertEquals(expectedTs.length, buckets.size());

            for (int i = 0; i < expectedTs.length; i++)
            {
                UnifiedCompactionStrategy.Bucket bucket = buckets.get(i);
                if (bucket.sstables.size() >= expectedTs[i])
                    assertFalse(bucket.getCompactionAggregate(entry.getKey()).isEmpty());
                else
                    assertTrue(bucket.getCompactionAggregate(entry.getKey()).isEmpty());
            }
        }
    }

    private static final class ArenaSpecs
    {
        private List<SSTableReader> sstables;
        private int[] expectedBuckets;

        ArenaSpecs(int[] expectedBuckets)
        {
            this.sstables = new ArrayList<>();
            this.expectedBuckets = expectedBuckets;
        }
    }

    private ArenaSpecs mockArena(Token min,
                                 Token max,
                                 Map<Long, Integer> sstables,
                                 boolean repaired,
                                 UUID pendingRepair,
                                 int diskIndex,
                                 int[] expectedBuckets)
    {
        ArenaSpecs arena = new ArenaSpecs(expectedBuckets);
        ByteBuffer bb = ByteBuffer.allocate(0);

        sstables.forEach((size, num) -> {
            // Generate a key inside the shard, but make sure it's not too close to the boundaries to compensate for
            // rounding differences between splitting directly and splitting first by disk and then by shard.
            Token first = min.getPartitioner().split(min, max, 0.01 + random.nextDouble() * 0.98);

            for (int i = 0; i < num; i++)
            {
                arena.sstables.add(mockSSTable(0,
                                               size,
                                               System.currentTimeMillis(),
                                               0.0,
                                               new BufferDecoratedKey(first, bb),
                                               new BufferDecoratedKey(max, bb),
                                               false,
                                               diskIndex,
                                               repaired,
                                               pendingRepair));
                first = first.increaseSlightly();
            }
        });

        return arena;
    }

    private List<PartitionPosition> makeBoundaries(int numShards, int numDisks)
    {
        IPartitioner partitioner = cfs.getPartitioner();
        assert numShards >= 1;
        assert numDisks >= 1;

        if (numShards * numDisks == 1)
            return ImmutableList.of(partitioner.getMaximumToken().maxKeyBound());

        Splitter splitter = partitioner.splitter().orElse(null);
        assertNotNull("The partitioner must support a splitter", splitter);

        int numBoundaries = useDiskBoundaries ? numDisks * numShards : numShards;
        Splitter.WeightedRange range = new Splitter.WeightedRange(1.0, new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken()));
        final List<PartitionPosition> shards = splitter.splitOwnedRanges(numBoundaries, ImmutableList.of(range), Splitter.SplitType.ALWAYS_SPLIT)
                                               .boundaries
                                               .stream()
                                               .map(Token::maxKeyBound)
                                               .collect(Collectors.toList());
        if (useDiskBoundaries)
        {
            diskBoundaryPositions = new ArrayList<>(numDisks);
            for (int i = 0; i < numDisks; ++i)
                diskBoundaryPositions.add(shards.get((i + 1) * numShards - 1));
        }
        return shards;
    }

    private List<ArenaSpecs> mockArenas(int diskIndex,
                                        int diskCount,
                                        boolean repaired,
                                        UUID pendingRepair,
                                        List<PartitionPosition> boundaries,
                                        Map<Long, Integer> sstables,
                                        int[] buckets)
    {
        List<ArenaSpecs> arenasList = new ArrayList<>();

        int numShards = boundaries.size() / diskCount;
        List<PartitionPosition> shardPositions = useDiskBoundaries
                                                 ? boundaries.subList(diskIndex * numShards, (diskIndex + 1) * numShards)
                                                 : boundaries;
        Token min = useDiskBoundaries && diskIndex > 0
                    ? boundaries.get(diskIndex * numShards - 1).getToken()
                    : partitioner.getMinimumToken();

        for (PartitionPosition boundary : shardPositions)
        {
            Token max = boundary.getToken();

            // what matters is the first key, which must be less than max
            arenasList.add(mockArena(min, max, sstables, repaired, pendingRepair, diskIndex, buckets));

            min = max;
        }

        return arenasList;
    }

    private static Map<Long, Integer> mapFromPair(Pair<Long, Integer> ... pairs)
    {
        Map<Long, Integer> ret = new HashMap<>();
        for (Pair<Long, Integer> pair : pairs)
        {
            ret.put(pair.left, pair.right);
        }

        return ret;
    }

    @Test
    public void testAllArenasOneBucket_NoShards()
    {
        testAllArenasOneBucket(1);
    }

    @Test
    public void testAllArenasOneBucket_MultipleShards()
    {
        testAllArenasOneBucket(5);
    }

    private void testAllArenasOneBucket(int numShards)
    {
        final int m = 2; // minimal sorted run size in MB
        final int W = 2; // => o = 1 => F = 4, T = 4: 0-8m, 8-32m, 32-128m

        List<PartitionPosition> boundaries = makeBoundaries(numShards, 2);
        List<ArenaSpecs> arenasList = new ArrayList<>();

        Map<Long, Integer> sstables = mapFromPair(Pair.create(4 * ONE_MB, 4));
        int[] buckets = new int[]{4};

        UUID pendingRepair = UUID.randomUUID();
        arenasList.addAll(mockArenas(0, 2, false, pendingRepair, boundaries, sstables, buckets)); // pending repair

        arenasList.addAll(mockArenas(0, 2, false, pendingRepair, boundaries, sstables, buckets)); // unrepaired
        arenasList.addAll(mockArenas(1, 2, false, null, boundaries, sstables, buckets)); // unrepaired, next disk

        arenasList.addAll(mockArenas(0, 2, true, null, boundaries, sstables, buckets)); // repaired
        arenasList.addAll(mockArenas(1, 2, true, null, boundaries, sstables, buckets)); // repaired, next disk

        testGetBucketsMultipleArenas(arenasList, W, m, boundaries);
    }

    @Test
    public void testRepairedOneDiskOneBucket_NoShards()
    {
        testRepairedOneDiskOneBucket(1);
    }

    @Test
    public void testRepairedOneDiskOneBucket_MultipleShards()
    {
        testRepairedOneDiskOneBucket(5);
    }

    private void testRepairedOneDiskOneBucket(int numShards)
    {
        final int m = 2; // minimal sorted run size in MB
        final int W = 2; // => o = 1 => F = 4, T = 4: 0-8m, 8-32m, 32-128m

        Map<Long, Integer> sstables = mapFromPair(Pair.create(4 * ONE_MB, 4));
        int[] buckets = new int[]{4};

        List<PartitionPosition> boundaries = makeBoundaries(numShards, 1);
        List<ArenaSpecs> arenas = mockArenas(0, 1, true, null, boundaries, sstables, buckets);
        testGetBucketsMultipleArenas(arenas, W, m, boundaries);
    }

    @Test
    public void testRepairedTwoDisksOneBucket_NoShards()
    {
        testRepairedTwoDisksOneBucket(1);
    }

    @Test
    public void testRepairedTwoDisksOneBucket_MultipleShards()
    {
        testRepairedTwoDisksOneBucket(5);
    }

    private void testRepairedTwoDisksOneBucket(int numShards)
    {
        final int m = 2; // minimal sorted run size in MB
        final int W = 2; // => o = 1 => F = 4, T = 4: 0-8m, 8-32m, 32-128m

        Map<Long, Integer> sstables = mapFromPair(Pair.create(4 * ONE_MB, 4));
        int[] buckets = new int[]{4};

        List<PartitionPosition> boundaries = makeBoundaries(numShards, 2);
        List<ArenaSpecs> arenas = new ArrayList<>();

        arenas.addAll(mockArenas(0, 2, true, null, boundaries, sstables, buckets));
        arenas.addAll(mockArenas(1, 2, true, null, boundaries, sstables, buckets));

        testGetBucketsMultipleArenas(arenas, W, m, boundaries);
    }

    @Test
    public void testRepairedMultipleDisksMultipleBuckets_NoShards()
    {
        testRepairedMultipleDisksMultipleBuckets(1);
    }

    @Test
    public void testRepairedMultipleDisksMultipleBuckets_MultipleShards()
    {
        testRepairedMultipleDisksMultipleBuckets(15);
    }

    private void testRepairedMultipleDisksMultipleBuckets(int numShards)
    {
        final int m = 2; // minimal sorted run size in MB
        final int W = 2; // => o = 1 => F = 4, T = 4: 0-8m, 8-32m, 32-128m

        List<PartitionPosition> boundaries = makeBoundaries(numShards, 6);
        List<ArenaSpecs> arenasList = new ArrayList<>();

        Map<Long, Integer> sstables1 = mapFromPair(Pair.create(2 * ONE_MB, 4), Pair.create(8 * ONE_MB, 4), Pair.create(32 * ONE_MB, 4));
        int[] buckets1 = new int[]{4,4,4};

        Map<Long, Integer> sstables2 = mapFromPair(Pair.create(8 * ONE_MB, 4), Pair.create(32 * ONE_MB, 8));
        int[] buckets2 = new int[]{0,4,8};

        for (int i = 0; i < 6; i++)
        {
            if (i % 2 == 0)
                arenasList.addAll(mockArenas(i, 6, true, null, boundaries, sstables1, buckets1));
            else
                arenasList.addAll(mockArenas(i, 6, true, null, boundaries, sstables2, buckets2));

        }

        testGetBucketsMultipleArenas(arenasList, W, m, boundaries);
    }

    @Test
    public void testRepairedUnrepairedOneDiskMultipleBuckets_NoShards()
    {
        testRepairedUnrepairedOneDiskMultipleBuckets(1);
    }

    @Test
    public void testRepairedUnrepairedOneDiskMultipleBuckets_MultipleShards()
    {
        testRepairedUnrepairedOneDiskMultipleBuckets(10);
    }

    private void testRepairedUnrepairedOneDiskMultipleBuckets(int numShards)
    {
        final int m = 2; // minimal sorted run size in MB
        final int W = 2; // => o = 1 => F = 4, T = 4: 0-8m, 8-32m, 32-128m

        List<PartitionPosition> boundaries = makeBoundaries(numShards, 1);
        List<ArenaSpecs> arenasList = new ArrayList<>();

        Map<Long, Integer> sstables1 = mapFromPair(Pair.create(2 * ONE_MB, 4), Pair.create(8 * ONE_MB, 4), Pair.create(32 * ONE_MB, 4));
        int[] buckets1 = new int[]{4,4,4};

        Map<Long, Integer> sstables2 = mapFromPair(Pair.create(8 * ONE_MB, 4), Pair.create(32 * ONE_MB, 8));
        int[] buckets2 = new int[]{0,4,8};

        arenasList.addAll(mockArenas(0, 1, true, null, boundaries, sstables2, buckets2)); // repaired
        arenasList.addAll(mockArenas(0, 1, false, null, boundaries, sstables1, buckets1)); // unrepaired

        testGetBucketsMultipleArenas(arenasList, W, m, boundaries);
    }

    @Test
    public void testRepairedUnrepairedTwoDisksMultipleBuckets_NoShards()
    {
        testRepairedUnrepairedTwoDisksMultipleBuckets(1);
    }

    @Test
    public void testRepairedUnrepairedTwoDisksMultipleBuckets_MultipleShards()
    {
        testRepairedUnrepairedTwoDisksMultipleBuckets(5);
    }

    private void testRepairedUnrepairedTwoDisksMultipleBuckets(int numShards)
    {
        final int m = 2; // minimal sorted run size in MB
        final int W = 2; // => o = 1 => F = 4, T = 4: 0-8m, 8-32m, 32-128m

        List<PartitionPosition> boundaries = makeBoundaries(numShards, 2);
        List<ArenaSpecs> arenasList = new ArrayList<>();

        Map<Long, Integer> sstables1 = mapFromPair(Pair.create(2 * ONE_MB, 4), Pair.create(8 * ONE_MB, 4), Pair.create(32 * ONE_MB, 4));
        int[] buckets1 = new int[]{4,4,4};

        Map<Long, Integer> sstables2 = mapFromPair(Pair.create(8 * ONE_MB, 4), Pair.create(32 * ONE_MB, 8));
        int[] buckets2 = new int[]{0,4,8};

        arenasList.addAll(mockArenas(0, 2, true, null, boundaries, sstables2, buckets2));  // repaired, first disk
        arenasList.addAll(mockArenas(1, 2, true, null, boundaries, sstables1, buckets1));  // repaired, second disk

        arenasList.addAll(mockArenas(0, 2, false, null, boundaries, sstables1, buckets1));  // unrepaired, first disk
        arenasList.addAll(mockArenas(1, 2, false, null, boundaries, sstables2, buckets2));  // unrepaired, second disk

        testGetBucketsMultipleArenas(arenasList, W, m, boundaries);
    }

    private void testGetBucketsMultipleArenas(List<ArenaSpecs> arenaSpecs, int W, int m, List<PartitionPosition> shards)
    {
        long minimalSizeBytes = m << 20;

        Controller controller = Mockito.mock(Controller.class);
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getW(anyInt())).thenReturn(W);
        when(controller.getSurvivalFactor()).thenReturn(1.0);

        when(controller.getNumShards()).thenReturn(shards.size());

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        List<SSTableReader> sstables = arenaSpecs.stream().flatMap(a -> a.sstables.stream()).collect(Collectors.toList());
        dataTracker.addInitialSSTables(sstables);

        Map<UnifiedCompactionStrategy.Shard, List<UnifiedCompactionStrategy.Bucket>> arenas = strategy.getShardsWithBuckets();
        assertNotNull(arenas);
        assertEquals(arenaSpecs.size(), arenas.size());

        int idx = 0;
        for (Map.Entry<UnifiedCompactionStrategy.Shard, List<UnifiedCompactionStrategy.Bucket>> entry : arenas.entrySet())
        {
            List<UnifiedCompactionStrategy.Bucket> buckets = entry.getValue();
            ArenaSpecs currentArenaSpecs = arenaSpecs.get(idx++);

            assertEquals(currentArenaSpecs.expectedBuckets.length, buckets.size());
            for (int i = 0; i < currentArenaSpecs.expectedBuckets.length; i++)
                assertEquals(currentArenaSpecs.expectedBuckets[i], buckets.get(i).sstables.size());
        }
    }

    @Test
    public void testShardBoundaries()
    {
        // no shards
        testShardBoundaries(ints(100), 1, 1, ints(10, 50));
        // split on disks at minimum
        testShardBoundaries(ints(30, 100), 1, 2, ints(10, 50));
        testShardBoundaries(ints(20, 30, 40, 50, 100), 1, 5, ints(10, 51, 61, 70));

        // no disks
        testShardBoundaries(ints(30, 100), 2, 1, ints(10, 50));
        testShardBoundaries(ints(20, 30, 40, 50, 100), 5, 1, ints(10, 51, 61, 70));

        // split
        testShardBoundaries(ints(10, 20, 30, 40, 50, 60, 70, 80, 100), 9, 3, ints(0, 90));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90, 100), 9, 3, ints(0, 51, 61, 100));
        testShardBoundaries(ints(10, 20, 30, 40, 60, 70, 80, 90, 100), 9, 3, ints(0, 49, 59, 100));
        testShardBoundaries(ints(12, 23, 33, 45, 56, 70, 80, 90, 100), 9, 3, ints(0, 9, 11, 20, 21, 39, 41, 50, 51, 60, 64, 68, 68, 100));

        // uneven
        testShardBoundaries(ints(11, 22, 33, 42, 50, 58, 67, 78, 89, 100), 10, 3, ints(0, 100));
        testShardBoundaries(ints(8, 17, 25, 38, 50, 58, 67, 75, 88, 100), 10, 4, ints(0, 100));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 60, 70, 80, 90, 100), 10, 5, ints(0, 100));
        testShardBoundaries(ints(8, 17, 33, 42, 50, 58, 67, 83, 92, 100), 10, 6, ints(0, 100));
        testShardBoundaries(ints(14, 21, 29, 43, 50, 57, 71, 79, 86, 100), 10, 7, ints(0, 100));
        testShardBoundaries(ints(13, 19, 25, 38, 50, 63, 69, 75, 88, 100), 10, 8, ints(0, 100));
        testShardBoundaries(ints(11, 22, 33, 44, 50, 56, 67, 78, 89, 100), 10, 9, ints(0, 100));

        // uneven again, where x0 are the disk boundaries and the others are inserted shard boundaries
        testShardBoundaries(ints(3, 7, 10, 12, 15, 18, 20, 23, 27, 100), 10, 3, ints(0, 30));
        testShardBoundaries(ints(3, 7, 10, 15, 20, 23, 27, 30, 35, 100), 10, 4, ints(0, 40));
        testShardBoundaries(ints(5, 10, 15, 20, 25, 30, 35, 40, 45, 100), 10, 5, ints(0, 50));
        testShardBoundaries(ints(5, 10, 20, 25, 30, 35, 40, 50, 55, 100), 10, 6, ints(0, 60));
        testShardBoundaries(ints(10, 15, 20, 30, 35, 40, 50, 55, 60, 100), 10, 7, ints(0, 70));
        testShardBoundaries(ints(10, 15, 20, 30, 40, 50, 60, 65, 70, 100), 10, 8, ints(0, 80));
        testShardBoundaries(ints(10, 20, 30, 40, 45, 50, 60, 70, 80, 100), 10, 9, ints(0, 90));
    }

    @Test
    public void testShardBoundariesWraparound()
    {
        // no shards
        testShardBoundaries(ints(100), 1, 1, ints(50, 10));
        // split on disks at minimum
        testShardBoundaries(ints(70, 100), 1, 2, ints(50, 10));
        testShardBoundaries(ints(10, 20, 30, 70, 100), 1, 5, ints(91, 31, 61, 71));
        // no disks
        testShardBoundaries(ints(70, 100), 2, 1, ints(50, 10));
        testShardBoundaries(ints(10, 20, 30, 70, 100), 5, 1, ints(91, 31, 61, 71));
        // split
        testShardBoundaries(ints(10, 20, 30, 40, 50, 60, 70, 90, 100), 9, 3, ints(81, 71));
        testShardBoundaries(ints(10, 20, 30, 40, 60, 70, 80, 90, 100), 9, 3, ints(51, 41));
        testShardBoundaries(ints(10, 30, 40, 50, 60, 70, 80, 90, 100), 9, 3, ints(21, 11));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 60, 70, 90, 100), 9, 3, ints(89, 79));
        testShardBoundaries(ints(10, 20, 30, 40, 60, 70, 80, 90, 100), 9, 3, ints(59, 49));
        testShardBoundaries(ints(10, 30, 40, 50, 60, 70, 80, 90, 100), 9, 3, ints(29, 19));

        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90, 100), 9, 3, ints(91, 51, 61, 91));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90, 100), 9, 3, ints(21, 51, 61, 21));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90, 100), 9, 3, ints(71, 51, 61, 71));
    }

    private int[] ints(int... values)
    {
        return values;
    }

    private void testShardBoundaries(int[] expected, int numShards, int numDisks, int[] rangeBounds)
    {
        IPartitioner partitioner = Murmur3Partitioner.instance;
        List<Splitter.WeightedRange> ranges = new ArrayList<>();
        for (int i = 0; i < rangeBounds.length; i += 2)
            ranges.add(new Splitter.WeightedRange(1.0, new Range<>(getToken(rangeBounds[i + 0]), getToken(rangeBounds[i + 1]))));
        SortedLocalRanges sortedRanges = SortedLocalRanges.forTesting(cfs, ranges);
        System.out.println(sortedRanges.getRanges());

        List<PartitionPosition> diskBoundaries = sortedRanges.split(numDisks);
        System.out.println(diskBoundaries);

        int[] result = UnifiedCompactionStrategy.computeShardBoundaries(sortedRanges, diskBoundaries, numShards, partitioner.splitter())
                                                .stream()
                                                .map(PartitionPosition::getToken)
                                                .mapToInt(this::fromToken)
                                                .toArray();

        Assert.assertArrayEquals("Disks " + numDisks + " shards " + numShards + " expected " + Arrays.toString(expected) + " was " + Arrays.toString(result), expected, result);
    }

    private Token getToken(int x)
    {
        IPartitioner partitioner = Murmur3Partitioner.instance;
        return partitioner.split(partitioner.getMinimumToken(), partitioner.getMaximumToken(), x * 0.01);
    }

    private int fromToken(Token t)
    {
        IPartitioner partitioner = Murmur3Partitioner.instance;
        return (int) Math.round(partitioner.getMinimumToken().size(t) * 100.0);
    }
}