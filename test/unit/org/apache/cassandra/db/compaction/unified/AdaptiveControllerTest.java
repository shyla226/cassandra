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

package org.apache.cassandra.db.compaction.unified;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.utils.ExpMovingAverage;
import org.apache.cassandra.utils.MovingAverage;
import org.apache.cassandra.utils.TestTimeSource;
import org.apache.cassandra.utils.TimeSource;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

public class AdaptiveControllerTest extends ControllerTest
{
    private CostsCalculator calculator;
    private TimeSource timeSource;

    private final int minW = -10;
    private final int maxW = 64;
    private final int W = 0;
    private final int interval = 60;
    private final int minCost = 5;
    private final double baseCost = minCost * 5;
    private final double gain = 0.15;

    @Before
    public void setup()
    {
        calculator = Mockito.mock(CostsCalculator.class);
        timeSource = new TestTimeSource();
    }

    private AdaptiveController makeController()
    {
        return makeController(dataSizeGB, numShards, sstableSizeMB);
    }

    private AdaptiveController makeController(int dataSizeGB, int numShards, int sstableSizeMB)
    {
        return new AdaptiveController(timeSource,
                                      env,
                                      W,
                                      Controller.DEFAULT_SURVIVAL_FACTOR,
                                      dataSizeGB << 10,
                                      numShards,
                                      sstableSizeMB,
                                      interval,
                                      minW,
                                      maxW,
                                      gain,
                                      minCost);
    }

    @Test
    public void testFromOptions()
    {
        Map<String, String> options = new HashMap<>();
        options.put(AdaptiveController.STARTING_W, "0");
        options.put(AdaptiveController.MIN_W, "-10");
        options.put(AdaptiveController.MAX_W, "32");
        options.put(AdaptiveController.INTERVAL_SEC, "120");
        options.put(AdaptiveController.GAIN, "0.15");
        options.put(AdaptiveController.MIN_COST, "5");

        Controller controller = testFromOptions(true, options);
        assertTrue(controller instanceof AdaptiveController);

        for (int i = 0; i < 10; i++)
            assertEquals(0, controller.getW(i));
    }

    @Test
    public void testValidateOptions()
    {
        Map<String, String> options = new HashMap<>();
        options.put(AdaptiveController.STARTING_W, "0");
        options.put(AdaptiveController.MIN_W, "-10");
        options.put(AdaptiveController.MAX_W, "32");
        options.put(AdaptiveController.INTERVAL_SEC, "120");
        options.put(AdaptiveController.GAIN, "0.15");
        options.put(AdaptiveController.MIN_COST, "5");

        super.testValidateOptions(options, true);
    }

    @Test
    public void testStartShutdown()
    {
        AdaptiveController controller = makeController();
        testStartShutdown(controller);
    }

    @Test
    public void testShutdownNotStarted()
    {
        AdaptiveController controller = makeController();
        testShutdownNotStarted(controller);
    }

    @Test(expected = IllegalStateException.class)
    public void testStartAlreadyStarted()
    {
        AdaptiveController controller = makeController();
        testStartAlreadyStarted(controller);
    }

    @Test
    public void testMinSSTableSizeDynamic()
    {
        // <= 50 MB, round up to 50 MB
        testMinSSTableSizeDynamic(1, 50);
        testMinSSTableSizeDynamic((50 << 20) - 1, 50);
        testMinSSTableSizeDynamic(50 << 20, 50);

        // <= 100 MB, round up to 100 MB
        testMinSSTableSizeDynamic((50 << 20) + 1, 100);
        testMinSSTableSizeDynamic((100 << 20) - 1, 100);
        testMinSSTableSizeDynamic(100 << 20, 100);

        // no flush size, 50 MB, then flush size of 100 MB + 1 returns 150MB
        testMinSSTableSizeDynamic(0, 50, (100 << 20) + 1, 150);
    }

    private void testMinSSTableSizeDynamic(long flushSizeBytes1, int minSSTableSizeMB1)
    {
        // The most common case, the second calculation is skipped so even if the env returns zero the second time, the result won't change
        testMinSSTableSizeDynamic(flushSizeBytes1, minSSTableSizeMB1, 0, minSSTableSizeMB1);
    }

    private void testMinSSTableSizeDynamic(long flushSizeBytes1, int minSSTableSizeMB1, long flushSizeBytes2, int minSSTableSizeMB2)
    {
        // create a controller with minSSTableSizeMB set to zero so that it will calculate the min sstable size from the flush size
        AdaptiveController controller = makeController(dataSizeGB, numShards, 0);

        when(env.flushSize()).thenReturn(flushSizeBytes1 * 1.0);
        assertEquals(minSSTableSizeMB1 << 20, controller.getMinSstableSizeBytes());

        when(env.flushSize()).thenReturn(flushSizeBytes2 * 1.0);
        assertEquals(minSSTableSizeMB2 << 20, controller.getMinSstableSizeBytes());
    }


    @Test
    public void testUpdateNotEnoughTimeElapsed()
    {
        AdaptiveController controller = makeController();
        controller.startup(strategy, calculator);

        // no update, not enough time elapsed
        controller.onStrategyBackgroundTaskRequest();
        assertEquals(W, controller.getW(0));
    }

    @Test
    public void testUpdateBelowMinCost() throws InterruptedException
    {
        AdaptiveController controller = makeController();
        controller.startup(strategy, calculator);

        // no update, <= min cost
        when(calculator.getReadCostForQueries(anyInt())).thenReturn((double) minCost);
        when(calculator.getReadCostForQueries(anyInt())).thenReturn(0.);
        MovingAverage spaceUsed = Mockito.mock(ExpMovingAverage.class);
        when(calculator.spaceUsed()).thenReturn(spaceUsed);
        when(spaceUsed.get()).thenReturn(1.0);

        timeSource.sleep(interval + 1, TimeUnit.SECONDS);
        controller.onStrategyBackgroundTaskRequest();
        assertEquals(W, controller.getW(0));
    }

    @Test
    public void testUpdateWithSize_min() throws InterruptedException
    {
        long totSize = (long) sstableSizeMB << 20;
        testUpdateWithSize(totSize, new double[]{ baseCost, 0, baseCost }, new double[]{ 0, baseCost, baseCost }, new int[]{ 0, 0, 0 });
    }

    @Test
    public void testUpdateWithSize_1GB() throws InterruptedException
    {
        long totSize = 1L << 31;
        testUpdateWithSize(totSize, new double[]{ baseCost, 0, baseCost }, new double[]{ 0, baseCost, baseCost }, new int[]{ -9, 31, -1 });
    }

    @Test
    public void testUpdateWithSize_2GB() throws InterruptedException
    {
        long totSize = 2L << 31;
        testUpdateWithSize(totSize, new double[]{ baseCost, 0, baseCost }, new double[]{ 0, baseCost, baseCost }, new int[]{ -5, 44, 1 } );
    }

    @Test
    public void testUpdateWithSize_128GB() throws InterruptedException
    {
        long totSize = 1L << 37;
        testUpdateWithSize(totSize, new double[] {baseCost, 0, baseCost}, new double[] {0, baseCost, baseCost}, new int[] {-8, 39, -1});
    }

    @Test
    public void testUpdateWithSize_512GB() throws InterruptedException
    {
        long totSize = 1L << 39;
        testUpdateWithSize(totSize, new double[] {baseCost, 0, baseCost}, new double[] {0, baseCost, baseCost}, new int[] {-7, 63, -1});
    }

    @Test
    public void testUpdateWithSize_1TB() throws InterruptedException
    {
        long totSize = 1L << 40;
        testUpdateWithSize(totSize, new double[] {baseCost, 0, baseCost}, new double[] {0, baseCost, baseCost}, new int[] {-7, 25, 1});
    }

    @Test
    public void testUpdateWithSize_5TB() throws InterruptedException
    {
        long totSize = 5 * (1L << 40);
        testUpdateWithSize(totSize, new double[] {baseCost, 0, baseCost}, new double[] {0, baseCost, baseCost}, new int[] {-10, 39, -1});
    }

    @Test
    public void testUpdateWithSize_10TB() throws InterruptedException
    {
        long totSize = 10 * (1L << 40);
        testUpdateWithSize(totSize, new double[] { baseCost, 0, baseCost}, new double[] { 0, baseCost, baseCost}, new int[] { -8, 46, -1});
    }

    @Test
    public void testUpdateWithSize_20TB() throws InterruptedException
    {
        long totSize = 20 * (1L << 49);
        testUpdateWithSize(totSize, new double[] { baseCost, 0, baseCost}, new double[] { 0, baseCost, baseCost}, new int[] { -8, 40, -1});
    }

    private void testUpdateWithSize(long totSize, double[] readCosts, double[] writeCosts, int[] expectedWs) throws InterruptedException
    {
        int shardSizeGB = (int) (totSize >> 30);
        AdaptiveController controller = makeController(shardSizeGB, 1, sstableSizeMB); // one unique shard
        controller.startup(strategy, calculator);

        assertEquals(readCosts.length, writeCosts.length);
        assertEquals(writeCosts.length, expectedWs.length);

        MovingAverage spaceUsed = Mockito.mock(ExpMovingAverage.class);
        when(calculator.spaceUsed()).thenReturn(spaceUsed);
        when(spaceUsed.get()).thenReturn((double) totSize);

        for (int i = 0; i < readCosts.length; i++)
        {
            final double readCost = readCosts[i];
            final double writeCost = writeCosts[i];

            when(calculator.getReadCostForQueries(anyInt())).thenAnswer(answ -> (int) answ.getArgument(0) * readCost);
            when(calculator.getWriteCostForQueries(anyInt())).thenAnswer(answ -> (int) answ.getArgument(0) * writeCost);

            timeSource.sleep(interval + 1, TimeUnit.SECONDS);

            controller.onStrategyBackgroundTaskRequest();
            assertEquals(expectedWs[i], controller.getW(0));
        }
    }
}
