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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SystemTimeSource;
import org.apache.cassandra.utils.TimeSource;

/**
 * The adaptive compaction controller dynamically calculates the optimal scaling factor W.
 * <p/>
 * Generally it tries to find a local minimum for the total IO cost that is projected
 * by the strategy. The projected IO cost is composed by two parts: the read amplification,
 * which is weighted by the number of partitions read by the user, and the write amplification, which
 * is weighted by the number of bytes inserted into memtables. Other parameters are also considered, such
 * as the cache miss rate and the time it takes to read and write from disk. See also the comments in
 * {@link CostsCalculator}.
 *
 * Design doc: TODO: link to design doc or SEP
 */
public class AdaptiveController extends Controller
{
    private final static Logger logger = LoggerFactory.getLogger(AdaptiveController.class);

    /** The starting value for W */
    final static String STARTING_W = "adaptive_starting_w";
    private final static int DEFAULT_STARTING_W = Integer.getInteger(PREFIX + STARTING_W, 0);

    /** The minimum valid value for W */
    final static String MIN_W = "adaptive_min_w";
    private final static int DEFAULT_MIN_W = Integer.getInteger(PREFIX + MIN_W, -10);

    /** The maximum valid value for W */
    final static String MAX_W = "adaptive_max_w";
    private final static int DEFAULT_MAX_W = Integer.getInteger(PREFIX + MIN_W, 36);

    /** The interval for periodically checking the optimal value for W */
    final static String INTERVAL_SEC = "adaptive_interval_sec";
    private final static int DEFAULT_INTERVAL_SEC = Integer.getInteger(PREFIX + INTERVAL_SEC, 300);

    /** The gain is a number between 0 and 1 used to determine if a new choice of W is better than the current one */
    final static String GAIN = "adaptive_gain";
    private final static double DEFAULT_GAIN = Double.parseDouble(System.getProperty(PREFIX + GAIN, "0.15"));

    /** Below the minimum cost we don't try to optimize W, we consider the current W good enough. This is necessary because the cost
     * can vanish to zero when there are neither reads nor writes and right now we don't know how to handle this case.  */
    final static String MIN_COST = "adaptive_min_cost";
    private final static int DEFAULT_MIN_COST = Integer.getInteger(PREFIX + MIN_COST, 1000);

    private final int intervalSec;
    private final int minW;
    private final int maxW;
    private final double gain;
    private final int minCost;

    private volatile int W;
    private volatile long lastChecked;

    @VisibleForTesting
    public AdaptiveController(TimeSource timeSource,
                              Environment env,
                              int W,
                              double survivalFactor,
                              long dataSetSizeMB,
                              int numShards,
                              long minSstableSizeMB,
                              int intervalSec,
                              int minW,
                              int maxW,
                              double gain,
                              int minCost)
    {
        super(timeSource, env, survivalFactor, dataSetSizeMB, numShards, minSstableSizeMB);

        this.W = W;
        this.intervalSec = intervalSec;
        this.minW = minW;
        this.maxW = maxW;
        this.gain = gain;
        this.minCost = minCost;
    }

    static Controller fromOptions(Environment env, double o, long dataSetSizeMB, int numShards, long minSstableSizeMB, Map<String, String> options)
    {
        int W = options.containsKey(STARTING_W) ? Integer.parseInt(options.get(STARTING_W)) : DEFAULT_STARTING_W;
        int minW = options.containsKey(MIN_W) ? Integer.parseInt(options.get(MIN_W)) : DEFAULT_MIN_W;
        int maxW = options.containsKey(MAX_W) ? Integer.parseInt(options.get(MAX_W)) : DEFAULT_MAX_W;
        int intervalSec = options.containsKey(INTERVAL_SEC) ? Integer.parseInt(options.get(INTERVAL_SEC)) : DEFAULT_INTERVAL_SEC;
        double gain = options.containsKey(GAIN) ? Double.parseDouble(options.get(GAIN)) : DEFAULT_GAIN;
        int minCost = options.containsKey(MIN_COST) ? Integer.parseInt(options.get(MIN_COST)) : DEFAULT_MIN_COST;

        if (minW >= maxW || W < minW || W > maxW)
            throw new IllegalArgumentException(String.format("Invalid configuration for W: %d, min: %d, max: %d", W, minW, maxW));

        if (intervalSec <= 0)
            throw new IllegalArgumentException(String.format("Invalid configuration for interval, it should be positive: %d", intervalSec));

        if (gain <= 0 || gain > 1)
            throw new IllegalArgumentException(String.format("Invalid configuration for gain, it should be within (0,1]: %f", gain));

        if (minCost <= 0)
            throw new IllegalArgumentException(String.format("Invalid configuration for minCost, it should be positive: %d", minCost));

        return new AdaptiveController(new SystemTimeSource(), env, W, o, dataSetSizeMB, numShards, minSstableSizeMB, intervalSec, minW, maxW, gain, minCost);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        options.remove(STARTING_W);
        options.remove(MIN_W);
        options.remove(MAX_W);
        options.remove(INTERVAL_SEC);
        options.remove(GAIN);
        options.remove(MIN_COST);
        return options;
    }

    @Override
    void startup(UnifiedCompactionStrategy strategy, CostsCalculator calculator)
    {
        super.startup(strategy, calculator);
        this.lastChecked = timeSource.nanoTime();
    }

    @Override
    public int getW(int index)
    {
        return W;
    }

    @Override
    public double getSurvivalFactor()
    {
        return survivalFactor;
    }

    @Override
    @Nullable
    public CostsCalculator getCalculator()
    {
        return calculator;
    }

    public int getInterval()
    {
        return intervalSec;
    }

    public int getMinW()
    {
        return minW;
    }

    public int getMaxW()
    {
        return maxW;
    }

    public double getGain()
    {
        return gain;
    }

    public int getMinCost()
    {
        return minCost;
    }

    @Override
    public void onStrategyBackgroundTaskRequest()
    {
        if (!isRunning())
            return;

        long now = timeSource.nanoTime();
        if (now - lastChecked < TimeUnit.SECONDS.toNanos(intervalSec))
            return;

        try
        {
            maybeUpdate(now);
        }
        finally
        {
            lastChecked = now;
        }
    }

    private void maybeUpdate(long now)
    {
        // Each shard will act independently and so our target size of the same as the shard size
        final long targetSizeMB = shardSizeMB;
        final long targetSize = Math.max(targetSizeMB << 20, (long) Math.ceil(calculator.spaceUsed().get()));

        final int RA = RA(targetSize, W);
        final int WA = WA(targetSize, W);

        final double readCost = calculator.getReadCostForQueries(RA);
        final double writeCost = calculator.getWriteCostForQueries(WA);
        final double cost =  readCost + writeCost;

        if (cost <= minCost)
        {
            logger.debug("Adaptive compaction controller not updated, cost for current W {} is below minimum cost {}: read cost: {}, write cost: {}\\nAverages: {}", W, minCost, readCost, writeCost, calculator);
            return;
        }

        final double[] totCosts = new double[maxW - minW + 1];
        final double[] readCosts = new double[maxW - minW + 1];
        final double[] writeCosts = new double[maxW - minW + 1];
        int candW = W;
        double candCost = cost;

        for (int i = minW; i <= maxW; i++)
        {
            final int idx = i - minW;
            if (i == W)
            {
                readCosts[idx] = readCost;
                writeCosts[idx] = writeCost;
            }
            else
            {
                final int ra = RA(targetSize, i);
                final int wa = WA(targetSize, i);

                readCosts[idx] = calculator.getReadCostForQueries(ra);
                writeCosts[idx] = calculator.getWriteCostForQueries(wa);
            }
            totCosts[idx] = readCosts[idx] + writeCosts[idx];
            // in case of a tie, for neg.ve Ws we prefer higher Ws (smaller WA), but not for pos.ve Ws we prefer lower Ws (more parallelism)
            if (totCosts[idx] < candCost || (i < 0 && totCosts[idx] == candCost))
            {
                candW = i;
                candCost = totCosts[idx];
            }
        }

        logger.debug("Min cost: {}, min W: {}, min sstable size: {}\nread costs: {}\nwrite costs: {}\ntot costs: {}\nAverages: {}",
                     candCost,
                     candW,
                     FBUtilities.prettyPrintMemory(getMinSstableSizeBytes()),
                     Arrays.toString(readCosts),
                     Arrays.toString(writeCosts),
                     Arrays.toString(totCosts),
                     calculator);

        StringBuilder str = new StringBuilder(100);
        str.append("Adaptive compaction controller ");

        if (W != candW && (cost - candCost) >= gain * cost)
        {
            str.append("updated ").append(W).append(" -> ").append(candW);
            this.W = candW;
        }
        else
        {
            str.append("unchanged");
        }

        str.append(", data size: ").append(FBUtilities.prettyPrintMemory(targetSize));
        str.append(", query cost: ").append(cost);
        str.append(", new query cost: ").append(candCost);
        str.append(", took ").append(TimeUnit.NANOSECONDS.toMicros(timeSource.nanoTime() - now)).append(" us");

        logger.debug(str.toString());
    }

    @Override
    public String toString()
    {
        return String.format("m: %d, o: %f, W: %d - %s", minSstableSizeMB, survivalFactor, W, calculator);
    }
}
