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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionStrategy;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.TimeSource;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
* The controller provides compaction parameters to the unified compaction strategy
*/
public abstract class Controller
{
    protected final static Logger logger = LoggerFactory.getLogger(Controller.class);
    private final static ConcurrentMap<TableMetadata, Controller.Metrics> allMetrics = new ConcurrentHashMap<>();

    final static String PREFIX = "dse.universal_compaction.";

    /** The data size in GB, it will be assumed that the node will have on disk roughly this size of data when it
     * reaches equilibrium. By default 1 TB. */
    final static String DATASET_SIZE_OPTION_GB = "dataset_size_in_gb";
    final static int DEFAULT_DATASET_SIZE_GB = Integer.getInteger(PREFIX + DATASET_SIZE_OPTION_GB, 1024);

    /** The number of shards. The shard size will be calculated by dividing the data size by this number.
     * By default 5, which means shards of 200 GB (1 TB / 5).*/
    final static String NUM_SHARDS_OPTION = "num_shards";
    final static int DEFAULT_NUM_SHARDS = Integer.getInteger(PREFIX + NUM_SHARDS_OPTION, 5);

    /**
     * The minimum sstable size determines various things:
     *
     * - first of all it determines the size of the buckets since we multiply this value by F.
     * - secondly, when sstables are split over shard, they must be at least as large as the minimum size
     *
     * When the minimum sstable size is zero in the compaction options, then it is calculated by the controller by
     * looking at the initial flush size.
     */
    final static String MIN_SSTABLE_SIZE_OPTION_MB = "min_sstable_size_in_mb";
    final static int DEFAULT_MIN_SSTABLE_SIZE_MB = Integer.getInteger(PREFIX + MIN_SSTABLE_SIZE_OPTION_MB, 0);

    /**
     * The maximum tolerable compaction-induced space amplification, as fraction of the dataset size. The idea behind
     * this property is to be able to tune how much to limit concurrent "oversized" compactions in different shards.
     * On one hand allowing such compactions concurrently running in all shards allows for STCS-like space
     * amplification, where at some point you might need free space double the size of your working set to do a (top
     * tier) compaction, while on the other hand limiting such compactions too much might lead to compaction lagging
     * behind, higher read amplification, and other problems of that nature.
     */
    final static String MAX_SPACE_OVERHEAD_OPTION = "max_space_overhead";
    final static double DEFAULT_MAX_SPACE_OVERHEAD = Double.parseDouble(System.getProperty(PREFIX + MAX_SPACE_OVERHEAD_OPTION, "0.2"));
    final static double MAX_SPACE_OVERHEAD_LOWER_BOUND = 0.01;
    final static double MAX_SPACE_OVERHEAD_UPPER_BOUND = 1.0;

    /**
     * This parameter is intended to modify the shape of the LSM by taking into account the survival ratio of data, for now it is fixed to one.
     */
    final static double DEFAULT_SURVIVAL_FACTOR = Double.parseDouble(System.getProperty(PREFIX + "survival_factor", "1"));

    /**
     * Either true or false. This parameter determines which controller will be used.
     */
    final static String ADAPTIVE_OPTION = "adaptive";
    final static boolean DEFAULT_ADAPTIVE = Boolean.parseBoolean(System.getProperty(PREFIX + ADAPTIVE_OPTION, "true"));

    protected final TimeSource timeSource;
    protected final Environment env;
    protected final double survivalFactor;
    protected final long dataSetSizeMB;
    protected final int numShards;
    protected final long shardSizeMB;
    protected volatile long minSstableSizeMB;
    protected final double maxSpaceOverhead;

    @Nullable
    protected volatile CostsCalculator calculator;
    @Nullable private volatile Metrics metrics;

    Controller(TimeSource timeSource,
               Environment env,
               double survivalFactor,
               long dataSetSizeMB,
               int numShards,
               long minSstableSizeMB,
               double maxSpaceOverhead)
    {
        this.timeSource = timeSource;
        this.env = env;
        this.survivalFactor = survivalFactor;
        this.dataSetSizeMB = dataSetSizeMB;
        this.numShards = numShards;
        this.shardSizeMB = (int) Math.ceil((double) dataSetSizeMB / numShards);
        this.minSstableSizeMB = minSstableSizeMB;
        this.maxSpaceOverhead = maxSpaceOverhead;
        double maxSpaceOverheadLowerBound = 1.0 / numShards;
        if (maxSpaceOverhead < MAX_SPACE_OVERHEAD_LOWER_BOUND || maxSpaceOverhead > MAX_SPACE_OVERHEAD_UPPER_BOUND)
        {
            logger.warn("The value for {} is {}, but it should be between {} and {} - setting it to the closer of the two bounds.",
                        MAX_SPACE_OVERHEAD_OPTION,
                        maxSpaceOverhead,
                        MAX_SPACE_OVERHEAD_LOWER_BOUND,
                        MAX_SPACE_OVERHEAD_UPPER_BOUND);
            maxSpaceOverhead = Math.min(MAX_SPACE_OVERHEAD_UPPER_BOUND,
                                        Math.max(MAX_SPACE_OVERHEAD_LOWER_BOUND,
                                                 maxSpaceOverhead));
        }
        if (maxSpaceOverhead < maxSpaceOverheadLowerBound)
        {
            logger.warn("{} shards are not enough to maintain the required maximum space overhead of {}!\n" +
                        "The system will need to perform the most space intensive compactions in at least 1 shard at a time, " +
                        "so it will do so in exactly 1 shard at a time, and it will behave as if {} has been set to ~{}.",
                        numShards,
                        maxSpaceOverhead,
                        MAX_SPACE_OVERHEAD_OPTION,
                        String.format("%.3f", maxSpaceOverheadLowerBound));
        }
    }

    @VisibleForTesting
    public Environment getEnv()
    {
        return env;
    }

    /**
     * @return the scaling factor O
     * @param index
     */
    public abstract int getW(int index);

    /**
     * @return the number of shards according to the dataset and shard sizes set by the user
     */
    public int getNumShards()
    {
        return numShards;
    }

    /**
     * @return the survival factor o
     */
    public double getSurvivalFactor()
    {
        return survivalFactor;
    }

    /**
     * The user specified dataset size.
     *
     * @return the target size of the entire data set, in bytes.
     */
    public long getDataSetSizeBytes()
    {
        return dataSetSizeMB << 20;
    }

    /**
     * The user specified shard, or compaction arena, size.
     *
     * @return the desired size of each shard, or compaction arena, in bytes.
     */
    public long getShardSizeBytes()
    {
        return shardSizeMB << 20;
    }

    /**
     * Return the sstable size in bytes.
     *
     * This is either set by the user in the options or calculated by rounding up the first flush size to 50 MB.
     *
     * @return the minimum sstable size in bytes.
     */
    public long getMinSstableSizeBytes()
    {
        if (minSstableSizeMB > 0)
            return minSstableSizeMB << 20;

        synchronized (this)
        {
            if (minSstableSizeMB > 0)
                return minSstableSizeMB << 20;

            // round the avg flush size to the nearest byte
            long envFlushSize = Math.round(env.flushSize());
            long fiftyMB = 50 << 20;

            // round up to 50 MB
            long flushSize = ((Math.max(1, envFlushSize) + fiftyMB - 1) / fiftyMB) * fiftyMB;

            // If the env flush size is positive, then we've flushed at least once and we use this value permanently
            if (envFlushSize > 0)
                minSstableSizeMB = flushSize >> 20;

            return flushSize;
        }
    }

    /**
     * Returns the maximum tolerable compaction-induced space amplification, as a fraction of the dataset size.
     * Currently this is not a strict limit for which compaction gives an ironclad guarantee never to exceed it, but
     * the main input in a simple heuristic that is designed to limit UCS' space amplification in exchange of some
     * delay in top bucket compactions.
     *
     * @return a {@code double} value between 0.01 and 1.0, representing the fraction of the expected uncompacted
     * dataset size that should be additionally available for compaction's space amplification overhead. 
     */
    public double getMaxSpaceOverhead()
    {
        return maxSpaceOverhead;
    }

    /**
     * Perform any initialization that requires the strategy.
     */
    public void startup(UnifiedCompactionStrategy strategy, ScheduledExecutorService executorService)
    {
        if (calculator != null)
            throw new IllegalStateException("Already started");

        startup(strategy, new CostsCalculator(env, timeSource, strategy, executorService, survivalFactor));
    }

    @VisibleForTesting
    void startup(UnifiedCompactionStrategy strategy, CostsCalculator calculator)
    {
        this.calculator = calculator;
        metrics = allMetrics.computeIfAbsent(strategy.getMetadata(), metadata -> new Metrics(metadata));
        metrics.add(this);
        logger.debug("Started compaction controller", this);
    }

    /**
     * Signals that the strategy is about to be deleted or stopped.
     */
    public void shutdown()
    {
        if (calculator == null)
            return;

        calculator.close();
        calculator = null;

        if (metrics != null)
        {
            metrics.remove(this);
            metrics = null;
        }

        logger.debug("Stopped compaction controller", this);
    }

    /**
     * @return true if the controller is running
     */
    public boolean isRunning()
    {
        return calculator != null;
    }

    /**
     * @return the cost calculator, will be null until {@link this#startup(UnifiedCompactionStrategy, ScheduledExecutorService)} is called.
     */
    @Nullable
    @VisibleForTesting
    public CostsCalculator getCalculator()
    {
        return calculator;
    }

    /**
     * The strategy will call this method each time {@link CompactionStrategy#getNextBackgroundTasks(int)} is called.
     */
    public void onStrategyBackgroundTaskRequest()
    {
    }

    /**
     * Calculate the read amplification assuming a single scaling factor W and a given total
     * length of data on disk.
     *
     * @param length the total length on disk
     * @param W the scaling factor to use for the calculation
     *
     * @return the read amplification of all the buckets needed to cover the total length
     */
    public int RA(long length, int W)
    {
        double o = getSurvivalFactor();
        long m = getMinSstableSizeBytes();

        int F = W < 0 ? 2 - W : 2 + W;
        int T = W < 0 ? 2 : F;
        int bucketIndex = Math.max(0, (int) Math.floor((Math.log(length) - Math.log(m)) / (Math.log(F) - Math.log(o))));

        int ret = 0;
        for (int i = 0; i <= bucketIndex; i++)
            ret += (W >= 0 ? T - 1 : 1); // W >= 0 => tiered compaction, <0 => leveled compaction

        return ret;
    }

    /**
     * Calculate the write amplification assuming a single scaling factor W and a given total
     * length of data on disk.
     *
     * @param size the total length on disk
     * @param W the scaling factor to use for the calculation
     *
     * @return the write amplification of all the buckets needed to cover the total length
     */
    public int WA(long size, int W)
    {
        double o = getSurvivalFactor();
        long m = getMinSstableSizeBytes();

        int F = W < 0 ? 2 - W : 2 + W;
        int maxIndex = Math.max(0, (int) Math.floor((Math.log(size) - Math.log(m)) / (Math.log(F) - Math.log(o))));

        int ret = 0;

        if (W >= 0)
        {   // for tiered, at each level the WA is 1. We start at level 0 and end up at level maxIndex so that's a WA of maxIndex.
            ret += maxIndex;
        }
        else
        {   // for leveled, at each level the WA is F - 1 except for the last one, where it's (size / size of previous level) - 1
            // or (size / (m*(o*F)^maxIndex)) - 1
            for (int i = 0; i < maxIndex; i++)
                ret += F - 1;

            ret += Math.max(0, Math.ceil(size / (m * Math.pow(o * F, maxIndex))) - 1);
        }

        return ret;
    }

    private double getReadIOCost()
    {
        if (calculator == null)
            return 0;

        int W = getW(0);
        long length = (long) Math.ceil(calculator.spaceUsed().get());
        return calculator.getReadCostForQueries(RA(length, W));
    }

    private double getWriteIOCost()
    {
        if (calculator == null)
            return 0;

        int W = getW(0);
        long length = (long) Math.ceil(calculator.spaceUsed().get());
        return calculator.getWriteCostForQueries(WA(length, W));
    }

    public static Controller fromOptions(ColumnFamilyStore cfs, Map<String, String> options)
    {
        boolean adaptive = options.containsKey(ADAPTIVE_OPTION) ? Boolean.parseBoolean(options.get(ADAPTIVE_OPTION)) : DEFAULT_ADAPTIVE;
        long dataSetSizeMb = (options.containsKey(DATASET_SIZE_OPTION_GB) ? Long.parseLong(options.get(DATASET_SIZE_OPTION_GB)) : DEFAULT_DATASET_SIZE_GB) << 10;
        int numShards = options.containsKey(NUM_SHARDS_OPTION) ? Integer.parseInt(options.get(NUM_SHARDS_OPTION)) : DEFAULT_NUM_SHARDS;
        long sstableSizeMb = options.containsKey(MIN_SSTABLE_SIZE_OPTION_MB) ? Long.parseLong(options.get(MIN_SSTABLE_SIZE_OPTION_MB)) : DEFAULT_MIN_SSTABLE_SIZE_MB;
        double maxSpaceOverhead = options.containsKey(MAX_SPACE_OVERHEAD_OPTION)
                ? Double.parseDouble(options.get(MAX_SPACE_OVERHEAD_OPTION))
                : DEFAULT_MAX_SPACE_OVERHEAD;

        if (dataSetSizeMb <= 0)
            throw new IllegalArgumentException(String.format("Invalid configuration, dataset size should be positive: %d", dataSetSizeMb));

        if (numShards <= 0)
            throw new IllegalArgumentException(String.format("Invalid configuration, num shards should be >= 1: %d", numShards));

        Environment env = new RealEnvironment(cfs);

        return adaptive
               ? AdaptiveController.fromOptions(env, DEFAULT_SURVIVAL_FACTOR, dataSetSizeMb, numShards, sstableSizeMb, maxSpaceOverhead, options)
               : StaticController.fromOptions(env, DEFAULT_SURVIVAL_FACTOR, dataSetSizeMb, numShards, sstableSizeMb, maxSpaceOverhead, options);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        options = new HashMap<>(options);

        boolean adaptive = options.containsKey(ADAPTIVE_OPTION) ? Boolean.parseBoolean(options.remove(ADAPTIVE_OPTION)) : DEFAULT_ADAPTIVE;
        options.remove(Controller.MIN_SSTABLE_SIZE_OPTION_MB);
        options.remove(Controller.DATASET_SIZE_OPTION_GB);
        options.remove(Controller.NUM_SHARDS_OPTION);

        return adaptive ? AdaptiveController.validateOptions(options) : StaticController.validateOptions(options);
    }

    static final class Metrics
    {
        private final TableMetadata metadata;
        private final MetricNameFactory factory;
        private final CopyOnWriteArrayList<Controller> controllers;
        private final Gauge<Double> totWAGauge;
        private final Gauge<Double> readIOCostGauge;
        private final Gauge<Double> writeIOCostGauge;
        private final Gauge<Double> totIOCostGauge;

        Metrics(TableMetadata metadata)
        {
            this.metadata = metadata;
            this.factory = new DefaultNameFactory("CompactionCosts", metadata.toString());
            this.controllers = new CopyOnWriteArrayList<>();
            this.totWAGauge = Metrics.register(factory.createMetricName("WA"), this::getMeasuredWA);
            this.readIOCostGauge = Metrics.register(factory.createMetricName("ReadIOCost"), this::getReadIOCost);
            this.writeIOCostGauge = Metrics.register(factory.createMetricName("WriteIOCost"), this::getWriteIOCost);
            this.totIOCostGauge = Metrics.register(factory.createMetricName("TotIOCost"), this::getTotalIOCost);
        }

        void add(Controller controller)
        {
            if (!controllers.contains(controller))
                controllers.add(controller);
        }

        void remove(Controller controller)
        {
            controllers.remove(controller);

        }

        // TODO - should be called when the table is dropped
        void release()
        {
            Metrics.remove(factory.createMetricName("WA"));
            Metrics.remove(factory.createMetricName("ReadIOCost"));
            Metrics.remove(factory.createMetricName("WriteIOCost"));
            Metrics.remove(factory.createMetricName("TotIOCost"));
        }

        double getMeasuredWA()
        {
            double ret = 0;
            for (Controller controller : controllers)
                ret += controller.env.WA();

            return ret;
        }

        double getReadIOCost()
        {
            double ret = 0;

            for (Controller controller : controllers)
                ret += controller.getReadIOCost();

            return ret;
        }

        double getWriteIOCost()
        {
            double ret = 0;

            for (Controller controller : controllers)
                ret += controller.getWriteIOCost();

            return ret;
        }

        double getTotalIOCost()
        {
            return getReadIOCost() + getWriteIOCost();
        }
    }
}