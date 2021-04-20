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

    /** The minimal sstable size determines the size of the buckets since we multiply this value by F. When the default minimal
     * sstable size is zero, then it is calculated by the controller by looking at the initial flush size. A positive value here
     * means that the user wishes to set this value manually thereby ignoring the calculations based on the flush size.
     */
    static String MINIMAL_SSTABLE_SIZE_OPTION_MB = "minimal_sstable_size_in_mb";
    static int DEFAULT_MINIMAL_SSTABLE_SIZE_MB = Integer.getInteger(PREFIX + MINIMAL_SSTABLE_SIZE_OPTION_MB, 0);

    /**
     * This parameter is intended to modify the shape of the LSM by taking into account the survival ratio of data, for now it is fixed to one.
     */
    static double DEFAULT_SURVIVAL_FACTOR = Double.parseDouble(System.getProperty(PREFIX + "survival_factor", "1"));

    /**
     * Either true or false. This parameter determines which controller will be used.
     */
    static String ADAPTIVE_OPTION = "adaptive";
    static boolean DEFAULT_ADAPTIVE = Boolean.parseBoolean(System.getProperty(PREFIX + ADAPTIVE_OPTION, "true"));

    protected final TimeSource timeSource;
    protected final Environment env;
    protected final double survivalFactor;
    protected volatile long minSSTableSizeMB;

    @Nullable
    protected volatile CostsCalculator calculator;
    @Nullable private volatile Metrics metrics;

    Controller(TimeSource timeSource, Environment env,  double survivalFactor, long minSSTableSizeMB)
    {
        this.timeSource = timeSource;
        this.env = env;
        this.survivalFactor = survivalFactor;
        this.minSSTableSizeMB = minSSTableSizeMB;
    }

    /**
     * @return the scaling factor O
     * @param index
     */
    public abstract int getW(int index);

    /**
     * @return the survival factor o
     */
    public double getSurvivalFactor()
    {
        return survivalFactor;
    }

    /**
     * Return the minimum sstable size in bytes.
     *
     * This is either set by the user in the options or calculated by rounding up the first flush size to 50 MB.
     *
     * @return the minimum sstable size in bytes.
     */
    public long getMinSSTableSizeBytes()
    {
        if (minSSTableSizeMB > 0)
            return minSSTableSizeMB << 20;

        synchronized (this)
        {
            if (minSSTableSizeMB > 0)
                return minSSTableSizeMB << 20;

            // round the avg flush size to the nearest byte
            long envFlushSize = Math.round(env.flushSize());
            long fiftyMB = 50 << 20;

            // round up to 50 MB
            long flushSize = ((Math.max(1, envFlushSize) + fiftyMB - 1) / fiftyMB) * fiftyMB;

            // If the env flush size is positive, then we've flushed at least once and we use this value permanently
            if (envFlushSize > 0)
                minSSTableSizeMB = flushSize >> 20;

            return flushSize;
        }
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
     * The strategy will call this method each time {@link CompactionStrategy#getNextBackgroundTask(int)} is called.
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
        long m = getMinSSTableSizeBytes();

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
        long m = getMinSSTableSizeBytes();

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
        long m = (options.containsKey(MINIMAL_SSTABLE_SIZE_OPTION_MB) ? Integer.parseInt(options.get(MINIMAL_SSTABLE_SIZE_OPTION_MB)) : DEFAULT_MINIMAL_SSTABLE_SIZE_MB);
        Environment env = new RealEnvironment(cfs);

        return adaptive
               ? AdaptiveController.fromOptions(env, DEFAULT_SURVIVAL_FACTOR, m, options)
               : StaticController.fromOptions(env, DEFAULT_SURVIVAL_FACTOR, m, options);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        options = new HashMap<>(options);

        boolean adaptive = options.containsKey(ADAPTIVE_OPTION) ? Boolean.parseBoolean(options.remove(ADAPTIVE_OPTION)) : DEFAULT_ADAPTIVE;
        options.remove(Controller.MINIMAL_SSTABLE_SIZE_OPTION_MB);

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