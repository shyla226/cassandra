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
package org.apache.cassandra.metrics;

import java.util.EnumMap;
import java.util.concurrent.ThreadPoolExecutor;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCMetrics;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;


/**
 * Metrics for {@link ThreadPoolExecutor}.
 */
public class TPCTotalMetrics
{
    /** Number of active tasks. */
    public final Gauge<Integer> activeTasks;
    /** Number of completed tasks. */
    public final Gauge<Long> completedTasks;
    /** Number of pending tasks. */
    public final Gauge<Integer> pendingTasks;
    /** Number of blocked tasks. */
    public final Gauge<Long> blockedTasks;

    public final TPCMetrics metrics;

    private final MetricNameFactory factory;
    private final EnumMap<TPCTaskType, TPCStageMetrics> stages;

    /**
     * Create metrics for given ThreadPoolExecutor.
     *
     * @param metrics The TPCMetrics to report
     * @param path Type of thread pool
     */
    public TPCTotalMetrics(TPCMetrics metrics, String path, String poolPrefix)
    {
        this.metrics = metrics;
        String poolName = poolPrefix;

        this.factory = new ThreadPoolMetricNameFactory("ThreadPools", path, poolName);

        activeTasks = Metrics.register(factory.createMetricName("ActiveTasks"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return getActiveTotal();
            }
        });
        completedTasks = Metrics.register(factory.createMetricName("CompletedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return getCompletedTotal();
            }
        });
        pendingTasks = Metrics.register(factory.createMetricName("PendingTasks"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return getPendingTotal();
            }
        });
        blockedTasks = Metrics.register(factory.createMetricName("TotalBlockedTasksGauge"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return getBlockedTotal();
            }
        });

        stages = new EnumMap<>(TPCTaskType.class);
        for (TPCTaskType s : TPCTaskType.values())
            stages.put(s, new TPCStageMetrics(metrics, s, path, poolPrefix));
    }

    private int getActiveTotal()
    {
        long active = 0;
        for (TPCTaskType s : TPCTaskType.values())
            if (s.includedInTotals())
                active += metrics.activeTaskCount(s);
        return (int) active;
    }

    private long getCompletedTotal()
    {
        long completed = 0;
        for (TPCTaskType s : TPCTaskType.values())
            if (s.includedInTotals())
                completed += metrics.completedTaskCount(s);
        return completed;
    }

    private int getPendingTotal()
    {
        long pending = 0;
        for (TPCTaskType s : TPCTaskType.values())
            if (s.includedInTotals())
                pending += metrics.pendingTaskCount(s);
        return (int) pending;
    }

    private long getBlockedTotal()
    {
        long blocked = 0;
        for (TPCTaskType s : TPCTaskType.values())
            if (s.includedInTotals())
                blocked += metrics.blockedTaskCount(s);
        return blocked;
    }

    public void release()
    {
        Metrics.remove(factory.createMetricName("ActiveTasks"));
        Metrics.remove(factory.createMetricName("CompletedTasks"));
        Metrics.remove(factory.createMetricName("PendingTasks"));
        Metrics.remove(factory.createMetricName("TotalBlockedTasksGauge"));
        for (TPCTaskType s : TPCTaskType.values())
            stages.get(s).release();
    }
}
