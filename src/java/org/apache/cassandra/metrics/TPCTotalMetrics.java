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

        stages = new EnumMap<>(TPCTaskType.class);
        for (TPCTaskType s : TPCTaskType.values())
            stages.put(s, new TPCStageMetrics(metrics, s, path, poolPrefix));
    }

    private int getActiveTotal()
    {
        long active = 0;
        for (TPCTaskType s : TPCTaskType.values())
            active += metrics.scheduledTaskCount(s) - metrics.completedTaskCount(s);
        return (int) active;
    }

    private long getCompletedTotal()
    {
        long completed = 0;
        for (TPCTaskType s : TPCTaskType.values())
            completed += metrics.completedTaskCount(s);
        return completed;
    }

    public void release()
    {
        Metrics.remove(factory.createMetricName("ActiveTasks"));
        Metrics.remove(factory.createMetricName("CompletedTasks"));
        for (TPCTaskType s : TPCTaskType.values())
            stages.get(s).release();
    }
}
