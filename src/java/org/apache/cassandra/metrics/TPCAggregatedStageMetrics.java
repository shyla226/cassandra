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

import java.util.concurrent.ThreadPoolExecutor;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.concurrent.TPCMetrics;
import org.apache.cassandra.concurrent.TPCTaskType;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;


/**
 * Metrics for {@link ThreadPoolExecutor}.
 */
public class TPCAggregatedStageMetrics
{
    /** Number of active tasks. */
    public final Gauge<Integer> activeTasks;
    /** Number of completed tasks. */
    public final Gauge<Long> completedTasks;
    /** Number of pending tasks. */
    public final Gauge<Integer> pendingTasks;
    /** Number of blocked tasks. */
    public final Gauge<Long> blockedTasks;

    public final TPCMetrics[] metrics;
    public final TPCTaskType stage;

    private final MetricNameFactory factory;

    /**
     * Create metrics for given ThreadPoolExecutor.
     *
     * @param metrics The TPCMetrics to report
     * @param stage The execution stage to report on
     * @param path Type of thread pool
     */
    public TPCAggregatedStageMetrics(TPCMetrics[] metrics, TPCTaskType stage, String path, String poolPrefix)
    {
        this.metrics = metrics;
        this.stage = stage;
        String poolName = poolPrefix + "/" + stage.name();

        this.factory = new ThreadPoolMetricNameFactory("ThreadPools", path, poolName);

        activeTasks = Metrics.register(factory.createMetricName("ActiveTasks"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                long v = 0;
                for (int i = 0; i < metrics.length; ++i)
                    v += metrics[i].activeTaskCount(stage);
                return (int) v;
            }
        });
        completedTasks = Metrics.register(factory.createMetricName("CompletedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                long v = 0;
                for (int i = 0; i < metrics.length; ++i)
                    v += metrics[i].completedTaskCount(stage);
                return v;
            }
        });
        if (stage.pendable)
        {
            pendingTasks = Metrics.register(factory.createMetricName("PendingTasks"), new Gauge<Integer>()
            {
                public Integer getValue()
                {
                    long v = 0;
                    for (int i = 0; i < metrics.length; ++i)
                        v += metrics[i].pendingTaskCount(stage);
                    return (int) v;
                }
            });
            blockedTasks = Metrics.register(factory.createMetricName("TotalBlockedTasksGauge"), new Gauge<Long>()
            {
                public Long getValue()
                {
                    long v = 0;
                    for (int i = 0; i < metrics.length; ++i)
                        return metrics[i].blockedTaskCount(stage);
                    return v;
                }
            });
        }
        else
        {
            pendingTasks = null;
            blockedTasks = null;
        }
    }

    public void release()
    {
        Metrics.remove(factory.createMetricName("ActiveTasks"));
        Metrics.remove(factory.createMetricName("CompletedTasks"));
        if (pendingTasks != null)
        {
            Metrics.remove(factory.createMetricName("PendingTasks"));
            Metrics.remove(factory.createMetricName("TotalBlockedTasksGauge"));
        }
    }
}
