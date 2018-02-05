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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.concurrent.LongAdder;


/**
 * Task counter for other threads used in TPC tasks. This can be called from multiple threads concurrently
 * and needs to do atomic counting.
 */
public class TPCMetricsAndLimits implements TPCMetrics
{
    static class TaskStats
    {
        LongAdder scheduledTasks = new LongAdder();
        LongAdder completedTasks = new LongAdder();
        LongAdder failedTasks = new LongAdder();
        LongAdder blockedTasks = new LongAdder();
        LongAdder pendingTasks = new LongAdder();
    }
    private final TaskStats[] stats;

    AtomicLong externallyCountedTasks = new AtomicLong();
    AtomicLong backpressureCountedTasks = new AtomicLong();
    LongAdder backpressureDelayedTasks = new LongAdder();

    // This max value is based on queues size in EpollTPCEventLoopGroup: this kind of hidden dependency is not great
    // design-wise and we can refactor later, let's not shoot ourselves in the feet for now.
    private final static int MAX_REQUESTS_SIZE = 1 << 16;
    //
    int maxConcurrentRequests = DatabaseDescriptor.getTPCConcurrentRequestsLimit();
    int maxPendingRequests = DatabaseDescriptor.getTPCPendingRequestsLimit();

    public TPCMetricsAndLimits()
    {
        this.stats = new TaskStats[TPCTaskType.values().length];
        for (int i=0;i<stats.length;i++)
            stats[i] = new TaskStats();
    }

    public TaskStats getTaskStats(TPCTaskType stage)
    {
        return stats[stage.ordinal()];
    }

    public void scheduled(TPCTaskType stage)
    {
        TaskStats stat = getTaskStats(stage);
        stat.scheduledTasks.add(1);
        if (stage.externalQueue())
            externallyCountedTasks.incrementAndGet();
    }

    public void starting(TPCTaskType stage)
    {
        // We don't currently track this.
    }

    public void failed(TPCTaskType stage, Throwable t)
    {
        TaskStats stat = getTaskStats(stage);
        stat.failedTasks.add(1);
        if (stage.externalQueue())
            externallyCountedTasks.decrementAndGet();
    }

    public void completed(TPCTaskType stage)
    {
        TaskStats stat = getTaskStats(stage);
        stat.completedTasks.add(1);
        if (stage.externalQueue())
            externallyCountedTasks.decrementAndGet();
    }

    public void cancelled(TPCTaskType stage)
    {
        // Count as completed. Maybe add cancelled later.
        completed(stage);
    }

    public void pending(TPCTaskType stage, int adjustment)
    {
        TaskStats stat = getTaskStats(stage);
        stat.pendingTasks.add(adjustment);
        if (stage.backpressured())
            backpressureCountedTasks.addAndGet(adjustment);
    }

    public void blocked(TPCTaskType stage)
    {
        TaskStats stat = getTaskStats(stage);
        stat.blockedTasks.add(1);
    }

    public long scheduledTaskCount(TPCTaskType stage)
    {
        TaskStats stat = getTaskStats(stage);
        return stat.scheduledTasks.longValue();
    }

    public long completedTaskCount(TPCTaskType stage)
    {
        TaskStats stat = getTaskStats(stage);
        return stat.completedTasks.longValue();
    }

    public long activeTaskCount(TPCTaskType stage)
    {
        TaskStats stat = getTaskStats(stage);
        return stat.scheduledTasks.longValue() - stat.completedTasks.longValue() - stat.pendingTasks.longValue() - stat.blockedTasks.longValue();
    }

    public long pendingTaskCount(TPCTaskType stage)
    {
        TaskStats stat = getTaskStats(stage);
        return stat.pendingTasks.longValue();
    }

    public long blockedTaskCount(TPCTaskType stage)
    {
        TaskStats stat = getTaskStats(stage);
        return stat.blockedTasks.longValue();
    }

    @Override
    public long backpressureCountedTaskCount()
    {
        return backpressureCountedTasks.get();
    }

    @Override
    public void backpressureDelayedTaskCount(int adjustment)
    {
        backpressureDelayedTasks.add(adjustment);
    }

    @Override
    public long backpressureDelayedTaskCount()
    {
        return backpressureDelayedTasks.longValue();
    }

    public int maxQueueSize()
    {
        long activeReads = externallyCountedTasks.get();
        return (int) Math.max(maxConcurrentRequests - activeReads, 0);
    }

    public int getMaxPendingRequests()
    {
        return maxPendingRequests;
    }

    public void setMaxPendingRequests(int maxPendingRequests)
    {
        if (maxPendingRequests > MAX_REQUESTS_SIZE)
            throw new IllegalArgumentException("Max pending requests must be <= " + MAX_REQUESTS_SIZE);

        this.maxPendingRequests = maxPendingRequests;
    }

    public int getMaxConcurrentRequests()
    {
        return maxConcurrentRequests;
    }

    public void setMaxConcurrentRequests(int maxConcurrentRequests)
    {
        if (maxConcurrentRequests > MAX_REQUESTS_SIZE)
            throw new IllegalArgumentException("Max concurrent requests must be <= " + MAX_REQUESTS_SIZE);

        this.maxConcurrentRequests = maxConcurrentRequests;
    }
}
