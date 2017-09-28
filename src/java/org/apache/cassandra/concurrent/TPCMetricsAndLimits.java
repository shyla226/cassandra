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

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.config.DatabaseDescriptor;


/**
 * Task counter for other threads used in TPC tasks. This can be called from multiple threads concurrently
 * and needs to do atomic counting.
 */
public class TPCMetricsAndLimits implements TPCMetrics
{
    static class TaskStats
    {
        AtomicLong scheduledTasks = new AtomicLong(0);
        AtomicLong completedTasks = new AtomicLong(0);
        AtomicLong failedTasks = new AtomicLong(0);
        AtomicLong blockedTasks = new AtomicLong(0);
        AtomicLong pendingTasks = new AtomicLong(0);
    }
    final EnumMap<TPCTaskType, TaskStats> stats;

    AtomicLong activeCountedTasks = new AtomicLong();

    int maxConcurrentRequests = DatabaseDescriptor.getTPCConcurrentRequestsLimit();
    int maxPendingQueueSize = DatabaseDescriptor.getTPCPendingRequestsLimit();

    public TPCMetricsAndLimits()
    {
        this.stats = new EnumMap<>(TPCTaskType.class);
        for (TPCTaskType s : TPCTaskType.values())
            stats.put(s, new TaskStats());
    }

    public void scheduled(TPCTaskType stage)
    {
        TaskStats stat = stats.get(stage);
        stat.scheduledTasks.incrementAndGet();
        if (stage.counted)
            activeCountedTasks.incrementAndGet();
    }


    public void starting(TPCTaskType stage)
    {
        // We don't currently track this.
    }

    public void failed(TPCTaskType stage, Throwable t)
    {
        TaskStats stat = stats.get(stage);
        stat.failedTasks.incrementAndGet();
        if (stage.counted)
            activeCountedTasks.decrementAndGet();
    }

    public void completed(TPCTaskType stage)
    {
        TaskStats stat = stats.get(stage);
        stat.completedTasks.incrementAndGet();
        if (stage.counted)
            activeCountedTasks.decrementAndGet();
    }

    public void cancelled(TPCTaskType stage)
    {
        // Count as completed. Maybe add cancelled later.
        completed(stage);
    }

    public void pending(TPCTaskType stage, int adjustment)
    {
        TaskStats stat = stats.get(stage);
        stat.pendingTasks.addAndGet(adjustment);
    }

    public void blocked(TPCTaskType stage)
    {
        TaskStats stat = stats.get(stage);
        stat.blockedTasks.incrementAndGet();
    }

    public long scheduledTaskCount(TPCTaskType stage)
    {
        TaskStats stat = stats.get(stage);
        return stat.scheduledTasks.get();
    }

    public long completedTaskCount(TPCTaskType stage)
    {
        TaskStats stat = stats.get(stage);
        return stat.completedTasks.get();
    }

    public long activeTaskCount(TPCTaskType stage)
    {
        TaskStats stat = stats.get(stage);
        return stat.scheduledTasks.get() - stat.completedTasks.get() - stat.pendingTasks.get();
    }

    public long pendingTaskCount(TPCTaskType stage)
    {
        TaskStats stat = stats.get(stage);
        return stat.pendingTasks.get();
    }

    public long blockedTaskCount(TPCTaskType stage)
    {
        TaskStats stat = stats.get(stage);
        return stat.blockedTasks.get();
    }

    public int maxQueueSize()
    {
        long activeReads = activeCountedTasks.get();
        return (int) Math.max(maxConcurrentRequests - activeReads, 0);
    }

    public int maxPendingQueueSize()
    {
        return maxPendingQueueSize;
    }

    public int getMaxConcurrentRequests()
    {
        return maxConcurrentRequests;
    }

    public void setMaxConcurrentRequests(int maxConcurrentRequests)
    {
        this.maxConcurrentRequests = maxConcurrentRequests;
    }
}
