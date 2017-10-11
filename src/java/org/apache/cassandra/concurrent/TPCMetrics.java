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

/**
 * Interface for recoding TPC thread metrics.
 */
public interface TPCMetrics extends TPCLimitsMBean
{
    // Task notifications

    // This will be called from the scheduling thread, possibly concurrently.
    public void scheduled(TPCTaskType stage);
    // These will be called from within the relevant thread.
    public void starting(TPCTaskType stage);
    public void failed(TPCTaskType stage, Throwable t);
    public void completed(TPCTaskType stage);
    // These could be called from another thread as well.
    public void cancelled(TPCTaskType stage);
    public void pending(TPCTaskType stage, int adjustment);
    public void blocked(TPCTaskType stage);

    // Information extraction for consumption by JMX etc
    public long scheduledTaskCount(TPCTaskType stage);
    public long completedTaskCount(TPCTaskType stage);
    public long activeTaskCount(TPCTaskType stage);
    public long pendingTaskCount(TPCTaskType stage);
    public long blockedTaskCount(TPCTaskType stage);

    /**
     * The maximum size of the TPC queue, which is calculated by subtracting the number of async read outstanding
     * from the maximum permitted concurrent requests.
     *
     * This is done because async reads do not show up in the TPC queue, and even if they did they would end up
     * combining multiple requests served by one cache fetch.
     */
    public int maxQueueSize();
}
