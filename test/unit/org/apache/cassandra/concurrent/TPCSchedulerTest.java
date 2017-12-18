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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import io.reactivex.disposables.Disposable;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;

public class TPCSchedulerTest
{
    private StagedScheduler scheduler;
    private CountDownLatch latch;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /**
     * Asserts that TPC metrics are still correct in the face of multiple (racy) cancellations of tasks scheduled on a
     * {@link TPCScheduler}.
     */
    @Test
    public void testTPCMetricsAfterRacyCancellations() throws InterruptedException
    {
        scheduler = TPC.bestTPCScheduler();
        TPCTaskType stage = TPCTaskType.UNKNOWN;
        int numIterations = 100_000;
        int initialTaskCount = getTotalCompletedTaskCount(stage);

        latch = new CountDownLatch(numIterations);
        for (int i = 0; i < numIterations; ++i)
        {
            Disposable task = scheduler.schedule(() -> latch.countDown(),
                                                 TPCTaskType.UNKNOWN,
                                                 0,
                                                 TimeUnit.MILLISECONDS);
            // This disposal is meant to race with the task execution in the scheduler
            task.dispose();
        }
        latch.await(5, TimeUnit.SECONDS);
        // If we have an ample headroom (i.e. sufficient latch await timeout) the latch count should correspond
        // exactly to the number of cancelled tasks.
        System.out.println("Cancelled or unfinished tasks count: " + latch.getCount());

        assertEquals(0, getTotalActiveTaskCount(stage));
        assertEquals(numIterations, getTotalCompletedTaskCount(stage) - initialTaskCount);
    }

    private static int getTotalActiveTaskCount(TPCTaskType stage)
    {
        int totalActiveCount = 0;
        for (int i = 0; i <= TPC.getNumCores(); ++i)
        {
            totalActiveCount += TPC.metrics(i).activeTaskCount(stage);
        }
        return totalActiveCount;
    }

    private static int getTotalCompletedTaskCount(TPCTaskType stage)
    {
        int totalCompletedCount = 0;
        for (int i = 0; i <= TPC.getNumCores(); ++i)
        {
            totalCompletedCount += TPC.metrics(i).completedTaskCount(stage);
        }
        return totalCompletedCount;
    }
}
