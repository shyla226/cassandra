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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.*;

public class ParkedThreadsMonitorTest
{
    private ParkedThreadsMonitor monitor = ParkedThreadsMonitor.instance.get();
    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test(timeout = 1000)
    public void shouldExecuteAction()
    {
        AtomicLong counter = new AtomicLong();
        monitor.addAction(() -> counter.incrementAndGet());
        while (counter.get() == 0)
        {
            Thread.yield();
        }
    }

    @Test(timeout = 1000)
    public void shouldMonitorThread() throws InterruptedException
    {
        AtomicLong counter = new AtomicLong();
        // thread is parked when counter is even, unpark makes it odd
        ParkedThreadsMonitor.MonitorableThread monitorableThread = new ParkedThreadsMonitor.MonitorableThread()
        {
            public void unpark()
            {
                counter.incrementAndGet();
            }

            public boolean shouldUnpark(long nanoTimeSinceStart)
            {
                return counter.get() % 2 == 0;
            }
        };
        monitor.addThreadToMonitor(monitorableThread);
        // added thread was even(0), wait for it to change
        while (counter.get() == 0)
        {
            Thread.yield();
        }
        counter.incrementAndGet(); // 1 -> 2
        while (counter.get() == 2) // monitor will unpark: 2 -> 3
        {
            Thread.yield();
        }
        counter.incrementAndGet(); // 3 -> 4
        while (counter.get() == 4) // monitor will unpark: 4 -> 5
        {
            Thread.yield();
        }

        monitor.removeThreadToMonitor(monitorableThread);
        // observe action on monitor
        observeMonitorAction();
        // increment
        counter.incrementAndGet(); // 5 -> 6
        // observe action on monitor
        observeMonitorAction();
        // sleep. If thread was still monitored the counter would be incremented
        Thread.sleep(100);
        assertEquals(counter.get(), 6l);
    }

    private void observeMonitorAction() throws InterruptedException
    {
        CountDownLatch latch1 = new CountDownLatch(1);
        monitor.addAction(() -> latch1.countDown());
        latch1.await();
    }
}