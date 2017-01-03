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

package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import junit.framework.Assert;

public class LongAdderTest
{
    @Test
    public void testSingleThread()
    {
        LongAdder longAdder = new LongAdder();
        for (int i = 1; i <= 1000; i++)
            longAdder.add(i);

        // the sum of the first n natural numbers is nx(n+1) / 2
        Assert.assertEquals(1000 * 1001 / 2, longAdder.sum());
    }

    @Test
    public void testMultipleThreads() throws InterruptedException
    {
        final int numThreads = 10;
        final int numUpdtes = 1000;
        final LongAdder longAdder = new LongAdder();
        final CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++)
        {
            Thread t = new Thread(() -> {
                try
                {
                    for (int j = 0; j < numUpdtes; j++)
                        longAdder.increment();
                }
                finally
                {
                    latch.countDown();
                }
            });
            t.setDaemon(true);
            t.start();
        }

        latch.await(10, TimeUnit.SECONDS);
        Assert.assertEquals(numThreads * numUpdtes, longAdder.sum());

        //now repeat for decrement
        final CountDownLatch latch2 = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++)
        {
            Thread t = new Thread(() -> {
                try
                {
                    for (int j = 0; j < numUpdtes; j++)
                        longAdder.decrement();
                }
                finally
                {
                    latch2.countDown();
                }
            });
            t.setDaemon(true);
            t.start();
        }

        latch2.await(10, TimeUnit.SECONDS);
        Assert.assertEquals(0, longAdder.sum());
    }
}
