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

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterators;
import org.junit.Test;

import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import junit.framework.Assert;
import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.apache.cassandra.utils.FBUtilities;

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
        Assert.assertEquals(1000 * 1001 / 2, longAdder.longValue());
        Assert.assertEquals(1000 * 1001 / 2, longAdder.intValue());
        Assert.assertEquals(1000 * 1001 / 2, longAdder.doubleValue(), 0.000001);
        Assert.assertEquals(1000 * 1001 / 2, longAdder.floatValue(), 0.000001);
        Assert.assertNotNull(longAdder.toString());

        longAdder.reset();
        Assert.assertEquals(0, longAdder.sum());
        Assert.assertEquals(0, longAdder.longValue());
        Assert.assertEquals(0, longAdder.intValue());
        Assert.assertEquals(0, longAdder.doubleValue(), 0.000001);
        Assert.assertEquals(0, longAdder.floatValue(), 0.000001);
        Assert.assertNotNull(longAdder.toString());
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
        Assert.assertEquals(numThreads * numUpdtes, longAdder.longValue());
        Assert.assertEquals(numThreads * numUpdtes, longAdder.intValue());
        Assert.assertEquals(numThreads * numUpdtes, longAdder.doubleValue(), 0.000001);
        Assert.assertEquals(numThreads * numUpdtes, longAdder.floatValue(), 0.000001);

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
        Assert.assertEquals(0, longAdder.longValue());
        Assert.assertEquals(0, longAdder.intValue());
        Assert.assertEquals(0, longAdder.doubleValue(), 0.000001);
        Assert.assertEquals(0, longAdder.floatValue(), 0.000001);
    }

    @Test
    public void testNettyEventLoops() throws InterruptedException
    {
        final int numCores = NettyRxScheduler.getNumCores();
        final int numThreads = numCores * 2; // half TPC threads and half non-TPC threads
        final int numUpdtes = 1000;
        final LongAdder longAdder = new LongAdder();
        EpollEventLoopGroup tpcLoops = new EpollEventLoopGroup(numCores,
                                                            new NettyRxScheduler.NettyRxThreadFactory("eventLoopBench",
                                                                                                      Thread.MAX_PRIORITY));

        EpollEventLoopGroup otherLoops = new EpollEventLoopGroup(numThreads - numCores);


        final CountDownLatch latchForSetup = new CountDownLatch(numCores);
        final AtomicInteger cpuId = new AtomicInteger(0);
        for (EventExecutor loop : tpcLoops)
        {
            loop.submit(() ->
                         {
                             NettyRxScheduler.register(loop, cpuId.getAndIncrement());
                             latchForSetup.countDown();
                         });
        }

        latchForSetup.await(10, TimeUnit.SECONDS);

        // fail test early if for any reason Rx scheduler not setup correctly
        for (int i = 0; i < numCores; i++)
            Assert.assertNotNull(NettyRxScheduler.getForCore(i));

        // test increment of counter
        final CountDownLatch latchForIncrement = new CountDownLatch(numThreads);
        for (Iterator<EventExecutor> it = Iterators.concat(tpcLoops.iterator(), otherLoops.iterator()); it.hasNext(); )
        {
            EventExecutor loop = it.next();
            loop.submit(() ->
                        {
                            try
                            {
                                for (int j = 0; j < numUpdtes; j++)
                                    longAdder.increment();
                            }
                            finally
                            {
                                latchForIncrement.countDown();
                            }
                        });
        }

        latchForIncrement.await(10, TimeUnit.SECONDS);
        FBUtilities.sleepQuietly(100);
        Assert.assertEquals(numThreads * numUpdtes, longAdder.sum());
        Assert.assertEquals(numThreads * numUpdtes, longAdder.longValue());
        Assert.assertEquals(numThreads * numUpdtes, longAdder.intValue());
        Assert.assertEquals(numThreads * numUpdtes, longAdder.doubleValue(), 0.000001);
        Assert.assertEquals(numThreads * numUpdtes, longAdder.floatValue(), 0.000001);

        //now repeat for decrement
        final CountDownLatch latchForDecrement = new CountDownLatch(numThreads);
        for (Iterator<EventExecutor> it = Iterators.concat(tpcLoops.iterator(), otherLoops.iterator()); it.hasNext(); )
        {
            EventExecutor loop = it.next();
            loop.submit(() ->
                        {
                            try
                            {
                                for (int j = 0; j < numUpdtes; j++)
                                    longAdder.decrement();
                            }
                            finally
                            {
                                latchForDecrement.countDown();
                            }
                        });
        }

        latchForDecrement.await(10, TimeUnit.SECONDS);
        Assert.assertEquals(0, longAdder.sum());
        Assert.assertEquals(0, longAdder.longValue());
        Assert.assertEquals(0, longAdder.intValue());
        Assert.assertEquals(0, longAdder.doubleValue(), 0.000001);
        Assert.assertEquals(0, longAdder.floatValue(), 0.000001);
    }
}
