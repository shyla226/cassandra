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

package org.apache.cassandra.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.LongOpOrderTest;
import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.concurrent.OpOrderThreaded;

/**
 * Quick test OpOrder in the way we use it.
 */
public class OpOrderTest
{
    private static final Logger logger = LoggerFactory.getLogger(LongOpOrderTest.class);

    static final int PRODUCERS_PER_THREAD = 16;
    static final int CONSUMERS = 2;

    static final long RUNTIME = TimeUnit.SECONDS.toMillis(120);
    static final long REPORT_INTERVAL = TimeUnit.SECONDS.toMillis(5);

    final AtomicLong errors = new AtomicLong(0);
    OpOrderThreaded order;
    State currentState;

    static final Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler()
    {
        @Override
        public void uncaughtException(Thread t, Throwable e)
        {
            System.err.println(t.getName() + ": " + e.getMessage());
            e.printStackTrace();
        }
    };


    @Test
    public void testOpOrder() throws InterruptedException
    {
        DatabaseDescriptor.daemonInitialization();
        NettyRxScheduler.register();
        Thread.setDefaultUncaughtExceptionHandler(handler);

        order = NettyRxScheduler.newOpOrderThreaded(this);
        currentState = new State();

        ExecutorService consumerExec = Executors.newFixedThreadPool(CONSUMERS);
        for (int i = 0; i < CONSUMERS; ++i)
        {
            consumerExec.execute(() ->
                                 {
                                     final long until = System.currentTimeMillis() + RUNTIME;
                                     long lastReport = System.currentTimeMillis();
                                     long count = 0;
                                     long opCount = 0;
                                     while (true)
                                     {
                                         long now = System.currentTimeMillis();
                                         if (now > until)
                                             break;

                                         if (now > lastReport + REPORT_INTERVAL)
                                         {
                                             lastReport = now;
                                             logger.info(String.format("%s: Executed %d barriers with %d operations. %.0f%% complete.",
                                                                       Thread.currentThread().getName(),
                                                                       count,
                                                                       opCount,
                                                                       100 * (1 - ((until - now) / (double) RUNTIME))));
                                         }

                                         try
                                         {
                                             Thread.sleep(0, ThreadLocalRandom.current().nextInt(50000));
                                         }
                                         catch (InterruptedException e)
                                         {
                                             e.printStackTrace();
                                             return;
                                         }

                                         State prevState = currentState;     // multiple threads could be accessing same prev. That's ok
                                         currentState = new State();
                                         order.awaitNewThreadedBarrier();            // happens-before point... currentState must be seen by other threads now
                                         if (!prevState.done)
                                         {
                                             prevState.done = true;
                                             opCount += prevState.touches.get();
                                         }

                                         count++;
                                     }
                                     shutdown = true;
                                     totalBarrierCount.addAndGet(count);
                                     totalOpCount.addAndGet(opCount);
                                 });
            Thread.sleep(0, ThreadLocalRandom.current().nextInt(50000));
        }

        for (int i = 0; i < NettyRxScheduler.NUM_NETTY_THREADS; ++i)
        {
            Producer[] producers = new Producer[PRODUCERS_PER_THREAD];
            Scheduler scheduler = new SchedulerNettyWDelay(i);
            for (int j = 0; j < producers.length; ++j)
                producers[j] = new Producer(producers, scheduler);
            scheduler.schedule(producers[0]);
        }

        consumerExec.shutdown();
        Thread.sleep(RUNTIME * 21 / 20);
        String msg = String.format("Did %,d ops, %,d barriers with %d errors%s", totalOpCount.get(), totalBarrierCount.get(), errors.get(), consumerExec.isTerminated() ? "" : " did not terminate");
        logger.info(msg);
        Assert.assertTrue(msg, consumerExec.isTerminated());
        Assert.assertEquals(msg, 0L, errors.get());
        Assert.assertTrue(msg,totalOpCount.get() > 1000 * PRODUCERS_PER_THREAD * NettyRxScheduler.NUM_NETTY_THREADS);
        Assert.assertTrue(msg,totalBarrierCount.get() > 100 * CONSUMERS);
    }

    volatile boolean shutdown = false;
    AtomicLong totalBarrierCount = new AtomicLong(0);
    AtomicLong totalOpCount = new AtomicLong(0);

    class State
    {
        volatile boolean done = false;
        AtomicInteger touches = new AtomicInteger(0);
    }

    class Producer implements Runnable
    {
        Scheduler scheduler;
        Producer[] othersInGroup;
        State s;
        AutoCloseable group;

        public Producer(Producer[] producers, Scheduler scheduler)
        {
            this.othersInGroup = producers;
            this.scheduler = scheduler;
        }

        public void run()
        {
            if (group != null)
            {
                if (s.done)
                {
                    logger.error("Touching completed state (where done is set post-barrier-await), touches: {}.",
                                 s.touches.get());
                    errors.incrementAndGet();
                }
                if (s.touches.incrementAndGet() >= 1000000)
                {
                    logger.info("m");
                    s.touches.set(0);
                }

                try
                {
                    group.close();
                }
                catch (Exception e)
                {
                    throw new AssertionError(e);
                }
            }

            if (!shutdown)
            {
                group = order.start();  // happens-before point; s must be earlier value
                s = currentState;
                scheduler.schedule(othersInGroup[ThreadLocalRandom.current().nextInt(othersInGroup.length)]);
            }
            else
            {
                // Run anything that has an unclosed group to completion.
                group = null;
                for (Producer p : othersInGroup)
                    if (p.group != null)
                    {
                        scheduler.schedule(p);
                        break;
                    }
            }
        }
    }

    interface Scheduler
    {
        void schedule(Runnable runnable);
    }

    class SchedulerES implements Scheduler
    {
        ExecutorService es = Executors.newFixedThreadPool(1);
        SchedulerES(int id)
        {
        }

        public void schedule(Runnable runnable)
        {
            es.execute(runnable);
        }
    }

    class SchedulerNetty implements Scheduler
    {
        NettyRxScheduler s;
        SchedulerNetty(int id)
        {
            s = NettyRxScheduler.getForCore(id);
        }

        public void schedule(Runnable runnable)
        {
            try
            {
                s.scheduleDirect(runnable);
            }
            catch (Throwable t)
            {
                logger.error("Schedure failure.", t);
            }
        }
    }

    class SchedulerNettyWDelay implements Scheduler
    {
        NettyRxScheduler s;
        SchedulerNettyWDelay(int id)
        {
            s = NettyRxScheduler.getForCore(id);
        }
        int scheduled;

        public void schedule(Runnable runnable)
        {
            try
            {
                if ((++scheduled & 0xFF) > 0)
                    s.scheduleDirect(runnable);
                else
                    s.scheduleDirect(runnable, 1, TimeUnit.MICROSECONDS);

            }
            catch (Throwable t)
            {
                logger.error("Schedure failure.", t);
            }
        }
    }
}
