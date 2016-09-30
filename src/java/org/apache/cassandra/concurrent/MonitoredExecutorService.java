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

import com.google.common.collect.Lists;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpscChunkedArrayQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Executor Service based on idea from Martin Thompson.
 *
 * Threads look for work in their queue. If they can't find any
 * work they park themselves.
 *
 * A single background monitor thread watches the queue for each executor in a loopr.
 * When the queue is not empty it unparks a thread then sleeps.
 *
 * This service incorporates work stealing by splitting Threads from Queues. Meaning,
 * any thread can fetch from any other executor queue once it's finished with it's local
 * queue.
 */
public class MonitoredExecutorService extends AbstractLocalAwareExecutorService
{
    private static final Logger logger = LoggerFactory.getLogger(MonitoredExecutorService.class);

    private static Thread monitorThread;

    private static final List<MonitoredExecutorService> monitoredExecutiveServices = Lists.newCopyOnWriteArrayList();
    private static final ThreadFactory threadFactory = new NamedThreadFactory("monitored-executor-service-worker");

    private ThreadWorker[] allWorkers;
    private Thread[] allThreads;

    final String name;
    final int maxThreads;
    final int maxQueuedItems;
    final MpmcArrayQueue<FutureTask<?>> workQueue;

    volatile boolean shuttingDown = false;
    final SimpleCondition shutdown = new SimpleCondition();

    public MonitoredExecutorService(String name, int maxThreads, int maxQueuedItems)
    {
        workQueue = new MpmcArrayQueue<>(1 << 20);
        this.name = name;
        this.maxQueuedItems = maxQueuedItems;
        this.maxThreads = maxThreads;

        allWorkers = new ThreadWorker[maxThreads];
        allThreads = new Thread[maxThreads];

        for (int i = 0; i < maxThreads; i++)
        {
            allWorkers[i] = new ThreadWorker(i, this);
            allThreads[i] = threadFactory.newThread(allWorkers[i]);
        }

        for (int i = 0; i < maxThreads; i++)
        {
            allThreads[i].setDaemon(true);
            allThreads[i].start();
        }

        startMonitoring(this);
    }

    /**
     * Kicks off the monitoring of a new ES.  Also spawns worker threads and monitor when first called.
     * @param service
     */
    private static synchronized void startMonitoring(MonitoredExecutorService service)
    {
        monitoredExecutiveServices.add(service);

        // Start monitor on first call
        if (monitorThread == null)
        {
            monitorThread = new Thread(() -> {
                while (true)
                {
                    for (int i = 0, length = monitoredExecutiveServices.size(); i < length; i++)
                        monitoredExecutiveServices.get(i).checkQueue();

                    LockSupport.parkNanos(1);
                }
            });

            monitorThread.setName("monitor-executor-service-thread");
            monitorThread.setDaemon(true);
            monitorThread.start();
        }
    }


    static class ThreadWorker implements Runnable
    {
        enum State
        {
            PARKED, WORKING
        }

        volatile State state;
        volatile MonitoredExecutorService primary;
        final int threadId;
        private static final int maxExtraSpins = 1024 * 10;
        private static final int yieldExtraSpins = 1024 * 8;
        private static final int parkExtraSpins = 1024 * 9;

        ThreadWorker(int threadId, MonitoredExecutorService executor)
        {
            this.threadId = threadId;
            this.state = State.PARKED;
            this.primary = executor;
        }

        @Override
        public void run()
        {

            while (true)
            {
                int spins = 0;
                int processed;
                //deal with spurious wakeups
                if (state == State.WORKING)
                {
                    //Find/Steal work then park self
                    while(true)
                    {
                        processed = primary.workQueue.drain(FutureTask::run, 8);

                        if (processed > 0 || ++spins < yieldExtraSpins)
                            continue;
                        else if (spins < parkExtraSpins)
                            Thread.yield();
                        else if (spins < maxExtraSpins)
                            LockSupport.parkNanos(1);
                        else
                            break;
                    }
                }

                park();
            }
        }

        public void park()
        {
            state = State.PARKED;
            LockSupport.park();
        }

        public void unpark(Thread thread)
        {
            state = State.WORKING;
            LockSupport.unpark(thread);
        }
    }

    private void checkQueue()
    {
        for (int i = 0; i < allWorkers.length; i++)
        {
            ThreadWorker t = allWorkers[i];

            if (t.state == ThreadWorker.State.PARKED && !workQueue.isEmpty())
            {
                t.unpark(allThreads[i]);
                LockSupport.parkNanos(1);
            }
        }
    }

    @Override
    protected void addTask(FutureTask<?> futureTask)
    {
        if (shuttingDown == true)
            throw new RuntimeException("ExecutorService has shutdown : " + name);

        if (!workQueue.offer(futureTask))
            throw new RuntimeException("Queue is full for ExecutorService : " + name);
    }

    protected void onCompletion()
    {

    }


    @Override
    public void maybeExecuteImmediately(Runnable command)
    {
        FutureTask<?> work = newTaskFor(command, null);

        addTask(work);
    }

    @Override
    public void shutdown()
    {
        shuttingDown = true;
        shutdown.signalAll();
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        shutdown();
        List<Runnable> aborted = new ArrayList<>();
        FutureTask<?> work;
        while ((work = workQueue.poll()) != null)
            aborted.add(work);

        return aborted;
    }

    @Override
    public boolean isShutdown()
    {
        return shuttingDown;
    }

    @Override
    public boolean isTerminated()
    {
        return shuttingDown && shutdown.isSignaled();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        shutdown.await(timeout, unit);
        return isTerminated();
    }
}
