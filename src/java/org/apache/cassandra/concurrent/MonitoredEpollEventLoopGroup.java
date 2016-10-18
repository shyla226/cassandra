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
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;


import com.google.common.util.concurrent.Uninterruptibles;

import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoop;
import io.netty.channel.epoll.Native;
import io.netty.util.concurrent.AbstractScheduledEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.RejectedExecutionHandlers;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.SpscArrayQueue;
import sun.misc.Contended;

public class MonitoredEpollEventLoopGroup extends MultithreadEventLoopGroup
{

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MonitoredEpollEventLoopGroup.class);

    @Contended
    private SingleCoreEventLoop[] initLoops;

    @Contended
    private Thread[] initThreads;

    @Contended
    private final SingleCoreEventLoop[] eventLoops;

    @Contended
    private final Thread[] runningThreads;

    private final Thread monitorThread;

    public MonitoredEpollEventLoopGroup(int nThreads)
    {
        this(nThreads, new DefaultThreadFactory(MonitoredEpollEventLoopGroup.class, Thread.MAX_PRIORITY));
    }

    /**
     * We pass default args for the newChild() since we need to init the data lazily
     * @param nThreads
     * @param threadFactory
     * @param args
     */
    public MonitoredEpollEventLoopGroup(int nThreads, ThreadFactory threadFactory, Object... args)
    {
        super(nThreads, threadFactory, nThreads, threadFactory, new AtomicInteger(0), args);

        eventLoops = initLoops;
        runningThreads = initThreads;

        monitorThread = threadFactory.newThread(() -> {
            int length = eventLoops.length;

            while (true)
            {
                for (int i = 0; i < length; i++)
                    eventLoops[i].checkQueues();

                LockSupport.parkNanos(1);
            }
        });

        monitorThread.setName("netty-monitor-event-loop-thread");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    @Override
    public void shutdown()
    {
        monitorThread.interrupt();
        super.shutdown();
    }

    private synchronized void maybeInit(int nThreads, ThreadFactory threadFactory)
    {
        if (initLoops != null)
            return;

        initLoops = new SingleCoreEventLoop[nThreads];
        initThreads = new Thread[nThreads];

        CountDownLatch ready = new CountDownLatch(nThreads);
        for (int i = 0; i < nThreads; i++)
        {
            initLoops[i] = new SingleCoreEventLoop(this, new MonitorableExecutor(threadFactory, i), i, nThreads);

            //Start the loop which sets the Thread
            initLoops[i].submit(ready::countDown);
        }

        Uninterruptibles.awaitUninterruptibly(ready);
    }

    protected EventLoop newChild(Executor executor, Object... args) throws Exception
    {
        assert args.length >= 2 : args.length;
        maybeInit((int)args[0], (ThreadFactory)args[1]);

        int offset = ((AtomicInteger)args[2]).getAndIncrement();
        if (offset >= initLoops.length)
            throw new RuntimeException("Trying to allocate more children than passed to the group");

        return initLoops[offset];
    }

    private class MonitorableExecutor implements Executor
    {
        final int offset;
        final ThreadFactory threadFactory;

        MonitorableExecutor(ThreadFactory threadFactory, int offset)
        {
            this.threadFactory = threadFactory;
            this.offset = offset;
        }

        public void execute(Runnable command)
        {
            Thread t = threadFactory.newThread(command);
            t.setDaemon(true);
            initThreads[offset] = t;

            t.start();
        }
    }

    private enum CoreState
    {
        PARKED,
        WORKING
    }

    private class SingleCoreEventLoop extends EpollEventLoop implements Runnable
    {
        private final int threadOffset;

        private static final int busyExtraSpins =  1024 * 8;
        private static final int yieldExtraSpins = 1024;
        private static final int parkExtraSpins = 1024; // 1024 is ~50ms

        private volatile int pendingEpollEvents = 0;

        @Contended
        private volatile CoreState state;

        @Contended
        private final MessagePassingQueue<Runnable> externalQueue;

        @Contended
        private final MessagePassingQueue<Runnable>[] incomingQueues;


        private SingleCoreEventLoop(EventLoopGroup parent, Executor executor, int threadOffset, int totalCores)
        {
            super(parent, executor, 0,  DefaultSelectStrategyFactory.INSTANCE.newSelectStrategy(), RejectedExecutionHandlers.reject());

            this.threadOffset = threadOffset;

            this.externalQueue = new MpscArrayQueue<>(1 << 16);

            this.incomingQueues = new MessagePassingQueue[totalCores];
            for (int i = 0; i < incomingQueues.length; i++)
            {
                incomingQueues[i] = new SpscArrayQueue<>(1 << 16);
            }

            this.state = CoreState.WORKING;
        }

        public void run()
        {
            try
            {
                while (true)
                {
                    //deal with spurious wakeups
                    if (state == CoreState.WORKING)
                    {
                        int spins = 0;
                        while (true)
                        {
                            int drained = drain();
                            if (drained > 0 || ++spins < busyExtraSpins)
                            {
                                if (drained > 0)
                                    spins = 0;

                                continue;
                            }
                            else if (spins < busyExtraSpins + yieldExtraSpins)
                            {
                                Thread.yield();
                            }
                            else if (spins < busyExtraSpins + yieldExtraSpins + parkExtraSpins)
                            {
                                LockSupport.parkNanos(1);
                            }
                            else
                                break;
                        }
                    }

                    if (isShuttingDown()) {
                        closeAll();
                        if (confirmShutdown()) {
                            return;
                        }
                    }

                    //Nothing todo; park
                    park();
                }
            }
            finally
            {

            }
        }

        private void park()
        {
            state = CoreState.PARKED;
            LockSupport.park();
        }

        private void unpark()
        {
            state = CoreState.WORKING;
            LockSupport.unpark(runningThreads[threadOffset]);
        }

        private void checkQueues()
        {
            if (state == CoreState.PARKED && !isEmpty())
                unpark();
        }

        public void addTask(Runnable task)
        {
            Thread currentThread = Thread.currentThread();

            MessagePassingQueue<Runnable> queue = null;

            if (runningThreads != null)
            {
                //Run local core tasks directly
                if (currentThread == runningThreads[threadOffset])
                {
                   queue = incomingQueues[threadOffset];
                }
                else
                {
                    for (int i = 0; i < eventLoops.length; i++)
                    {
                        if (currentThread == runningThreads[i])
                        {
                            queue = incomingQueues[eventLoops[i].threadOffset];
                            break;
                        }
                    }
                }

                if (queue == null)
                    queue = externalQueue;

                if (!queue.offer(task))
                    throw new RuntimeException("Backpressure");
            }
            else
            {
                task.run();
            }
        }

        int drain()
        {
            int processed = drainEpoll();
            return drainTasks() + processed;
        }

        int drainEpoll()
        {
            try
            {
                int t;

                if (this.pendingEpollEvents > 0)
                {
                    t = pendingEpollEvents;
                    pendingEpollEvents = 0;
                }
                else
                {
                    t = this.selectStrategy.calculateStrategy(this.selectNowSupplier, hasTasks());
                    switch (t)
                    {
                        case -2:
                            return 0;
                        case -1:
                            t = this.epollWait(WAKEN_UP_UPDATER.getAndSet(this, 0) == 1);
                            if (this.wakenUp == 1)
                            {
                                Native.eventFdWrite(this.eventFd.intValue(), 1L);
                            }
                        default:
                    }
                }

                if (t > 0)
                {
                    this.processReady(this.events, t);
                }

                if (this.allowGrowing && t == this.events.length())
                {
                    this.events.increase();
                }

                return Math.max(t, 0);
            }
            catch (Exception e)
            {
                logger.error("Unexpected exception in the selector loop.", e);

                // Prevent possible consecutive immediate failures that lead to
                // excessive CPU consumption.
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

                return 0;
            }
        }

        @Inline
        int drainTasks()
        {
            fetchFromDelayedQueue();

            int processed = 0;

            for (int i = 0; i < incomingQueues.length; i++)
                processed += incomingQueues[i].drain(Runnable::run);

            processed += externalQueue.drain(Runnable::run);

            return processed;
        }

        @Override
        @Inline
        protected boolean hasTasks()
        {
            for (int i = 0; i < incomingQueues.length; i++)
            {
                if (!incomingQueues[i].isEmpty())
                    return true;
            }

            boolean empty = externalQueue.isEmpty();

            if (empty)
                empty = hasScheduledTasks();

            return !empty;
        }

        @Inline
        boolean isEmpty()
        {
            boolean empty = !hasTasks();

            try
            {
                int t;
                if (empty)
                {
                    t = this.epollWait(WAKEN_UP_UPDATER.getAndSet(this, 0) == 1);

                    if (t > 0)
                        pendingEpollEvents = t;
                    else
                        Native.eventFdWrite(this.eventFd.intValue(), 1L);

                    return t <= 0;
                }
            }
            catch (Exception e)
            {
                logger.error("Error selecting socket ", e);
            }

            return empty;
        }

        @Inline
        void fetchFromDelayedQueue()
        {
            long nanoTime = AbstractScheduledEventExecutor.nanoTime();
            Runnable scheduledTask  = pollScheduledTask(nanoTime);
            while (scheduledTask != null)
            {
                submit(scheduledTask);
                scheduledTask = pollScheduledTask(nanoTime);
            }
        }
    }
}
