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


import java.util.ArrayDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;

import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.SelectStrategy;
import io.netty.channel.epoll.EpollEventLoop;
import io.netty.channel.epoll.Native;
import io.netty.util.concurrent.AbstractScheduledEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.RejectedExecutionHandlers;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.reactivex.plugins.RxJavaPlugins;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.jctools.queues.MpscArrayQueue;
import sun.misc.Contended;

/**
 * Groups our TPC event loops.
 * <p>
 * This is mainly a container for our per-core event loop threads, implemented by {@link SingleCoreEventLoop}, that are
 * in charge of executing all-the-things[1]. In particular, the event loops execute both our internal Apollo tasks as
 * well as Netty's tasks, which is why it extends Netty's {@link MultithreadEventLoopGroup}.
 * <p>
 * When an event loop has no work to do for some time, it will eventually park itself (see {@link SingleCoreEventLoop}
 * for details) and so the group uses a monitoring thread that continuously check if a parked event loop needs to be
 * un-parked.
 *
 * [1]: not exactly everything yet, but they should  eventually.
 */
public class MonitoredEpollEventLoopGroup extends MultithreadEventLoopGroup
{
    private static final String DEBUG_RUNNING_TIME_NAME = "cassandra.debug_tpc_task_running_time_seconds";
    private static final long DEBUG_RUNNING_TIME_NANOS = TimeUnit.NANOSECONDS.convert(Integer.parseInt(System.getProperty(DEBUG_RUNNING_TIME_NAME, "0")),
                                                                                      TimeUnit.SECONDS);

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MonitoredEpollEventLoopGroup.class);

    @Contended
    private final SingleCoreEventLoop[] eventLoops;

    private final Thread monitorThread;

    private volatile boolean shutdown;

    /**
     * Creates new a {@code MonitoredEpollEventLoopGroup} using the provided number of event loops.
     *
     * @param nThreads the number of event loops to use (not that every loop is exactly one thread, but the group also
     *                 use an additional thread for monitoring).
     */
    public MonitoredEpollEventLoopGroup(int nThreads)
    {
        this(nThreads, new TPCScheduler.NettyRxThreadFactory(MonitoredEpollEventLoopGroup.class, Thread.MAX_PRIORITY));
    }

    /**
     * This constructor is called by jmh benchmarks, when using a custom executor.
     * It should only be called for testing, not production code, since it wires the NettyRxScheduler
     * as well, but unlike {@link org.apache.cassandra.service.CassandraDaemon#initializeTPC()},
     * it will not set any CPU affinity or epoll IO ratio.
     *
     * @param nThreads - the number of threads
     * @param name - the thread name (unused)
     */
    @VisibleForTesting
    public MonitoredEpollEventLoopGroup(int nThreads, String name)
    {
        this(nThreads, new TPCScheduler.NettyRxThreadFactory(MonitoredEpollEventLoopGroup.class, Thread.MAX_PRIORITY));

        CountDownLatch ready = new CountDownLatch(eventLoops.length);

        for (int i = 0; i < eventLoops.length; i++)
        {
            final int cpuId = i;
            EventLoop loop = eventLoops[i];
            loop.schedule(() -> {
                try
                {
                    TPCScheduler.register(loop, cpuId);
                }
                catch (Exception ex)
                {
                    logger.error("Failed to initialize monitored epoll event loop group due to exception", ex);
                }
                finally
                {
                    ready.countDown();
                }
            }, 0, TimeUnit.SECONDS);
        }

        Uninterruptibles.awaitUninterruptibly(ready);
    }

    private MonitoredEpollEventLoopGroup(int nThreads, ThreadFactory threadFactory)
    {
        // The 1st threadFactory is for the super class ctor, the 2nd one is so it gets passed to newChild()
        super(nThreads, threadFactory, threadFactory);

        this.eventLoops = extractEventLoopArray();

        monitorThread = new Thread(() -> {
            int length = eventLoops.length;

            while (!shutdown)
            {
                long nanoTime = SingleCoreEventLoop.nanoTime();
                for (int i = 0; i < length; i++)
                    eventLoops[i].checkQueues(nanoTime);

                LockSupport.parkNanos(1);
                ApproximateTime.tick();
            }

            for (int i = 0; i < length; i++)
                eventLoops[i].unpark();
        });

        monitorThread.setName("CoreThreadMonitor");
        monitorThread.setPriority(Thread.MAX_PRIORITY);
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    private SingleCoreEventLoop[] extractEventLoopArray()
    {
        SingleCoreEventLoop[] loops = new SingleCoreEventLoop[executorCount()];
        int i = 0;
        for (EventExecutor eventExecutor : this)
            loops[i++] = (SingleCoreEventLoop)eventExecutor;
        return loops;
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        shutdown = true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        long start = System.nanoTime();
        if (!super.awaitTermination(timeout, unit))
            return false;

        long elapsedNanos = System.nanoTime() - start;
        long timeLeftNanos = TimeUnit.NANOSECONDS.convert(timeout, unit) - elapsedNanos;

        monitorThread.join(timeLeftNanos / 1_000_000L, (int)(timeLeftNanos % 1_000_000L));
        return !monitorThread.isAlive();
    }

    protected EventLoop newChild(Executor executor, Object... args) throws Exception
    {
        assert args.length >= 1 : args.length;
        return new SingleCoreEventLoop(this, new MonitorableExecutor((ThreadFactory) args[0]));
    }

    private static class MonitorableExecutor implements Executor
    {
        private final ThreadFactory threadFactory;
        Thread thread;

        private MonitorableExecutor(ThreadFactory threadFactory)
        {
            this.threadFactory = threadFactory;
        }

        public void execute(Runnable command)
        {
            thread = threadFactory.newThread(command);
            thread.setDaemon(true);
            thread.start();
        }
    }

    private static class SingleCoreEventLoop extends EpollEventLoop
    {
        private enum CoreState
        {
            PARKED,
            WORKING
        }

        private final MonitoredEpollEventLoopGroup parent;
        private final Thread thread;

        private static final int busyExtraSpins =  1024 * 128;
        private static final int yieldExtraSpins = 1024 * 8;
        private static final int parkExtraSpins = 1024; // 1024 is ~50ms

        private volatile int pendingEpollEvents = 0;
        private volatile long delayedNanosDeadline = -1;

        @Contended
        private volatile CoreState state;

        @Contended
        private final MpscArrayQueue<Runnable> externalQueue;

        @Contended
        private final ArrayDeque<Runnable> internalQueue;

        private volatile long lastDrainTime;

        private SingleCoreEventLoop(MonitoredEpollEventLoopGroup parent, MonitorableExecutor executor)
        {
            super(parent, executor, 0,  DefaultSelectStrategyFactory.INSTANCE.newSelectStrategy(), RejectedExecutionHandlers.reject());

            this.parent = parent;
            this.externalQueue = new MpscArrayQueue<>(1 << 16);
            this.internalQueue = new ArrayDeque<>(1 << 16);

            this.state = CoreState.WORKING;
            this.lastDrainTime = -1;

            // Start the loop which sets the Thread
            Futures.getUnchecked(submit(() -> {}));

            this.thread = executor.thread;
            assert this.thread != null;
        }

        /**
         * The actual event loop.
         * <p>
         * Each loop consists in checking for both events on epoll and tasks on our internal/external queues.
         * Any available work is executed and we then loop.
         */
        @Override
        protected void run()
        {
            while (!parent.shutdown)
            {
                //deal with spurious wake-ups
                if (state == CoreState.WORKING)
                {
                    int spins = 0;
                    while (!parent.shutdown)
                    {
                        int drained = drain();
                        if (drained > 0)
                        {
                            spins = 0;
                        }
                        else
                        {
                            ++spins;
                            if (spins < busyExtraSpins)
                                continue;
                            else if (spins < busyExtraSpins + yieldExtraSpins)
                                Thread.yield();
                            else if (spins < busyExtraSpins + yieldExtraSpins + parkExtraSpins)
                                LockSupport.parkNanos(1);
                            else
                                break;
                        }
                    }
                }

                if (isShuttingDown())
                {
                    closeAll();
                    if (confirmShutdown())
                        return;
                }

                //Nothing todo; park
                park();
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
            LockSupport.unpark(thread);
        }

        private void checkLongRunningTasks(long nanoTime)
        {
            if (lastDrainTime > 0 && Math.abs(nanoTime - lastDrainTime) > DEBUG_RUNNING_TIME_NANOS)
            {
                logger.debug("Detected task running for {} seconds for thread with stack:\n{}",
                             TimeUnit.SECONDS.convert(Math.abs(nanoTime - lastDrainTime), TimeUnit.NANOSECONDS),
                             FBUtilities.Debug.getStackTrace(thread));

                lastDrainTime = -1;
            }
        }

        private void checkQueues(long nanoTime)
        {
            delayedNanosDeadline = nanoTime;

            if (DEBUG_RUNNING_TIME_NANOS > 0 && state == CoreState.WORKING)
                checkLongRunningTasks(nanoTime);

            if (state == CoreState.PARKED && !isEmpty())
                unpark();
        }

        @Override
        protected void addTask(Runnable task)
        {
            Thread currentThread = Thread.currentThread();

            // Side-note: 'thread' will be null the very first time this is called for the empty task submitted in the
            // ctor to kick-start the loop. This is fine, but that means 'thread' shouldn't be de-referenced.
            if (currentThread == thread)
            {
                if (!internalQueue.offer(task))
                    reject(task);
            }
            else
            {
                if (!externalQueue.relaxedOffer(task))
                    reject(task);
            }
        }

        private int drain()
        {
            if (DEBUG_RUNNING_TIME_NANOS > 0)
                lastDrainTime = nanoTime();

            int processed = drainEpoll();
            return drainTasks() + processed;
        }

        private int drainEpoll()
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
                        case SelectStrategy.CONTINUE:
                            return 0;
                        case SelectStrategy.SELECT:
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
        private int drainTasks()
        {
            int processed = 0;

            try
            {
                if (delayedNanosDeadline > 0)
                {
                    fetchFromDelayedQueue(delayedNanosDeadline);
                    delayedNanosDeadline = -1;
                }

                Runnable r;
                while ((r = internalQueue.poll()) != null && processed < Short.MAX_VALUE)
                {
                    r.run();
                    ++processed;
                }

                while ((r = externalQueue.relaxedPoll()) != null && processed < Short.MAX_VALUE * 2)
                {
                    r.run();
                    ++processed;
                }
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);

                logger.error("Task exception encountered: ", t);
                try
                {
                    RxJavaPlugins.getErrorHandler().accept(t);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }

            return processed;
        }

        @Override
        @Inline
        protected boolean hasTasks()
        {
            boolean hasTasks = internalQueue.peek() != null || externalQueue.relaxedPeek() != null;

            if (!hasTasks && delayedNanosDeadline > 0)
                hasTasks = hasScheduledTasks(delayedNanosDeadline);

            return hasTasks;
        }

        @Inline
        boolean isEmpty()
        {
            if (hasTasks())
                return false;

            try
            {
                int t = this.epollWait(WAKEN_UP_UPDATER.getAndSet(this, 0) == 1);

                if (t > 0)
                    pendingEpollEvents = t;
                else
                    Native.eventFdWrite(this.eventFd.intValue(), 1L);

                return t <= 0;
            }
            catch (Exception e)
            {
                logger.error("Error selecting socket ", e);
                return true;
            }
        }

        protected static long nanoTime()
        {
            return AbstractScheduledEventExecutor.nanoTime();
        }

        @Inline
        private void fetchFromDelayedQueue(long nanoTime)
        {
            Runnable scheduledTask = pollScheduledTask(nanoTime);
            while (scheduledTask != null)
            {
                submit(scheduledTask);
                scheduledTask = pollScheduledTask(nanoTime);
            }
        }

    }
}
