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


import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.SelectStrategy;
import io.netty.channel.epoll.EpollEventLoop;
import io.netty.channel.epoll.Native;
import io.netty.util.concurrent.AbstractScheduledEventExecutor;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.reactivex.plugins.RxJavaPlugins;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.jctools.queues.MpscArrayQueue;
import sun.misc.Contended;

/**
 * A TPC event loop group that uses EPoll for I/O tasks.
 */
public class EpollTPCEventLoopGroup extends MultithreadEventLoopGroup implements TPCEventLoopGroup
{
    private static final String DEBUG_RUNNING_TIME_NAME = "cassandra.debug_tpc_task_running_time_seconds";
    private static final long DEBUG_RUNNING_TIME_NANOS = TimeUnit.NANOSECONDS.convert(Integer.parseInt(System.getProperty(DEBUG_RUNNING_TIME_NAME, "0")),
                                                                                      TimeUnit.SECONDS);

    private static final Logger logger = LoggerFactory.getLogger(EpollTPCEventLoopGroup.class);

    @Contended
    private final ImmutableList<SingleCoreEventLoop> eventLoops;

    private final Thread monitorThread;

    private volatile boolean shutdown;

    /**
     * Constructor used by jmh through reflection (so it must have this exact signature, even though the name is
     * ignored) in some benchmarks (grep for "-Djmh.executor.class=org.apache.cassandra.concurrent.EpollTPCEventLoopGroup"
     * to see where it's used).
     */
    @VisibleForTesting
    public EpollTPCEventLoopGroup(int nThreads, String name)
    {
        this(nThreads);
    }

    /**
     * Creates new a {@code EpollTPCEventLoopGroup} using the provided number of event loops.
     *
     * @param nThreads the number of event loops to use (not that every loop is exactly one thread, but the group also
     *                 use an additional thread for monitoring).
     */
    public EpollTPCEventLoopGroup(int nThreads)
    {
        super(nThreads, TPCThread.newTPCThreadFactory());

        this.eventLoops = ImmutableList.copyOf(Iterables.transform(this, e -> (SingleCoreEventLoop) e));

        monitorThread = new Thread(() -> {
            while (!shutdown)
            {
                long nanoTime = SingleCoreEventLoop.nanoTime();
                for (SingleCoreEventLoop loop : eventLoops)
                    loop.checkQueues(nanoTime);

                LockSupport.parkNanos(1);
                ApproximateTime.tick();
            }

            for (SingleCoreEventLoop loop : eventLoops)
                loop.unpark();
        });

        monitorThread.setName("CoreThreadMonitor");
        monitorThread.setPriority(Thread.MAX_PRIORITY);
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    public ImmutableList<? extends TPCEventLoop> eventLoops()
    {
        return eventLoops;
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
        assert executor instanceof TPCThread.TPCThreadsCreator;
        return new SingleCoreEventLoop(this, (TPCThread.TPCThreadsCreator)executor);
    }

    private static class SingleCoreEventLoop extends EpollEventLoop implements TPCEventLoop
    {
        private enum CoreState
        {
            PARKED,
            WORKING
        }

        private final EpollTPCEventLoopGroup parent;
        private final TPCThread thread;
        private final TPCMetrics metrics;

        private static final int busyExtraSpins =  1024 * 128;
        private static final int yieldExtraSpins = 1024 * 8;
        private static final int parkExtraSpins = 1024; // 1024 is ~50ms

        private volatile int pendingEpollEvents = 0;
        private volatile long delayedNanosDeadline = -1;

        @Contended
        private volatile CoreState state;

        private final MpscArrayQueue<Runnable> queue;
        private final MpscArrayQueue<TPCRunnable> pendingQueue;


        private volatile long lastDrainTime;

        private SingleCoreEventLoop(EpollTPCEventLoopGroup parent, TPCThread.TPCThreadsCreator executor)
        {
            super(parent, executor, 0,  DefaultSelectStrategyFactory.INSTANCE.newSelectStrategy(), RejectedExecutionHandlers.reject(), TPC.USE_AIO);

            this.parent = parent;
            this.queue = new MpscArrayQueue<>(1 << 16);

            this.state = CoreState.WORKING;
            this.lastDrainTime = -1;

            // Start the loop, which forces the creation of the Thread using 'executor' so we can get a reference to it
            // easily.
            Futures.getUnchecked(submit(() -> {}));

            this.thread = executor.lastCreatedThread();
            assert this.thread != null;
            TPCMetrics metrics = this.thread.metrics();

            this.pendingQueue = new MpscArrayQueue<>(metrics.maxPendingQueueSize());
            this.metrics = metrics;
        }

        public TPCThread thread()
        {
            return thread;
        }

        @Override
        public TPCEventLoopGroup parent()
        {
            return parent;
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
            if (task instanceof TPCRunnable)
            {
                TPCRunnable tpc = (TPCRunnable) task;
                if (tpc.isPendable())
                {
                    // If we already have something in the pending queue, this task should not jump it.
                    if (pendingQueue.isEmpty() && queue.offerIfBelowThreshold(task, metrics.maxQueueSize()))
                        return;

                    if (pendingQueue.relaxedOffer(tpc))
                    {
                        tpc.setPending();
                        return;
                    }
                    else
                    {
                        tpc.blocked();
                        reject(task);
                    }
                }
            }

            if (!queue.relaxedOffer(task))
                reject(task);
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
                                Native.eventFdWrite(this.eventFd.intValue(), 1L);
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
            movePending();

            try
            {
                if (delayedNanosDeadline > 0)
                {
                    fetchFromDelayedQueue(delayedNanosDeadline);
                    delayedNanosDeadline = -1;
                }

                Runnable r;

                while (processed < Short.MAX_VALUE && (r = queue.relaxedPoll()) != null)
                {
                    r.run();
                    ++processed;
                    movePending();
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

        private void movePending()
        {
            if (metrics != null && queue.size() < metrics.maxQueueSize())
            {
                TPCRunnable tpc = pendingQueue.relaxedPoll();
                if (tpc != null)
                {
                    queue.relaxedOffer(tpc);
                    tpc.unsetPending();
                }
            }
        }

        @Override
        @Inline
        protected boolean hasTasks()
        {
            boolean hasTasks = queue.relaxedPeek() != null;

            if (!hasTasks && delayedNanosDeadline > 0)
                hasTasks = hasScheduledTasks(delayedNanosDeadline);

            return hasTasks;
        }

        @Inline
        boolean isEmpty()
        {
            if (hasTasks())
                return false;
            if (pendingQueue != null && !pendingQueue.isEmpty())
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
