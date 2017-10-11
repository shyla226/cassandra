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


import java.util.ArrayList;
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
import io.netty.channel.epoll.EpollEventLoop;
import io.netty.util.concurrent.AbstractScheduledEventExecutor;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.reactivex.plugins.RxJavaPlugins;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.jctools.queues.MpscArrayQueue;
import sun.misc.Contended;

/**
 * A TPC event loop group that uses EPoll for I/O tasks.
 */
public class EpollTPCEventLoopGroup extends MultithreadEventLoopGroup implements TPCEventLoopGroup
{
    private static final String DEBUG_RUNNING_TIME_NAME = "dse.tpc.debug_task_running_time_seconds";
    private static final long DEBUG_RUNNING_TIME_NANOS = TimeUnit.NANOSECONDS.convert(Integer.parseInt(System.getProperty(DEBUG_RUNNING_TIME_NAME, "0")),
                                                                                      TimeUnit.SECONDS);
    private static final Logger logger = LoggerFactory.getLogger(EpollTPCEventLoopGroup.class);

    private static final long EPOLL_CHECK_INTERVAL_NANOS = Long.parseLong(System.getProperty("netty.epoll_check_interval_nanos", "10000"));
    private static final int BUSY_EXTRA_SPINS = Integer.parseInt(System.getProperty("netty.eventloop.busy_extra_spins", "32"));
    private static final int YIELD_EXTRA_SPINS = Integer.parseInt(System.getProperty("netty.eventloop.yield_extra_spins", "32"));
    private static final int PARK_EXTRA_SPINS = Integer.parseInt(System.getProperty("netty.eventloop.park_extra_spins", "32")); // Hint: 1024 is ~50ms

    @Contended
    private final ImmutableList<SingleCoreEventLoop> eventLoops;

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

        //Register these loop threads with the Watcher
        WatcherThread.instance.get().addThreadsToMonitor(new ArrayList<>(eventLoops));
    }

    public ImmutableList<? extends TPCEventLoop> eventLoops()
    {
        return eventLoops;
    }

    @Override
    public void shutdown()
    {
        super.shutdown();

        WatcherThread.instance.get().removeThreadsToMonitor(new ArrayList<>(eventLoops));
        shutdown = true;
    }

    protected EventLoop newChild(Executor executor, Object... args) throws Exception
    {
        assert executor instanceof TPCThread.TPCThreadsCreator;
        return new SingleCoreEventLoop(this, (TPCThread.TPCThreadsCreator)executor);
    }

    public static class SingleCoreEventLoop extends EpollEventLoop implements TPCEventLoop, WatcherThread.MonitorableThread
    {
        private final EpollTPCEventLoopGroup parent;
        private final TPCThread thread;
        private final TPCMetrics metrics;
        private final TPCMetricsAndLimits.TaskStats busySpinStats;
        private final TPCMetricsAndLimits.TaskStats yieldStats;
        private final TPCMetricsAndLimits.TaskStats parkStats;
        private final int maxQueueSize;

        private volatile int pendingEpollEvents = 0;
        private volatile long delayedNanosDeadline = -1;
        private long lastEpollCheckTime = -1;

        @Contended
        private volatile ThreadState state;

        private final MpscArrayQueue<Runnable> queue;
        private final MpscArrayQueue<TPCRunnable> pendingQueue;


        private volatile long lastDrainTime;

        private SingleCoreEventLoop(EpollTPCEventLoopGroup parent, TPCThread.TPCThreadsCreator executor)
        {
            super(parent, executor, 0,  DefaultSelectStrategyFactory.INSTANCE.newSelectStrategy(), RejectedExecutionHandlers.reject(), TPC.USE_AIO);

            this.parent = parent;
            this.queue = new MpscArrayQueue<>(1 << 16);

            this.state = ThreadState.WORKING;
            this.lastDrainTime = -1;

            // Start the loop, which forces the creation of the Thread using 'executor' so we can get a reference to it
            // easily.
            Futures.getUnchecked(submit(() -> {}));

            this.thread = executor.lastCreatedThread();
            assert this.thread != null;
            TPCMetricsAndLimits metrics = (TPCMetricsAndLimits) this.thread.metrics();

            this.pendingQueue = new MpscArrayQueue<>(metrics.maxPendingQueueSize());
            this.metrics = metrics;
            this.busySpinStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_SPIN);
            this.yieldStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_YIELD);
            this.parkStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_PARK);
            this.maxQueueSize = metrics.maxQueueSize();
        }

        public TPCThread thread()
        {
            return thread;
        }

        public ThreadState getThreadState()
        {
            return state;
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
                if (state == ThreadState.WORKING)
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
                            if (spins < BUSY_EXTRA_SPINS)
                            {
                                if (busySpinStats != null)
                                {
                                    busySpinStats.scheduledTasks.add(1);
                                    busySpinStats.completedTasks.add(1);
                                }
                            }
                            else if (spins < BUSY_EXTRA_SPINS + YIELD_EXTRA_SPINS)
                            {
                                if (yieldStats != null)
                                {
                                    yieldStats.scheduledTasks.add(1);
                                    yieldStats.completedTasks.add(1);
                                }
                                Thread.yield();
                            }
                            else if (spins < BUSY_EXTRA_SPINS + YIELD_EXTRA_SPINS + PARK_EXTRA_SPINS)
                            {
                                if (parkStats != null)
                                {
                                    parkStats.scheduledTasks.add(1);
                                    parkStats.completedTasks.add(1);
                                }
                                LockSupport.parkNanos(1);
                            }
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

        public void park()
        {
            state = ThreadState.PARKED;
            LockSupport.park();
        }

        public void unpark()
        {
            state = ThreadState.WORKING;
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

        public boolean shouldUnpark(long nanoTimeSinceStartup)
        {
            delayedNanosDeadline = nanoTimeSinceStartup;

            if (DEBUG_RUNNING_TIME_NANOS > 0 && state == ThreadState.WORKING)
                checkLongRunningTasks(nanoTimeSinceStartup);

            checkEpoll(nanoTimeSinceStartup);

            return state == ThreadState.PARKED && !isEmpty();
        }

        @Override
        public boolean canExecuteImmediately(TPCRunnable task)
        {
            if (coreId() != TPC.getCoreId())
                return false;
            if (!task.isPendable())
                return true;
            if (!pendingQueue.isEmpty())
                return false;
            if (queue.size() >= metrics.maxQueueSize())
                return false;
            return true;
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
                    if (pendingQueue.isEmpty() && queue.offerIfBelowThreshold(task, maxQueueSize))
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
                int t = 0;

                if (this.pendingEpollEvents > 0)
                    t = pendingEpollEvents;

                if (t > 0)
                {
                    this.processReady(this.events, t);
                    pendingEpollEvents = 0;
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
                while (processed < Short.MAX_VALUE && (r = queue.relaxedPoll()) != null)
                {
                    r.run();
                    ++processed;
                }

                movePending();
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
            if (metrics != null)
            {
                TPCRunnable tpc;
                while (queue.size() < maxQueueSize && (tpc = pendingQueue.relaxedPeek()) != null)
                {
                    if (queue.relaxedOffer(tpc))
                    {
                        pendingQueue.relaxedPoll();
                        tpc.unsetPending();
                    }
                    else
                    {
                        break;
                    }
                }
            }
        }

        @Override
        @Inline
        protected boolean hasTasks()
        {
            boolean hasTasks = queue.relaxedPeek() != null ||
                               (pendingQueue != null && pendingQueue.relaxedPeek() != null);

            if (!hasTasks && delayedNanosDeadline > 0)
                hasTasks = hasScheduledTasks(delayedNanosDeadline);

            return hasTasks;
        }

        void checkEpoll(long nanoTime)
        {
            if (pendingEpollEvents > 0)
                return;

            if (nanoTime - lastEpollCheckTime < EPOLL_CHECK_INTERVAL_NANOS)
                return;

            lastEpollCheckTime = nanoTime;
            
            try
            {
                int t = selectNowSupplier.get();
                assert t >= 0;

                if (t > 0)
                    pendingEpollEvents = t;
            }
            catch (Exception e)
            {
                logger.error("Error selecting socket ", e);
            }
        }

        @Inline
        boolean isEmpty()
        {
            return pendingEpollEvents == 0 && !hasTasks();
        }

        public static long nanoTime()
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


        /**
         * We want to make sure netty doesn't wake up epoll when we
         * add a task.
         * @param task
         * @return
         */
        @Override
        public boolean wakesUpForTask(Runnable task)
        {
            return false;
        }
    }
}
