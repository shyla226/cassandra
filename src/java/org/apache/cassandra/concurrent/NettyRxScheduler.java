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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.internal.disposables.EmptyDisposable;
import io.reactivex.internal.schedulers.ImmediateThinScheduler;
import io.reactivex.internal.schedulers.ScheduledRunnable;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RxScheduler which wraps the Netty event loop scheduler.
 *
 * This is initialized on startup:
 * @see NativeTransportService#initializeTPC()
 *
 * Each netty loop run managed on a single thread which may be pinned to a particular
 * CPU.  Cassandra can route tasks relative to a particular partition to a single loop
 * thereby avoiding any multi-threaded access, removing the need to concurrent datastructures
 * and locks.
 *
 */
public class NettyRxScheduler extends Scheduler
{
    private static final Logger logger = LoggerFactory.getLogger(NettyRxScheduler.class);

    public final static FastThreadLocal<NettyRxScheduler> localNettyEventLoop = new FastThreadLocal<NettyRxScheduler>()
    {
        protected NettyRxScheduler initialValue()
        {
            return new NettyRxScheduler(GlobalEventExecutor.INSTANCE, Integer.MAX_VALUE);
        }
    };

    final static Map<String, List<PartitionPosition>> keyspaceToRangeMapping = new HashMap<>();

    public static final int NUM_NETTY_THREADS = Integer.valueOf(System.getProperty("io.netty.eventLoopThreads", String.valueOf(FBUtilities.getAvailableProcessors())));

    //Each array entry maps to a cpuId.
    final static NettyRxScheduler[] perCoreSchedulers = new NettyRxScheduler[NUM_NETTY_THREADS];
    static
    {
        perCoreSchedulers[0] = new NettyRxScheduler(GlobalEventExecutor.INSTANCE, 0);
    }

    /**
     * This flag is set to false once core zero is assigned to its final scheduler, whilst
     * this is true core zero is assigned to the netty global event executor and all other
     * cores are unassigned yet. When this flag becomes false, it doesn't mean all cores
     * are assigned, but only that the first one is assigned and final.
     */
    private static volatile boolean isStartup = true;

    /** The event loop for executing tasks on the thread assigned to a core. Note that all threads have a thread
     * local NettyRxScheduler, but only threads assigned to a core have a dedicated event loop, the other ones
     * share the Netty global executor and should really not rely on the event loop, so this must stay private.
     */
    private final EventExecutorGroup eventLoop;

    /** The cpu id assigned to this scheduler, or Integer.MAX_VALUE for non-assigned threads */
    public final int cpuId;

    /** The thread of which we are the scheduler */
    public final Thread cpuThread;

    /** Return a thread local instance of this class */
    public static NettyRxScheduler instance()
    {
        return localNettyEventLoop.get();
    }

    public static synchronized NettyRxScheduler register(EventExecutor loop, int cpuId)
    {
        NettyRxScheduler scheduler = localNettyEventLoop.get();
        if (scheduler.eventLoop != loop)
        {
            assert loop.inEventLoop();

            if (cpuId == 0)
                awaitInactivity(scheduler);

            scheduler = new NettyRxScheduler(loop, cpuId);
            localNettyEventLoop.set(scheduler);
            perCoreSchedulers[cpuId] = scheduler;
            logger.info("Putting {} on core {}", Thread.currentThread().getName(), cpuId);
            isStartup = false;
        }

        return scheduler;
    }

    /**
     * Wait up to a second for inactivity on the global event loop. This ensures that we can
     * switch from the startup thread that is assigned to core zero, the Netty global executor, to
     * the new executor without causing races, for example in
     * {{{@link org.apache.cassandra.utils.concurrent.OpOrder}}} or other structures that have been
     * partitioned by core.
     *
     * @param scheduler - a scheduler whose eventLoop instance must be GlobalEventExecutor.INSTANCE
     */
    private static void awaitInactivity(NettyRxScheduler scheduler)
    {
        assert scheduler.eventLoop == GlobalEventExecutor.INSTANCE;
        try
        {
            if (!GlobalEventExecutor.INSTANCE.awaitInactivity(1, TimeUnit.SECONDS))
                logger.warn("Failed to wait for inactivity when switching away from global event executor");
        }
        catch (InterruptedException ex)
        {
            logger.warn("Got interrupted exception when switching away from global event executor", ex);
        }
    }

    private static Integer getCoreId()
    {
        if (isStartup)
            return 0;

        NettyRxScheduler instance = instance();
        return isValidCoreId(instance.cpuId) ? instance.cpuId : null;
    }

    public static boolean isTPCThread()
    {
        return isValidCoreId(instance().cpuId);
    }

    public static Integer getCoreId(Scheduler scheduler)
    {
        if (scheduler instanceof NettyRxScheduler)
            return ((NettyRxScheduler)scheduler).cpuId;

        return null;
    }

    private NettyRxScheduler(EventExecutorGroup eventLoop, int cpuId)
    {
        assert eventLoop != null;
        assert cpuId >= 0;
        this.eventLoop = eventLoop;
        this.cpuId = cpuId;
        this.cpuThread = Thread.currentThread();
    }

    public static int getNumCores()
    {
        return perCoreSchedulers.length;
    }

    /**
     * Return a scheduler for a specific core. If the scheduler for that core is null,
     * then that means that the core is unassigned yet, in this case, execute on core zero,
     * which uses the global netty executor. Note that isStartup may already be false, since
     * that only means that core zero was assigned.
     *
     * @param core - the core number for which we want a scheduler of
     *
     * @return - the scheduler of the core specified, or the scheduler of core zero if not yet assigned
     */
    public static Scheduler getForCore(int core)
    {
        NettyRxScheduler scheduler = perCoreSchedulers[core];
        return scheduler == null ? perCoreSchedulers[0] : scheduler;
    }

    /**
     * Return a scheduler for a specific core, if assigned, otherwise null.
     *
     * @param core - the core number for which we want a scheduler of
     * @return - the scheduler of the core specified, or null if not yet assigned
     */
    public static NettyRxScheduler maybeGetForCore(int core)
    {
        return perCoreSchedulers[core];
    }

    public static boolean isValidCoreId(Integer coreId)
    {
        return coreId != null && coreId >= 0 && coreId <= getNumCores();
    }

    public static Scheduler getForKey(String keyspaceName, DecoratedKey key, boolean useImmediateForLocal)
    {
        // force all system table operations to go through a single core
        if (isStartup)
            return ImmediateThinScheduler.INSTANCE;

        // Convert OP partitions to top level partitioner
        // Needed for 2i and System tables
        if (key.getPartitioner() != DatabaseDescriptor.getPartitioner())
        {
            key = DatabaseDescriptor.getPartitioner().decorateKey(key.getKey());
        }

        List<PartitionPosition> keyspaceRanges = getRangeList(keyspaceName);

        PartitionPosition rangeStart = keyspaceRanges.get(0);
        for (int i = 1; i < keyspaceRanges.size(); i++)
        {
            PartitionPosition next = keyspaceRanges.get(i);
            if (key.compareTo(rangeStart) >= 0 && key.compareTo(next) < 0)
            {
                if (useImmediateForLocal)
                {
                    Integer callerCoreId = getCoreId();
                    return callerCoreId != null && callerCoreId == i - 1 ? ImmediateThinScheduler.INSTANCE : getForCore(i - 1);
                }

                return getForCore(i - 1);
            }

            rangeStart = next;
        }

        throw new IllegalStateException(String.format("Unable to map %s to cpu for %s", key, keyspaceName));
    }

    public static List<PartitionPosition> getRangeList(String keyspaceName)
    {
        return getRangeList(keyspaceName, true);
    }

    public static List<PartitionPosition> getRangeList(String keyspaceName, boolean persist)
    {
        List<PartitionPosition> ranges = keyspaceToRangeMapping.get(keyspaceName);

        if (ranges != null)
            return ranges;

        List<Range<Token>> localRanges = StorageService.getStartupTokenRanges(keyspaceName);
        List<PartitionPosition> splits = StorageService.getCpuBoundries(localRanges, DatabaseDescriptor.getPartitioner(), NUM_NETTY_THREADS);

        if (persist)
        {
            if (instance().cpuId == 0)
            {
                keyspaceToRangeMapping.put(keyspaceName, splits);
            }
            else
            {
                CountDownLatch ready = new CountDownLatch(1);

                getForCore(0).scheduleDirect(() -> {
                    keyspaceToRangeMapping.put(keyspaceName, splits);
                    ready.countDown();
                });

                Uninterruptibles.awaitUninterruptibly(ready);
            }
        }

        return splits;
    }


    @Override
    public Scheduler.Worker createWorker()
    {
        return new Worker(eventLoop);
    }

    private static class Worker extends Scheduler.Worker
    {
        private final EventExecutorGroup nettyEventLoop;

        private final CompositeDisposable tasks;

        volatile boolean disposed;

        Worker(EventExecutorGroup nettyEventLoop)
        {
            this.nettyEventLoop = nettyEventLoop;
            this.tasks = new CompositeDisposable();
        }

        @Override
        public void dispose()
        {
            if (!disposed)
            {
                disposed = true;
                tasks.dispose();
            }
        }

        @Override
        public boolean isDisposed()
        {
            return disposed;
        }

        @Override
        public Disposable schedule(Runnable action)
        {
            if (disposed)
            {
                return EmptyDisposable.INSTANCE;
            }

            return scheduleActual(action, 0, null, tasks);
        }

        @Override
        public Disposable schedule(Runnable action, long delayTime, TimeUnit unit)
        {
            if (disposed)
            {
                return EmptyDisposable.INSTANCE;
            }

            return scheduleActual(action, delayTime, unit, tasks);
        }

        public ScheduledRunnable scheduleActual(final Runnable run, long delayTime, TimeUnit unit, CompositeDisposable parent)
        {
            Runnable decoratedRun = RxJavaPlugins.onSchedule(run);

            ScheduledRunnable sr = new ScheduledRunnable(decoratedRun, parent);

            if (parent != null)
            {
                if (!parent.add(sr))
                {
                    return sr;
                }
            }

            Future<?> f;
            try
            {
                if (delayTime <= 0)
                {
                    f = nettyEventLoop.submit((Callable)sr);
                }
                else
                {
                    f = nettyEventLoop.schedule((Callable)sr, delayTime, unit);
                }
                sr.setFuture(f);
            }
            catch (RejectedExecutionException ex)
            {
                RxJavaPlugins.onError(ex);
            }

            return sr;
        }
    }

    public static void initRx()
    {
        final Scheduler ioScheduler = Schedulers.from(Executors.newFixedThreadPool(DatabaseDescriptor.getConcurrentWriters()));
        RxJavaPlugins.setComputationSchedulerHandler((s) -> NettyRxScheduler.instance());
        RxJavaPlugins.initIoScheduler(() -> ioScheduler);
        RxJavaPlugins.setErrorHandler(t -> logger.error("RxJava unexpected Exception ", t));
    }

}
