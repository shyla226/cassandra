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
import java.util.IdentityHashMap;
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
import org.apache.cassandra.db.ColumnFamilyStore;
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

    final static IdentityHashMap<Thread, Integer> eventLoopThreads = new IdentityHashMap<>();

    public static final int NUM_NETTY_THREADS = Integer.valueOf(System.getProperty("io.netty.eventLoopThreads", String.valueOf(FBUtilities.getAvailableProcessors())));

    //Each array entry maps to a cpuId.
    final static NettyRxScheduler[] perCoreSchedulers = new NettyRxScheduler[NUM_NETTY_THREADS];
    static
    {
        perCoreSchedulers[0] = new NettyRxScheduler(GlobalEventExecutor.INSTANCE.next(), 0);
    }

    private static volatile boolean isStartup = true;


    final EventExecutorGroup eventLoop;
    public final int cpuId;
    public final Thread cpuThread;

    public static NettyRxScheduler instance()
    {
        return localNettyEventLoop.get();
    }

    public static synchronized NettyRxScheduler register(EventExecutor loop, int cpuId)
    {
        NettyRxScheduler scheduler = localNettyEventLoop.get();
        if (scheduler == null || scheduler.eventLoop != loop)
        {
            assert loop.inEventLoop();
            scheduler = new NettyRxScheduler(loop, cpuId);
            localNettyEventLoop.set(scheduler);
            perCoreSchedulers[cpuId] = scheduler;
            eventLoopThreads.put(Thread.currentThread(), cpuId);
            logger.info("Putting {} on core {}", Thread.currentThread().getName(), cpuId);
            isStartup = false;
        }

        return scheduler;
    }

    public static boolean inEventLoop()
    {
        return isStartup || eventLoopThreads.containsKey(Thread.currentThread());
    }

    public static Integer getCoreId()
    {
        if (isStartup)
            return 0;

        return eventLoopThreads.get(Thread.currentThread());
    }

    public static boolean isStartup()
    {
        return isStartup;
    }

    public static Integer getCoreId(Scheduler scheduler)
    {
        if (scheduler instanceof NettyRxScheduler)
            return ((NettyRxScheduler)scheduler).cpuId;

        return null;
    }

    public static int getNumNettyThreads()
    {
        return isStartup ? 1 : NUM_NETTY_THREADS;
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

    public static Scheduler getForCore(int core)
    {
        NettyRxScheduler scheduler = perCoreSchedulers[core];
        assert scheduler != null && scheduler.cpuId == core : scheduler == null ? "NPE" : "" + scheduler.cpuId + " != " + core;

        if (isStartup)
            return ImmediateThinScheduler.INSTANCE;

        return scheduler;
    }

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
