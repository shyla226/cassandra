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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;
import io.reactivex.internal.disposables.DisposableContainer;
import io.reactivex.internal.disposables.EmptyDisposable;
import io.reactivex.internal.disposables.ListCompositeDisposable;
import io.reactivex.internal.schedulers.ImmediateThinScheduler;
import io.reactivex.internal.schedulers.ScheduledRunnable;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
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

    final static Map<String, List<Long>> keyspaceToRangeMapping = new HashMap<>();

    public static final int NUM_NETTY_THREADS = Integer.valueOf(System.getProperty("io.netty.eventLoopThreads", String.valueOf(FBUtilities.getAvailableProcessors())));

    // monotonically increased in order to distribute in a round robin fashion the next core for scheduling a task
    private final static AtomicInteger roundRobinIndex = new AtomicInteger(0);

    //Each array entry maps to a cpuId.
    final static NettyRxScheduler[] perCoreSchedulers = new NettyRxScheduler[NUM_NETTY_THREADS];
    static
    {
        perCoreSchedulers[0] = new NettyRxScheduler(new DefaultEventExecutor(new NettyRxThreadFactory("startup-scheduler", Thread.MAX_PRIORITY)), 0);
    }

    private final static class NettyRxThread extends FastThreadLocalThread
    {
        private final int cpuId;

        public NettyRxThread(ThreadGroup group, Runnable target, String name, int cpuId)
        {
            super(group, target, name);
            this.cpuId = cpuId;
        }

        public int getCpuId()
        {
            return cpuId;
        }
    }

    public final static class NettyRxThreadFactory extends DefaultThreadFactory
    {
        AtomicInteger cpuId = new AtomicInteger(0);

        public NettyRxThreadFactory(Class<?> poolType, int priority)
        {
            super(poolType, priority);
        }

        public NettyRxThreadFactory(String poolName, int priority)
        {
            super(poolName, priority);
        }

        protected Thread newThread(Runnable r, String name)
        {
            return new NettyRxThread(this.threadGroup, r, name, cpuId.getAndIncrement());
        }
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

    public static synchronized NettyRxScheduler register(EventExecutor loop)
    {
        NettyRxScheduler scheduler = localNettyEventLoop.get();
        if (scheduler.eventLoop != loop)
        {
            assert loop.inEventLoop();

            Thread t = Thread.currentThread();
            assert t instanceof NettyRxThread;

            int cpuId = ((NettyRxThread)t).getCpuId();
            assert cpuId >= 0 && cpuId < perCoreSchedulers.length;

            if (cpuId == 0)
                shutdown(perCoreSchedulers[0].eventLoop);

            scheduler = new NettyRxScheduler(loop, cpuId);
            localNettyEventLoop.set(scheduler);
            perCoreSchedulers[cpuId] = scheduler;

            logger.info("Putting {} on core {}", t.getName(), cpuId);
            isStartup = false;
        }

        return scheduler;
    }

    private static void shutdown(EventExecutorGroup loop)
    {
        try
        {
           loop.shutdownGracefully();
           loop.awaitTermination(1, TimeUnit.SECONDS);
        }
        catch (InterruptedException ex)
        {
            logger.warn("Got interrupted exception when switching away from global event executor", ex);
        }
    }

    /**
     * @return the core id for netty threads, otherwise the number of cores. Callers can verify if the returned
     * core is valid via {@link NettyRxScheduler#isValidCoreId(Integer)}, or alternatively can allocate an
     * array with length num_cores + 1, and use thread safe operations only on the last element.
     */
    public static int getCoreId()
    {
        Thread t = Thread.currentThread();
        return t instanceof NettyRxThread ? ((NettyRxThread)t).getCpuId() : perCoreSchedulers.length;
    }

    public static boolean isTPCThread()
    {
        return Thread.currentThread() instanceof NettyRxThread;
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
     * Return the next core for scheduling one or more tasks.
     *
     * @return a valid core id, distributed in a round-robin way
     */
    public static int getNextCore()
    {
        return roundRobinIndex.getAndIncrement() % getNumCores();
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
    public static NettyRxScheduler getForCore(int core)
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

    @Inline
    public static Scheduler getForKey(String keyspaceName, DecoratedKey key, boolean useImmediateForLocal)
    {
        // force all system table operations to go through a single core
        if (isStartup)
            return ImmediateThinScheduler.INSTANCE;

        int callerCoreId = -1;
        if (useImmediateForLocal)
            callerCoreId = getCoreId();

        // Convert OP partitions to top level partitioner for secondary indexes; always route
        // system table mutations through core 0
        if (key.getPartitioner() != DatabaseDescriptor.getPartitioner())
        {
            if (SchemaConstants.isSystemKeyspace(keyspaceName))
                return getForCore(0);

            key = DatabaseDescriptor.getPartitioner().decorateKey(key.getKey());
        }

        List<Long> keyspaceRanges = getRangeList(keyspaceName, true);
        Long keyToken = (Long)key.getToken().getTokenValue();

        Long rangeStart = keyspaceRanges.get(0);
        for (int i = 1; i < keyspaceRanges.size(); i++)
        {
            Long next = keyspaceRanges.get(i);
            if (keyToken.compareTo(rangeStart) >= 0 && keyToken.compareTo(next) < 0)
            {
                //logger.info("Read moving to {} from {}", i-1, getCoreId());

                if (useImmediateForLocal)
                    return callerCoreId == i - 1 ? ImmediateThinScheduler.INSTANCE : getForCore(i - 1);

                return getForCore(i - 1);
            }

            rangeStart = next;
        }

        throw new IllegalStateException(String.format("Unable to map %s to cpu for %s", key, keyspaceName));
    }

    public static List<Long> getRangeList(String keyspaceName, boolean persist)
    {
        return getRangeList(Keyspace.open(keyspaceName), persist);
    }

    public static List<Long> getRangeList(Keyspace keyspace, boolean persist)
    {
        List<Long> ranges = keyspaceToRangeMapping.get(keyspace.getName());

        if (ranges != null)
            return ranges;

        List<Range<Token>> localRanges = StorageService.getStartupTokenRanges(keyspace);
        List<Long> splits = StorageService.getCpuBoundries(localRanges, DatabaseDescriptor.getPartitioner(), NUM_NETTY_THREADS)
                                          .stream()
                                          .map(s -> (Long) s.getToken().getTokenValue())
                                          .collect(Collectors.toList());

        if (persist)
        {
            if (instance().cpuId == 0)
            {
                keyspaceToRangeMapping.put(keyspace.getName(), splits);
            }
            else
            {
                CountDownLatch ready = new CountDownLatch(1);

                getForCore(0).scheduleDirect(() -> {
                    keyspaceToRangeMapping.put(keyspace.getName(), splits);
                    ready.countDown();
                });

                Uninterruptibles.awaitUninterruptibly(ready);
            }
        }

        return splits;
    }


    @Override
    public Disposable scheduleDirect(Runnable run, long delay, TimeUnit unit)
    {
        return createWorker().scheduleDirect(run, delay, unit);
    }

    @Override
    public Worker createWorker()
    {
        return new Worker(eventLoop);
    }

    private static class Worker extends Scheduler.Worker
    {
        private final EventExecutorGroup nettyEventLoop;

        private final ListCompositeDisposable tasks;

        volatile boolean disposed;

        Worker(EventExecutorGroup nettyEventLoop)
        {
            this.nettyEventLoop = nettyEventLoop;
            this.tasks = new ListCompositeDisposable();
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

        public Disposable scheduleDirect(final Runnable run, long delayTime, TimeUnit unit)
        {
            Runnable decoratedRun = RxJavaPlugins.onSchedule(run);
            try
            {
                Future<?> f;
                if (delayTime <= 0)
                {
                    f = nettyEventLoop.submit(decoratedRun);
                }
                else
                {
                    f = nettyEventLoop.schedule(decoratedRun, delayTime, unit);
                }
                return Disposables.fromFuture(f);
            }
            catch (RejectedExecutionException ex)
            {
                RxJavaPlugins.onError(ex);
                return EmptyDisposable.INSTANCE;
            }
        }

        public ScheduledRunnable scheduleActual(final Runnable run, long delayTime, TimeUnit unit, DisposableContainer parent)
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
        //RxSubscriptionDebugger.enable();
    }
}
