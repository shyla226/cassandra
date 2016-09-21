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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
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
import io.reactivex.disposables.Disposables;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.internal.disposables.EmptyDisposable;
import io.reactivex.internal.schedulers.ScheduledRunnable;
import io.reactivex.plugins.RxJavaPlugins;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.service.StorageService;


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
    public final static FastThreadLocal<NettyRxScheduler> localNettyEventLoop = new FastThreadLocal<NettyRxScheduler>()
    {
        protected NettyRxScheduler initialValue()
        {
            return new NettyRxScheduler(GlobalEventExecutor.INSTANCE, Integer.MAX_VALUE);
        }
    };

    final static Map<String, List<PartitionPosition>> keyspaceToRangeMapping = new HashMap<>();

    //Each array entry maps to a cpuId. We assume there will be < 1024 CPUs
    final static NettyRxScheduler[] perCoreSchedulers = new NettyRxScheduler[1024];
    private static int numActiveCpus = 0;

    final EventExecutorGroup eventLoop;
    public final int cpuId;

    public static NettyRxScheduler instance()
    {
        return localNettyEventLoop.get();
    }

    public static NettyRxScheduler instance(EventExecutor loop, int cpuId)
    {
        NettyRxScheduler scheduler = localNettyEventLoop.get();
        if (scheduler == null || scheduler.eventLoop != loop)
        {
            assert loop.inEventLoop();
            scheduler = new NettyRxScheduler(loop, cpuId);
            localNettyEventLoop.set(scheduler);
            perCoreSchedulers[cpuId] = scheduler;

            numActiveCpus++;
        }

        return scheduler;
    }

    private NettyRxScheduler(EventExecutorGroup eventLoop, int cpuId)
    {
        assert eventLoop != null;
        assert cpuId >= 0;
        this.eventLoop = eventLoop;
        this.cpuId = cpuId;
    }

    public static NettyRxScheduler getForCore(int core)
    {
        NettyRxScheduler scheduler = perCoreSchedulers[core];
        assert scheduler != null && scheduler.cpuId == core : scheduler == null ? "NPE" : "" + scheduler.cpuId + " != " + core;

        return scheduler;
    }

    public static NettyRxScheduler getForKey(ColumnFamilyStore cfs, DecoratedKey key)
    {
        // force all system table operations to go through a single core
        if (SchemaConstants.SYSTEM_KEYSPACE_NAMES.contains(cfs.keyspace.getName()))
        {
            if (perCoreSchedulers[0] != null)
                return perCoreSchedulers[0];

            // during initial startup we have no schedulers, and should run tasks directly
            return null;
        }

        List<PartitionPosition> keyspaceRanges = getRangeList(cfs);

        PartitionPosition rangeStart = keyspaceRanges.get(0);
        for (int i = 1; i < keyspaceRanges.size(); i++)
        {
            PartitionPosition next = keyspaceRanges.get(i);
            if (key.compareTo(rangeStart) >= 0 && key.compareTo(next) < 0)
                return getForCore(i - 1);

            rangeStart = next;
        }

        throw new IllegalStateException(String.format("Unable to map %s to cpu for %s.%s", key, cfs.keyspace.getName(), cfs.getTableName()));
    }

    private static List<PartitionPosition> getRangeList(ColumnFamilyStore cfs)
    {
        List<PartitionPosition> ranges = keyspaceToRangeMapping.get(cfs.keyspace.getName());

        if (ranges != null)
            return ranges;

        List<Range<Token>> localRanges = StorageService.getLocalRanges(cfs);
        List<PartitionPosition> splits = StorageService.getCpuBoundries(localRanges, cfs.getPartitioner(), numActiveCpus);

        if (instance().cpuId == 0)
        {
            keyspaceToRangeMapping.put(cfs.keyspace.getName(), splits);
        }
        else
        {
            CountDownLatch ready = new CountDownLatch(1);

            getForCore(0).scheduleDirect(() -> {
                keyspaceToRangeMapping.put(cfs.keyspace.getName(), splits);
                ready.countDown();
            });

            Uninterruptibles.awaitUninterruptibly(ready);
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
}
