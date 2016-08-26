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

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

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


/**
 *
 */
public class NettyRxScheduler extends Scheduler
{
    public final static FastThreadLocal<NettyRxScheduler> localNettyEventLoop = new FastThreadLocal<NettyRxScheduler>()
    {
        protected NettyRxScheduler initialValue()
        {
            return new NettyRxScheduler(GlobalEventExecutor.INSTANCE);
        }
    };

    final EventExecutorGroup eventLoop;

    public static NettyRxScheduler instance()
    {
        return localNettyEventLoop.get();
    }

    public static NettyRxScheduler instance(EventExecutor loop)
    {
        NettyRxScheduler scheduler = localNettyEventLoop.get();
        if (scheduler == null || scheduler.eventLoop != loop)
        {
            assert loop.inEventLoop();
            scheduler = new NettyRxScheduler(loop);
            localNettyEventLoop.set(scheduler);
        }

        return scheduler;
    }

    private NettyRxScheduler(EventExecutorGroup eventLoop)
    {
        assert eventLoop != null;
        this.eventLoop = eventLoop;
    }

    public NettyRxScheduler asGroup()
    {
        if (eventLoop instanceof GlobalEventExecutor)
            return this;

        return new NettyRxScheduler( ((EventExecutor)eventLoop).parent());
    }

    @Override
    public Worker createWorker()
    {
        return new Worker(eventLoop);
    }

    public static class Worker extends Scheduler.Worker
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
