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

import com.google.common.annotations.VisibleForTesting;

import io.netty.channel.EventLoop;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;
import io.reactivex.internal.disposables.DisposableContainer;
import io.reactivex.internal.disposables.EmptyDisposable;
import io.reactivex.internal.disposables.ListCompositeDisposable;
import io.reactivex.internal.schedulers.ScheduledRunnable;
import io.reactivex.plugins.RxJavaPlugins;

/**
 * Creates a RxJava scheduler that uses an event loop for execution.
 * <p>
 * Note: the real class used in the code is {@link TPCScheduler} which extends this and offer a few additional
 * TPC-related convenience, but the reason we have this is testing: it allows us to warp the raw Netty event loops
 * easily so we can bench them against our own loops easily.
 */
public class EventLoopBasedScheduler<E extends EventLoop> extends Scheduler implements TracingAwareExecutor
{
    /**
     * The wrapped event loop.
     */
    protected final E eventLoop;

    @VisibleForTesting
    public EventLoopBasedScheduler(E eventLoop)
    {
        assert eventLoop != null;
        this.eventLoop = eventLoop;
    }

    @Override // TracingAwareExecutor
    public void execute(Runnable runnable, ExecutorLocals locals)
    {
        scheduleDirect(new ExecutorLocals.WrappedRunnable(runnable, locals));
    }

    @Override
    public Disposable scheduleDirect(Runnable run, long delay, TimeUnit unit)
    {
        //TODO - shouldn't the worker be disposed?
        return createWorker().scheduleDirect(run, delay, unit);
    }

    @Override
    public Worker createWorker()
    {
        return new Worker(eventLoop);
    }

    private static class Worker extends Scheduler.Worker
    {
        private final EventLoop loop;

        private final ListCompositeDisposable tasks;

        volatile boolean disposed;

        private Worker(EventLoop loop)
        {
            this.loop = loop;
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
                return EmptyDisposable.INSTANCE;

            return scheduleActual(action, 0, null, tasks);
        }

        @Override
        public Disposable schedule(Runnable action, long delayTime, TimeUnit unit)
        {
            if (disposed)
                return EmptyDisposable.INSTANCE;

            return scheduleActual(action, delayTime, unit, tasks);
        }

        private Disposable scheduleDirect(final Runnable run, long delayTime, TimeUnit unit)
        {
            Runnable decoratedRun = RxJavaPlugins.onSchedule(run);
            try
            {
                Future<?> f;
                if (delayTime <= 0)
                {
                    f = loop.submit(decoratedRun);
                }
                else
                {
                    f = loop.schedule(decoratedRun, delayTime, unit);
                }
                return Disposables.fromFuture(f);
            }
            catch (RejectedExecutionException ex)
            {
                RxJavaPlugins.onError(ex);
                return EmptyDisposable.INSTANCE;
            }
        }

        private ScheduledRunnable scheduleActual(final Runnable run, long delayTime, TimeUnit unit, DisposableContainer parent)
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
                    f = loop.submit((Callable)sr);
                }
                else
                {
                    f = loop.schedule((Callable)sr, delayTime, unit);
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
