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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoop;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.DisposableContainer;
import io.reactivex.internal.disposables.EmptyDisposable;
import io.reactivex.internal.disposables.ListCompositeDisposable;
import io.reactivex.internal.schedulers.ScheduledRunnable;
import io.reactivex.plugins.RxJavaPlugins;

/**
 * A rx Java {@link Scheduler.Worker} based on a {@link ScheduledExecutorService}, either
 * a single threaded executor (for the IO scheduler) or a Netty loop (for {@link EventLoopBasedScheduler}.
 */
class ExecutorBasedWorker extends Scheduler.Worker
{
    final static ExecutorBasedWorker DISPOSED = ExecutorBasedWorker.disposed();

    private final ScheduledExecutorService executor;
    private final boolean shutdown;

    private final ListCompositeDisposable tasks;

    private volatile boolean disposed;

    protected ExecutorBasedWorker(ScheduledExecutorService executor, boolean shutdown)
    {
        this.executor = executor;
        this.shutdown = shutdown;
        this.tasks = new ListCompositeDisposable();
    }

    static ExecutorBasedWorker withEventLoop(EventLoop loop)
    {
        return new ExecutorBasedWorker(loop, false);
    }

    static ExecutorBasedWorker withExecutor(ScheduledExecutorService executorService)
    {
        return new ExecutorBasedWorker(executorService, true);
    }

    static ExecutorBasedWorker singleThreaded(ThreadFactory threadFactory)
    {
        return withExecutor(Executors.newScheduledThreadPool(1, threadFactory));
    }

    private static ExecutorBasedWorker disposed()
    {
        ExecutorBasedWorker ret = new ExecutorBasedWorker(Executors.newSingleThreadScheduledExecutor(), true);
        ret.dispose();
        return ret;
    }

    public Executor getExecutor()
    {
        return executor;
    }

    @Override
    public void dispose()
    {
        if (!disposed)
        {
            disposed = true;
            tasks.dispose();

            if (shutdown)
                executor.shutdown();
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

    public Disposable scheduleDirect(final Runnable run, long delayTime, TimeUnit unit)
    {
        Runnable decoratedRun = RxJavaPlugins.onSchedule(run);
        try
        {
            Future<?> f;
            if (delayTime <= 0)
            {
                f = executor.submit(decoratedRun);
            }
            else
            {
                f = executor.schedule(decoratedRun, delayTime, unit);
            }
            return new Disposable()
            {
                public void dispose()
                {
                    f.cancel(false);
                    if (f.isCancelled() && run instanceof TPCRunnable)
                        ((TPCRunnable) run).cancelled();
                }

                public boolean isDisposed()
                {
                    return f.isCancelled();
                }
            };
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
                f = executor.submit((Callable) sr);
            }
            else
            {
                f = executor.schedule((Callable) sr, delayTime, unit);
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
