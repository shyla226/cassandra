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

    /** Used by IOScheduler to put inactive threads to sleep */
    private long unusedSince;

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

    ScheduledRunnable scheduleActual(final Runnable decoratedRun, long delayTime, TimeUnit unit, DisposableContainer parent)
    {
        // We should be able to avoid the decoration if we can establish an invariant that RxJava's decoration
        // mechanism should have already been used before delegating to the underlying scheduler worker.
        // This would save some allocations and TPC metrics pollution in some cases (e.g. if the decoration wraps
        // as TPCRunnable, and the given runnable is not one already - a typical example being a wrapper over
        // TPCRunnable...). If we want or have to decorate here too, we should either accept the redundancy, or
        // add some checks during the decoration.
        // Runnable decoratedRun = RxJavaPlugins.onSchedule(run);

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

    public long unusedTime(long currrentTime)
    {
        return currrentTime - unusedSince;
    }

    public void markUse(long currentTime)
    {
        unusedSince = currentTime;
    }
}
