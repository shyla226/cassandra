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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.util.HashedWheelTimer;
import io.reactivex.Scheduler;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.EmptyDisposable;
import io.reactivex.internal.schedulers.ScheduledRunnable;
import io.reactivex.plugins.RxJavaPlugins;

/**
 * Created by jake on 3/22/16.
 */
public class MonitoredTPCRxScheduler
{
    private static final HashedWheelTimer timer = new HashedWheelTimer(new NamedThreadFactory("rx-scheduled-cpu"), 1L, TimeUnit.MILLISECONDS, 1024);

    private static final Scheduler anyCpuScheduler = new AnyCoreScheduler();

    public static Scheduler any()
    {
        return anyCpuScheduler;
    }

    public static Scheduler forCpu(int coreId)
    {
        System.err.println("NEW");
        return new MonitoredTPCRxScheduler.SingleCoreScheduler(coreId);
    }

    private static class AnyCoreScheduler extends Scheduler
    {
        public Worker createWorker()
        {
            return new MonitoredTPCRxScheduler.Worker(MonitoredTPCExecutorService.instance().any());
        }
    }

    private static class SingleCoreScheduler extends Scheduler
    {
        private final int coreId;
        private final MonitoredTPCExecutorService.SingleCoreExecutor executor;
        public SingleCoreScheduler(int coreId)
        {
            this.coreId = coreId;
            executor = MonitoredTPCExecutorService.instance().one(coreId);
        }

        public Worker createWorker()
        {
            return new MonitoredTPCRxScheduler.Worker(executor);
        }
    }


    public static class Worker extends Scheduler.Worker
    {
        private final MonitoredTPCExecutorService.SingleCoreExecutor cpu;

        private final CompositeDisposable tasks;

        volatile boolean disposed;

        Worker(MonitoredTPCExecutorService.SingleCoreExecutor cpu)
        {
            this.cpu = cpu;
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

            try
            {
                FutureTask<?> f = new FutureTask<>(sr::run, null);

                if (delayTime <= 0)
                {
                    cpu.addTask(f);
                }
                else
                {
                    timer.newTimeout(t -> f.run(), delayTime, unit);
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
