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

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.EmptyDisposable;

/**
 * The scheduler used for IO operations, or any other operation that might block.
 * <p>
 * This scheduler must guarantee that any request will be executed immediately, by creating a new worker if
 * needed, to avoid the risk of deadlocks (a thread blocked on a task also executed on the IO scheduler, with
 * said task also ending up being assigned to the very same thread - this can't happen and this scheduler must
 * ensure this).
 * <p>
 * The scheduler is based on rx Java schedulers and each worker is implemented as a single threaded executor via
 * {@link ExecutorBasedWorker}.
 * <p>
 * Rx java {@link io.reactivex.internal.schedulers.IoScheduler} implements the IO scheduler by creating an unlimited number
 * of workers, and then caching them in an unbounded queue, with a periodic task that expires workers idle for longer
 * than {@link io.reactivex.internal.schedulers.IoScheduler#KEEP_ALIVE_TIME}
 * {@link io.reactivex.internal.schedulers.IoScheduler#KEEP_ALIVE_UNIT}, by default 60 seconds.
 * <p>
 * This implementation also creates an unlimited number of workers as requested, but it will never cache more than
 * {@link IOScheduler#MAX_POOL_SIZE} workers. When this pool size is exceeded, any worker returned to the pool is expired
 * immediately. If the pool size reaches below {@link IOScheduler#MIN_POOL_SIZE} workers, then the workers returned
 * when the queue is below this threshold are never expired. Otherwise, if the queue size is between MIN and MAX when
 * the worker is returned, the worker is used to schedule a task after {@link #KEEP_ALIVE_TIME_SECS} seconds. This task
 * will expire the worker if it is still in the queue, and the queue is still bigger than {@link IOScheduler#MIN_POOL_SIZE}.
 * <p>
 * Note 1:
 * The IO scheduler currently uses {@link TPCRunnable} instances on NUM_CORES +1, but we could extend the metrics system
 * to have dedicated IO scheduler metrics by adding an IORunnable (TODO).
 * <p>
 * Note 2:
 * An alternative implementation, in addition to the two implementations above, would have been to use an unbounded
 * queue of tasks serviced by a pool of threads but this was more work (due to delayed tasks), would require guessing
 * a correct pool size, and it would not guarantee that we could not have a deadlock if all the threads in the pool were
 * waiting on each other in some pathological way. Extremely unlikely but not impossible for s very small pool size.
 */
public class IOScheduler extends StagedScheduler
{
    @VisibleForTesting
    static final int MIN_POOL_SIZE = Integer.valueOf(System.getProperty("dse.io.sched.min_pool_size", "8"));

    @VisibleForTesting
    public static final int MAX_POOL_SIZE = Integer.valueOf(System.getProperty("dse.io.sched.max_pool_size", "256"));

    @VisibleForTesting
    static final int KEEP_ALIVE_TIME_SECS = Integer.valueOf(System.getProperty("dse.io.sched.keep_alive_secs", "5"));

    private final Function<ThreadFactory, ExecutorBasedWorker> workerSupplier;
    private final AtomicReference<WorkersPool> pool;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    IOScheduler()
    {
        this(factory -> ExecutorBasedWorker.singleThreaded(factory));
    }

    @VisibleForTesting
    IOScheduler(Function<ThreadFactory, ExecutorBasedWorker> workerSupplier)
    {
        this.workerSupplier = workerSupplier;
        this.pool = new AtomicReference<>(new WorkersPool(workerSupplier));
    }

    public int metricsCoreId()
    {
        return TPC.getNumCores();
    }

    public void enqueue(TPCRunnable runnable)
    {
        ((PooledWorker)createWorker()).execute(runnable);
    }

    @Override
    public Disposable scheduleDirect(Runnable run, long delay, TimeUnit unit)
    {
        return super.scheduleDirect(TPCRunnable.wrap(run,
                                                     delay != 0 ? TPCTaskType.TIMED_UNKNOWN : TPCTaskType.UNKNOWN,
                                                     TPC.getNextCore()),
                                    delay,
                                    unit);
    }

    @Override
    public Disposable scheduleDirect(Runnable run, TPCTaskType stage, long delay, TimeUnit unit)
    {
        return super.scheduleDirect(TPCRunnable.wrap(run, stage, TPC.getNextCore()), delay, unit);
    }

    @Override
    public boolean isOnScheduler(Thread thread)
    {
        return thread instanceof IOThread;
    }


    @Override
    public Worker createWorker()
    {
        return new PooledWorker(pool.get());
    }

    @Override
    public void start()
    {
        while (pool.get() == null)
        {
            WorkersPool next = new WorkersPool(workerSupplier);
            if (pool.compareAndSet(null, next))
                break;

            next.shutdown();
        }
    }

    @Override
    public void shutdown()
    {
        if (!shutdown.compareAndSet(false, true))
            return;

        WorkersPool current = pool.get();
        while (current != null)
        {
            if (pool.compareAndSet(current, null))
            {
                current.shutdown();
                break;
            }

            current = pool.get();
        }
    }

    public boolean isShutdown()
    {
        return shutdown.get();
    }

    @VisibleForTesting
    int numCachedWorkers()
    {
        WorkersPool pool = this.pool.get();
        return pool == null ? 0 : pool.cachedSize();
    }

    @Override
    public String toString()
    {
        return String.format("IO scheduler: cached workers: %d", numCachedWorkers());
    }

    static final class WorkersPool
    {
        private final ThreadFactory threadFactory;
        private final Function<ThreadFactory, ExecutorBasedWorker> workerSupplier;
        private final Queue<ExecutorBasedWorker> workersQueue;
        private final CompositeDisposable allWorkers;
        private final AtomicBoolean shutdown;

        WorkersPool(Function<ThreadFactory, ExecutorBasedWorker> workerSupplier)
        {
            this.threadFactory = new IOThread.Factory();
            this.workerSupplier = workerSupplier;
            this.workersQueue = new LinkedBlockingQueue<>(MAX_POOL_SIZE);
            this.allWorkers = new CompositeDisposable();
            this.shutdown = new AtomicBoolean(false);
        }

        int cachedSize()
        {
            return workersQueue.size();
        }

        ExecutorBasedWorker get()
        {
            if (shutdown.get() || allWorkers.isDisposed())
                return ExecutorBasedWorker.DISPOSED;

            while (!workersQueue.isEmpty())
            {
                ExecutorBasedWorker threadWorker = workersQueue.poll();
                if (threadWorker != null)
                    return threadWorker;
            }

            // No cached worker found, so create a new one.
            ExecutorBasedWorker w = workerSupplier.apply(threadFactory);
            allWorkers.add(w);
            return w;
        }

        void release(ExecutorBasedWorker worker)
        {
            if (shutdown.get() || !workersQueue.offer(worker))
                worker.dispose(); // no space or shutdown
            else
                worker.scheduleDirect(() -> {
                    if (shutdown.get() || workersQueue.size() > MIN_POOL_SIZE)
                    {   // there's a risk that the queue may decrease to less than MIN_POOL_SIZE if two threads
                        // race each other, but I think this is of no consequence
                        if (workersQueue.remove(worker))
                            worker.dispose();
                    }
                }, KEEP_ALIVE_TIME_SECS, TimeUnit.SECONDS);
        }

        void shutdown()
        {
            if (!shutdown.compareAndSet(false, true))
                return;

            allWorkers.dispose();
        }
    }

    static final class PooledWorker extends Worker
    {
        private final CompositeDisposable tasks;
        private final WorkersPool pool;
        private final ExecutorBasedWorker worker;
        private final AtomicBoolean disposed;

        PooledWorker(WorkersPool pool)
        {
            this.tasks = new CompositeDisposable();
            this.pool = pool;
            this.worker = pool.get();
            this.disposed = new AtomicBoolean(worker == ExecutorBasedWorker.DISPOSED);
        }

        @Override
        public void dispose()
        {
            if (disposed.compareAndSet(false, true))
            {
                tasks.dispose();
                pool.release(worker);
            }
        }

        @Override
        public boolean isDisposed()
        {
            return disposed.get();
        }

        public void execute(Runnable runnable)
        {
            if (disposed.get())
                return;

            try
            {
                worker.getExecutor().execute(runnable);
            }
            finally
            {
                dispose();
            }
        }

        @Override
        public Disposable schedule(Runnable action, long delayTime, TimeUnit unit)
        {
            if (disposed.get())
                return EmptyDisposable.INSTANCE;

            return worker.scheduleActual(action, delayTime, unit, tasks);
        }
    }
}
