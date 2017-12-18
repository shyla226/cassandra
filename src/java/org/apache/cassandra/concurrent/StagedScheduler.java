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

import java.util.EnumMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.plugins.RxJavaPlugins;

/**
 * A scheduler with some additional methods for supporting stages: {@link TPCTaskType}.
 * <p>
 * It is also capable of working out if a thread is part of its own thread pool.
 */
public abstract class StagedScheduler extends Scheduler
{
    private final EnumMap<TPCTaskType, TracingAwareExecutor> executorsForTaskType = new EnumMap<>(TPCTaskType.class);

    /**
     * Return true if the thread belongs to this scheduler, that is the thread is part
     * of the thread pool that supports this scheduler.
     *
     * @param thread - the thread to check
     *
     * @return true if the thread is part of the thread pool for the scheduler, false otherwise.
     */
    protected abstract boolean isOnScheduler(Thread thread);

    /**
     * Returns true if the specified task type can be executed immediately.
     * As a minumum this requires that the thread we are currently on is a thread of this scheduler. Some schedulers
     * also delay tasks if there are too many active ones in the queue.
     */
    boolean canExecuteImmediately(TPCTaskType taskType)
    {
        return isOnScheduler(Thread.currentThread());
    }

    /**
     * This is a public-facing version of the above which may be used whenever something needs to apply an operation
     * and wants to avoid wrapping it into a runnable to call our execute method.
     *
     * Since we want to always track the start and completion of some types of operations, we can only allow this
     * if the task type is not always logged.
     *
     * This is normally used in the following pattern:
     *
     *     if (scheduler.canRunDirectly(taskType))
     *         doSomething(...);
     *     else
     *         scheduler.execute(() -> doSomething(...), taskType);
     *
     * Note that even if this returns false the execute method may still run the task immediately and on the same
     * thread.
     */
    public boolean canRunDirectly(TPCTaskType taskType)
    {
        return !taskType.logIfExecutedImmediately() && canExecuteImmediately(taskType);
    }

    public abstract int metricsCoreId();

    public abstract void enqueue(TPCRunnable runnable);

    public void execute(TPCRunnable runnable)
    {
        if (canExecuteImmediately(runnable.taskType()))
            runnable.run();
        else
            enqueue(runnable);
    }

    public void execute(Runnable runnable, TPCTaskType taskType)
    {
        if (!canExecuteImmediately(taskType))
            enqueue(wrap(runnable, taskType));
        else if (taskType.logIfExecutedImmediately())
            wrap(runnable, taskType).run();
        else
            runnable.run();
    }

    public Disposable schedule(Runnable run, TPCTaskType taskType, long delay, TimeUnit unit)
    {
        return scheduleDirect(wrap(run, taskType), delay, unit);
    }

    @Override
    public Disposable scheduleDirect(Runnable run, long delay, TimeUnit unit)
    {
        final Worker w = createWorker();

        final Runnable decoratedRun = RxJavaPlugins.onSchedule(run);

        TPCAwareDisposeTask task = new TPCAwareDisposeTask(decoratedRun, w);

        w.schedule(task, delay, unit);

        return task;
    }

    @Override
    public Disposable schedulePeriodicallyDirect(Runnable run, long initialDelay, long period, TimeUnit unit)
    {
        // The default RxJava implementation of this API does not play well with some of our TPC-specific
        // implementation details. For example, disposing the Disposable returned by the default implementation won't
        // cancel the TPCRunnable that we may have passed for periodic scheduling. As another example, the default
        // implementation includes multiple calls to RxJavaPlugins.onSchedule() coupled with multiple wrappings of the
        // thus decorated Runnable in internal Disposable types. This can result in multiple redundant TPCRunnables
        // created as a result of the decorations. Until we decide it pays off to reimplement this code path as we've
        // done for the scheduleDirect code path, it's best to throw here.
        // Also see https://github.com/riptano/apollo/pull/586
        throw new UnsupportedOperationException();
    }

    public TPCRunnable wrap(Runnable runnable, TPCTaskType taskType)
    {
        return TPCRunnable.wrap(runnable, ExecutorLocals.create(), taskType, metricsCoreId());
    }

    /**
     * Returns an executor that assigns the given task type to the runnables it receives.
     */
    public TracingAwareExecutor forTaskType(TPCTaskType type)
    {
        TracingAwareExecutor executor = executorsForTaskType.get(type);
        if (executor != null)
            return executor;

        synchronized (executorsForTaskType)
        {
            return executorsForTaskType.computeIfAbsent(type, this::makeExecutor);
        }
    }

    private TracingAwareExecutor makeExecutor(TPCTaskType type)
    {
        // Because this executor explicitly specifies locals that need to be set, we always wrap the runnable.
        return (runnable, locals) -> execute(TPCRunnable.wrap(runnable, locals, type, metricsCoreId()));
    }

    /**
     * Used to execute a given runnable, and to dispose of resources related to the execution. If the runnable is a
     * <code>TPCRunnable</code>, special care is taken in case of disposal before the runnable execution has completed.
     * Inspired by the internal dispose tasks used by {@link Scheduler}.
     */
    static final class TPCAwareDisposeTask implements Runnable, Callable<Object>, Disposable
    {
        private final Runnable runnable;
        /**
         * The worker that would be running this dispose task, and respectively its runnable.
         */
        private final Worker w;
        /**
         * Shows whether the runnable has progressed - either by having started running, or by getting cancelled.
         */
        private final AtomicBoolean hasProgressed;
        
        TPCAwareDisposeTask(Runnable runnable, Worker w)
        {
            this.runnable = runnable;
            this.w = w;
            this.hasProgressed = new AtomicBoolean();
        }

        @Override
        public void run()
        {
            try
            {
                if (hasProgressed.compareAndSet(false, true))
                    runnable.run();
            }
            finally
            {
                dispose();
            }
        }

        @Override
        public Object call()
        {
            run();
            return null;
        }

        @Override
        public void dispose()
        {
            w.dispose();
            if (runnable instanceof TPCRunnable && hasProgressed.compareAndSet(false, true))
                ((TPCRunnable) runnable).cancelled();
        }

        @Override
        public boolean isDisposed()
        {
            return w.isDisposed();
        }
    }
}
