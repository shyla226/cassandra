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
import java.util.concurrent.TimeUnit;

import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;

/**
 * A scheduler with some additional methods for supporting stages: {@link TPCTaskType}.
 * <p>
 * It is also capable of working out if a thread is part of its own thread pool.
 */
public abstract class StagedScheduler extends Scheduler
{
    private final EnumMap<TPCTaskType, TracingAwareExecutor> executorsForTaskType = new EnumMap<>(TPCTaskType.class);

    public abstract Disposable scheduleDirect(Runnable run, TPCTaskType stage, long delay, TimeUnit unit);

    /**
     * Return true if the thread belongs to this scheduler, that is the thread is part
     * of the thread pool that supports this scheduler.
     *
     * @param thread - the thread to check
     *
     * @return true if the thread is part of the thread pool for the scheduler, false otherwise.
     */
    public abstract boolean isOnScheduler(Thread thread);

    public abstract int metricsCoreId();

    public abstract void enqueue(TPCRunnable runnable);

    public void execute(TPCRunnable runnable)
    {
        if (isOnScheduler(Thread.currentThread()))
            runnable.run();
        else
            enqueue(runnable);
    }

    public void execute(Runnable runnable, ExecutorLocals locals, TPCTaskType stage)
    {
        execute(TPCRunnable.wrap(runnable, locals, stage, metricsCoreId()));
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
        return (runnable, locals) -> execute(runnable, locals, type);
    }
}
