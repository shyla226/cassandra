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

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import io.netty.channel.EventLoop;
import io.reactivex.disposables.Disposable;

/**
 * Creates a RxJava scheduler that uses an event loop for execution.
 * <p>
 * Note: the real class used in the code is {@link TPCScheduler} which extends this and offers additional
 * TPC-related convenience, but the reason we have this is testing: it allows us to warp the raw Netty event loops
 * easily so we can bench them against our own loops easily. For this reason the runnables are not tagged
 * with the metrics.
 */
@VisibleForTesting
public class EventLoopBasedScheduler<E extends EventLoop> extends StagedScheduler
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

    @Override
    public int metricsCoreId()
    {
        return TPCScheduler.coreIdOf(this);
    }

    @Override
    public void enqueue(TPCRunnable runnable)
    {
        eventLoop.execute(runnable);
    }

    @Override
    public Disposable scheduleDirect(Runnable run, long delay, TimeUnit unit)
    {
        return createWorker().scheduleDirect(run, delay, unit);
    }

    public Disposable scheduleDirect(Runnable run, TPCTaskType stage, long delay, TimeUnit unit)
    {
        throw new UnsupportedOperationException("Should not be called");
    }

    /**
     * To access the underlying executor
     */
    public Executor getExecutor()
    {
        return eventLoop;
    }

    @Override
    public boolean isOnScheduler(Thread thread)
    {
        return false; // no information
    }

    @Override
    public ExecutorBasedWorker createWorker()
    {
        return ExecutorBasedWorker.withEventLoop(eventLoop);
    }
}
