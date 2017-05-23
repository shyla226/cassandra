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

import io.netty.util.concurrent.EventExecutorGroup;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;
import io.reactivex.internal.disposables.DisposableContainer;
import io.reactivex.internal.disposables.EmptyDisposable;
import io.reactivex.internal.disposables.ListCompositeDisposable;
import io.reactivex.internal.schedulers.ScheduledRunnable;
import io.reactivex.plugins.RxJavaPlugins;

/**
 * RxJava Scheduler which wraps a TPC event loops.
 */
public class TPCScheduler extends EventLoopBasedScheduler<TPCEventLoop>
{
    /**
     * Creates a new TPC scheduler that wraps the provided TPC event loop.
     *
     * @param eventLoop the event loop to wrap.
     */
    @VisibleForTesting
    public TPCScheduler(TPCEventLoop eventLoop)
    {
        super(eventLoop);
    }

    /**
     * The TPC thread for which this is a scheduler.
     */
    public TPCThread thread()
    {
        return eventLoop.thread();
    }

    /**
     * The core corresponding to this scheduler.
     * <p>
     * This is simply a shortcut for {@code thread().coreId()}.
     */
    public int coreId()
    {
        return thread().coreId();
    }
}
