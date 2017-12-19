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

package org.apache.cassandra.rx;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.CompletableSource;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.ListCompositeDisposable;
import io.reactivex.plugins.RxJavaPlugins;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;


/**
 * A copy of CompletableTimeout that avoids using Hash based CompositeDisposable since it affects perf
 * Instead this uses a ListCompositeDisposable
 */
public final class CompletableTimeout extends Completable
{
    final CompletableSource source;
    final long timeout;
    final TimeUnit unit;
    final TPCScheduler scheduler;
    final CompletableSource other;

    public CompletableTimeout(CompletableSource source, long timeout, TimeUnit unit)
    {
        this(source, timeout, unit, TPC.bestTPCScheduler(), null);
    }

    public CompletableTimeout(CompletableSource source, long timeout, TimeUnit unit, TPCScheduler scheduler, CompletableSource other)
    {
        this.source = source;
        this.timeout = timeout;
        this.unit = unit;
        this.scheduler = scheduler;
        this.other = other;
    }

    public void subscribeActual(CompletableObserver s)
    {
        ListCompositeDisposable set = new ListCompositeDisposable();
        s.onSubscribe(set);
        AtomicBoolean once = new AtomicBoolean();
        Disposable timer = this.scheduler.schedule(new DisposeTask(once, set, s), TPCTaskType.TIMED_TIMEOUT, this.timeout, this.unit);
        set.add(timer);
        this.source.subscribe(new TimeOutObserver(set, once, s));
    }

    final class DisposeTask implements Runnable
    {
        private final AtomicBoolean once;
        final ListCompositeDisposable set;
        final CompletableObserver s;

        DisposeTask(AtomicBoolean once, ListCompositeDisposable set, CompletableObserver s)
        {
            this.once = once;
            this.set = set;
            this.s = s;
        }

        public void run()
        {
            if (this.once.compareAndSet(false, true))
            {
                this.set.clear();
                if (CompletableTimeout.this.other == null)
                {
                    this.s.onError(new TimeoutException());
                }
                else
                {
                    CompletableTimeout.this.other.subscribe(new DisposeTask.DisposeObserver());
                }
            }
        }

        final class DisposeObserver implements CompletableObserver
        {
            DisposeObserver()
            {
            }

            public void onSubscribe(Disposable d)
            {
                DisposeTask.this.set.add(d);
            }

            public void onError(Throwable e)
            {
                DisposeTask.this.set.dispose();
                DisposeTask.this.s.onError(e);
            }

            public void onComplete()
            {
                DisposeTask.this.set.dispose();
                DisposeTask.this.s.onComplete();
            }
        }
    }

    static final class TimeOutObserver implements CompletableObserver
    {
        private final ListCompositeDisposable set;
        private final AtomicBoolean once;
        private final CompletableObserver s;

        TimeOutObserver(ListCompositeDisposable set, AtomicBoolean once, CompletableObserver s)
        {
            this.set = set;
            this.once = once;
            this.s = s;
        }

        public void onSubscribe(Disposable d)
        {
            this.set.add(d);
        }

        public void onError(Throwable e)
        {
            if (this.once.compareAndSet(false, true))
            {
                this.set.dispose();
                this.s.onError(e);
            }
            else
            {
                RxJavaPlugins.onError(e);
            }
        }

        public void onComplete()
        {
            if (this.once.compareAndSet(false, true))
            {
                this.set.dispose();
                this.s.onComplete();
            }
        }
    }
}
