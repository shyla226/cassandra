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

package org.apache.cassandra.utils.flow;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.CompletableOperator;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.SingleOperator;
import io.reactivex.disposables.Disposable;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;

public class RxThreads
{
    public static <T> Single<T> subscribeOn(Single<T> source, Scheduler scheduler, TPCTaskType taskType)
    {
        class SubscribeOn extends Single<T>
        {
            protected void subscribeActual(SingleObserver<? super T> subscriber)
            {
                if (TPC.isOnScheduler(scheduler))
                    source.subscribe(subscriber);
                else
                    scheduler.scheduleDirect(TPCRunnable.wrap(() -> source.subscribe(subscriber),
                                                              taskType,
                                                              TPCScheduler.coreIdOf(scheduler)));
            }
        }
        return new SubscribeOn();
    }

    public static <T> Single<T> subscribeOnIo(Single<T> source, TPCTaskType taskType)
    {
        return subscribeOn(source, TPC.ioScheduler(), taskType);
    }

    public static <T> Single<T> subscribeOnBackgroundIo(Single<T> source, TPCTaskType taskType)
    {
        return subscribeOn(source, TPC.backgroundIOScheduler(), taskType);
    }

    public static <T> Single<T> subscribeOnCore(Single<T> source, int coreId, TPCTaskType taskType)
    {
        class SubscribeOn extends Single<T>
        {
            protected void subscribeActual(SingleObserver<? super T> subscriber)
            {
                if (TPC.isOnCore(coreId))
                    source.subscribe(subscriber);
                else
                    TPC.getForCore(coreId).scheduleDirect(TPCRunnable.wrap(() -> source.subscribe(subscriber),
                                                          taskType,
                                                          coreId));
            }
        }
        return new SubscribeOn();
    }

    public static Completable subscribeOn(Completable source, Scheduler scheduler, TPCTaskType taskType)
    {
        class SubscribeOn extends Completable
        {
            protected void subscribeActual(CompletableObserver subscriber)
            {
                if (TPC.isOnScheduler(scheduler))
                    source.subscribe(subscriber);
                else
                    scheduler.scheduleDirect(TPCRunnable.wrap(() -> source.subscribe(subscriber),
                                                              taskType,
                                                              TPCScheduler.coreIdOf(scheduler)));
            }
        }
        return new SubscribeOn();
    }

    public static Completable subscribeOnIo(Completable source, TPCTaskType taskType)
    {
        return subscribeOn(source, TPC.ioScheduler(), taskType);
    }

    public static Completable subscribeOnBackgroundIo(Completable source, TPCTaskType taskType)
    {
        return subscribeOn(source, TPC.backgroundIOScheduler(), taskType);
    }

    public static <T> SingleOperator<T, T> observeOnSingle(Scheduler scheduler, TPCTaskType taskType)
    {
        class ObserveOn implements SingleObserver<T>
        {
            final SingleObserver<? super T> subscriber;
            final ExecutorLocals locals = ExecutorLocals.create();

            ObserveOn(SingleObserver<? super T> subscriber)
            {
                this.subscriber = subscriber;
            }

            public void onSubscribe(Disposable disposable)
            {
                subscriber.onSubscribe(disposable);
            }

            public void onSuccess(T value)
            {
                if (TPC.isOnScheduler(scheduler))
                    subscriber.onSuccess(value);
                else
                    scheduler.scheduleDirect(new TPCRunnable(() -> subscriber.onSuccess(value),
                                                             locals,
                                                             taskType,
                                                             TPCScheduler.coreIdOf(scheduler)));
            }

            public void onError(Throwable throwable)
            {
                subscriber.onError(throwable);
            }
        }
        return subscriber -> new ObserveOn(subscriber);
    }

    public static <T> Single<T> observeOn(Single<T> source, Scheduler scheduler, TPCTaskType taskType)
    {
        return source.lift(observeOnSingle(scheduler, taskType));
    }

    public static <T> Single<T> observeOnIo(Single<T> source, TPCTaskType taskType)
    {
        return observeOn(source, TPC.ioScheduler(), taskType);
    }

    public static CompletableOperator observeOnCompletable(Scheduler scheduler, TPCTaskType taskType)
    {
        class ObserveOn extends TPCRunnable implements CompletableObserver
        {
            final CompletableObserver subscriber;

            ObserveOn(CompletableObserver subscriber)
            {
                // Create TPCRunnable on subscription so that we track the waiting task.
                super(subscriber::onComplete,
                      ExecutorLocals.create(),
                      taskType,
                      TPCScheduler.coreIdOf(scheduler));
                this.subscriber = subscriber;
            }

            public void onSubscribe(Disposable disposable)
            {
                subscriber.onSubscribe(disposable);
            }

            public void onComplete()
            {
                if (TPC.isOnScheduler(scheduler))
                    run();
                else
                    scheduler.scheduleDirect(this);
            }

            public void onError(Throwable throwable)
            {
                subscriber.onError(throwable);
            }
        }
        return subscriber -> new ObserveOn(subscriber);
    }

    public static Completable observeOn(Completable source, Scheduler scheduler, TPCTaskType taskType)
    {
        return source.lift(observeOnCompletable(scheduler, taskType));
    }
}
