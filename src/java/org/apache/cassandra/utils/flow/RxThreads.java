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
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCTaskType;

public class RxThreads
{
    public static <T> Single<T> subscribeOn(Single<T> source, StagedScheduler scheduler, TPCTaskType taskType)
    {
        class SubscribeOn extends Single<T>
        {
            protected void subscribeActual(SingleObserver<? super T> subscriber)
            {
                if (scheduler.canRunDirectly(taskType))
                    source.subscribe(subscriber);
                else
                    scheduler.execute(() -> source.subscribe(subscriber),
                                      taskType);
            }
        }
        return new SubscribeOn();
    }

    public static <T> Single<T> subscribeOnIo(Single<T> source, TPCTaskType taskType)
    {
        return subscribeOn(source, TPC.ioScheduler(), taskType);
    }

    public static <T> Single<T> subscribeOnCore(Single<T> source, int coreId, TPCTaskType taskType)
    {
        return subscribeOn(source, TPC.getForCore(coreId), taskType);
    }

    public static Completable subscribeOn(Completable source, StagedScheduler scheduler, TPCTaskType taskType)
    {
        class SubscribeOn extends Completable
        {
            protected void subscribeActual(CompletableObserver subscriber)
            {
                if (scheduler.canRunDirectly(taskType))
                    source.subscribe(subscriber);
                else
                    scheduler.execute(() -> source.subscribe(subscriber),
                                      taskType);
            }
        }
        return new SubscribeOn();
    }

    public static Completable subscribeOnIo(Completable source, TPCTaskType taskType)
    {
        return subscribeOn(source, TPC.ioScheduler(), taskType);
    }

    private static CompletableOperator awaitAndContinueOnCompletable(StagedScheduler scheduler, TPCTaskType taskType)
    {
        class AwaitAndContinueOn extends TPCRunnable implements CompletableObserver
        {
            final CompletableObserver subscriber;

            AwaitAndContinueOn(CompletableObserver subscriber)
            {
                // Create TPCRunnable on subscription so that we track the waiting task and executor locals.
                super(subscriber::onComplete,
                      ExecutorLocals.create(),
                      taskType,
                      scheduler.metricsCoreId());
                this.subscriber = subscriber;
            }

            public void onSubscribe(Disposable disposable)
            {
                subscriber.onSubscribe(disposable);
            }

            public void onComplete()
            {
                try
                {
                    scheduler.execute(this);
                }
                catch(Throwable ex)
                {
                    cancelled();
                    subscriber.onError(ex);
                }
            }

            public void onError(Throwable throwable)
            {
                subscriber.onError(throwable);
            }
        }
        return subscriber -> new AwaitAndContinueOn(subscriber);
    }

    /**
     * Create a completable which waits for a signal given on a separate shared thread.
     * Captures the context (executorLocals) of the task on subscription and passes control to it on the given
     * scheduler when the signal is given.
     * See BatchCommitLogService.maybeWaitForSync for usage example.
     */
    public static Completable awaitAndContinueOn(Completable source, StagedScheduler scheduler, TPCTaskType taskType)
    {
        return source.lift(awaitAndContinueOnCompletable(scheduler, taskType));
    }
}
