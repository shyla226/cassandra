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

import java.util.concurrent.*;
import java.util.function.Supplier;

import com.google.common.base.Throwables;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.internal.disposables.EmptyDisposable;
import org.apache.cassandra.utils.concurrent.ExecutableLock;

public class TPCUtils
{
    public final static class WouldBlockException extends RuntimeException
    {
        public WouldBlockException(String message)
        {
            super(message);
        }
    }

    public static <T> T blockingGet(Single<T> single)
    {
        if (TPC.isTPCThread())
            throw new WouldBlockException("Calling blockingGet would block TPC thread " + Thread.currentThread().getName());

        return single.blockingGet();
    }

    public static void blockingAwait(Completable completable)
    {
        if (TPC.isTPCThread())
            throw new WouldBlockException("Calling blockingAwait would block TPC thread " + Thread.currentThread().getName());

        completable.blockingAwait();
    }

    public static <T> T blockingGet(CompletableFuture<T> future)
    {
        if (TPC.isTPCThread())
            throw new WouldBlockException("Calling blockingGet would block TPC thread " + Thread.currentThread().getName());

        try
        {
            return future.get();
        }
        catch (CompletionException e)
        {
            throw Throwables.propagate(e.getCause());
        }
        catch (ExecutionException|InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void blockingAwait(CompletableFuture future)
    {
        if (TPC.isTPCThread())
            throw new WouldBlockException("Calling blockingAwait would block TPC thread " + Thread.currentThread().getName());

        try
        {
            future.get();
        }
        catch (CompletionException e)
        {
            throw Throwables.propagate(e.getCause());
        }
        catch (ExecutionException|InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Convert a rx java Single into a Java completable future by subscribing to the single
     * and completing the future when the single completes.
     *
     * @param single - the single to subscribe to
     * @param <T> - the type of the single value
     *
     * @return a completable future that completes when the single does.
     */
    public static <T> CompletableFuture<T> toFuture(Single<T> single)
    {
        CompletableFuture<T> ret = new CompletableFuture<>();
        single.subscribe(ret::complete, ret::completeExceptionally);
        return ret;
    }

    /**
     * Convert a rx java Single into a Java completable void future by subscribing to the single
     * and completing the future when the single completes, ignoring the result.
     *
     * @param single - the single to subscribe to
     * @param <T> - the type of the single value
     *
     * @return a completable future that completes when the single does.
     */
    public static <T> CompletableFuture<Void> toFutureVoid(Single<T> single)
    {
        CompletableFuture<Void> ret = new CompletableFuture<>();
        single.subscribe(result -> ret.complete(null), ret::completeExceptionally);
        return ret;
    }

    /**
     * Convert a rx java Completable into a Java completable future by subscribing to the completable
     * and completing the future when the rx completable completes.
     *
     * @param completable - the rx completable to subscribe to
     *
     * @return a completable future that completes when the completable does.
     */
    public static CompletableFuture<Void> toFuture(Completable completable)
    {
        CompletableFuture<Void> ret = new CompletableFuture<>();
        completable.subscribe(()-> ret.complete(null), ret::completeExceptionally);
        return ret;
    }

    public static Completable toCompletable(CompletableFuture<Void> future)
    {
        return new Completable()
        {
            protected void subscribeActual(CompletableObserver observer)
            {
                observer.onSubscribe(EmptyDisposable.INSTANCE);
                future.whenComplete((res, err) ->{
                    if (err == null)
                        observer.onComplete();
                    else
                        observer.onError(err);
                });
            }
        };
    }

    public static <T> Single<T> toSingle(CompletableFuture<T> future)
    {
        return new Single<T>()
        {
            protected void subscribeActual(SingleObserver<? super T> observer)
            {
                observer.onSubscribe(EmptyDisposable.INSTANCE);
                future.whenComplete((res, err) -> {
                    if (err == null)
                        observer.onSuccess(res);
                    else
                        observer.onError(err);
                });
            }
        };
    }

    /**
     * @return a completed Void future.
     */
    public static CompletableFuture<Void> completedFuture()
    {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * @return a future already completed with the specified value.
     */
    public static <T> CompletableFuture<T> completedFuture(T value)
    {
        return CompletableFuture.completedFuture(value);
    }

    /**
     * Return a future that will execute the callable on the specified executor,
     * converting any exceptions into {@link CompletionException}, and returning the
     * result of the callable.
     *
     * @param callable - the callable to execute
     * @param executor - the executor onto which the callable should execute
     * @param <T> - the type of the result returned by the callable
     * @return a future that will complete when the callable has produced a result
     */
    public static <T> CompletableFuture<T> completableFuture(Callable<T> callable, ExecutorService executor)
    {
        assert callable != null : "Received null callable";
        assert executor != null : "Received null executor";
        return CompletableFuture.supplyAsync(() -> {
            try
            {
                return callable.call();
            }
            catch (Exception ex)
            {
                throw new CompletionException(ex);
            }
        }, executor);
    }


    /**
     * Return a void future that will execute the callable on the specified executor,
     * converting any exceptions into {@link CompletionException}, and ignoring the result
     * of the callable.
     *
     * @param callable - the callable to execute
     * @param executor - the executor onto which the callable should execute
     * @param <T> - the type of the result returned by the callable
     * @return a future that will complete when the callable has produced a result
     */
    public static <T> CompletableFuture<Void> completableFutureVoid(Callable<T> callable, ExecutorService executor)
    {
        assert callable != null : "Received null callable";
        assert executor != null : "Received null executor";
        return CompletableFuture.supplyAsync(() -> {
            try
            {
                callable.call();
                return null;
            }
            catch (Exception ex)
            {
                throw new CompletionException(ex);
            }
        }, executor);
    }

    /**
     * Wraps the future returned by the action parameter with a future that completes only when the lock is available:
     * in other words, the wrapped future (action) will be performed only as soon as the lock is available, and guarded
     * by such lock, which will be released as soon as the action completes.
     * <p>
     * Please note this method doesn't block on the lock: if the lock is not available, the future will be scheduled
     * on the best available TPC scheduler.
     *
     * @param lock - the lock to acquire in order to run the action
     * @param action - the action to execute with the lock taken
     * @param <T> - the result type of the action to perform
     *
     * @return a future chain that guarantees that the action will occur with the lock taken
     */
    public static <T> CompletableFuture<T> withLock(ExecutableLock lock, Supplier<CompletableFuture<T>> action)
    {
        return lock.execute(action, TPC.bestTPCScheduler().getExecutor());
    }

    /**
     * Blocking version of {@link #withLock(ExecutableLock, Supplier)}.
     *
     * @param lock - the lock to acquire in order to run the action
     * @param action - the action to execute with the lock taken
     * @param <T> - the result type of the action to perform
     *
     * @return the result of the action
     */
    public static <T> T withLockBlocking(ExecutableLock lock, Callable<T> action)
    {
        try
        {
            return lock.executeBlocking(action);
        }
        catch (Exception ex)
        {
            throw Throwables.propagate(ex);
        }
    }
}