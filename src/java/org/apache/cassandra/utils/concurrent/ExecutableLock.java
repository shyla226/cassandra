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
package org.apache.cassandra.utils.concurrent;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

/**
 * Allows to execute actions guarded by a lock, either in non-blocking, without actually blocking for the
 * lock to be available, or blocking fashion.
 */
public class ExecutableLock
{
    private final Queue<AsyncAction> queue = new ConcurrentLinkedQueue<>();
    private final Semaphore lock;

    public ExecutableLock()
    {
        this.lock = new Semaphore(1);
    }

    public ExecutableLock(Semaphore lock)
    {
        this.lock = lock;
    }

    /**
     * Executes the given action guarded by this lock, either synchronously if the lock is available, or asynchronously
     * on the given executor if the lock is not available, hence without ever blocking on the lock if not available.
     * <p>
     * Please note in both cases the lock will be released automatically after the future returned by the action is completed.
     *
     * @param <T> The action future return type.
     * @param action The action to execute, returning a future that will complete when the final result is available.
     * @param executor The executor to run the action asynchronously.
     * @return The future which will be completed when the action is executed and its future completes.
     */
    public <T> CompletableFuture<T> execute(Supplier<CompletableFuture<T>> action, Executor executor)
    {
        CompletableFuture<T> initiator = new CompletableFuture<>();

        tryExecute(initiator, executor);

        return initiator.thenCompose(f -> action.get())
            .whenComplete((result, error) -> unlockAndTryNext());
    }

    /**
     * Executes the given action guarded by this lock, blocking on the lock if not available.
     *
     * @param <T> The action return type.
     * @param action The action to execute.
     * @return The action result.
     */
    public <T> T executeBlocking(Callable<T> action) throws Exception
    {
        lock.acquireUninterruptibly();
        try
        {
            return action.call();
        }
        finally
        {
            unlockAndTryNext();
        }
    }

    private <T> void tryExecute(CompletableFuture<T> future, Executor executor)
    {
        queue.add(new AsyncAction<>(future, executor));
        if (lock.tryAcquire())
        {
            try
            {
                if (!future.isDone())
                    future.complete(null);
                else
                    unlockAndTryNext();
            }
            catch (Exception ex)
            {
                future.completeExceptionally(ex);
            }
        }
    }

    private void unlockAndTryNext()
    {
        lock.release();
        AsyncAction next = queue.poll();
        while (next != null)
        {
            if (!next.isDone())
            {
                next.run();
                next = null;
            }
            else
            {
                next = queue.poll();
            }
        }
    }

    private class AsyncAction<T> implements Runnable
    {
        private final CompletableFuture<T> future;
        private final Executor executor;

        AsyncAction(CompletableFuture<T> future, Executor executor)
        {
            this.future = future;
            this.executor = executor;
        }

        @Override
        public void run()
        {
            executor.execute(() -> tryExecute(future, executor));
        }

        public boolean isDone()
        {
            return future.isDone();
        }
    }
}
