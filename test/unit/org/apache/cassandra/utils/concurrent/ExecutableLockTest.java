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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.util.concurrent.MoreExecutors;

import org.junit.Assert;
import org.junit.Test;

public class ExecutableLockTest
{
    @Test
    public void testNormalExecution() throws Exception
    {
        Supplier<CompletableFuture<String>> action = () -> CompletableFuture.completedFuture("done");
        Executor executor = MoreExecutors.directExecutor();

        ExecutableLock lock = new ExecutableLock();
        CompletableFuture<String> future = lock.execute(action, executor);
        Assert.assertTrue(future.isDone());
        Assert.assertFalse(future.isCompletedExceptionally());
        Assert.assertEquals("done", future.get(10, TimeUnit.SECONDS));
    }

    @Test
    public void testExceptionalExecution() throws Exception
    {
        Supplier<CompletableFuture<String>> action = () ->
        {
            CompletableFuture<String> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("expected"));
            return future;
        };
        Executor executor = MoreExecutors.directExecutor();

        ExecutableLock lock = new ExecutableLock();
        CompletableFuture<String> future = lock.execute(action, executor);
        Assert.assertTrue(future.isDone());
        Assert.assertTrue(future.isCompletedExceptionally());

        try
        {
            future.get(10, TimeUnit.SECONDS);
            Assert.fail("Expected exception!");
        }
        catch (ExecutionException ex)
        {
            Assert.assertEquals("expected", ex.getCause().getMessage());
        }
    }

    @Test
    public void testMultipleExecutions() throws Exception
    {
        Executor executor = MoreExecutors.directExecutor();

        ExecutableLock lock = new ExecutableLock();

        Supplier<CompletableFuture<String>> action = () -> CompletableFuture.completedFuture("done1");
        CompletableFuture<String> future = lock.execute(action, executor);
        Assert.assertTrue(future.isDone());
        Assert.assertEquals("done1", future.get(10, TimeUnit.SECONDS));

        action = () -> CompletableFuture.completedFuture("done2");
        future = lock.execute(action, executor);
        Assert.assertTrue(future.isDone());
        Assert.assertEquals("done2", future.get(10, TimeUnit.SECONDS));
    }

    @Test
    public void testConcurrentExecution() throws Exception
    {
        CountDownLatch startAction1 = new CountDownLatch(1);
        CountDownLatch pauseAction1 = new CountDownLatch(1);
        AtomicReference<CompletableFuture<String>> future1 = new AtomicReference<>();
        Supplier<CompletableFuture<String>> action1 = () ->
        {
            try
            {
                startAction1.countDown();
                pauseAction1.await();
                return CompletableFuture.completedFuture("done1");
            }
            catch (InterruptedException ex)
            {
                CompletableFuture<String> future = new CompletableFuture<>();
                future.completeExceptionally(ex);
                return future;
            }
        };

        Supplier<CompletableFuture<String>> action2 = () -> CompletableFuture.completedFuture("done2");

        Executor executor = MoreExecutors.directExecutor();

        ExecutableLock lock = new ExecutableLock();

        // Start action1 on a thread, as it will block on pauseAction1:
        Thread t1 = new Thread()
        {
            @Override
            public void run()
            {
                future1.set(lock.execute(action1, executor));
            }
        };
        t1.start();

        // Await for action1 to start:
        startAction1.await(10, TimeUnit.SECONDS);

        // The next try should not be able to complete the future because of the concurrently running action1:
        CompletableFuture<String> future2 = lock.execute(action2, executor);
        Assert.assertFalse(future2.isDone());

        // Unblock action1, which will then unblock action2:
        pauseAction1.countDown();

        t1.join();

        // First verify action2, then verify action1 (to be sure the reference is set:
        Assert.assertEquals("done2", future2.get(10, TimeUnit.SECONDS));
        Assert.assertEquals("done1", future1.get().get(10, TimeUnit.SECONDS));
    }

    @Test
    public void testConcurrentBlockingExecution() throws Exception
    {
        CountDownLatch startT1 = new CountDownLatch(1);
        CountDownLatch pauseT1 = new CountDownLatch(1);
        CompletableFuture<String> future1 = new CompletableFuture<>();

        Supplier<CompletableFuture<String>> action2 = () -> CompletableFuture.completedFuture("done2");

        Executor executor = MoreExecutors.directExecutor();

        ExecutableLock lock = new ExecutableLock();

        // Start thread t1 which will acquire a standard lock and pause:
        Thread t1 = new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    future1.complete(lock.executeBlocking(() ->
                    {
                        startT1.countDown();
                        pauseT1.await();
                        return "done1";
                    }));
                }
                catch (Exception ex)
                {
                    future1.completeExceptionally(ex);
                }
            }
        };
        t1.start();

        // Awaits for t1 to start:
        startT1.await(10, TimeUnit.SECONDS);

        // The next try should not be able to complete the future because of the concurrently running t1:
        CompletableFuture<String> future2 = lock.execute(action2, executor);
        Assert.assertFalse(future2.isDone());

        // Unblock t1, which will then unblock action2:
        pauseT1.countDown();

        // Verify both t1 and action2 are done:
        Assert.assertEquals("done1", future1.get(10, TimeUnit.SECONDS));
        Assert.assertEquals("done2", future2.get(10, TimeUnit.SECONDS));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExecutionWithRandomConcurrency() throws Exception
    {
        int actions = 100;
        int threads = 8;

        CountDownLatch latch = new CountDownLatch(actions);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        ExecutableLock lock = new ExecutableLock();

        AtomicReference[] futures = new AtomicReference[actions];
        for (int i = 0; i < actions; i++)
        {
            futures[i] = new AtomicReference();
        }

        // Run all actions concurrently, each one returning an integer associated to the corresponding future:
        for (int i = 0; i < actions; i++)
        {
            int nr = i;
            executor.execute(() ->
            {
                Supplier<CompletableFuture<Integer>> action = () -> CompletableFuture.completedFuture(nr);
                futures[nr].set(lock.execute(action, executor));
                latch.countDown();
            });
        }

        // Await for all futures to be collected:
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

        // Verify all futures are completed with the right integer:
        for (int i = 0; i < actions; i++)
        {
            Assert.assertEquals(i, (int) ((CompletableFuture) futures[i].get()).get(10, TimeUnit.SECONDS));
        }

        // Shutdown cleanly:
        executor.shutdown();
        Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
    }
}
