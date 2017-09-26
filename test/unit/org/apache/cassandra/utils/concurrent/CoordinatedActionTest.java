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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.utils.TestTimeSource;
import org.apache.cassandra.utils.TimeSource;

public class CoordinatedActionTest
{
    private TimeSource timeSource;

    @Before
    public void before()
    {
        timeSource = new TestTimeSource();
    }

    @Test
    public void testCoordinatedExecution() throws Exception
    {
        AtomicInteger counter = new AtomicInteger();
        Supplier<CompletableFuture<Void>> supplier = () -> CompletableFuture.completedFuture(null).thenRun(() -> counter.incrementAndGet());

        CoordinatedAction action = new CoordinatedAction(supplier, 2, timeSource.currentTimeMillis(), 1, TimeUnit.SECONDS, timeSource);

        // The first future is not done yet as only invoked once:
        CompletableFuture<Void> future1 = action.get();
        Assert.assertFalse(future1.isDone());
        // Invoke a second time:
        CompletableFuture<Void> future2 = action.get();
        // Now both futures are done:
        Assert.assertTrue(future1.isDone());
        Assert.assertTrue(future2.isDone());
        Assert.assertNull(future1.get());
        Assert.assertNull(future2.get());
        // The counter has been incremented only once because only a single action executes the actual action:
        Assert.assertEquals(1, counter.get());
    }

    @Test
    public void testTimeout() throws Exception
    {
        AtomicInteger counter = new AtomicInteger();
        Supplier<CompletableFuture<Void>> supplier = () -> CompletableFuture.completedFuture(null).thenRun(() -> counter.incrementAndGet());

        CoordinatedAction action = new CoordinatedAction(supplier, 2, timeSource.currentTimeMillis(), 1, TimeUnit.SECONDS, timeSource);

        // Invoke the first time:
        CompletableFuture<Void> future1 = action.get();
        // Advance time by timeout:
        timeSource.sleepUninterruptibly(1, TimeUnit.SECONDS);
        // Invoke the second time, its future is completed exceptionally straight away due to timeout:
        CompletableFuture<Void> future2 = action.get();
        Assert.assertTrue(future2.isCompletedExceptionally());
        // Both futures are completed exceptionally:
        Assert.assertTrue(future1.isCompletedExceptionally());
        Assert.assertTrue(future2.isCompletedExceptionally());
        // The counter has not been incremented:
        Assert.assertEquals(0, counter.get());
    }

    @Test
    public void testException() throws Exception
    {
        Supplier<CompletableFuture<Void>> supplier = () -> CompletableFuture.completedFuture(null).thenRun(() -> { throw new RuntimeException("expected"); });

        CoordinatedAction action = new CoordinatedAction(supplier, 2, timeSource.currentTimeMillis(), 1, TimeUnit.SECONDS, timeSource);

        // Invoke the first time:
        CompletableFuture<Void> future1 = action.get();
        // Invoke the second time:
        CompletableFuture<Void> future2 = action.get();
        // Both futures are completed exceptionally:
        Assert.assertTrue(future1.isCompletedExceptionally());
        Assert.assertTrue(future2.isCompletedExceptionally());
        // Verify exceptions:
        try
        {
            future1.get();
        }
        catch(ExecutionException ex)
        {
            Assert.assertEquals("expected", ex.getCause().getMessage());
        }
        try
        {
            future2.get();
        }
        catch(ExecutionException ex)
        {
            Assert.assertEquals("expected", ex.getCause().getMessage());
        }
    }
}
