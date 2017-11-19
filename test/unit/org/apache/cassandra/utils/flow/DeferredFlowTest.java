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

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DeferredFlowTest
{
    @BeforeClass
    public static void init() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testRequestAfterSource() throws Exception
    {
        final int size = 100000;
        final Flow<Integer> source = Flow.fromIterable(() -> IntStream.range(0, size).iterator());

        final DeferredFlow<Integer> deferred = DeferredFlow.createWithTimeout(TimeUnit.SECONDS.toNanos(1));
        deferred.onSource(source);
        assertTrue(deferred.hasSource());

        final long res = deferred.countBlocking();
        assertEquals(size, res);
    }

    @Test
    public void testRequestBeforeSource() throws Exception
    {
        final int size = 100000;
        final Flow<Integer> source = Flow.fromIterable(() -> IntStream.range(0, size).iterator());

        final DeferredFlow<Integer> deferred = DeferredFlow.createWithTimeout(TimeUnit.SECONDS.toNanos(1));
        TPC.bestTPCScheduler().scheduleDirect(() -> deferred.onSource(source), 10, TimeUnit.MILLISECONDS);

        final long res = deferred.countBlocking();
        assertEquals(size, res);
    }

    @Test
    public void testRequestConcurrentlyWithSource() throws Exception
    {
        final int attempts = 100;
        final int size = 100000;

        for (int i = 0; i < attempts; i++)
        {
            final Flow<Integer> source = Flow.fromIterable(() -> IntStream.range(0, size).iterator());
            final DeferredFlow<Integer> deferred = DeferredFlow.createWithTimeout(TimeUnit.SECONDS.toNanos(1));

            CyclicBarrier semaphore = new CyclicBarrier(2);
            AtomicReference<Throwable> error = new AtomicReference<>(null);

            Thread t1 = NamedThreadFactory.createThread(() -> {
                try
                {
                    semaphore.await();
                    deferred.onSource(source);
                }
                catch (Throwable t)
                {
                    error.compareAndSet(null, t);
                }
            });

            Thread t2 = NamedThreadFactory.createThread(() -> {
                try
                {
                    semaphore.await();
                    assertEquals(size, deferred.countBlocking());
                }
                catch (Throwable t)
                {
                    error.compareAndSet(null, t);
                }
            });

            t1.start();
            t2.start();

            t1.join();
            t2.join();

            assertNull(error.get());

        }
    }

    @Test
    public void testCloseConcurrentlyWithSource() throws Exception
    {
        final int attempts = 100;
        final int size = 100000;

        for (int i = 0; i < attempts; i++)
        {
            final Flow<Integer> source = Flow.fromIterable(() -> IntStream.range(0, size).iterator());
            final DeferredFlow<Integer> deferred = DeferredFlow.createWithTimeout(TimeUnit.SECONDS.toNanos(1));

            CyclicBarrier semaphore = new CyclicBarrier(2);
            AtomicReference<Throwable> error = new AtomicReference<>(null);

            Thread t1 = NamedThreadFactory.createThread(() -> {
                try
                {
                    semaphore.await();
                    deferred.onSource(source);
                }
                catch (Throwable t)
                {
                    error.compareAndSet(null, t);
                }
            });

            Thread t2 = NamedThreadFactory.createThread(() -> {
                try
                {
                    semaphore.await();
                    OpsTest.subscribeAndClose(deferred);
                }
                catch (Throwable t)
                {
                    error.compareAndSet(null, t);
                }
            });

            t1.start();
            t2.start();

            t1.join();
            t2.join();

            if (error.get() != null)
                error.get().printStackTrace();
            assertNull(error.get());

        }
    }

    @Test
    public void testTimeout() throws Exception
    {
        final DeferredFlow<Integer> deferred = DeferredFlow.create(System.nanoTime() + TimeUnit.SECONDS.toNanos(1),
                                                                   () -> TPC.bestTPCScheduler(),
                                                                   () -> Flow.error(new TimeoutException()));

        try
        {
            deferred.countBlocking();
            fail("Expected to fail with timeout exception");
        }
        catch (Throwable t)
        {
            if (t instanceof RuntimeException && t.getCause() != null)
                t = t.getCause();

            assertTrue(String.format("Got %s, expected timeout exception", t.getClass()),
                       t instanceof TimeoutException);
        }
    }

    @Test
    public void testError() throws Exception
    {
        final DeferredFlow<Integer> deferred = DeferredFlow.createWithTimeout(TimeUnit.SECONDS.toNanos(1));
        final RuntimeException testException = new RuntimeException("Test exception");
        TPC.bestTPCScheduler().scheduleDirect(() -> deferred.onSource(Flow.error(testException)));

        try
        {
            deferred.countBlocking();
            fail("Expected to fail with test exception");
        }
        catch (Throwable t)
        {
            if (t instanceof RuntimeException && t.getCause() != null)
                t = t.getCause();

            assertEquals(String.format("Got %s, expected %s", t, testException), testException, t);
        }
    }
}
