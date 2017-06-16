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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.BeforeClass;
import org.junit.Test;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import org.apache.cassandra.config.DatabaseDescriptor;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.hibernate.validator.internal.util.Contracts.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class OpsTest
{
    @BeforeClass
    public static void init() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testSkippingOpDoesntOverflowStack()
    {
        final int size = 100000;
        final int result = CsFlow.fromIterable(() -> IntStream.range(0, size).iterator())
                                 .skippingMap(i -> i == size - 1 ? i : null)
                                 .blockingSingle();

        assertEquals(size -1, result);
    }

    @Test
    public void testFilterOpDoesntOverflowStack()
    {
        final int size = 100000;
        final int result = CsFlow.fromIterable(() -> IntStream.range(0, size).iterator())
                                 .filter(i -> i == size - 1)
                                 .blockingSingle();

        assertEquals(size -1, result);
    }

    @Test
    public void testGroupOpDoesntOverflowStack()
    {
        final int size = 100000;
        final List<Integer> result = CsFlow.fromIterable(() -> IntStream.range(0, size).iterator())
                                           .toList()
                                           .blockingSingle();

        assertEquals(size, result.size());
    }

    @Test
    public void testReduceToCompletable()
    {
        final AtomicInteger result = new AtomicInteger(0);
        final int size = 1000;

        CsFlow.fromIterable(() -> IntStream.range(0, size).iterator())
              .processToRxCompletable(result::addAndGet)
              .blockingAwait();

        assertEquals((size-1) * size / 2, result.get()); // n(a1 + an) / 2
    }

    @Test
    public void testReduceToCompletableWithDispose()
    {
        final AtomicInteger result = new AtomicInteger(0);
        final int size = 1000;
        final AtomicBoolean completed = new AtomicBoolean(false);
        final AtomicReference<Throwable> error = new AtomicReference<>(null);

        CsFlow.fromIterable(() -> IntStream.range(0, size).iterator())
              .processToRxCompletable(result::addAndGet)
              .subscribe(new CompletableObserver()
              {
                  public void onSubscribe(Disposable d)
                  {
                      d.dispose();
                  }

                  public void onComplete()
                  {
                      completed.set(true);
                  }

                  public void onError(Throwable e)
                  {
                      error.set(e);
                  }
              });

        // no onXXXX method should be called after disposing
        assertNull(error.get());
        assertFalse(completed.get());
    }

    @Test
    public void testReduceToCompletableWithError()
    {
        final int size = 1000;
        final AtomicBoolean completed = new AtomicBoolean(false);
        final AtomicReference<Throwable> error = new AtomicReference<>(null);

        CsFlow.fromIterable(() -> IntStream.range(0, size).iterator())
              .processToRxCompletable(i -> {throw new RuntimeException("TestException");})
              .subscribe(new CompletableObserver()
              {
                  public void onSubscribe(Disposable d)
                  {
                  }

                  public void onComplete()
                  {
                      completed.set(true);
                  }

                  public void onError(Throwable e)
                  {
                      error.set(e);
                  }
              });

        assertNotNull(error.get());
        assertEquals("TestException", error.get().getMessage());
        assertFalse(completed.get());
    }

    @Test
    public void testReduceToSingle()
    {
        final int size = 1000;
        final int result = CsFlow.fromIterable(() -> IntStream.range(0, size).iterator())
                                 .reduceToRxSingle(0, (r, i) -> r + i)
                                 .blockingGet();

        assertEquals((size-1) * size / 2, result); // n(a1 + an) / 2
    }

    @Test
    public void testReduceToSingleWithDispose()
    {
        final int size = 1000;
        final AtomicBoolean completed = new AtomicBoolean(false);
        final AtomicReference<Throwable> error = new AtomicReference<>(null);

        CsFlow.fromIterable(() -> IntStream.range(0, size).iterator())
              .reduceToRxSingle(0, (r, i) -> r + i)
              .subscribe(new SingleObserver<Integer>()
              {
                  public void onSubscribe(Disposable d)
                  {
                     d.dispose();
                  }

                  public void onSuccess(Integer integer)
                  {
                      completed.set(true);
                  }

                  public void onError(Throwable e)
                  {
                      error.set(e);
                  }
              });

        // no onXXXX method should be called after disposing
        assertNull(error.get());
        assertFalse(completed.get());
    }

    @Test
    public void testReduceToSingleWithError()
    {
        final int size = 1000;
        final AtomicBoolean completed = new AtomicBoolean(false);
        final AtomicReference<Throwable> error = new AtomicReference<>(null);

        CsFlow.fromIterable(() -> IntStream.range(0, size).iterator())
              .reduceToRxSingle(0, (r, i) -> { throw new RuntimeException("TestException"); })
              .subscribe(new SingleObserver<Integer>()
              {
                  public void onSubscribe(Disposable d)
                  {
                  }

                  public void onSuccess(Integer integer)
                  {
                      completed.set(true);
                  }

                  public void onError(Throwable e)
                  {
                      error.set(e);
                  }
              });

        assertNotNull(error.get());
        assertEquals("TestException", error.get().getMessage());
        assertFalse(completed.get());
    }

    @Test
    public void testFlatMapCompletable()
    {
        final AtomicInteger result = new AtomicInteger(0);
        final int size = 1000;

        CsFlow.fromIterable(() -> IntStream.range(0, size).iterator())
              .flatMapCompletable(i -> { result.addAndGet(i); return Completable.complete(); })
              .blockingAwait();

        assertEquals((size-1) * size / 2, result.get()); // n(a1 + an) / 2
    }

    @Test
    public void testFlatMapCompletableWithDispose()
    {
        final AtomicInteger result = new AtomicInteger(0);
        final int size = 1000;
        final AtomicBoolean completed = new AtomicBoolean(false);
        final AtomicReference<Throwable> error = new AtomicReference<>(null);

        CsFlow.fromIterable(() -> IntStream.range(0, size).iterator())
              .flatMapCompletable(i -> { result.addAndGet(i); return Completable.complete(); })
              .subscribe(new CompletableObserver()
              {
                  public void onSubscribe(Disposable d)
                  {
                      d.dispose();
                  }

                  public void onComplete()
                  {
                      completed.set(true);
                  }

                  public void onError(Throwable e)
                  {
                      error.set(e);
                  }
              });

        // no onXXXX method should be called after disposing
        assertNull(error.get());
        assertFalse(completed.get());
    }

    @Test
    public void testFlatMapCompletableWithError()
    {
        final int size = 1000;
        final AtomicBoolean completed = new AtomicBoolean(false);
        final AtomicReference<Throwable> error = new AtomicReference<>(null);

        CsFlow.fromIterable(() -> IntStream.range(0, size).iterator())
              .flatMapCompletable(i -> {throw new RuntimeException("TestException");})
              .subscribe(new CompletableObserver()
              {
                  public void onSubscribe(Disposable d)
                  {
                  }

                  public void onComplete()
                  {
                      completed.set(true);
                  }

                  public void onError(Throwable e)
                  {
                      error.set(e);
                  }
              });

        assertNotNull(error.get());
        assertEquals("TestException", error.get().getMessage());
        assertFalse(completed.get());
    }

    // test that doOnClose and doOnError execute as expected
    @Test
    public void testDoOnOperations() throws Exception
    {
        final AtomicReference<Throwable> error = new AtomicReference<>(null);
        final AtomicBoolean closed = new AtomicBoolean(false);

        CsFlow.fromIterable(() -> IntStream.range(0, 10).iterator())
              .doOnClose(() -> assertTrue(closed.compareAndSet(false, true)))
              .doOnError(e -> assertTrue(error.compareAndSet(null, e))).countBlocking();

        assertTrue(closed.get());
        assertNull(error.get());

        closed.set(false);

        try
        {
            CsFlow.fromIterable(() -> IntStream.range(0, 10).iterator())
                  .map(i ->
                       {
                           if (i == 5) throw new RuntimeException("Test ex");
                           return i;
                       })
                  .doOnClose(() -> assertTrue(closed.compareAndSet(false, true)))
                  .doOnError(e -> assertTrue(error.compareAndSet(null, e))).countBlocking();

            fail("Exception expected");
        }
        catch (Throwable t)
        {
            assertEquals("Test ex", t.getMessage());  // expected
        }

        assertTrue(closed.get());
        assertNotNull(error.get());
    }

    // Test that we can replace the first exception with the second one by using onErrorResumeNext
    @Test
    public void testOnErrorResumeNext() throws Exception
    {
        final RuntimeException ex1 = new RuntimeException("Initial error");
        final RuntimeException ex2 = new RuntimeException("On error resumed");

        final AtomicReference<Throwable> error = new AtomicReference<>(null);
        final AtomicBoolean completed = new AtomicBoolean(false);

        final CsSubscription subscription = CsFlow.fromIterable(() -> IntStream.range(0, 1).iterator())
                                                  .onErrorResumeNext(e -> CsFlow.error(ex2)).subscribe(new CsSubscriber<Integer>()
        {
            public void onNext(Integer item)
            {
                throw ex1;
            }

            public void onComplete()
            {
                completed.set(true);
            }

            public void onError(Throwable t)
            {
                error.set(t);
            }
        });

        subscription.request();

        assertFalse(completed.get());
        assertNotNull(error.get());
        assertEquals(ex2.getMessage(), error.get().getMessage());
    }


}
