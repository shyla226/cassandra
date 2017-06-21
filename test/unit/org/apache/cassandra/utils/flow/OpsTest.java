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
import org.apache.cassandra.utils.LineNumberInference;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.hibernate.validator.internal.util.Contracts.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class OpsTest
{
    @BeforeClass
    public static void init() throws Exception
    {
        LineNumberInference.init();
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
              .reduce(result::addAndGet)
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
              .reduce(result::addAndGet)
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
              .reduce(i -> {throw new RuntimeException("TestException");})
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
                                 .reduce(0, (r, i) -> r + i)
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
              .reduce(0, (r, i) -> r + i)
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
              .reduce(0, (r, i) -> { throw new RuntimeException("TestException"); })
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
}
