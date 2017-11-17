/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.memory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.BeforeClass;
import org.junit.Test;

import io.reactivex.Single;
import junit.framework.Assert;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.metrics.Timer;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.OpOrderSimple;

public class NativeAllocatorTest
{

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    CountDownLatch canClean;
    CountDownLatch isClean;

    @Test
    public void testBookKeeping() throws ExecutionException, InterruptedException
    {
        {
            final ScheduledExecutorService exec = Executors.newScheduledThreadPool(2);
            final StagedScheduler scheduler = TPC.ioScheduler();
            final OpOrder order = new OpOrderSimple();
            final OpOrder.Group group = order.start();
            canClean = new CountDownLatch(1);
            isClean = new CountDownLatch(1);
            CompletableFuture<Void> finished = new CompletableFuture<>();
            final AtomicReference<NativeAllocator> allocatorRef = new AtomicReference<>();
            final AtomicReference<OpOrder.Barrier> barrier = new AtomicReference<>();
            final NativeAllocator allocator = new NativeAllocator(new NativePool(1, 100, 0.75f, new Runnable()
            {
                public void run()
                {
                    try
                    {
                        canClean.await();
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError();
                    }
                    if (isClean.getCount() > 0)
                    {
                        allocatorRef.get().offHeap().released(80);
                        isClean.countDown();
                    }
                }
            }), -1);
            allocatorRef.set(allocator);
            final Runnable markBlocking = new Runnable()
            {

                public void run()
                {
                    barrier.set(order.newBarrier());
                    barrier.get().issue();
                    barrier.get().markBlocking();
                }
            };
            final Runnable run = new Runnable()
            {
                public void run()
                {
                    try
                    {
                        wrappedRun();
                        finished.complete(null);
                    }
                    catch (Throwable t)
                    {
                        finished.completeExceptionally(t);
                    }
                }

                public void wrappedRun() throws Throwable
                {
                    // allocate normal, check accounted and not cleaned
                    allocator.allocate(10);
                    Assert.assertEquals(10, allocator.offHeap().owns());
                    // confirm adjustment works
                    allocator.offHeap().adjust(-10);
                    Assert.assertEquals(0, allocator.offHeap().owns());
                    allocator.offHeap().adjust(10);
                    Assert.assertEquals(10, allocator.offHeap().owns());
                    // confirm we cannot allocate negative
                    boolean success = false;
                    try
                    {
                        allocator.offHeap().allocated(-10);
                    }
                    catch (AssertionError e)
                    {
                        success = true;
                    }
                    Assert.assertTrue(success);
                    Uninterruptibles.sleepUninterruptibly(10L, TimeUnit.MILLISECONDS);
                    Assert.assertEquals(1, isClean.getCount());

                    Callable<Single<Integer>> single = () -> Single.just(0);

                    AtomicBoolean completed = new AtomicBoolean(false);
                    AtomicReference<Throwable> error = new AtomicReference<>(null);

                    // Check if allowed to continue
                    allocator.whenBelowLimits(single, group, scheduler, TPCTaskType.UNKNOWN)
                             .subscribe(aVoid -> completed.set(true),
                                        t -> error.set(t));
                    Assert.assertTrue(completed.get());

                    // allocate above watermark, check cleaned
                    allocator.allocate(70);
                    Assert.assertEquals(80, allocator.offHeap().owns());
                    canClean.countDown();
                    Uninterruptibles.awaitUninterruptibly(isClean,10L, TimeUnit.MILLISECONDS);
                    Assert.assertEquals(0, isClean.getCount());
                    Assert.assertEquals(0, allocator.offHeap().owns());

                    final Timer blockedTimer = Memtable.MEMORY_POOL.blockedOnAllocating;
                    long blockedCount = blockedTimer.getCount();
                    double blockedAverage = blockedTimer.getSnapshot().getMean();
                    System.out.format("Blocked count %d average time %.3f\n", blockedCount, blockedAverage);

                    isClean = new CountDownLatch(1);
                    canClean = new CountDownLatch(1);

                    // allocate above limit
                    allocator.allocate(110);

                    completed.set(false);

                    allocator.whenBelowLimits(single, group, scheduler, TPCTaskType.UNKNOWN)
                             .subscribe(aVoid -> completed.set(true),
                                        t -> error.set(t));

                    // check not ready
                    Assert.assertFalse(completed.get());
                    Throwables.maybeFail(error.get());
                    Assert.assertEquals(1, TPC.metrics().activeTaskCount(TPCTaskType.WRITE_POST_MEMTABLE_FULL));

                    // mark barrier blocking, which should allow the completable to continue
                    exec.schedule(markBlocking, 17L, TimeUnit.MILLISECONDS);

                    // wait for completion
                    while (!completed.get() && error.get() == null)
                        Thread.yield();

                    // check completable released
                    Throwables.maybeFail(error.get());
                    Assert.assertTrue(completed.get());

                    // Wait a little to let the whenBelowLimit task complete
                    Uninterruptibles.sleepUninterruptibly(10L, TimeUnit.MILLISECONDS);
                    Assert.assertEquals(0, TPC.metrics().activeTaskCount(TPCTaskType.WRITE_POST_MEMTABLE_FULL));
                    Assert.assertEquals(1, TPC.metrics().completedTaskCount(TPCTaskType.WRITE_POST_MEMTABLE_FULL));

                    // Check time measured in blockedOnAllocating stats
                    Uninterruptibles.sleepUninterruptibly(DatabaseDescriptor.getMetricsHistogramUpdateTimeMillis(), TimeUnit.MILLISECONDS); // wait for histograms to aggregate
                    long currBlockedCount = blockedTimer.getCount();
                    double currBlockedAverage = blockedTimer.getSnapshot().getMean();
                    System.out.format("Blocked count %d average time %.3f\n", currBlockedCount, currBlockedAverage);
                    Assert.assertTrue(currBlockedCount > blockedCount);
                    Assert.assertTrue(currBlockedAverage > TimeUnit.MILLISECONDS.toNanos(16)); // leave a little leeway to avoid flakes
                    blockedCount = currBlockedCount;


                    // check barrier is done
                    Assert.assertNotNull(barrier.get());
                    Assert.assertEquals(110, allocator.offHeap().owns());

                    group.close();

                    completed.set(false);

                    OpOrder.Group group2 = order.start();
                    allocator.whenBelowLimits(single, group2, scheduler, TPCTaskType.UNKNOWN)
                             .subscribe(aVoid -> completed.set(true),
                                        t -> error.set(t));

                    // check not ready
                    Assert.assertFalse(completed.get());
                    Throwables.maybeFail(error.get());
                    Assert.assertEquals(1, TPC.metrics().activeTaskCount(TPCTaskType.WRITE_POST_MEMTABLE_FULL));

                    Uninterruptibles.sleepUninterruptibly(34L, TimeUnit.MILLISECONDS);

                    // release some space
                    canClean.countDown();
                    Uninterruptibles.awaitUninterruptibly(isClean, 10, TimeUnit.MILLISECONDS);

                    // wait for completion
                    while (!completed.get() && error.get() == null)
                        Thread.yield();

                    // check completable released
                    Throwables.maybeFail(error.get());
                    Assert.assertTrue(completed.get());

                    // Wait a little to let the whenBelowLimit task complete
                    Uninterruptibles.sleepUninterruptibly(10L, TimeUnit.MILLISECONDS);
                    Assert.assertEquals(0, TPC.metrics().activeTaskCount(TPCTaskType.WRITE_POST_MEMTABLE_FULL));
                    Assert.assertEquals(2, TPC.metrics().completedTaskCount(TPCTaskType.WRITE_POST_MEMTABLE_FULL));

                    Assert.assertEquals(30, allocator.offHeap().owns());

                    // Check time measured in blockedOnAllocating stats
                    Uninterruptibles.sleepUninterruptibly(DatabaseDescriptor.getMetricsHistogramUpdateTimeMillis(), TimeUnit.MILLISECONDS); // wait for histograms to aggregate
                    currBlockedCount = blockedTimer.getCount();
                    currBlockedAverage = blockedTimer.getSnapshot().getMean();
                    System.out.format("Blocked count %d average time %.3f\n", currBlockedCount, currBlockedAverage);
                    Assert.assertTrue(currBlockedCount > blockedCount);
                    Assert.assertTrue(currBlockedAverage > TimeUnit.MILLISECONDS.toNanos(24)); // leave a little leeway to avoid flakes
                }
            };
            scheduler.execute(run, TPCTaskType.UNKNOWN);
            finished.join();
        }
    }

}
