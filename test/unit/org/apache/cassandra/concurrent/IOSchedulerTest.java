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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.DisposableContainer;
import io.reactivex.internal.disposables.EmptyDisposable;
import io.reactivex.internal.schedulers.ScheduledRunnable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Throwables;

import static org.junit.Assert.*;

public class IOSchedulerTest
{
    private IOScheduler scheduler;
    private volatile CountDownLatch latch;
    private volatile Throwable errors = null;
    private volatile int numChecks = 0;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void tearDown()
    {
        latch = null;
        errors = null;
        numChecks = 0;
        scheduler.shutdown();
    }

    @Test
    public void testIsOnScheduler() throws InterruptedException
    {
        scheduler = new IOScheduler();
        assertFalse(scheduler.toString().isEmpty());

        assertFalse(scheduler.isOnScheduler(Thread.currentThread()));

        int NUM_CHECKS = 5;
        latch = new CountDownLatch(NUM_CHECKS);

        scheduler.scheduleDirect(() -> check(() -> assertTrue(scheduler.isOnScheduler(Thread.currentThread()))));

        scheduler.scheduleDirect(() -> check(() -> assertTrue(scheduler.isOnScheduler(Thread.currentThread()))),
                                 1,
                                 TimeUnit.MILLISECONDS);

        scheduler.scheduleDirect(() -> check(() -> assertTrue(scheduler.isOnScheduler(Thread.currentThread()))),
                                 TPCTaskType.UNKNOWN,
                                 1,
                                 TimeUnit.MILLISECONDS);

        scheduler.execute(() -> check(() -> assertTrue(scheduler.isOnScheduler(Thread.currentThread()))),
                          ExecutorLocals.create(),
                          TPCTaskType.UNKNOWN);

        Scheduler.Worker worker = scheduler.createWorker();
        assertNotNull(worker);
        worker.schedule(() -> check(() -> assertTrue(scheduler.isOnScheduler(Thread.currentThread()))));

        latch.await(1, TimeUnit.MINUTES);
        assertNull(errors);
        assertEquals(NUM_CHECKS, numChecks);
    }

    @Test
    public void testWorkerCached()
    {
        scheduler = new IOScheduler();
        assertEquals(0, scheduler.numCachedWorkers());

        for (int i = 0; i < 5; i++)
        {
            Scheduler.Worker worker = scheduler.createWorker();
            assertEquals(0, scheduler.numCachedWorkers());

            worker.dispose();
            assertTrue(worker.schedule(() -> {}).isDisposed());
            assertEquals(1, scheduler.numCachedWorkers());
        }
    }

    @Test
    public void testWorkersCachedAndReleased()
    {
        TestWorker testWorker = new TestWorker();
        scheduler = new IOScheduler(threadFactory -> testWorker);
        assertEquals(0, scheduler.numCachedWorkers());

        Scheduler.Worker[] workers = new Scheduler.Worker[IOScheduler.MAX_POOL_SIZE + 1];
        for (int i = 0; i < workers.length; i++)
        {
            workers[i] = scheduler.createWorker();
            assertEquals(0, scheduler.numCachedWorkers());
        }

        for (Scheduler.Worker worker : workers)
            worker.dispose();

        assertEquals(IOScheduler.MAX_POOL_SIZE, scheduler.numCachedWorkers()); // only MAX POOL size cached...
        assertEquals(IOScheduler.MAX_POOL_SIZE, testWorker.tasks.size()); // ...and scheduled for checking later on
        assertEquals(workers.length - IOScheduler.MAX_POOL_SIZE, testWorker.numTimesDisposed); // remaining tasks alredy disposed

        for (Runnable disposeTask : testWorker.tasks)
            disposeTask.run();

        assertEquals(IOScheduler.MIN_POOL_SIZE, scheduler.numCachedWorkers());
        assertEquals(workers.length - IOScheduler.MIN_POOL_SIZE, testWorker.numTimesDisposed); // all tasks disposed except for MIN POOL SIZE
    }

    @Test
    public void testWorkersDisposedWithAllCalls() throws InterruptedException
    {
        TestWorker testWorker = new TestWorker();
        scheduler = new IOScheduler(threadFactory -> testWorker);

        scheduler.execute(() -> numChecks++, ExecutorLocals.create(), TPCTaskType.UNKNOWN);
        assertEquals(2, testWorker.tasks.size());
        testWorker.tasks.remove(0).run(); // the first task is the user action
        assertEquals(1, scheduler.numCachedWorkers());
        assertEquals(1, numChecks);

        scheduler.scheduleDirect(() -> numChecks++, 1, TimeUnit.MICROSECONDS);
        assertEquals(2, testWorker.tasks.size());
        testWorker.tasks.remove(1).run(); // the second task is the user action
        assertEquals(2, numChecks);

        assertEquals(1, scheduler.numCachedWorkers());

        for (Runnable disposeTask : testWorker.tasks)
            disposeTask.run();

        assertEquals(0, testWorker.numTimesDisposed);
        assertEquals(1, scheduler.numCachedWorkers());
    }

    @Test
    public void testShutdown()
    {
        TestWorker worker = new TestWorker();
        scheduler = new IOScheduler((factory) -> worker);

        scheduler.createWorker().dispose();
        assertEquals(1, worker.tasks.size());
        assertEquals(1, scheduler.numCachedWorkers());
        worker.tasks.remove(0).run();
        assertEquals(1, scheduler.numCachedWorkers());
        assertEquals(0, worker.numTimesDisposed);

        scheduler.shutdown();
        assertEquals(1, worker.numTimesDisposed);
        assertEquals(0, scheduler.numCachedWorkers());
    }

    @Test
    public void testRestart()
    {
        TestWorker worker = new TestWorker();
        scheduler = new IOScheduler((factory) -> worker);

        scheduler.createWorker().dispose();
        assertEquals(1, worker.tasks.size());
        assertEquals(1, scheduler.numCachedWorkers());
        worker.tasks.remove(0).run();
        assertEquals(1, scheduler.numCachedWorkers());
        assertEquals(0, worker.numTimesDisposed);

        scheduler.shutdown();
        assertEquals(1, worker.numTimesDisposed);
        assertEquals(0, scheduler.numCachedWorkers());

        scheduler.start();
        scheduler.createWorker().dispose();
        assertEquals(1, scheduler.numCachedWorkers());
    }

    private synchronized void check(Runnable runnable)
    {
        errors = Throwables.perform(errors, runnable::run);
        numChecks++;
        latch.countDown();
    }

    private final static class TestWorker extends ExecutorBasedWorker
    {
        List<Runnable> tasks = new ArrayList<>();
        int numTimesDisposed;

        TestWorker()
        {
            super(null, false);
        }

        @Override
        public Executor getExecutor()
        {
            return cmd -> tasks.add(cmd);
        }

        @Override
        public Disposable scheduleDirect(Runnable runnable, long delay, TimeUnit unit)
        {
            tasks.add(runnable);
            return EmptyDisposable.INSTANCE;
        }

        @Override
        public ScheduledRunnable scheduleActual(final Runnable runnable, long delayTime, TimeUnit unit, DisposableContainer parent)
        {
            ScheduledRunnable ret = new ScheduledRunnable(runnable, parent);
            tasks.add(ret);
            return ret;
        }

        @Override
        public void dispose()
        {
            numTimesDisposed++;
        }
    }
}
