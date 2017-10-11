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

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import io.netty.util.concurrent.AbstractScheduledEventExecutor;

/**
 * This class is responsible for keeping tabs on threads that register as a {@link MonitorableThread}.
 *
 * When a {@link MonitorableThread} parks itself this thread wakes it up when work is ready.
 * This allows Threads to (for example) use a non-blocking queue without constantly spinning.
 *
 * The watcher will call {@link MonitorableThread#shouldUnpark(long)} and when that
 * method returns true, unparks the thread.  When the thread has no work left it parks itself.
 *
 * It's upto the {@link MonitorableThread#shouldUnpark(long)} implementation to track its own state.
 *
 */
public class WatcherThread
{
    public static final Supplier<WatcherThread> instance = Suppliers.memoize(WatcherThread::new);

    private final Thread watcherThread;
    private volatile boolean shutdown;
    private final CopyOnWriteArrayList<MonitorableThread> monitoredThreads;
    private final CopyOnWriteArrayList<Runnable> loopActions;

    private WatcherThread()
    {
        this.shutdown = false;
        this.monitoredThreads = new CopyOnWriteArrayList<>();
        this.loopActions = new CopyOnWriteArrayList<>();

        watcherThread = new Thread(() -> {
            while (!shutdown)
            {
                long nanoTime = TPC.nanoTimeSinceStartup();
                for (MonitorableThread thread : monitoredThreads)
                    if (thread.shouldUnpark(nanoTime))
                        thread.unpark();

                for (Runnable action : loopActions)
                    action.run();

                LockSupport.parkNanos(1);
            }

            for (MonitorableThread thread : monitoredThreads)
                thread.unpark();
        });

        watcherThread.setName("ThreadWatcher");
        watcherThread.setPriority(Thread.MAX_PRIORITY);
        watcherThread.setDaemon(true);
        watcherThread.start();
    }

    /**
     * Adds a collection of threads to monitor
     */
    public void addThreadsToMonitor(Collection<MonitorableThread> threads)
    {
        monitoredThreads.addAll(threads);
    }

    /**
     * Adds a thread to monitor
     */
    public void addThreadToMonitor(MonitorableThread thread)
    {
        monitoredThreads.add(thread);
    }

    /**
     * Removes a thread from monitoring
     */
    public void removeThreadToMonitor(MonitorableThread thread)
    {
        monitoredThreads.remove(thread);
    }

    /**
     * Removes a collection of threads from monitoring
     */
    public void removeThreadsToMonitor(Collection<MonitorableThread> threads)
    {
        monitoredThreads.removeAll(threads);
    }

    /**
     * Runs the specified action in each loop. Mainly used to avoid many threads doing the same work
     * over and over, example {@link org.apache.cassandra.db.monitoring.ApproximateTime}
     */
    public void addAction(Runnable action)
    {
        loopActions.add(action);
    }

    public void shutdown()
    {
        shutdown = true;
    }

    public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException
    {
        shutdown();
        watcherThread.join(timeUnit.toMillis(timeout));
        return !watcherThread.isAlive();
    }

    /**
     * Interface for threads that take their work from a non-blocking queue and
     * wish to be watched by the {@link WatcherThread}
     *
     * When a Thread has no work todo it should park itself and
     */
    public static interface MonitorableThread
    {
        /**
         * What a MonitorableThread should use to track it's current state
         */
        enum ThreadState
        {
            PARKED,
            WORKING
        }

        /**
         * Will unpark a thread in a parked state.
         * Called by {@link WatcherThread}
         */
        void unpark();

        /**
         * Called continuously by the {@link WatcherThread}
         * to decide if unpark() should be called on a Thread
         *
         * Should return true IFF the thread is parked and there is work
         * to be done when the thread is unparked.
         */
        boolean shouldUnpark(long nanoTimeSinceStart);
    }
}
