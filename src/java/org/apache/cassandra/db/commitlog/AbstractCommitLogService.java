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
package org.apache.cassandra.db.commitlog;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import io.reactivex.*;
import io.reactivex.subjects.BehaviorSubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;
import org.apache.cassandra.utils.NoSpamLogger;

public abstract class AbstractCommitLogService
{
    private Thread thread;
    private volatile boolean shutdown = false;

    // all Allocations written before this time will be synced
    protected volatile long lastSyncedAt = System.currentTimeMillis();

    // counts of total written, and pending, log messages
    private final AtomicLong written = new AtomicLong(0);
    protected final AtomicLong pending = new AtomicLong(0);

    private BehaviorSubject<Long> syncTimePublisher = BehaviorSubject.createDefault(0L);

    final CommitLog commitLog;
    private final String name;
    private final long pollIntervalNanos;

    private static final Logger logger = LoggerFactory.getLogger(AbstractCommitLogService.class);

    /**
     * CommitLogService provides a fsync service for Allocations, fulfilling either the
     * Batch or Periodic contract.
     *
     * Subclasses may be notified when a sync finishes by using the syncComplete WaitQueue.
     */
    AbstractCommitLogService(final CommitLog commitLog, final String name, final long pollIntervalMillis)
    {
        this.commitLog = commitLog;
        this.name = name;
        this.pollIntervalNanos = TimeUnit.NANOSECONDS.convert(pollIntervalMillis, TimeUnit.MILLISECONDS);
    }

    // Separated into individual method to ensure relevant objects are constructed before this is started.
    void start()
    {
        if (pollIntervalNanos < 1)
            throw new IllegalArgumentException(String.format("Commit log flush interval must be positive: %fms",
                                                             pollIntervalNanos * 1e-6));

        Runnable runnable = new Runnable()
        {
            public void run()
            {
                long firstLagAt = 0;
                long totalSyncDuration = 0; // total time spent syncing since firstLagAt
                long syncExceededIntervalBy = 0; // time that syncs exceeded pollInterval since firstLagAt
                int lagCount = 0;
                int syncCount = 0;

                while (true)
                {
                    // always run once after shutdown signalled
                    boolean shutdownRequested = shutdown;

                    try
                    {
                        // sync and signal
                        long syncStarted = System.nanoTime();
                        // This is a target for Byteman in CommitLogSegmentManagerTest
                        commitLog.sync();
                        lastSyncedAt = syncStarted;

                        syncTimePublisher.onNext(syncStarted);

                        // sleep any time we have left before the next one is due
                        long now = System.nanoTime();
                        long wakeUpAt = syncStarted + pollIntervalNanos;
                        if (wakeUpAt < now)
                        {
                            // if we have lagged noticeably, update our lag counter
                            if (firstLagAt == 0)
                            {
                                firstLagAt = now;
                                totalSyncDuration = syncExceededIntervalBy = syncCount = lagCount = 0;
                            }
                            syncExceededIntervalBy += now - wakeUpAt;
                            lagCount++;
                        }
                        syncCount++;
                        totalSyncDuration += now - syncStarted;

                        if (firstLagAt > 0)
                        {
                            //Only reset the lag tracking if it actually logged this time
                            boolean logged = NoSpamLogger.log(logger,
                                                              NoSpamLogger.Level.WARN,
                                                              5,
                                                              TimeUnit.MINUTES,
                                                              "Out of {} commit log syncs over the past {}s with average duration of {}ms, {} have exceeded the configured commit interval by an average of {}ms",
                                                              syncCount,
                                                              String.format("%.2f", (now - firstLagAt) * 1e-9d),
                                                              String.format("%.2f", totalSyncDuration * 1e-6d / syncCount),
                                                              lagCount,
                                                              String.format("%.2f", syncExceededIntervalBy * 1e-6d / lagCount));
                           if (logged)
                               firstLagAt = 0;
                        }

                        if (shutdownRequested)
                            return;

                        if (wakeUpAt > now)
                            LockSupport.parkNanos(wakeUpAt - now);
                    }
                    catch (Throwable t)
                    {
                        if (!CommitLog.handleCommitError("Failed to persist commits to disk", t))
                        {
                            syncTimePublisher.onError(t);
                            break;
                        }

                        // sleep for full poll-interval after an error, so we don't spam the log file
                        LockSupport.parkNanos(pollIntervalNanos);
                    }
                }
            }
        };

        shutdown = false;
        thread = NamedThreadFactory.createThread(runnable, name);
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Block for @param alloc to be sync'd as necessary, and handle bookkeeping
     */
    public Completable finishWriteFor(Allocation alloc, Scheduler observeOn)
    {
        return maybeWaitForSync(alloc, observeOn).doOnComplete(() -> written.incrementAndGet());
    }

    protected abstract Completable maybeWaitForSync(Allocation alloc, Scheduler observeOn);

    /**
     * Request an additional sync cycle without blocking.
     */
    public void requestExtraSync()
    {
        LockSupport.unpark(thread);
    }

    public void shutdown()
    {
        shutdown = true;
        requestExtraSync();
    }

    /**
     * Request sync and wait until the current state is synced.
     *
     * Note: If a sync is in progress at the time of this request, the call will return after both it and a cycle
     * initiated immediately afterwards complete.
     */
    public void syncBlocking()
    {
        long requestTime = System.nanoTime();
        requestExtraSync();
        awaitSyncAt(requestTime).blockingGet();
    }

    Completable awaitSyncAt(long syncTime)
    {
        return syncTimePublisher.filter(v -> v >= syncTime)
                                .first(0L)
                                .toCompletable();
    }

    public void awaitTermination() throws InterruptedException
    {
        thread.join();
    }

    public long getCompletedTasks()
    {
        return written.get();
    }

    public long getPendingTasks()
    {
        return pending.get();
    }
}
