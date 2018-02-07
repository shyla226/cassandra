/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.memory;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.metrics.Timer;
import org.apache.cassandra.utils.concurrent.OpOrder;

public abstract class MemtableAllocator
{
    private final SubAllocator onHeap;
    private final SubAllocator offHeap;
    volatile LifeCycle state = LifeCycle.LIVE;

    enum LifeCycle
    {
        LIVE, DISCARDING, DISCARDED;
        LifeCycle transition(LifeCycle targetState)
        {
            switch (targetState)
            {
                case DISCARDING:
                    assert this == LifeCycle.LIVE;
                    return LifeCycle.DISCARDING;

                case DISCARDED:
                    assert this == LifeCycle.DISCARDING;
                    return LifeCycle.DISCARDED;

                default:
                    throw new IllegalStateException();
            }
        }
    }

    MemtableAllocator(SubAllocator onHeap, SubAllocator offHeap)
    {
        this.onHeap = onHeap;
        this.offHeap = offHeap;
    }

    public abstract Row.Builder rowBuilder();
    public abstract DecoratedKey clone(DecoratedKey key);
    public abstract EnsureOnHeap ensureOnHeap();

    public SubAllocator onHeap()
    {
        return onHeap;
    }

    public SubAllocator offHeap()
    {
        return offHeap;
    }

    /**
     * Defers the given Single construction, waiting for space if the memtable limits have been exceeded. If the limits
     * have not been exceeded, the single is constructed and subscribed to immediately. Otherwise the constructing
     * function is called when space has become available again or if {@code opGroup} becomes blocking. The latter is
     * done to break the deadlock when a memtable flush is waiting for said operation to complete before it can start
     * and release memory.
     *
     * Since the signals to continue are given on a thread that cannot execute any work, the construction and
     * subscription are done on the supplied scheduler.
     */
    public <T> Single<T> whenBelowLimits(Callable<Single<T>> singleCallable, OpOrder.Group opGroup, StagedScheduler observeOnScheduler, TPCTaskType nextTaskType)
    {
        return new Single<T>()
        {
            @Override
            protected void subscribeActual(SingleObserver<? super T> subscriber)
            {
                class WhenBelowLimits implements Disposable
                {
                    final AtomicReference<TPCRunnable> task = new AtomicReference<>(null);
                    final Timer.Context timerContext;

                    WhenBelowLimits()
                    {
                        // Grab the future before checking fullness so it will certainly fire if a release happens after we have checked a limit.
                        // The future is shared between the on/offHeap subpools.
                        CompletableFuture<Void> releaseFuture = onHeap.parent.releaseFuture();

                        if (opGroup.isBlocking() || onHeap.parent.belowLimit() && offHeap.parent.belowLimit())
                        {
                            observeOnScheduler.execute(this::subscribeChild, nextTaskType);
                            timerContext = null;
                            return;
                        }

                        timerContext = onHeap.parent.blockedTimerContext();

                        // Create a TPC task which tracks current locals and the number of tasks blocked by this.
                        task.set(TPCRunnable.wrap(this::subscribeChild,
                                                  ExecutorLocals.create(),
                                                  TPCTaskType.WRITE_POST_MEMTABLE_FULL,
                                                  observeOnScheduler));

                        opGroup.whenBlocking().thenRun(this::complete);
                        releaseFuture.thenRun(this::onRelease);
                    }

                    void subscribeChild()
                    {
                        Single<T> child;
                        try
                        {
                            child = singleCallable.call();
                        }
                        catch (Throwable t)
                        {
                            subscriber.onError(t);
                            return;
                        }

                        child.subscribe(subscriber);
                    }

                    public void dispose()
                    {
                        TPCRunnable prev = task.getAndSet(null);
                        if (prev != null)
                        {
                            timerContext.close();
                            prev.cancelled();
                        }
                    }

                    public boolean isDisposed()
                    {
                        return task.get() == null;
                    }

                    public void complete()
                    {
                        TPCRunnable prev = task.getAndSet(null);
                        if (prev != null)
                        {
                            timerContext.close();
                            try
                            {
                                observeOnScheduler.execute(prev);
                            }
                            catch (RejectedExecutionException t)
                            {
                                prev.cancelled();
                                subscriber.onError(t);
                            }
                            catch (Throwable t)
                            {
                                subscriber.onError(t);
                            }
                        }
                    }

                    public void onRelease()
                    {
                        if (task.get() == null)
                            return;     // cancelled or completed

                        // Grab the future before checking fullness so it will certainly fire if a release happens after we have checked a limit.
                        CompletableFuture<Void> releaseFuture = onHeap.parent.releaseFuture();
                        if (offHeap.parent.belowLimit() && onHeap.parent.belowLimit())
                            complete();
                        else
                            releaseFuture.thenRun(this::onRelease);
                    }
                }

                new WhenBelowLimits();
            }
        };
    }

    /**
     * Mark this allocator reclaiming; this will permit any outstanding allocations to temporarily
     * overshoot the maximum memory limit so that flushing can begin immediately
     */
    public void setDiscarding()
    {
        state = state.transition(LifeCycle.DISCARDING);
        // mark the memory owned by this allocator as reclaiming
        onHeap.markAllReclaiming();
        offHeap.markAllReclaiming();
    }

    /**
     * Indicate the memory and resources owned by this allocator are no longer referenced,
     * and can be reclaimed/reused.
     */
    public void setDiscarded()
    {
        state = state.transition(LifeCycle.DISCARDED);
        // release any memory owned by this allocator; automatically signals waiters
        onHeap.releaseAll();
        offHeap.releaseAll();
    }

    public boolean isLive()
    {
        return state == LifeCycle.LIVE;
    }

    /** Mark the BB as unused, permitting it to be reclaimed */
    public static final class SubAllocator
    {
        // the tracker we are owning memory from
        private final MemtablePool.SubPool parent;

        // the amount of memory/resource owned by this object
        private volatile long owns;
        // the amount of memory we are reporting to collect; this may be inaccurate, but is close
        // and is used only to ensure that once we have reclaimed we mark the tracker with the same amount
        private volatile long reclaiming;

        SubAllocator(MemtablePool.SubPool parent)
        {
            this.parent = parent;
        }

        // should only be called once we know we will never allocate to the object again.
        // currently no corroboration/enforcement of this is performed.
        void releaseAll()
        {
            parent.released(ownsUpdater.getAndSet(this, 0));
            parent.reclaimed(reclaimingUpdater.getAndSet(this, 0));
        }

        // like allocate, but permits allocations to be negative
        public void adjust(long size)
        {
            if (size <= 0)
                released(-size);
            else
                allocated(size);
        }

        // mark an amount of space as allocated in the tracker, and owned by us
        public void allocated(long size)
        {
            parent.allocated(size);
            ownsUpdater.addAndGet(this, size);
        }

        void released(long size)
        {
            parent.released(size);
            ownsUpdater.addAndGet(this, -size);
        }

        // mark everything we currently own as reclaiming, both here and in our parent
        void markAllReclaiming()
        {
            while (true)
            {
                long cur = owns;
                long prev = reclaiming;
                if (!reclaimingUpdater.compareAndSet(this, prev, cur))
                    continue;

                parent.reclaiming(cur - prev);
                return;
            }
        }

        public long owns()
        {
            return owns;
        }

        public float ownershipRatio()
        {
            float r = owns / (float) parent.limit;
            if (Float.isNaN(r))
                return 0;
            return r;
        }

        private static final AtomicLongFieldUpdater<SubAllocator> ownsUpdater = AtomicLongFieldUpdater.newUpdater(SubAllocator.class, "owns");
        private static final AtomicLongFieldUpdater<SubAllocator> reclaimingUpdater = AtomicLongFieldUpdater.newUpdater(SubAllocator.class, "reclaiming");
    }


}
