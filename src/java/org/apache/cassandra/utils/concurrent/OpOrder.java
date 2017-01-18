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
package org.apache.cassandra.utils.concurrent;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Scheduler;
import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.apache.cassandra.concurrent.TPCOpOrder;

/**
 * <p>A class for providing synchronization between producers and consumers that do not
 * communicate directly with each other, but where the consumers need to process their
 * work in contiguous batches. In particular this is useful for both CommitLog and Memtable
 * where the producers (writing threads) are modifying a structure that the consumer
 * (flush executor) only batch syncs, but needs to know what 'position' the work is at
 * for co-ordination with other processes,
 *
 * <p>The typical usage is something like:
 * <pre>
 * {@code
     public final class ExampleShared
     {
        final OpOrder order = new OpOrder();
        volatile SharedState state;

        static class SharedState
        {
            volatile Barrier barrier;

            // ...
        }

        public void consume()
        {
            SharedState state = this.state;
            state.setReplacement(new State())
            state.doSomethingToPrepareForBarrier();

            state.barrier = order.newBarrier();
            // seal() MUST be called after newBarrier() else barrier.isAfter()
            // will always return true, and barrier.await() will fail
            state.barrier.issue();

            // wait for all producer work started prior to the barrier to complete
            state.barrier.await();

            // change the shared state to its replacement, as the current state will no longer be used by producers
            this.state = state.getReplacement();

            state.doSomethingWithExclusiveAccess();
        }

        public void produce()
        {
            try (Group opGroup = order.start())
            {
                SharedState s = state;
                while (s.barrier != null && !s.barrier.isAfter(opGroup))
                    s = s.getReplacement();
                s.doProduceWork();
            }
        }
    }
 * }
 * </pre>
 */
public class OpOrder
{
    private static final Logger logger = LoggerFactory.getLogger(OpOrder.class);

    final TPCOpOrder localOpOrders[] = new TPCOpOrder[NettyRxScheduler.NUM_NETTY_THREADS];
    final Object creator;

    public OpOrder(Object creator)
    {
        for (int i = 0; i < NettyRxScheduler.NUM_NETTY_THREADS; i++)
            localOpOrders[i] = new TPCOpOrder(i, this);

        this.creator = creator;
    }

    /**
     * Start an operation against this OpOrder.
     * Once the operation is completed Ordered.close() MUST be called EXACTLY once for this operation.
     *
     * @return the Ordered instance that manages this OpOrder
     */
    public TPCOpOrder.Group start()
    {
        Integer coreId = NettyRxScheduler.getCoreId();

        if (coreId == null)
            coreId = 0;

        return localOpOrders[coreId].start();
    }

    public TPCOpOrder.Group getCurrent(int coreId)
    {
        return localOpOrders[coreId].getCurrent();
    }

    /**
     * Creates a new barrier. The barrier is only a placeholder until barrier.issue() is called on it,
     * after which all new operations will start against a new Group that will not be accepted
     * by barrier.isAfter(), and barrier.await() will return only once all operations started prior to the issue
     * have completed.
     *
     * @return
     */
    public Barrier newBarrier()
    {
        return new Barrier();
    }

    public void awaitNewBarrier()
    {
        Barrier barrier = newBarrier();
        barrier.issue();
        barrier.await();
    }


    /**
     * This class represents a synchronisation point providing ordering guarantees on operations started
     * against the enclosing OpOrder.  When issue() is called upon it (may only happen once per Barrier), the
     * Barrier atomically partitions new operations from those already running (by expiring the current Group),
     * and activates its isAfter() method
     * which indicates if an operation was started before or after this partition. It offers methods to
     * determine, or block until, all prior operations have finished, and a means to indicate to those operations
     * that they are blocking forward progress. See {@link OpOrder} for idiomatic usage.
     */
    public final class Barrier
    {
        /*
         * During startup we have 1 thread. so if a barrier is issued during startup
         * but awaited after TPC engine, we can hang. This ensures we properly handle barriers
         * during startup
         */
        private final boolean isStartup = NettyRxScheduler.isStartup();
        private final int numCores = isStartup ? 1 : NettyRxScheduler.getNumNettyThreads();
        private final TPCOpOrder.Barrier tpcBarriers[] = new TPCOpOrder.Barrier[numCores];

        /**
         * @return true if @param group was started prior to the issuing of the barrier.
         *
         * (Until issue is called, always returns true, but if you rely on this behavior you are probably
         * Doing It Wrong.)
         */
        public boolean isAfter(TPCOpOrder.Group group)
        {
            TPCOpOrder.Barrier barrier = group.coreId >= numCores ? null : tpcBarriers[group.coreId];
            if (barrier == null)
                return true;

            return barrier.isAfter(group);
        }

        /**
         * Issues (seals) the barrier, meaning no new operations may be issued against it, and expires the current
         * Group.  Must be called before await() for isAfter() to be properly synchronised.
         */
        public void issue()
        {
            CountDownLatch latch = new CountDownLatch(numCores);

            for (int i = 0; i < numCores; i++)
            {
                final int fi = i;

                Runnable r = () ->
                {
                    try
                    {
                        tpcBarriers[fi] = localOpOrders[fi].newBarrier();
                        tpcBarriers[fi].issue();
                        latch.countDown();
                    }
                    catch (Throwable e)
                    {
                        logger.error("Deadlocking OpOrder issue : ", e);
                        throw new RuntimeException(e);
                    }
                };


                Scheduler scheduler = NettyRxScheduler.getForCore(i);
                Integer callerCore = NettyRxScheduler.getCoreId();
                assert isStartup ||  callerCore == null || callerCore != i : "A barrier should never be issued from a TPC thread";

                scheduler.scheduleDirect(r);
            }

            Uninterruptibles.awaitUninterruptibly(latch);
        }

        /**
         * Mark all prior operations as blocking, potentially signalling them to more aggressively make progress
         */
        public void markBlocking()
        {

            CountDownLatch latch = new CountDownLatch(numCores);

            for (int i = 0; i < numCores; i++)
            {
                final int fi = i;

                Runnable r = () -> {
                    tpcBarriers[fi].markBlocking();
                    latch.countDown();
                };

                Scheduler scheduler = NettyRxScheduler.getForCore(i);
                Integer callerCore = NettyRxScheduler.getCoreId();
                assert isStartup ||  callerCore == null || callerCore != i : "A barrier should never be markedBlocked from a TPC thread";

                scheduler.scheduleDirect(r);
            }

            Uninterruptibles.awaitUninterruptibly(latch);
        }


        public boolean allPriorOpsAreFinished()
        {
            CountDownLatch latch = new CountDownLatch(numCores);

            boolean allPriorFinished[] = new boolean[numCores];

            for (int i = 0; i < numCores; i++)
            {
                final int fi = i;

                Runnable r = () ->
                {
                    allPriorFinished[fi] = tpcBarriers[fi].allPriorOpsAreFinished();
                    latch.countDown();
                };

                Scheduler scheduler = NettyRxScheduler.getForCore(i);
                Integer callerCore = NettyRxScheduler.getCoreId();
                assert isStartup ||  callerCore == null || callerCore != i : "A barrier should never be accessed TPC thread";

                scheduler.scheduleDirect(r);
            }

            Uninterruptibles.awaitUninterruptibly(latch);
            for (int i = 0; i < numCores; i++)
                if (!allPriorFinished[i])
                    return false;

            return true;
        }

        /**
         * wait for all operations started prior to issuing the barrier to complete
         */
        public void await()
        {

            CountDownLatch latch = new CountDownLatch(numCores);
            WaitQueue.Signal signals[] = new WaitQueue.Signal[numCores];
            final Thread caller = Thread.currentThread();

            for (int i = 0; i < numCores; i++)
            {
                final int fi = i;

                Runnable r = () ->
                {
                    Optional<WaitQueue.Signal> signal = tpcBarriers[fi].await(caller);
                    if (signal.isPresent())
                        signals[fi] = signal.get();

                    latch.countDown();
                };

                Scheduler scheduler = NettyRxScheduler.getForCore(i);
                Integer callerCore = NettyRxScheduler.getCoreId();
                assert isStartup ||  callerCore == null || callerCore != i : "A barrier should never be awaited from a TPC thread";

                scheduler.scheduleDirect(r);
            }

            Uninterruptibles.awaitUninterruptibly(latch);
            for (int i = 0; i < numCores; i++)
                if (signals[i] != null)
                    signals[i].awaitUninterruptibly();
        }

        public TPCOpOrder.Group getSyncPoint(int coreId)
        {
            return tpcBarriers[coreId].getSyncPoint();
        }
    }
}
