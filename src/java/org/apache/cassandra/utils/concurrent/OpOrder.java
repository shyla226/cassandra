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

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        NettyRxScheduler scheduler = NettyRxScheduler.instance();
        int coreId = NettyRxScheduler.isValidCoreId(scheduler.cpuId) ? scheduler.cpuId : 0;
        return localOpOrders[coreId].start(scheduler.cpuId);
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

    @Override
    public String toString()
    {
        return String.format("OpOrder {} with creator {}/{}", hashCode(), creator.getClass().getName(), creator.hashCode());
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
        private final TPCOpOrder.Barrier tpcBarriers[] = new TPCOpOrder.Barrier[NettyRxScheduler.NUM_NETTY_THREADS];

        /**
         * @return true if @param group was started prior to the issuing of the barrier.
         *
         * (Until issue is called, always returns true, but if you rely on this behavior you are probably
         * Doing It Wrong.)
         */
        public boolean isAfter(TPCOpOrder.Group group)
        {
            TPCOpOrder.Barrier barrier = tpcBarriers[group.coreId];
            if (barrier == null)
                return true;

            return barrier.isAfter(group);
        }

        /**
         * Execute the task on each TPC thread, assuming they have been assigned (scheduler is not null).
         *
         * @return a countdown latch that will complete once all threads have executed their task
         */
        private CountDownLatch dispatchToTPCTheads(final Consumer<NettyRxScheduler> function)
        {
            final CountDownLatch latch = new CountDownLatch(tpcBarriers.length);

            for (int i = 0; i < tpcBarriers.length; i++)
            {
                final NettyRxScheduler scheduler = NettyRxScheduler.getForCore(i);
                scheduler.scheduleDirect(() -> {
                        try
                        {
                            function.accept(scheduler);
                        }
                        catch (Throwable e)
                        {
                            logger.error("Deadlocking OpOrder issue : ", e);
                            throw new RuntimeException(e);
                        }
                        finally
                        {
                            latch.countDown();
                        }
                    });
            }

            return latch;
        }

        /**
         * Issues (seals) the barrier, meaning no new operations may be issued against it, and expires the current
         * Group.  Must be called before await() for isAfter() to be properly synchronised.
         */
        public void issue()
        {
            assert !NettyRxScheduler.isTPCThread() : "A barrier should never be issued from a TPC thread";
            final CountDownLatch latch = dispatchToTPCTheads(scheduler -> {
                tpcBarriers[scheduler.cpuId] = localOpOrders[scheduler.cpuId].newBarrier();
                tpcBarriers[scheduler.cpuId].issue();
            });

            Uninterruptibles.awaitUninterruptibly(latch);
        }

        /**
         * Mark all prior operations as blocking, potentially signalling them to more aggressively make progress
         */
        public void markBlocking()
        {
            assert !NettyRxScheduler.isTPCThread() : "This method should never be issued from a TPC thread";
            final CountDownLatch latch = dispatchToTPCTheads(scheduler -> {
                if (tpcBarriers[scheduler.cpuId] != null)
                    tpcBarriers[scheduler.cpuId].markBlocking();
            });

            Uninterruptibles.awaitUninterruptibly(latch);
        }


        public boolean allPriorOpsAreFinished()
        {
            assert !NettyRxScheduler.isTPCThread() : "This method should never be issued from a TPC thread";
            final boolean allPriorFinished[] = new boolean[tpcBarriers.length];
            //initialize to true in case some TPC threads are not yet running
            Arrays.fill(allPriorFinished, true);
            final CountDownLatch latch = dispatchToTPCTheads(scheduler -> allPriorFinished[scheduler.cpuId] =
                                                                          tpcBarriers[scheduler.cpuId] == null
                                                                          ? true
                                                                          : tpcBarriers[scheduler.cpuId].allPriorOpsAreFinished());

            Uninterruptibles.awaitUninterruptibly(latch);

            for (int i = 0; i < allPriorFinished.length; i++)
                if (!allPriorFinished[i])
                    return false;

            return true;
        }

        /**
         * wait for all operations started prior to issuing the barrier to complete
         */
        public void await()
        {
            assert !NettyRxScheduler.isTPCThread() : "This method should never be issued from a TPC thread";

            final WaitQueue.Signal signals[] = new WaitQueue.Signal[tpcBarriers.length];
            final Thread caller = Thread.currentThread();

            final CountDownLatch latch = dispatchToTPCTheads(scheduler ->
                                                             {
                                                                 if (tpcBarriers[scheduler.cpuId] == null)
                                                                     return; // the barrier was issued just before the TPC thread was initialized,

                                                                 Optional<WaitQueue.Signal> signal = tpcBarriers[scheduler.cpuId].await(caller);
                                                                 if (signal.isPresent())
                                                                     signals[scheduler.cpuId] = signal.get();
                                                             });

            Uninterruptibles.awaitUninterruptibly(latch);

            for (int i = 0; i < signals.length; i++)
                if (signals[i] != null)
                    signals[i].awaitUninterruptibly();
        }

        public TPCOpOrder.Group getSyncPoint(int coreId)
        {
            return tpcBarriers[coreId].getSyncPoint();
        }
    }
}
