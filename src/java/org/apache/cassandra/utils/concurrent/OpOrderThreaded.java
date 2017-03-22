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

package org.apache.cassandra.utils.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NettyRxScheduler;

/**
 * A relaxed OpOrder variation with less overhead when called from specific highly-used threads.
 * It effectively consists of OpOrder per thread (so that the group start overhead is minimal)
 * and provides a mechanism of making sure prior operations have completed.
 *
 * Unlike OpOrder, this does not provide true barriers, in the sense that operations on some threads may have
 * started after the barrier point of another. The only guarantee this gives is that any operation started before
 * the beginning of the threaded barrier issue call will have completed after the await returns. The await may require
 * for some operations started after that point in time to complete.
 */
public class OpOrderThreaded
{
    private static final Logger logger = LoggerFactory.getLogger(OpOrderThreaded.class);

    public interface ThreadIdentifier
    {
        int idLimit();
        int idFor(Thread d);

        boolean barrierPermitted();
    }

    final OpOrder threadOpOrders[];
    final ThreadIdentifier mapper;
    final Object creator;

    public OpOrderThreaded(Object creator, ThreadIdentifier mapper)
    {
        this.mapper = mapper;
        int size = mapper.idLimit();
        threadOpOrders = new OpOrder[size];
        for (int i = 0; i < threadOpOrders.length; i++)
            threadOpOrders[i] = new OpOrder();

        this.creator = creator;
    }

    /**
     * Start an operation against this OpOrder.
     * Once the operation is completed Ordered.close() MUST be called EXACTLY once for this operation.
     *
     * @return the Ordered instance that manages this OpOrder
     */
    public OpOrder.Group start()
    {
        int coreId = mapper.idFor(Thread.currentThread());
        return threadOpOrders[coreId].start();
    }

    /**
     * Creates a new barrier. The barrier is only a placeholder until barrier.issue() is called on it.
     */
    public Barrier newThreadedBarrier()
    {
        return new Barrier();
    }

    public void awaitNewThreadedBarrier()
    {
        Barrier barrier = newThreadedBarrier();
        barrier.issue();
        barrier.await();
    }

    @Override
    public String toString()
    {
        return String.format("OpOrderThreaded {} with creator {}/{}", hashCode(), creator.getClass().getName(), creator.hashCode());
    }

    /**
     * This class represents a synchronisation point providing ordering guarantees on operations started
     * against the enclosing OpOrderThreaded.  When issue() is called upon it (may only happen once per Barrier), the
     * Barrier provides a means of guaranteeing all operations started before that point in time to have
     * completed.
     */
    public final class Barrier
    {
        private final OpOrder.Barrier threadBarriers[] = new OpOrder.Barrier[threadOpOrders.length];

        /**
         * Issues (seals) the barrier, meaning no new operations may be issued against it, and expires the current
         * Group.  Must be called before await() for isAfter() to be properly synchronised.
         */
        public void issue()
        {
            for (int i = 0; i < threadBarriers.length; i++)
            {
                threadBarriers[i] = threadOpOrders[i].newBarrier();
                threadBarriers[i].issue();
            };
        }

        /**
         * wait for all operations started prior to issuing the barrier to complete
         */
        public void await()
        {
            assert mapper.barrierPermitted();

            for (int i = 0; i < threadBarriers.length; i++)
            {
                threadBarriers[i].await();
            }
        }

        /**
         * Mark all prior operations as blocking, potentially signalling them to more aggressively make progress
         */
        public void markBlocking()
        {
            for (int i = 0; i < threadBarriers.length; i++)
            {
                if (threadBarriers[i] != null)
                    threadBarriers[i].markBlocking();
            };
        }

        /**
         * Check if the barrier is after the given opGroup _for the specified thread_.
         */
        public boolean isAfter(Thread thread, OpOrder.Group opGroup)
        {
            return threadBarriers[mapper.idFor(thread)].isAfter(opGroup);
        }
    }
}
