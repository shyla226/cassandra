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

/**
 * A threaded version of OpOrder which uses thread-specific groups so that the cost of starting and closing an
 * order group is much lower (due to the lack of contention) at the expense of maintaining multiple groups and
 * having to duplicate and check all when applying barrier operations.
 */
public class OpOrderThreaded implements OpOrder
{
    private static final Logger logger = LoggerFactory.getLogger(OpOrderThreaded.class);

    /**
     * Thread identifier used to select the per-thread group.
     */
    public interface ThreadIdentifier
    {
        /**
         * Return the id (between 0 and idLimit) that is associated with this thread.
         */
        int idFor(Thread d);

        /**
         * Diagnostic method.
         *
         * Called (via an assertion) to make sure barriers are permitted. Used to prevent some types of deadlock
         * (e.g. thread-per-core thread awaiting barriers which can only be fulfilled by the same thread).
         */
        boolean barrierPermitted();
    }

    final ThreadIdentifier mapper;
    final Object creator;
    private volatile OpOrderSimple.Group current[];

    public OpOrderThreaded(Object creator, ThreadIdentifier mapper, int idLimit)
    {
        this.mapper = mapper;
        this.creator = creator;

        OpOrderSimple.Group[] groups = new OpOrderSimple.Group[idLimit];
        for (int i = 0; i < idLimit; ++i)
            groups[i] = new OpOrderSimple.Group();
        current = groups;
    }

    /**
     * Start an operation against this OpOrder.
     * Once the operation is completed Ordered.close() MUST be called EXACTLY once for this operation.
     *
     * @return the Ordered instance that manages this OpOrder
     */
    public Group start()
    {
        int coreId = mapper.idFor(Thread.currentThread());

        while (true)
        {
            OpOrderSimple.Group current = this.current[coreId];
            if (current.register())
                return current;
        }
    }

    /**
     * Creates a new barrier. The barrier is only a placeholder until barrier.issue() is called on it.
     */
    public Barrier newBarrier()
    {
        return new Barrier();
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
    public final class Barrier implements OpOrder.Barrier
    {
        // this Barrier was issued after all Group operations started against orderOnOrBefore
        private OpOrderSimple.Group orderOnOrBefore[] = null;

        /**
         * @return true if @param group was started prior to the issuing of the barrier.
         *
         * (Until issue is called, always returns true, but if you rely on this behavior you are probably
         * Doing It Wrong.)
         */
        public boolean isAfter(OpOrder.Group group)
        {
            if (orderOnOrBefore == null)
                return true;
            // we subtract to permit wrapping round the full range of Long - so we only need to ensure
            // there are never Long.MAX_VALUE * 2 total Group objects in existence at any one time which will
            // take care of itself
            return orderOnOrBefore[0].id - ((OpOrderSimple.Group) group).id >= 0;
        }

        /**
         * Issues (seals) the barrier, meaning no new operations may be issued against it, and expires the current
         * Group.  Must be called before await() for isAfter() to be properly synchronised.
         */
        public void issue()
        {
            if (orderOnOrBefore != null)
                throw new IllegalStateException("Can only call issue() once on each Barrier");

            final OpOrderSimple.Group[] current;
            synchronized (OpOrderThreaded.this)
            {
                current = OpOrderThreaded.this.current;
                orderOnOrBefore = current;
                OpOrderSimple.Group[] groups = new OpOrderSimple.Group[current.length];
                for (int i = 0; i < current.length; ++i)
                    groups[i] = current[i].next = new OpOrderSimple.Group(current[i]);
                OpOrderThreaded.this.current = groups;
            }

            for (OpOrderSimple.Group g : current)
                g.expire();
        }

        /**
         * Mark all prior operations as blocking, potentially signalling them to more aggressively make progress
         */
        public void markBlocking()
        {
            for (OpOrderSimple.Group g : orderOnOrBefore)
                markBlocking(g);
        }

        private void markBlocking(OpOrderSimple.Group current)
        {
            while (current != null)
            {
                current.isBlocking = true;
                current.isBlockingSignal.signalAll();
                current = current.prev;
            }
        }

        /**
         * Register to be signalled once allPriorOpsAreFinished() or allPriorOpsAreFinishedOrSafe() may return true
         */
        public WaitQueue.Signal register()
        {
            WaitQueue.Signal[] signals = new WaitQueue.Signal[orderOnOrBefore.length];
            for (int i = 0; i < orderOnOrBefore.length; ++i)
                signals[i] = orderOnOrBefore[i].waiting.register();
            return WaitQueue.any(signals);
        }

        /**
         * @return true if all operations started prior to barrier.issue() have completed
         */
        public boolean allPriorOpsAreFinished()
        {
            OpOrderSimple.Group[] current = orderOnOrBefore;
            if (current == null)
                throw new IllegalStateException("This barrier needs to have issue() called on it before prior operations can complete");
            for (OpOrderSimple.Group g : current)
                if (g.next.prev != null)
                    return false;
            return true;
        }

        /**
         * wait for all operations started prior to issuing the barrier to complete
         */
        public void await()
        {
            assert mapper.barrierPermitted();

            OpOrderSimple.Group[] current = orderOnOrBefore;
            if (current == null)
                throw new IllegalStateException("This barrier needs to have issue() called on it before prior operations can complete");

            for (OpOrderSimple.Group g : current)
            {
                if (g.next.prev != null)
                {
                    WaitQueue.Signal signal = g.waiting.register();
                    if (g.next.prev != null)
                        signal.awaitUninterruptibly();
                    else
                        signal.cancel();
                }
            }
        }
    }
}
