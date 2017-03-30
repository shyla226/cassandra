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

public interface OpOrder
{
    /**
     * Start an operation against this OpOrder.
     * Once the operation is completed Ordered.close() MUST be called EXACTLY once for this operation.
     *
     * @return the Ordered instance that manages this OpOrder
     */
    public Group start();

    /**
     * Creates a new barrier. The barrier is only a placeholder until barrier.issue() is called on it,
     * after which all new operations will start against a new Group that will not be accepted
     * by barrier.isAfter(), and barrier.await() will return only once all operations started prior to the issue
     * have completed.
     *
     * @return
     */
    public Barrier newBarrier();

    public default void awaitNewBarrier()
    {
        Barrier barrier = newBarrier();
        barrier.issue();
        barrier.await();
    }

    /**
     * Represents a group of identically ordered operations, i.e. all operations started in the interval between
     * two barrier issuances. For each register() call this is returned, close() must be called exactly once.
     * It should be treated like taking a lock().
     */
    public interface Group extends AutoCloseable
    {
        /**
         * To be called exactly once for each register() call this object is returned for, indicating the operation
         * is complete
         */
        public void close();

        /**
         * @return true if a barrier we are behind is, or may be, blocking general progress,
         * so we should try more aggressively to progress
         */
        public boolean isBlocking();
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
    public interface Barrier
    {
        /**
         * @return true if @param group was started prior to the issuing of the barrier.
         *
         * (Until issue is called, always returns true, but if you rely on this behavior you are probably
         * Doing It Wrong.)
         */
        public boolean isAfter(Group group);

        /**
         * Issues (seals) the barrier, meaning no new operations may be issued against it, and expires the current
         * Group.  Must be called before await() for isAfter() to be properly synchronised.
         */
        public void issue();

        /**
         * Mark all prior operations as blocking, potentially signalling them to more aggressively make progress
         */
        public void markBlocking();

        /**
         * Register to be signalled once allPriorOpsAreFinished() or allPriorOpsAreFinishedOrSafe() may return true
         */
        public WaitQueue.Signal register();

        /**
         * @return true if all operations started prior to barrier.issue() have completed
         */
        public boolean allPriorOpsAreFinished();

        /**
         * wait for all operations started prior to issuing the barrier to complete
         */
        public void await();
    }
}
