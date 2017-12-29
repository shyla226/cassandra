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

/**
 * Type of scheduled TPC task. The type of task has an effect on how the task is scheduled and counted.
 *
 * The task type is used for several purposes:
 * - To count how many tasks each TPC core has in flight. Most tasks end up in the scheduler's queue and are
 *   automatically counted, but others wait to be triggered by an external event (e.g. async read) and need to be
 *   explicitly marked as active.
 * - To identify "pendable" tasks which are delayed if too many tasks are currently in flight on the given core.
 *   Pendable tasks are usually the ones that start the work on a request. This is done to avoid having an operation's
 *   state in memory while the task has no chance to finish quickly, which causes severe GC problems as temporary
 *   objects get promoted to long-lived.
 * - To display an event name associated with the type of task in the list of active, completed, pending and blocked
 *   operations in the TPC metrics.
 * - To decide if a task should be counted in the metrics only if it forces a delay in the processing (e.g. thread
 *   switching), or every time it is executed.
 * - To decide whether or not to include some types of operation in the per-core totals.
 *
 * The comment above each task type is to be used for event description in the documentation.
 */
public enum TPCTaskType
{
    /** Unknown task */
    UNKNOWN,
    /** Single-partition read request */
    READ(Features.PENDABLE),
    /** Single-partition read request that will be first scheduled on an eventloop */
    READ_DEFERRED(Features.PENDABLE),
    /** Single-partition read scheduled on local node */
    READ_RESPONSE("READ_SWITCH_FOR_RESPONSE"),
    /** Partition range read request */
    READ_RANGE(Features.PENDABLE),
    /** Partition range read response, not always counted */
    READ_RANGE_RESPONSE("READ_RANGE_SWITCH_FOR_RESPONSE"),
    /** Switching thread to read from an iterator */
    READ_FROM_ITERATOR("READ_SWITCH_FOR_ITERATOR"),   // test-only
    /** Switching thread to read from secondary index */
    READ_SECONDARY_INDEX,
    /** Waiting for data from disk */
    READ_DISK_ASYNC(Features.EXTERNAL_QUEUE),
    /** Write request */
    WRITE(Features.PENDABLE),
    /** Write response, not always counted */
    WRITE_RESPONSE("WRITE_SWITCH_FOR_RESPONSE"),
    /** Write issued to defragment data that required too many sstables to read */
    WRITE_DEFRAGMENT(Features.PENDABLE),
    /** Switching thread to write in memtable when not already on the correct thread */
    WRITE_MEMTABLE("WRITE_SWITCH_FOR_MEMTABLE"),
    /** Write request is waiting for the commit log segment to switch */
    WRITE_POST_COMMIT_LOG_SEGMENT("WRITE_AWAIT_COMMITLOG_SEGMENT", Features.EXTERNAL_QUEUE),
    /** Write request is waiting for commit log to sync to disk */
    WRITE_POST_COMMIT_LOG_SYNC("WRITE_AWAIT_COMMITLOG_SYNC", Features.EXTERNAL_QUEUE),
    /** Write request is waiting for space in memtable */
    WRITE_POST_MEMTABLE_FULL("WRITE_MEMTABLE_FULL", Features.EXTERNAL_QUEUE),
    /** Replaying a batch mutation */
    BATCH_REPLAY(Features.ALWAYS_COUNT),
    /** Store a batchlog entry */
    BATCH_STORE(Features.PENDABLE),
    /** Response to a batchlog entry store */
    BATCH_STORE_RESPONSE, // TODO Does this really need its own task type? Or is it better to just throw it in the "unknown" bundle?
    /** Remove a batchlog entry */
    BATCH_REMOVE(Features.PENDABLE),
    /** Acquiring counter lock */
    COUNTER_ACQUIRE_LOCK(Features.ALWAYS_COUNT),
    /** Executing a statement */
    EXECUTE_STATEMENT(Features.ALWAYS_COUNT),
    /** Executing compare-and-set */
    CAS(Features.ALWAYS_COUNT),
    /** Preparation phase of light-weight transaction. */
    LWT_PREPARE(Features.PENDABLE),
    /** Proposal phase of light-weight transaction. */
    LWT_PROPOSE(Features.PENDABLE),
    /** Commit phase of light-weight transaction. */
    LWT_COMMIT(Features.PENDABLE),
    /** Truncate request */
    TRUNCATE(Features.PENDABLE),
    /** NodeSync validation of a partition */
    NODESYNC_VALIDATION(Features.ALWAYS_COUNT),
    /** Authentication request */
    AUTHENTICATION(Features.ALWAYS_COUNT),
    /** Authorization request */
    AUTHORIZATION(Features.ALWAYS_COUNT),
    /** Unknown timed task */
    TIMED_UNKNOWN(Features.TIMED),
    /** Periodic histogram aggregation */
    TIMED_HISTOGRAM_AGGREGATE(Features.TIMED),
    /** Meter clock tick */
    TIMED_METER_TICK(Features.TIMED),
    /** Scheduled speculative read */
    TIMED_SPECULATE(Features.TIMED),
    /** Scheduled timeout task */
    TIMED_TIMEOUT(Features.TIMED),
    /** Number of busy spin cycles done by this TPC thread when it has no tasks to perform */
    EVENTLOOP_SPIN(Features.EXCLUDE_FROM_TOTALS),
    /** Number of Thread.yield() calls done by this TPC thread when it has no tasks to perform */
    EVENTLOOP_YIELD(Features.EXCLUDE_FROM_TOTALS),
    /** Number of LockSupport.park() calls done by this TPC thread when it has no tasks to perform */
    EVENTLOOP_PARK(Features.EXCLUDE_FROM_TOTALS),
    /** Hint dispatch request */
    HINT_DISPATCH(Features.PENDABLE),
    /** Hint dispatch response */
    HINT_RESPONSE;

    // Using the constants in the enum class causes "Illegal forward reference", using a nested static class works.
    private static class Features
    {
        static final int PENDABLE = TPCTaskType.PENDABLE | TPCTaskType.ALWAYS_COUNT;
        static final int ALWAYS_COUNT = TPCTaskType.ALWAYS_COUNT;
        static final int EXTERNAL_QUEUE = TPCTaskType.EXTERNAL_QUEUE;
        static final int TIMED = TPCTaskType.EXCLUDE_FROM_TOTALS;
        static final int EXCLUDE_FROM_TOTALS = TPCTaskType.EXCLUDE_FROM_TOTALS;
    }

    private static final int PENDABLE = 1;
    private static final int EXTERNAL_QUEUE = 2;
    private static final int ALWAYS_COUNT = 4;
    private static final int EXCLUDE_FROM_TOTALS = 8;

    private final int flags;

    /**
     * Whether the task is pendable, i.e. if the task should not be executed until the number of active tasks
     * on a TPC thread is below the threshold.
     * Currently ignored if the processing is initiated on non-TPC threads (e.g. IO)
     * These are normally messaging-service initiated tasks that start the processing of a request.
     */
    public final boolean pendable()
    {
        return (flags & PENDABLE) != 0;
    }

    /**
     * Whether the task should be counted as active even if it is not in the thread's TPC queue. Some tasks need to
     * be scheduled using a different mechanism (e.g. in response to the completion of a CompletableFuture) but still
     * counted as active for the purpose of deciding whether or not to delay a pendable task.
     * These tasks are manually wrapped in a TPCRunnable.
     */
    public final boolean externalQueue()
    {
        return (flags & EXTERNAL_QUEUE) != 0;
    }
    /**
     * Whether the execution of the task should be counted in the TPC metrics regardless of whether the task was
     * scheduled for delayed execution or not.
     */
    public final boolean logIfExecutedImmediately()
    {
        return (flags & ALWAYS_COUNT) != 0;
    }

    /**
     * Whether or not this event should be included in the core totals.
     * Timed tasks generally shouldn't, because they artificially inflate the number of active tasks.
     */
    public final boolean includedInTotals()
    {
        return (flags & EXCLUDE_FROM_TOTALS) == 0;
    }

    /**
     * Name of event to display in the TPC metrics.
     */
    public final String loggedEventName;

    TPCTaskType(String loggedEventName, int flags)
    {
        this.loggedEventName = loggedEventName != null ? loggedEventName : name();
        this.flags = flags;
    }

    TPCTaskType(int flags)
    {
        this(null, flags);
    }

    TPCTaskType(String eventName)
    {
        this(eventName, 0);
    }

    TPCTaskType()
    {
        this(null, 0);
    }
}
