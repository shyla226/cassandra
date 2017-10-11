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
 * Type of scheduled TPC task. Used mainly by TPC metrics, where we print
 * out the number of completed and active (scheduled but incomplete) tasks.
 */
public enum TPCTaskType
{
    UNKNOWN,
    READ(true, false),
    READ_RANGE(true, false),
    READ_SWITCH_FOR_MEMTABLE,
    READ_FROM_ITERATOR,
    READ_SECONDARY_INDEX,
    READ_DISK_ASYNC(false, true),
    WRITE(true, false),
    WRITE_DEFRAGMENT(true, false),
    WRITE_SWITCH_FOR_MEMTABLE,
    WRITE_POST_COMMIT_LOG_SEGMENT(false, true),
    WRITE_POST_COMMIT_LOG_SYNC(false, true),
    COUNTER_ACQUIRE_LOCK,
    VIEW_ACQUIRE_LOCK,
    EXECUTE_STATEMENT_INTERNAL,
    EXECUTE_STATEMENT,
    BATCH_REPLAY,
    CAS,
    LWT_PREPARE,
    LWT_PROPOSE,
    LWT_COMMIT,
    TRUNCATE,
    COMMIT_LOG_REPLAY,
    ANNOUNCE_TABLE,
    MIGRATION,
    VALIDATION,
    AUTHENTICATION,
    TIMED_UNKNOWN,
    TIMED_HISTOGRAM_AGGREGATE,
    TIMED_METER_TICK,
    TIMED_SPECULATE,
    TIMED_TIMEOUT,
    BATCH_WRITE,
    BATCH_REMOVE,
    HINT_RECEIVE,
    HINT_SUBMIT,
    ROW_CACHE_LOAD,
    COUNTER_CACHE_LOAD,
    EVENTLOOP_SPIN,
    EVENTLOOP_YIELD,
    EVENTLOOP_PARK;

    public final boolean pendable;
    public final boolean counted;

    TPCTaskType(boolean pendable, boolean counted)
    {
        this.pendable = pendable;
        this.counted = counted;
    }

    TPCTaskType()
    {
        this(false, false);
    }
}
