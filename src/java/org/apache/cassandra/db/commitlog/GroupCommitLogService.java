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

import io.reactivex.Completable;

import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.TimeSource;
import org.apache.cassandra.utils.flow.RxThreads;

/**
 * A commitlog service that will block returning an ACK back to the a coordinator/client
 * for a minimum amount of time as we wait until the the commit log segment is flushed.
 */
public class GroupCommitLogService extends AbstractCommitLogService
{
    public GroupCommitLogService(CommitLog commitLog, TimeSource timeSource)
    {
        super(commitLog, "GROUP-COMMIT-LOG-WRITER", (int) DatabaseDescriptor.getCommitLogSyncGroupWindow(), timeSource);
    }

    protected Completable maybeWaitForSync(CommitLogSegment.Allocation alloc, StagedScheduler observeOn)
    {
        // wait until record has been safely persisted to disk
        pending.incrementAndGet();
        long startTime = timeSource.nanoTime();

        Completable sync = awaitSyncAt(startTime)
                           .doOnComplete(() ->
                                         {
                                             commitLog.metrics.waitingOnCommit.update(timeSource.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                                             pending.decrementAndGet();
                                         });

        return RxThreads.awaitAndContinueOn(sync, observeOn, TPCTaskType.WRITE_POST_COMMIT_LOG_SYNC);
    }
}

