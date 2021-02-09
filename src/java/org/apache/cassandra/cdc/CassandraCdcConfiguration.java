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
package org.apache.cassandra.cdc;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

/**
 * All configs used by a Cassandra connector agent.
 */
public class CassandraCdcConfiguration
{

    /**
     * The set of predefined SnapshotMode options.
     */
    public enum SnapshotMode {

        /**
         * Perform a snapshot whenever a new table with cdc enabled is detected. This is detected by periodically
         * scanning tables in Cassandra.
         */
        ALWAYS,

        /**
         * Perform a snapshot for unsnapshotted tables upon initial startup of the cdc agent.
         */
        INITIAL,

        /**
         * Never perform a snapshot, instead change events are only read from commit logs.
         */
        NEVER;

        public static Optional<SnapshotMode> fromText(String text) {
            return Arrays.stream(values())
                         .filter(v -> text != null && v.name().toLowerCase().equals(text.toLowerCase()))
                         .findFirst();
        }
    }


    /**
     * Reprocess on error commitlogs.
     */
    Boolean errorCommitLogReprocessEnabled;

    /**
     * Cassandra commitlogs relocation directory
     */
    String commitLogRelocationDir;

    /**
     * The directory to store offset tracking files.
     */
    String offsetBackingStoreDir;


    String snapshotConsistency;
    SnapshotMode snapshotMode;

    /**
     * The minimum amount of time to wait before committing the offset. The default value of 0 implies
     * the offset will be flushed every time.
     */
    int offsetFlushIntervalMs = 0;

    /**
     * The maximum records that are allowed to be processed until it is required to flush offset to disk.
     * This config is effective only if offset_flush_interval_ms != 0
     */
    int maxOffsetFlushSize = 100;

    /**
     * The maximum amount of time to wait on each poll before reattempt.
     */
    Duration cdcDirPollIntervalMs = Duration.of(10, ChronoUnit.SECONDS);

    /**
     * Positive integer value that specifies the number of milliseconds the snapshot processor should wait before
     * re-scanning tables to look for new cdc-enabled tables. Defaults to 10000 milliseconds, or 10 seconds.
     */
    int snapshotScanIntervalMs = 10000;


    Boolean tombstonesOnDelete = true;

    /**
     * Fetch the cassandra row before sending a pulsar message.
     */
    Boolean fetchRow = false;

    // In-memory Queue settings
    /**
     * Positive integer value that specifies the number of milliseconds the commit log processor should wait during
     * each iteration for new change events to appear in the queue. Defaults to 1000 milliseconds, or 1 second.
     */
    public Duration pollIntervalMs = Duration.of(1, ChronoUnit.SECONDS);
    public int maxQueueSize = 1024;
}
