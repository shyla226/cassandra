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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.util.Arrays;
import java.util.UUID;
import javax.inject.Singleton;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.SnitchProperties;
import org.apache.cassandra.schema.Schema;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

/**
 * Detect and read commitlogs files.
 *
 * @author vroyer
 */
@Singleton
public class CommitLogProcessor extends AbstractProcessor implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogProcessor.class);

    private static final String NAME = "Commit Log Processor";

    private final CassandraCdcConfiguration config;
    private final CommitLogTransfer commitLogTransfer;
    private final File cdcDir;
    private final AbstractDirectoryWatcher newCommitLogWatcher;
    private boolean initial = true;

    CommitLogReaderProcessor commitLogReaderProcessor;
    OffsetFileWriter offsetFileWriter;
    UUID localHostId = null;



    public CommitLogProcessor(CassandraCdcConfiguration config,
                              CommitLogTransfer commitLogTransfer,
                              OffsetFileWriter offsetFileWriter,
                              CommitLogReaderProcessor commitLogReaderProcessor) throws IOException
    {
        super(NAME, 0);
        this.config = config;
        this.commitLogReaderProcessor = commitLogReaderProcessor;
        this.commitLogTransfer = commitLogTransfer;
        this.offsetFileWriter = offsetFileWriter;

        this.cdcDir = new File(DatabaseDescriptor.getCDCLogLocation());
        this.newCommitLogWatcher = new AbstractDirectoryWatcher(cdcDir.toPath(), config.cdcDirPollIntervalMs, ImmutableSet.of(ENTRY_CREATE, ENTRY_MODIFY)) {
            @Override
            void handleEvent(WatchEvent<?> event, Path path) throws IOException
            {
                if (path.toString().endsWith(".log")) {
                    commitLogReaderProcessor.submitCommitLog(path.toFile());
                }
                if (path.toString().endsWith("_cdc.idx")) {
                    commitLogReaderProcessor.submitCommitLog(path.toFile());
                }
            }
        };

    }

    /**
     * Override destroy to clean up resources after stopping the processor
     */
    @Override
    public void close() {
    }

    @Override
    public void process() throws IOException, InterruptedException
    {
        if (config.errorCommitLogReprocessEnabled) {
            LOGGER.debug("Moving back error commitlogs for reprocessing");
            commitLogTransfer.getErrorCommitLogFiles();
        }


        // load existing commitlogs files when initializing
        if (initial) {
            LOGGER.info("Reading existing commit logs in {}", cdcDir);
            File[] commitLogFiles = CommitLogUtil.getCommitLogs(cdcDir);
            Arrays.sort(commitLogFiles, CommitLogUtil::compareCommitLogs);
            File youngerCdcIdxFile = null;
            for (File file : commitLogFiles) {
                // filter out already processed commitlogs
                long segmentId = CommitLogUtil.extractTimestamp(file.getName());
                if (file.getName().endsWith(".log")) {
                    // only submit logs, not _cdc.idx
                    if(segmentId >= offsetFileWriter.offset().segmentId) {
                        commitLogReaderProcessor.submitCommitLog(file);
                    }
                } else if (file.getName().endsWith("_cdc.idx")) {
                    if (youngerCdcIdxFile == null ||  segmentId > CommitLogUtil.extractTimestamp(youngerCdcIdxFile.getName())) {
                        youngerCdcIdxFile = file;
                    }
                }
            }
            if (youngerCdcIdxFile != null) {
                // init the last synced position
                LOGGER.debug("Read last synced position from file={}", youngerCdcIdxFile);
                commitLogReaderProcessor.submitCommitLog(youngerCdcIdxFile);
            }
            initial = false;
        }

        // collect new segment files
        newCommitLogWatcher.poll();
    }
}
