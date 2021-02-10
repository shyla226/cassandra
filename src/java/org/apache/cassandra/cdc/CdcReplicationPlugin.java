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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdcReplicationPlugin
{
    private static final Logger logger = LoggerFactory.getLogger(CdcReplicationPlugin.class);

    OffsetFileWriter offsetFileWriter;
    MutationSender<Mutation> mutationSender;
    CommitLogTransfer commitLogTransfer;
    CommitLogProcessor commitLogProcessor;
    CommitLogReaderProcessor commitLogReaderProcessor;

    public static final CdcReplicationPlugin instance = new CdcReplicationPlugin();

    private CdcReplicationPlugin()
    {
        this.mutationSender = new QuasarMutationSender();
        this.offsetFileWriter = new OffsetFileWriter(mutationSender);
        this.commitLogTransfer = new BlackHoleCommitLogTransfer();
        this.commitLogReaderProcessor = new CommitLogReaderProcessor(new CommitLogReadHandlerImpl(offsetFileWriter, mutationSender), offsetFileWriter);
        this.commitLogProcessor = new CommitLogProcessor(new CassandraCdcConfiguration(), commitLogTransfer, offsetFileWriter, commitLogReaderProcessor);
    }

    public void initialize() throws Exception
    {
        offsetFileWriter.initialize();
        commitLogProcessor.initialize();
        commitLogReaderProcessor.initialize();
    }

    public void start()
    {
        // watch new commitlog files
        ExecutorService commitLogExecutor = Executors.newSingleThreadExecutor();
        commitLogExecutor.submit(() -> {
            try {
                commitLogProcessor.start();
            } catch(Exception e) {
                logger.error("commitLogProcessor error:", e);
            }
        });

        // process commitlog mutation, after we know the synced position
        ExecutorService commitLogReaderExecutor = Executors.newSingleThreadExecutor();
        commitLogReaderExecutor.submit(() -> {
            try {
                commitLogReaderProcessor.awaitSyncedPosition();
                commitLogReaderProcessor.start();
            } catch(Exception e) {
                logger.error("commitLogProcessor error:", e);
            }
        });

        // persist the offset of replicated mutation.
        ExecutorService offsetWriterExecutor = Executors.newSingleThreadExecutor();
        offsetWriterExecutor.submit(() -> {
            try {
                // wait for the synced position
                offsetFileWriter.start();
            } catch(Exception e) {
                logger.error("commitLogProcessor error:", e);
            }
        });
        logger.info("CDC replication started");
    }
}
