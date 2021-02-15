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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cdc.quasar.QuasarMutationEmitter;
import org.apache.cassandra.concurrent.JMXEnabledSingleThreadExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.utils.MBeanWrapper;

public class CdcReplicationPlugin implements CdcReplicationPluginMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CdcReplicationPlugin.class);

    OffsetFileWriter offsetFileWriter;
    MutationEmitter<Mutation> mutationEmitter;
    CommitLogTransfer commitLogTransfer;
    CommitLogReaderProcessor commitLogReaderProcessor;

    public static final String MBEAN_NAME = "org.apache.cassandra.cdc:type=CdcReplicationMBean";
    public final CdcReplicationMetrics metrics;

    public static final CdcReplicationPlugin instance = new CdcReplicationPlugin();

    private CdcReplicationPlugin()
    {
        this.mutationEmitter = new QuasarMutationEmitter();
        this.offsetFileWriter = new OffsetFileWriter(mutationEmitter);
        this.commitLogReaderProcessor = new CommitLogReaderProcessor(new CommitLogReadHandlerImpl(offsetFileWriter, mutationEmitter), offsetFileWriter);
        this.metrics = new CdcReplicationMetrics(commitLogReaderProcessor, this.offsetFileWriter);
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
        CdcSyncListeners.instance.register(commitLogReaderProcessor);
    }

    public void initialize() throws Exception
    {
        offsetFileWriter.initialize();
        commitLogReaderProcessor.initialize();
    }

    public void start()
    {
        // replay CL on error
        /*
        if (config.errorCommitLogReprocessEnabled) {
            logger.debug("Moving back error commitlogs for reprocessing");
            commitLogTransfer.getErrorCommitLogFiles();
        }
        */

        // load existing commitlogs files when initializing
        File cdcDir = new File(DatabaseDescriptor.getCDCLogLocation());
        logger.info("Reading existing commit logs in {}", cdcDir);
        File[] commitLogFiles = CommitLogUtil.getCommitLogs(cdcDir);
        Arrays.sort(commitLogFiles, CommitLogUtil::compareCommitLogs);
        File youngerCdcIdxFile = null;
        for (File file : commitLogFiles) {
            // filter out already processed commitlogs
            long segmentId = CommitLogUtil.extractTimestamp(file.getName());
            if (file.getName().endsWith(".log")) {
                // only submit logs, not _cdc.idx
                if(segmentId >= offsetFileWriter.sentOffset().segmentId) {
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
            logger.debug("Read last synced position from file={}", youngerCdcIdxFile);
            try {
                long seg = CommitLogUtil.extractTimestamp(youngerCdcIdxFile.getName());
                List<String> lines = Files.readAllLines(youngerCdcIdxFile.toPath(), Charset.forName("UTF-8"));
                int pos = Integer.parseInt(lines.get(0));
                boolean completed = false;
                try {
                    if("COMPLETED".equals(lines.get(1))) {
                        completed = true;
                    }
                } catch(Exception ex) {
                }
                commitLogReaderProcessor.updateSyncedOffset(seg, pos, completed);
            } catch(IOException ex) {
                logger.warn("error while reading file=" + youngerCdcIdxFile.getName(), ex);
            }
        }

        // process commitlog mutation, after we know the synced position
        JMXEnabledSingleThreadExecutor commitLogReaderExecutor = new JMXEnabledSingleThreadExecutor("CdcCommitLogReader", "internal");
        commitLogReaderExecutor.execute(() -> {
            try {
                commitLogReaderProcessor.start();
            } catch(Throwable t) {
                logger.error("commitLogReader error:", t);
            }
            logger.error("commitLogReader ending");
        });

        // persist the offset of replicated mutation.
        JMXEnabledSingleThreadExecutor offsetWriterExecutor = new JMXEnabledSingleThreadExecutor("CdcOffsetWriter", "internal");
        offsetWriterExecutor.execute(() -> {
            try {
                // wait for the synced position
                offsetFileWriter.start();
            } catch(Exception e) {
                logger.error("offsetWriter error:", e);
            }
        });
        logger.info("CDC replication plugin started");
    }

    @Override
    public void flush()
    {
        this.offsetFileWriter.flush();
    }

    @Override
    public void pause()
    {
        this.commitLogReaderProcessor.pause();
    }

    @Override
    public void resume()
    {
        this.commitLogReaderProcessor.resume();
    }

    @Override
    public boolean isPaused()
    {
        return this.commitLogReaderProcessor.isPaused();
    }

    @Override
    public long getReplicated()
    {
        return this.metrics.replicated.getCount();
    }

    @Override
    public long getErrors()
    {
        return this.metrics.errors.getCount();
    }

    @Override
    public long getFlushes()
    {
        return this.metrics.flushes.getCount();
    }

    @Override
    public int getPendingCommitLogFiles()
    {
        return this.metrics.pendingCommitLogFiles.getValue();
    }

    @Override
    public int getPendingSentMutations()
    {
        return this.metrics.pendingSentMutations.getValue();
    }

    @Override
    public long getReplicationLag() {
        return this.metrics.replicationLag.getValue();
    }

    @Override
    public String getEmittedOffset()
    {
        CommitLogPosition commitLogPosition = offsetFileWriter.emittedOffset();
        return commitLogPosition.segmentId + ":" + commitLogPosition.position;
    }

    @Override
    public String getSentOffset()
    {
        CommitLogPosition commitLogPosition = offsetFileWriter.sentOffset();
        return commitLogPosition.segmentId + ":" + commitLogPosition.position;
    }

    @Override
    public String getSyncedOffset()
    {
        CommitLogPosition commitLogPosition = commitLogReaderProcessor.syncedOffsetRef.get();
        return commitLogPosition.segmentId + ":" + commitLogPosition.position;
    }

    @Override
    public String getCommitedOffset()
    {
        CommitLogPosition commitLogPosition = offsetFileWriter.fileOffsetRef.get();
        return commitLogPosition.segmentId + ":" + commitLogPosition.position;
    }
}
