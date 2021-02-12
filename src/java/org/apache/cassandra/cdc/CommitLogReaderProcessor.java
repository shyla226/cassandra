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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;

/**
 * Consume a queue of commitlog files to read mutations.
 *
 * @author vroyer
 */
@Singleton
public class CommitLogReaderProcessor extends AbstractProcessor implements AutoCloseable, CdcSyncListeners.CdcSyncListener
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReaderProcessor.class);
    private static final String NAME = "CommitLogReader Processor";

    public static final String MBEAN_NAME = "org.apache.cassandra.cdc:type=SyncedOffset";

    public static final String ARCHIVE_FOLDER = "archive";
    public static final String ERROR_FOLDER = "error";

    // synced position
    protected AtomicReference<CommitLogPosition> syncedOffsetRef = new AtomicReference<>(new CommitLogPosition(0, 0));
    private CountDownLatch syncedOffsetLatch = new CountDownLatch(1);

    final PriorityBlockingQueue<File> commitLogQueue = new PriorityBlockingQueue<>(128, CommitLogUtil::compareCommitLogs);

    final CommitLogReadHandler commitLogReadHandler;
    final OffsetFileWriter offsetFileWriter;
    final CommitLogTransfer commitLogTransfer;

    volatile CountDownLatch pauseCountLatch = null;

    public CommitLogReaderProcessor(CommitLogReadHandler commitLogReadHandler,
                                    OffsetFileWriter offsetFileWriter) {
        super(NAME, 100);
        this.commitLogReadHandler = commitLogReadHandler;
        this.offsetFileWriter = offsetFileWriter;
        this.commitLogTransfer = new BlackHoleCommitLogTransfer();
    }

    @Override
    public void notify(int version, long segment, int offset, boolean completed)
    {
        updateSyncedOffset(segment, offset, completed);
        commitLogQueue.put(new File(DatabaseDescriptor.getCDCLogLocation(), CommitLogDescriptor.fileName(version, segment)));
    }

    public void updateSyncedOffset(long seg, int pos, boolean completed)
    {
        syncedOffsetRef.set(new CommitLogPosition(seg, pos));
        logger.debug("New synced position={} completed={}", syncedOffsetRef.get(), completed);

        // unlock the processing of commitlogs
        if (syncedOffsetLatch.getCount() > 0)
        {
            logger.debug("Releasing the syncedOffsetLatch");
            syncedOffsetLatch.countDown();
        }
    }

    public void submitCommitLog(File file)
    {
        logger.debug("submit file={}", file.getAbsolutePath());
        commitLogQueue.put(file);
    }

    public synchronized void pause() {
        if (pauseCountLatch == null)
            pauseCountLatch = new CountDownLatch(1);
    }

    public synchronized void resume() {
        if (pauseCountLatch != null)
        {
            pauseCountLatch.countDown();
            pauseCountLatch = null;
        }
    }

    public synchronized boolean isPaused() {
        return pauseCountLatch != null;
    }

    @Override
    public void process()
    {
        try
        {
            syncedOffsetLatch.await();
            logger.debug("syncedOffsetLatch released");
        } catch(InterruptedException e) {
            logger.error("error:", e);
        }

        assert this.offsetFileWriter.committedOffset().segmentId < this.syncedOffsetRef.get().segmentId ||
               (this.offsetFileWriter.committedOffset().segmentId == this.syncedOffsetRef.get().segmentId &&
                this.offsetFileWriter.committedOffset().position <= this.syncedOffsetRef.get().position) : "file offset is greater than synced offset";
        try
        {
            while (true)
            {
                if (pauseCountLatch != null) {
                    try {
                        pauseCountLatch.await();
                    } catch(InterruptedException e) {
                    }
                }

                File file = this.commitLogQueue.take();
                long seg = CommitLogUtil.extractTimestamp(file.getName());

                // ignore file before the last write offset
                if (seg < this.offsetFileWriter.committedOffset().segmentId)
                {
                    logger.debug("Ignoring file={} before the replicated segment={}", file.getName(), this.offsetFileWriter.committedOffset().segmentId);
                    continue;
                }
                // ignore file beyond the last synced commitlog, it will be re-queued on a file modification.
                if (seg > this.syncedOffsetRef.get().segmentId)
                {
                    logger.debug("Ignore a not synced file={}, last synced offset={}", file.getName(), this.syncedOffsetRef.get());
                    continue;
                }
                logger.debug("Processing file={} synced offset={}", file.getName(), this.syncedOffsetRef.get());
                assert seg <= this.syncedOffsetRef.get().segmentId : "reading a commitlog ahead the last synced offset";

                CommitLogReader commitLogReader = new CommitLogReader();
                try
                {
                    // hack to use a dummy min position for segment ahead of the offetFile.
                    CommitLogPosition minPosition = (seg > offsetFileWriter.sentOffset().segmentId)
                                                    ? new CommitLogPosition(seg, 0)
                                                    : offsetFileWriter.sentOffset();

                    logger.debug("Processing commitlog immutable={} minPosition={} file={}",
                                 seg < this.syncedOffsetRef.get().segmentId, minPosition, file.getName());
                    commitLogReader.readCommitLogSegment(commitLogReadHandler, file, minPosition, false);
                    logger.debug("Successfully processed commitlog immutable={} minPosition={} file={}",
                                 seg < this.syncedOffsetRef.get().segmentId, minPosition, file.getName());
                    if (seg < this.syncedOffsetRef.get().segmentId)
                    {
                        commitLogTransfer.onSuccessTransfer(file);
                    }
                }
                catch (Exception e)
                {
                    logger.warn("Failed to read commitlog immutable=" + (seg < this.syncedOffsetRef.get().segmentId) + "file=" + file.getName(), e);
                    if (seg < this.syncedOffsetRef.get().segmentId)
                    {
                        commitLogTransfer.onErrorTransfer(file);
                    }
                }
            }
        } catch(Throwable t) {
            logger.error("unexpected error:", t);
        }
    }

    @Override
    public void initialize() throws Exception
    {
        String cdcLogLocation = DatabaseDescriptor.getCDCLogLocation();

        File archiveDir = new File(cdcLogLocation, ARCHIVE_FOLDER);
        if (!archiveDir.exists()) {
            if (!archiveDir.mkdir()) {
                throw new IOException("Failed to create " + archiveDir);
            }
        }
        File errorDir = new File(cdcLogLocation, ERROR_FOLDER);
        if (!errorDir.exists()) {
            if (!errorDir.mkdir()) {
                throw new IOException("Failed to create " + errorDir);
            }
        }
    }

    /**
     * Override destroy to clean up resources after stopping the processor
     */
    @Override
    public void close() {
    }
}
