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
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.cassandra.utils.MBeanWrapper;

/**
 * Consume a queue of commitlog files to read mutations.
 *
 * @author vroyer
 */
@Singleton
public class CommitLogReaderProcessor extends AbstractProcessor implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReaderProcessor.class);
    private static final String NAME = "CommitLogReader Processor";

    public static final String MBEAN_NAME = "org.apache.cassandra.cdc:type=SyncedOffset";

    public static final String ARCHIVE_FOLDER = "archive";
    public static final String ERROR_FOLDER = "error";

    // synced position
    private AtomicReference<CommitLogPosition> syncedOffsetRef = new AtomicReference<>(new CommitLogPosition(0, 0));

    private CountDownLatch syncedOffsetLatch = new CountDownLatch(1);

    private final PriorityBlockingQueue<File> commitLogQueue = new PriorityBlockingQueue<>(128, CommitLogUtil::compareCommitLogs);

    private final CommitLogReadHandler commitLogReadHandler;
    private final OffsetFileWriter offsetFileWriter;
    private final CommitLogTransfer commitLogTransfer;

    public CommitLogReaderProcessor(CommitLogReadHandler commitLogReadHandler,
                                    OffsetFileWriter offsetFileWriter) {
        super(NAME, 0);
        this.commitLogReadHandler = commitLogReadHandler;
        this.offsetFileWriter = offsetFileWriter;
        this.commitLogTransfer = new BlackHoleCommitLogTransfer();
    }

    public void submitCommitLog(File file)  {
        logger.debug("submit file={}", file.getAbsolutePath());
        if (file.getName().endsWith("_cdc.idx")) {
            // you can have old _cdc.idx file, ignore it
            long seg = CommitLogUtil.extractTimestamp(file.getName());
            int pos = 0;
            if (seg >= this.syncedOffsetRef.get().segmentId) {
                try {
                    List<String> lines = Files.readAllLines(file.toPath(), Charset.forName("UTF-8"));
                    pos = Integer.parseInt(lines.get(0));
                    boolean completed = false;
                    try {
                        if("COMPLETED".equals(lines.get(1))) {
                            completed = true;
                        }
                    } catch(Exception ex) {
                    }
                    syncedOffsetRef.set(new CommitLogPosition(seg, pos));
                    logger.debug("New synced position={} completed={}", syncedOffsetRef.get(), completed);

                    // unlock the processing of commitlogs
                    if (syncedOffsetLatch.getCount() > 0)
                    {
                        logger.debug("Releasing the syncedOffsetLatch");
                        syncedOffsetLatch.countDown();
                    }
                } catch(IOException ex) {
                    logger.warn("error while reading file=" + file.getName(), ex);
                }
            } else {
                logger.debug("Ignoring old synced position from file={} pos={}", file.getName(), pos);
            }
        } else {
            this.commitLogQueue.add(file);
        }
    }

    public void awaitSyncedPosition() throws InterruptedException
    {
        syncedOffsetLatch.await();
        logger.debug("syncedOffsetLatch released");
    }

    @Override
    public void process() throws InterruptedException
    {
        assert this.offsetFileWriter.offset().segmentId <= this.syncedOffsetRef.get().segmentId || this.offsetFileWriter.offset().position <= this.offsetFileWriter.offset().position : "file offset is greater than synced offset";
        File file = null;
        while(true) {
            file = this.commitLogQueue.take();
            long seg = CommitLogUtil.extractTimestamp(file.getName());

            // ignore file before the last write offset
            if (seg < this.offsetFileWriter.offset().segmentId) {
                logger.debug("Ignoring file={} before the replicated segment={}", file.getName(), this.offsetFileWriter.offset().segmentId);
                continue;
            }
            // ignore file beyond the last synced commitlog, it will be re-queued on a file modification.
            if (seg > this.syncedOffsetRef.get().segmentId) {
                logger.debug("Ignore a not synced file={}, last synced offset={}", file.getName(), this.syncedOffsetRef.get());
                continue;
            }
            logger.debug("processing file={} synced offset={}", file.getName(), this.syncedOffsetRef.get());
            assert seg <= this.syncedOffsetRef.get().segmentId: "reading a commitlog ahead the last synced offset";

            CommitLogReader commitLogReader = new CommitLogReader();
            try {
                // hack to use a dummy min position for segment ahead of the offetFile.
                CommitLogPosition minPosition = (seg > offsetFileWriter.offset().segmentId)
                        ? new CommitLogPosition(seg, 0)
                        : offsetFileWriter.offset();

                commitLogReader.readCommitLogSegment(commitLogReadHandler, file, minPosition, false);
                logger.debug("Successfully processed commitlog immutable={} minPosition={} file={}",
                        seg < this.syncedOffsetRef.get().segmentId, minPosition, file.getName());
                if (seg < this.syncedOffsetRef.get().segmentId) {
                    commitLogTransfer.onSuccessTransfer(file);
                }
            } catch(Exception e) {
                logger.warn("Failed to read commitlog immutable="+(seg < this.syncedOffsetRef.get().segmentId)+"file="+file.getName(), e);
                if (seg < this.syncedOffsetRef.get().segmentId) {
                    commitLogTransfer.onErrorTransfer(file);
                }
            }
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
