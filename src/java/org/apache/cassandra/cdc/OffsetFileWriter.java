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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;

/**
 * Track the last acknowledged replicated mutation.
 */
@Singleton
public class OffsetFileWriter extends AbstractProcessor implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(OffsetFileWriter.class);

    public static final String COMMITLOG_OFFSET_FILE = "commitlog_offset.dat";

    private final File offsetFile;
    private final MutationEmitter mutationEmitter;

    final AtomicReference<CommitLogPosition> sentOffsetRef = new AtomicReference<>(new CommitLogPosition(0, 0));
    final AtomicReference<CommitLogPosition> emittedOffsetRef = new AtomicReference<>(new CommitLogPosition(0, 0));
    final AtomicReference<CommitLogPosition> fileOffsetRef = new AtomicReference<>(new CommitLogPosition(0, 0));
    final BlockingQueue<MutationEmitter.MutationFuture> sentMutations = new ArrayBlockingQueue<>(128);

    private final OffsetFlushPolicy offsetFlushPolicy;
    volatile long timeOfLastFlush = System.currentTimeMillis();
    volatile long notCommittedEvents = 0L;
    volatile long replicationLag = -1L;

    public OffsetFileWriter(MutationEmitter mutationEmitter)
    {
        super("OffsetFileWriter", 1000);
        this.mutationEmitter = mutationEmitter;
        this.offsetFlushPolicy = new OffsetFlushPolicy.AlwaysFlushOffsetPolicy();
        this.offsetFile = new File(DatabaseDescriptor.getCDCLogLocation(), COMMITLOG_OFFSET_FILE);
    }

    public CommitLogPosition committedOffset()
    {
        return this.fileOffsetRef.get();
    }

    public CommitLogPosition sentOffset()
    {
        return this.sentOffsetRef.get();
    }

    public CommitLogPosition emittedOffset()
    {
        return this.emittedOffsetRef.get();
    }

    public void markOffset(CommitLogPosition sourceOffset)
    {
        this.fileOffsetRef.set(sourceOffset);
    }

    public void flush()
    {
        saveOffset();
        notCommittedEvents = 0L;
        timeOfLastFlush = System.currentTimeMillis();
        CdcReplicationPlugin.instance.metrics.flushes.mark();
        logger.debug("Offset flushed file=" + offsetFile.getAbsolutePath());
    }

    @Override
    public void initialize() throws IOException
    {
        if (offsetFile.exists())
        {
            loadOffset();
        }
        else
        {
            Path parentPath = offsetFile.toPath().getParent();
            if (!parentPath.toFile().exists())
                Files.createDirectories(parentPath);
            saveOffset();
        }
    }

    @Override
    public void close() throws IOException
    {
        saveOffset();
    }

    /**
     * The actual work the processor is doing. This method will be executed in a while loop
     * until processor stops or encounters exception.
     */
    @Override
    public void process() throws InterruptedException, IOException
    {
        while (true)
        {
            try
            {
                MutationEmitter.MutationFuture mutationFuture = this.sentMutations.take();
                while (true)
                {
                    try
                    {
                        final long mutationTimestamp = mutationFuture.mutation.ts;
                        mutationFuture.sentFuture.thenApply(l -> {
                            // update metrics
                            CdcReplicationPlugin.instance.metrics.replicated.mark();
                            replicationLag = System.currentTimeMillis() - mutationTimestamp;
                            return l;
                        }).get();

                        if (mutationFuture.sentFuture.isCompletedExceptionally() || mutationFuture.sentFuture.isCancelled())
                        {
                            logger.debug("mutation={} not replicated, retrying", mutationFuture.mutation);
                            CdcReplicationPlugin.instance.metrics.errors.mark();
                            mutationFuture = mutationFuture.retry(mutationEmitter);
                        }
                        else
                        {
                            logger.debug("mutation={} replicated", mutationFuture.mutation);
                            sentOffsetRef.set(new CommitLogPosition(mutationFuture.mutation.segment, mutationFuture.mutation.position));
                            maybeCommitOffset(mutationFuture.mutation);
                            break;
                        }
                    }
                    catch (Exception e)
                    {
                        logger.warn("error:", e);
                        Thread.sleep(10000);    // retry 10s later
                        CdcReplicationPlugin.instance.metrics.errors.mark();
                        mutationFuture = mutationFuture.retry(mutationEmitter);
                    }
                }
            }
            catch (Exception e)
            {
                logger.error("error:", e);
            }
        }
    }


    public static String serializePosition(CommitLogPosition commitLogPosition)
    {
        return Long.toString(commitLogPosition.segmentId) + File.pathSeparatorChar + Integer.toString(commitLogPosition.position);
    }

    public static CommitLogPosition deserializePosition(String s)
    {
        String[] segAndPos = s.split(Character.toString(File.pathSeparatorChar));
        return new CommitLogPosition(Long.parseLong(segAndPos[0]), Integer.parseInt(segAndPos[1]));
    }

    private synchronized void saveOffset()
    {
        try (FileWriter out = new FileWriter(this.offsetFile))
        {
            out.write(serializePosition(fileOffsetRef.get()));
        }
        catch (IOException e)
        {
            logger.error("Failed to save offset for file " + offsetFile.getName(), e);
        }
    }

    private synchronized void loadOffset() throws IOException
    {
        try (BufferedReader br = new BufferedReader(new FileReader(offsetFile)))
        {
            fileOffsetRef.set(deserializePosition(br.readLine()));
            logger.debug("file offset={}", fileOffsetRef.get());
        }
        catch (IOException e)
        {
            logger.error("Failed to load offset for file " + offsetFile.getName(), e);
            throw e;
        }
    }

    void maybeCommitOffset(Mutation record)
    {
        long timeSinceLastFlush = System.currentTimeMillis() - timeOfLastFlush;
        if (offsetFlushPolicy.shouldFlush(Duration.ofMillis(timeSinceLastFlush), notCommittedEvents))
        {
            SourceInfo source = record.source;
            markOffset(source.commitLogPosition);
            flush();
        }
    }

    public long replicationLag()
    {
        return this.replicationLag;
    }
}
