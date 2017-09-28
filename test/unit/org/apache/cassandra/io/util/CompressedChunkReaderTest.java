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

package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.junit.Assert.*;

public class CompressedChunkReaderTest
{
    @BeforeClass
    public static void setupClass()
    {
        System.setProperty("cassandra.native.aio.force", "true");
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testStandardChunkReader() throws IOException
    {
        final int chunkSize = 1024;
        final String message = "The quick brown fox jumps over the lazy dog";

        for (int i = -1; i < 2; i++)
            testReadChunkOnTPC_Standard(chunkSize, message, i, false);
    }

    @Test
    public void testStandardChunkReader_CorruptData() throws IOException
    {
        final int chunkSize = 1024;
        final String message = "The quick brown fox jumps over the lazy dog";

        for (int i = -1; i < 2; i++)
            testReadChunkOnTPC_Standard(chunkSize, message, i, true);
    }

    /**
     * Create a compressed file containing the message and uncompress it on the TPC thread.
     * Configure {@link BlockingCompressorMock} to throw a {@link org.apache.cassandra.concurrent.TPCUtils.WouldBlockException}
     * after {@code throwAt} invocations on a TPC thread.
     */
    private void testReadChunkOnTPC_Standard(int chunkSize, String message, int throwAt, boolean corruptData) throws IOException
    {
        CompressionParams params = BlockingCompressorMock.compressionParams(chunkSize, throwAt);
        ByteBuffer buffer = BufferPool.get(chunkSize, BufferType.OFF_HEAP);
        File file = createCompressedFile(params, message, corruptData);

        CompressionMetadata metadata = new CompressionMetadata(file.getAbsolutePath() + ".metadata", file.length(), true);
        try (AsynchronousChannelProxy channel = new AsynchronousChannelProxy(file.toString(), false);
             CompressedChunkReader.Standard reader = new CompressedChunkReader.Standard(channel, metadata))
        {
            CompletableFuture task = CompletableFuture.supplyAsync(() -> reader.readChunk(0, buffer), tpcExecutor())
                                                      .thenCompose(fut-> fut.handle((res, err) -> {
                                                          if (err != null)
                                                          { // this should only happen if the data was corrupted
                                                              assertTrue(corruptData);
                                                              assertTrue(err instanceof CorruptSSTableException);
                                                          }
                                                          else
                                                          {
                                                              checkBufferContent(message, res);
                                                          }
                                                          return CompletableFuture.completedFuture(null); // task done
                                                      }));

            TPCUtils.blockingAwait(task);
        }
        finally
        {
            BufferPool.put(buffer);
        }
    }

    @Test
    public void testMmapChunkReader() throws IOException
    {
        final int chunkSize = 1024;
        final String message = "The quick brown fox jumps over the lazy dog";

        for (int i = -1; i < 2; i++)
            testReadChunkOnTPC_Mmap(chunkSize, message, i, false);
    }

    @Test
    public void testMmapChunkReader_CorruptData() throws IOException
    {
        final int chunkSize = 1024;
        final String message = "The quick brown fox jumps over the lazy dog";

        for (int i = -1; i < 2; i++)
            testReadChunkOnTPC_Mmap(chunkSize, message, i, true);
    }

    /**
     * Create a compressed file containing the message and uncompress it on the TPC thread.
     * Configure {@link BlockingCompressorMock} to throw a {@link org.apache.cassandra.concurrent.TPCUtils.WouldBlockException}
     * after {@code throwAt} invocations on a TPC thread.
     */
    private void testReadChunkOnTPC_Mmap(int chunkSize, String message, int throwAt, boolean corruptData) throws IOException
    {
        CompressionParams params = BlockingCompressorMock.compressionParams(chunkSize, throwAt);
        ByteBuffer buffer = BufferPool.get(chunkSize, BufferType.OFF_HEAP);
        File file = createCompressedFile(params, message, corruptData);

        CompressionMetadata metadata = new CompressionMetadata(file.getAbsolutePath() + ".metadata", file.length(), true);
        try (AsynchronousChannelProxy asyncChannel = new AsynchronousChannelProxy(file.toString(), false);
             ChannelProxy channel = new ChannelProxy(file.toString());
             MmappedRegions regions = MmappedRegions.map(channel, metadata);
             CompressedChunkReader.Mmap reader = new CompressedChunkReader.Mmap(asyncChannel, metadata, regions))
        {

            CompletableFuture task = CompletableFuture.supplyAsync(() -> reader.readChunk(0, buffer), tpcExecutor())
                                                      .thenCompose(fut-> fut.handle((res, err) -> {
                                                          if (err != null)
                                                          { // this should only happen if the data was corrupted
                                                              assertTrue(corruptData);
                                                              assertTrue(err instanceof CorruptSSTableException);
                                                          }
                                                          else
                                                          {
                                                              checkBufferContent(message, res);
                                                          }
                                                          return CompletableFuture.completedFuture(null); // task done
                                                      }));
            TPCUtils.blockingAwait(task);

        }
        finally
        {
            BufferPool.put(buffer);
        }
    }

    private static void checkBufferContent(String message, ByteBuffer res)
    {
        assertNotNull(res);
        byte[] bb = new byte[message.length()];
        res.get(bb);
        assertTrue(Arrays.equals(message.getBytes(), bb));
    }

    private static Executor tpcExecutor()
    {
        return (runnable) -> TPC.bestTPCScheduler().execute(runnable, ExecutorLocals.create(), TPCTaskType.UNKNOWN);
    }

    private File createCompressedFile(CompressionParams params, String message, boolean corruptData) throws IOException
    {
        File f = File.createTempFile("compressed_", "");
        f.deleteOnExit();
        String filename = f.getAbsolutePath();
        MetadataCollector sstableMetadataCollector = new MetadataCollector(new ClusteringComparator(BytesType.instance));
        try (CompressedSequentialWriter writer = new CompressedSequentialWriter(f, filename + ".metadata",
                                                                                null, SequentialWriterOption.DEFAULT,
                                                                                params,
                                                                                sstableMetadataCollector))
        {
            writer.write(message.getBytes());
            writer.finish();
        }

        if (!corruptData)
            return f;

        try(RandomAccessFile modifier = new RandomAccessFile(f, "rw"))
        {
            modifier.seek(f.length() / 2);
            modifier.writeInt(0xFF);
            SyncUtil.sync(modifier);
        }

        return f;
    }
}
