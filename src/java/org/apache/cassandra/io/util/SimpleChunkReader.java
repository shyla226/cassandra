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

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;

class SimpleChunkReader extends AbstractReaderFileProxy implements ChunkReader
{
    private final static Logger logger = LoggerFactory.getLogger(SimpleChunkReader.class);
    private final int bufferSize;
    private final BufferType bufferType;

    SimpleChunkReader(AsynchronousChannelProxy channel, long fileLength, BufferType bufferType, int bufferSize)
    {
        super(channel, fileLength);
        this.bufferSize = bufferSize;
        this.bufferType = TPC.USE_AIO
                          ? BufferType.OFF_HEAP_ALIGNED
                          : bufferType;
    }

    private boolean isAlignedRead(long position, ByteBuffer buffer)
    {
        int blockMask = sectorSize - 1;
        return (position & blockMask) == 0
               && buffer.isDirect()
               && (buffer.capacity() & blockMask) == 0
               && (UnsafeByteBufferAccess.getAddress(buffer) & blockMask) == 0;
    }

    @Override
    public CompletableFuture<ByteBuffer> readChunk(long position, ByteBuffer buffer)
    {
        if (!channel.requiresAlignment() || isAlignedRead(position, buffer))
            return readChunkDirect(position, buffer);
        else
            return readChunkWithAlignment(position, buffer);
    }

    private CompletableFuture<ByteBuffer> readChunkDirect(long position, ByteBuffer buffer)
    {
        buffer.clear();

        CompletableFuture<ByteBuffer> futureBuffer = new CompletableFuture<>();
        channel.read(buffer, position, new CompletionHandler<Integer, ByteBuffer>()
        {
            public void completed(Integer result, ByteBuffer attachment)
            {
                buffer.flip();
                if (!futureBuffer.complete(buffer))
                    logger.warn("Failed to complete read from {}, already timed out.", channel.filePath);
            }

            public void failed(Throwable exc, ByteBuffer attachment)
            {
                // Make sure reader does not see stale data.
                buffer.position(0).limit(0);
                futureBuffer.completeExceptionally(new CorruptSSTableException(exc, channel.filePath()));
            }
        });

        return futureBuffer;
    }

    private CompletableFuture<ByteBuffer> readChunkWithAlignment(long position, ByteBuffer buffer)
    {
        buffer.clear();
        BufferHandle scratchHandle = getScratchHandle();
        ByteBuffer scratchBuffer = scratchHandle.get((buffer.capacity() + sectorSize * 2 - 1) & -sectorSize);

        CompletableFuture<ByteBuffer> futureBuffer = new CompletableFuture<>();

        //O_DIRECT read positions must be aligned to DMA size
        long alignedOffset = roundDownToBlockSize(position);
        int alignmentShift = Ints.checkedCast(position - alignedOffset);

        scratchBuffer.clear();
        scratchBuffer.limit(roundUpToBlockSize(buffer.capacity() + alignmentShift));

        channel.read(scratchBuffer, alignedOffset, new CompletionHandler<Integer, ByteBuffer>()
        {
            public void completed(Integer result, ByteBuffer attachment)
            {
                buffer.clear();
                if (result >= alignmentShift)
                {
                    scratchBuffer.limit(Math.min(result, buffer.capacity() + alignmentShift));
                    scratchBuffer.position(alignmentShift);
                    buffer.put(scratchBuffer);
                }
                buffer.flip();
                scratchHandle.recycle();
                if (!futureBuffer.complete(buffer))
                    logger.warn("Failed to complete read from {}, already timed out.", channel.filePath);
            }

            public void failed(Throwable exc, ByteBuffer attachment)
            {
                // Make sure reader does not see stale data.
                buffer.position(0).limit(0);

                scratchHandle.recycle();
                futureBuffer.completeExceptionally(new CorruptSSTableException(exc, channel.filePath()));
            }
        });

        return futureBuffer;
    }

    @Override
    public int chunkSize()
    {
        return bufferSize;
    }

    @Override
    public BufferType preferredBufferType()
    {
        return bufferType;
    }

    @Override
    public boolean isMmap()
    {
        return false;
    }

    @Override
    public ChunkReader withChannel(AsynchronousChannelProxy channel)
    {
        return new SimpleChunkReader(channel, fileLength, bufferType, bufferSize);
    }

    @Override
    @SuppressWarnings("resource") // channel closed by the PrefetchingRebufferer
    public Rebufferer instantiateRebufferer(FileAccessType accessType)
    {
        if (accessType == FileAccessType.RANDOM || Integer.bitCount(bufferSize) != 1 || PrefetchingRebufferer.READ_AHEAD_SIZE_KB <= 0)
            return instantiateRebufferer(this);

        AsynchronousChannelProxy channel = this.channel.maybeBatched(PrefetchingRebufferer.READ_AHEAD_VECTORED);
        return new PrefetchingRebufferer(instantiateRebufferer(withChannel(channel)), channel);
    }

    private Rebufferer instantiateRebufferer(ChunkReader reader)
    {
        if (Integer.bitCount(bufferSize) == 1)
            return new BufferManagingRebufferer.Aligned(reader);
        else
            return new BufferManagingRebufferer.Unaligned(reader);
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s - chunk length %d, data length %d)",
                             getClass().getSimpleName(),
                             channel.filePath(),
                             bufferSize,
                             fileLength());
    }
}