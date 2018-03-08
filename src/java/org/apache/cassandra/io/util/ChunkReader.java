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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.compress.BufferType;
import org.jctools.queues.MpmcArrayQueue;

/**
 * RandomFileReader component that reads data from a file into a provided buffer and may have requirements over the
 * size and alignment of reads.
 * A caching or buffer-managing rebufferer will reference one of these to do the actual reading.
 * Note: Implementations of this interface must be thread-safe!
 */
public interface ChunkReader extends RebuffererFactory
{
    /**
     * Read the chunk at the given position, attempting to fill the capacity of the given buffer.
     * The filled buffer must be positioned at 0, with limit set at the size of the available data.
     * The source may have requirements for the positioning and/or size of the buffer (e.g. chunk-aligned and
     * chunk-sized). These must be satisfied by the caller. 
     */
    CompletableFuture<ByteBuffer> readChunk(long position, ByteBuffer buffer);

    /**
     * Buffer size required for this rebufferer. Must be power of 2 if alignment is required.
     */
    int chunkSize();

    /**
     * Specifies type of buffer the caller should attempt to give.
     * This is not guaranteed to be fulfilled.
     */
    BufferType preferredBufferType();

    /**
     * @return true if this chunk reader is using memory mapped regions instead of a file channel for reading.
     */
    boolean isMmap();

    /**
     * Return a new chunk reader identical to the current chunk reader but using the new channel, if possible.
     * This is currently not supported for {@link CompressedChunkReader.Mmap} because it is not required and
     * it would require new memory mapped regions. Therefore if {@link #isMmap()} returns true, this method
     * will throw an unsupported operation exception.
     *
     * @throws UnsupportedOperationException for {@link CompressedChunkReader.Mmap}
     * @return a copy of the chunk reader using the new channel.
     */
    ChunkReader withChannel(AsynchronousChannelProxy channel);

    // Scratch buffers for performing unaligned reads of chunks, where buffers need to be larger than 64k and thus
    // unsuitable for BufferPool. These buffers may grow until they reach the maximum size in use, and will not shrink.
    // TODO: This should eventually be handled by the BufferPool
    static final MpmcArrayQueue<BufferHandle> scratchBuffers = new MpmcArrayQueue<>(Short.MAX_VALUE);
    static final Long memoryLimit = Long.getLong("dse.total_chunk_reader_buffer_limit_mb", 128) * 1024 * 1024;
    static final AtomicLong bufferSize = new AtomicLong();

    default BufferHandle getScratchHandle()
    {
        BufferHandle handle = scratchBuffers.relaxedPoll();
        return handle == null ? new BufferHandle() : handle;
    }

    static class BufferHandle
    {
        private ByteBuffer alignedBuffer;

        BufferHandle()
        {
            this.alignedBuffer = null;
        }

        ByteBuffer get(int size)
        {
            if (alignedBuffer == null || size > alignedBuffer.capacity())
            {
                if (alignedBuffer != null)
                {
                    bufferSize.getAndAdd(-alignedBuffer.capacity());
                    FileUtils.clean(alignedBuffer, true);
                    alignedBuffer = null;
                }

                alignedBuffer = BufferType.OFF_HEAP_ALIGNED.allocate(size);
                bufferSize.getAndAdd(alignedBuffer.capacity());
                return alignedBuffer;
            }

            return alignedBuffer;
        }

        void recycle()
        {
            //If we are over our limit then we can free this buffer
            if (bufferSize.get() > memoryLimit || !scratchBuffers.relaxedOffer(this))
            {
                bufferSize.getAndAdd(-alignedBuffer.capacity());
                FileUtils.clean(alignedBuffer, true);
            }
        }
    }

    @VisibleForTesting
    public static ChunkReader simple(AsynchronousChannelProxy channel, long fileLength, BufferType bufferType, int bufferSize)
    {
        return new SimpleChunkReader(channel, fileLength, bufferType, bufferSize);
    }
}