/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.utils.memory.BufferPool;

/**
 * Buffer manager used for reading from a ChunkReader when cache is not in use. Instances of this class are
 * reader-specific and thus do not need to be thread-safe since the reader itself isn't.
 */
public abstract class BufferManagingRebufferer implements Rebufferer
{
    private static final BufferPool bufferPool = new BufferPool();
    private final Deque<BufferHolderImpl> buffers;
    protected final ChunkReader source;

    abstract long alignedPosition(long position);

    BufferManagingRebufferer(ChunkReader wrapped)
    {
        this.buffers = new ArrayDeque<>();
        this.source = wrapped;
    }

    @Override
    public void closeReader()
    {
    }

    @Override
    public void close()
    {
        source.close();
    }

    @Override
    public AsynchronousChannelProxy channel()
    {
        return source.channel();
    }

    @Override
    public long fileLength()
    {
        return source.fileLength();
    }

    @Override
    public BufferHolder rebuffer(long position)
    {
        assert !TPC.isTPCThread();

        BufferHolder ret = newBufferHolder(alignedPosition(position));
        try
        {
            source.readChunk(ret.offset(), ret.buffer()).join();
        }
        catch (CompletionException t)
        {
            ret.release();
            if (t.getCause() != null && t.getCause() instanceof RuntimeException)
                throw (RuntimeException) t.getCause();

            throw t;
        }

        return ret;
    }

    @Override
    public CompletableFuture<BufferHolder> rebufferAsync(long position)
    {
        BufferHolder ret = newBufferHolder(alignedPosition(position));
        return source.readChunk(ret.offset(), ret.buffer())
                     .handle((buffer, err) -> {
                         if (err != null)
                         {
                             ret.release();
                             throw new CompletionException(err);
                         }
                         return ret;
                     });
    }

    @Override
    public int rebufferSize()
    {
        return source.chunkSize();
    }

    public BufferHolder rebuffer(long position, ReaderConstraint constraint)
    {
        if (constraint == ReaderConstraint.ASYNC)
        {
            // There is no point to try to support this as all async operations require a minimum of cached data where
            // multiple reads are re-done on a cache failure (e.g. when initializing a partition iterator).
            throw new UnsupportedOperationException("Async read is not supported without caching.");
        }

        return rebuffer(position);
    }

    @Override
    public double getCrcCheckChance()
    {
        return source.getCrcCheckChance();
    }

    @Override
    public String toString()
    {
        return "BufferManagingRebufferer." + getClass().getSimpleName() + ':' + source;
    }

    BufferHolder newBufferHolder(long offset)
    {
        BufferHolderImpl ret = buffers.pollFirst();
        if (ret == null)
            ret = new BufferHolderImpl();

        return ret.init(offset);
    }

    private class BufferHolderImpl implements Rebufferer.BufferHolder
    {
        private long offset = -1L;
        private ByteBuffer buffer = null;

        private BufferHolderImpl init(long offset)
        {
            assert this.offset == -1L && this.buffer == null : "Attempted to initialize before releasing for previous use";

            this.offset = offset;
            this.buffer = bufferPool.get(source.chunkSize(), source.preferredBufferType()).order(ByteOrder.BIG_ENDIAN);
            this.buffer.limit(0);

            return this;
        }

        public ByteBuffer buffer()
        {
            return buffer;
        }

        public long offset()
        {
            return offset;
        }

        public void release()
        {
            assert offset != -1 && buffer != null : "released twice";

            bufferPool.put(buffer);
            buffer = null;
            offset = -1;

            buffers.offerFirst(this);
        }
    }

    static class Unaligned extends BufferManagingRebufferer
    {
        Unaligned(ChunkReader wrapped)
        {
            super(wrapped);
        }

        @Override
        long alignedPosition(long position)
        {
            return position;
        }
    }

    static class Aligned extends BufferManagingRebufferer
    {
        Aligned(ChunkReader wrapped)
        {
            super(wrapped);
            assert Integer.bitCount(wrapped.chunkSize()) == 1;
        }

        @Override
        long alignedPosition(long position)
        {
            return position & -source.chunkSize();
        }
    }
}
