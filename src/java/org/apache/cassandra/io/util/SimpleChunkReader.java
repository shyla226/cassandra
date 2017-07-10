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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.CorruptSSTableException;

class SimpleChunkReader extends AbstractReaderFileProxy implements ChunkReader
{
    private final static Logger logger = LoggerFactory.getLogger(SimpleChunkReader.class);
    private final int bufferSize;
    private final BufferType bufferType;

    SimpleChunkReader(AsynchronousChannelProxy channel, long fileLength, BufferType bufferType, int bufferSize)
    {
        super(channel, fileLength);
        this.bufferSize = bufferSize;
        this.bufferType = bufferType;
    }

    @Override
    public CompletableFuture<ByteBuffer> readChunk(long position, ByteBuffer buffer)
    {
        buffer.clear();

        CompletableFuture<ByteBuffer> futureBuffer = new CompletableFuture<>();
        channel.read(buffer, position, new CompletionHandler<Integer, ByteBuffer>()
        {
            public void completed(Integer result, ByteBuffer attachment)
            {
                buffer.flip();
                futureBuffer.complete(buffer);
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
    public Rebufferer instantiateRebufferer()
    {
        return new BufferManagingRebufferer.Unaligned(this);
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