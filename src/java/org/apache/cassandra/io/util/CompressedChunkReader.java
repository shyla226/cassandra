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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.ChecksumType;

public abstract class CompressedChunkReader extends AbstractReaderFileProxy implements ChunkReader
{
    final CompressionMetadata metadata;
    final int maxCompressedLength;

    protected CompressedChunkReader(AsynchronousChannelProxy channel, CompressionMetadata metadata)
    {
        super(channel, metadata.dataLength);
        this.metadata = metadata;
        this.maxCompressedLength = metadata.maxCompressedLength();
        assert Integer.bitCount(metadata.chunkLength()) == 1; //must be a power of two
    }

    @VisibleForTesting
    public double getCrcCheckChance()
    {
        return metadata.parameters.getCrcCheckChance();
    }

    public boolean maybeCheckCrc()
    {
        return metadata.parameters.maybeCheckCrc();
    }

    @Override
    public String toString()
    {
        return String.format("CompressedChunkReader.%s(%s - %s, chunk length %d, data length %d)",
                             getClass().getSimpleName(),
                             channel.filePath(),
                             metadata.compressor().getClass().getSimpleName(),
                             metadata.chunkLength(),
                             metadata.dataLength);
    }

    @Override
    public int chunkSize()
    {
        return metadata.chunkLength();
    }

    @Override
    public BufferType preferredBufferType()
    {
        return metadata.compressor().preferredBufferType();
    }

    @Override
    public Rebufferer instantiateRebufferer()
    {
        return new BufferManagingRebufferer.Aligned(this);
    }

    public static class Standard extends CompressedChunkReader
    {
        // we read the raw compressed bytes into this buffer, then uncompressed them into the provided one.
        private final ThreadLocal<ByteBuffer> compressedHolder;

        public Standard(AsynchronousChannelProxy channel, CompressionMetadata metadata)
        {
            super(channel, metadata);
            compressedHolder = ThreadLocal.withInitial(this::allocateBuffer);
        }

        public ByteBuffer allocateBuffer()
        {
            return allocateBuffer(Math.min(maxCompressedLength,
                                           metadata.compressor().initialCompressedBufferLength(metadata.chunkLength())));
        }

        public ByteBuffer allocateBuffer(int size)
        {
            return metadata.compressor().preferredBufferType().allocate(size);
        }

        @Override
        public CompletableFuture<ByteBuffer> readChunk(long position, ByteBuffer uncompressed)
        {
            CompletableFuture<ByteBuffer> futureBuffer = new CompletableFuture<>();


            // accesses must always be aligned
            assert (position & -uncompressed.capacity()) == position;
            assert position <= fileLength;

            CompressionMetadata.Chunk chunk = metadata.chunkFor(position);
            ByteBuffer currentCompressed = compressedHolder.get();

            if (currentCompressed.capacity() < chunk.length)
            {
                currentCompressed = allocateBuffer(chunk.length);
                compressedHolder.set(currentCompressed);
            }
            else
            {
                currentCompressed.clear();
            }

            currentCompressed.limit(chunk.length);
            ByteBuffer compressed = currentCompressed;

            channel.read(compressed, chunk.offset, new CompletionHandler<Integer, ByteBuffer>()
            {
                public void completed(Integer result, ByteBuffer attachment)
                {
                    if (result != chunk.length)
                    {
                        futureBuffer.completeExceptionally(new CorruptBlockException(channel.filePath() + " result = "+ result, chunk));
                        return;
                    }

                    compressed.flip();
                    uncompressed.clear();

                    try
                    {
                        metadata.compressor().uncompress(compressed, uncompressed);
                    }
                    catch (IOException e)
                    {
                        futureBuffer.completeExceptionally(new CorruptBlockException(channel.filePath(), chunk, e));
                        return;
                    }
                    finally
                    {
                        uncompressed.flip();
                    }

                    if (getCrcCheckChance() > ThreadLocalRandom.current().nextDouble())
                    {
                        compressed.rewind();
                        int checksum = (int) ChecksumType.CRC32.of(compressed);

                        compressed.clear().limit(Integer.BYTES);
                        channel.read(compressed, chunk.offset + chunk.length, new CompletionHandler<Integer, ByteBuffer>()
                        {
                            public void completed(Integer result, ByteBuffer attachment)
                            {
                                if (result != Integer.BYTES || compressed.getInt(0) != checksum)
                                {
                                    futureBuffer.completeExceptionally(new CorruptBlockException(channel.filePath(), chunk));
                                    return;
                                }

                                futureBuffer.complete(uncompressed);
                            }

                            public void failed(Throwable exc, ByteBuffer attachment)
                            {
                                futureBuffer.completeExceptionally(exc);
                            }
                        });
                    }
                    else
                    {
                        futureBuffer.complete(uncompressed);
                    }
                }

                public void failed(Throwable exc, ByteBuffer attachment)
                {
                    futureBuffer.completeExceptionally(exc);
                }
            });

            return futureBuffer;
        }
    }

    public static class Mmap extends CompressedChunkReader
    {
        protected final MmappedRegions regions;

        public Mmap(AsynchronousChannelProxy channel, CompressionMetadata metadata, MmappedRegions regions)
        {
            super(channel, metadata);
            this.regions = regions;
        }

        @Override
        public CompletableFuture<ByteBuffer> readChunk(long position, ByteBuffer uncompressed)
        {
            try
            {
                // accesses must always be aligned
                assert (position & -uncompressed.capacity()) == position;
                assert position <= fileLength;

                CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

                MmappedRegions.Region region = regions.floor(chunk.offset);
                long segmentOffset = region.offset();
                int chunkOffset = Ints.checkedCast(chunk.offset - segmentOffset);
                ByteBuffer compressedChunk = region.buffer();

                compressedChunk.position(chunkOffset).limit(chunkOffset + chunk.length);

                uncompressed.clear();

                try
                {
                    if (chunk.length <= maxCompressedLength)
                        metadata.compressor().uncompress(compressedChunk, uncompressed);
                    else
                        uncompressed.put(compressedChunk);
                }
                catch (IOException e)
                {
                    throw new CorruptBlockException(channel.filePath(), chunk, e);
                }
                uncompressed.flip();

                if (maybeCheckCrc())
                {
                    compressedChunk.position(chunkOffset).limit(chunkOffset + chunk.length);

                    int checksum = (int) ChecksumType.CRC32.of(compressedChunk);

                    compressedChunk.limit(compressedChunk.capacity());
                    if (compressedChunk.getInt() != checksum)
                        throw new CorruptBlockException(channel.filePath(), chunk);
                }
            }
            catch (CorruptBlockException e)
            {
                // Make sure reader does not see stale data.
                uncompressed.position(0).limit(0);
                throw new CorruptSSTableException(e, channel.filePath());
            }

            return CompletableFuture.completedFuture(uncompressed);
        }

        public void close()
        {
            regions.closeQuietly();
            super.close();
        }
    }
}
