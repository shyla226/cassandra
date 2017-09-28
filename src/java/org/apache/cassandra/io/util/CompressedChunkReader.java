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
import java.util.concurrent.Executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.ChecksumType;

public abstract class CompressedChunkReader extends AbstractReaderFileProxy implements ChunkReader
{
    static final int CHECKSUM_BYTES = Integer.BYTES;

    private final static Logger logger = LoggerFactory.getLogger(CompressedChunkReader.class);
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

    public boolean shouldCheckCrc()
    {
        return metadata.parameters.shouldCheckCrc();
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
        return TPC.USE_AIO
               ? BufferType.OFF_HEAP_ALIGNED
               : metadata.compressor().preferredBufferType();
    }

    @Override
    public Rebufferer instantiateRebufferer()
    {
        return new BufferManagingRebufferer.Aligned(this);
    }

    private static Executor ioExecutor()
    {
        return (runnable) -> TPC.ioScheduler().execute(runnable, ExecutorLocals.create(), TPCTaskType.UNKNOWN);
    }

    public static class Standard extends CompressedChunkReader
    {
        final Supplier<Integer> bufferSize;

        public Standard(AsynchronousChannelProxy channel, CompressionMetadata metadata)
        {
            super(channel, metadata);

            bufferSize = Suppliers.memoize(() -> {
                int size = Math.min(maxCompressedLength,
                                    metadata.compressor().initialCompressedBufferLength(metadata.chunkLength()));

                // O_DIRECT requires position and length to be aligned to page size.

                // This means we could only use the target buffer directly if it is aligned, and even then we would
                // have to read the checksum separately, which we'd rather avoid.
                // So make sure we have space for the aligned read.
                size = Math.max(size, metadata.chunkLength());
                size += CHECKSUM_BYTES;

                // Alignment means we need to round the size up (to cover length of request plus extra to fill block)
                // plus one more for alignment (one part of which sits before wanted data, the rest after it).
                return TPC.roundUpToBlockSize(size) + TPC.AIO_BLOCK_SIZE;
            });
        }

        @Override
        public CompletableFuture<ByteBuffer> readChunk(long position, ByteBuffer uncompressed)
        {
            CompletableFuture<ByteBuffer> ret = new CompletableFuture<>();
            BufferHandle bufferHandle = scratchBuffers.get();

            try
            {
                doReadChunk(position, uncompressed, ret, bufferHandle);
            }
            catch (TPCUtils.WouldBlockException e)
            {
                ioExecutor().execute(() -> doReadChunk(position, uncompressed, ret, bufferHandle));
            }
            catch (Throwable t)
            {
                error(t, uncompressed, ret, bufferHandle);
            }

            return ret;
        }

        private void doReadChunk(long position, ByteBuffer uncompressed, CompletableFuture<ByteBuffer> futureBuffer, BufferHandle bufferHandle)
        {
            // accesses must always be aligned
            assert (position & -metadata.chunkLength()) == position;
            assert position <= fileLength;

            CompressionMetadata.Chunk chunk = metadata.chunkFor(position);
            ByteBuffer compressed = bufferHandle.get(bufferSize.get());

            // TODO: We need to evaluate what effect alignment has on Java AIO, and whether or not we should
            // have a CompressedChunkReader subclass that does not apply alignment (and therefore is also
            // able to skip copying for non-compressed chunks).

            //O_DIRECT read positions must be aligned to DMA size
            long alignedOffset = TPC.roundDownToBlockSize(chunk.offset);
            int alignmentShift = Ints.checkedCast(chunk.offset - alignedOffset);

            // We could optimize for non-compressed chunk with alignmentShift == 0 && !shouldCheckCrc, but that will
            // unfortunately be too rare to have any noticeable effect.

            compressed.clear();
            compressed.limit(TPC.roundUpToBlockSize(chunk.length + alignmentShift + CHECKSUM_BYTES));

            channel.read(compressed, alignedOffset, new CompletionHandler<Integer, ByteBuffer>()
            {
                public void completed(Integer result, ByteBuffer attachment)
                {
                    try
                    {
                        if (result < chunk.length + alignmentShift + CHECKSUM_BYTES)
                            throw new CorruptBlockException(channel.filePath() + " result = " + result, chunk);

                        compressed.limit(chunk.length + alignmentShift);
                        compressed.position(alignmentShift);
                        uncompressed.clear();

                        //CASSANDRA-10520 adds this threshold where we skip decompressing if
                        //the compression ratio is not enough of a win to be worth it.
                        if (chunk.length < maxCompressedLength)
                        {
                            try
                            {
                                metadata.compressor().uncompress(compressed, uncompressed);
                            }
                            catch (IOException e)
                            {
                                throw new CorruptBlockException(channel.filePath(), chunk, e);
                            }
                        }
                        else
                        {
                            uncompressed.put(compressed);
                        }
                        uncompressed.flip();

                        if (shouldCheckCrc())
                        {
                            compressed.limit(chunk.length + alignmentShift).position(alignmentShift);
                            int checksum = (int) ChecksumType.CRC32.of(compressed);

                            //Change the limit to include the checksum
                            compressed.limit(compressed.capacity());
                            if (compressed.getInt() != checksum)
                                throw new CorruptBlockException(channel.filePath(), chunk);
                        }

                    }
                    catch (TPCUtils.WouldBlockException ex)
                    {
                        ioExecutor().execute(() -> completed(result, attachment));
                        return;
                    }
                    catch (Throwable t)
                    {
                        error(t, uncompressed, futureBuffer, bufferHandle);
                        return;
                    }

                    // Pass control outside the try..finally block as we don't want to catch processing exceptions.
                    bufferHandle.recycle();
                    futureBuffer.complete(uncompressed);
                }

                public void failed(Throwable t, ByteBuffer attachment)
                {
                    error(t, uncompressed, futureBuffer, bufferHandle);
                }
            });
        }

        void error(Throwable t, ByteBuffer uncompressed, CompletableFuture<ByteBuffer> futureBuffer, BufferHandle bufferHandle)
        {
            // Make sure reader does not see stale data.
            uncompressed.position(0).limit(0);

            bufferHandle.recycle();
            futureBuffer.completeExceptionally(new CorruptSSTableException(t, channel.filePath()));
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
            CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
            try
            {
                future.complete(doReadChunk(position, uncompressed));
            }
            catch (TPCUtils.WouldBlockException e)
            {
                // we use this instead of CompletableFuture.supplyAsync() to avoid wrapping exceptions into CompletionException
                ioExecutor().execute(() -> {
                    try
                    {
                        future.complete(doReadChunk(position, uncompressed));
                    }
                    catch (Throwable t)
                    {
                        future.completeExceptionally(t);
                    }
                });
            }
            catch (Throwable t)
            {
               future.completeExceptionally(t);
            }
            return future;
        }

        private ByteBuffer doReadChunk(long position, ByteBuffer uncompressed)
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
                    if (chunk.length < maxCompressedLength)
                        metadata.compressor().uncompress(compressedChunk, uncompressed);
                    else
                        uncompressed.put(compressedChunk);
                }
                catch (IOException e)
                {
                    throw new CorruptBlockException(channel.filePath(), chunk, e);
                }
                uncompressed.flip();

                if (shouldCheckCrc())
                {
                    compressedChunk.position(chunkOffset).limit(chunkOffset + chunk.length);

                    int checksum = (int) ChecksumType.CRC32.of(compressedChunk);

                    compressedChunk.limit(compressedChunk.capacity());
                    if (compressedChunk.getInt() != checksum)
                        throw new CorruptBlockException(channel.filePath(), chunk);
                }

                return uncompressed;
            }
            catch (CorruptBlockException ex)
            {
                // Make sure reader does not see stale data.
                uncompressed.position(0).limit(0);

               throw new CorruptSSTableException(ex, channel.filePath());
            }
        }

        public void close()
        {
            regions.closeQuietly();
            super.close();
        }
    }
}
