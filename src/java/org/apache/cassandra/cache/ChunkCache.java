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
package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.*;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.reactivex.Scheduler;
import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.metrics.CacheMissMetrics;
import org.apache.cassandra.metrics.Timer;
import org.apache.cassandra.utils.memory.BufferPool;
import org.jctools.queues.MpscArrayQueue;

public class ChunkCache
        implements CacheLoader<ChunkCache.Key, ChunkCache.Buffer>, RemovalListener<ChunkCache.Key, ChunkCache.Buffer>, CacheSize
{
    private static final Logger logger = LoggerFactory.getLogger(ChunkCache.class);
    public static final int RESERVED_POOL_SPACE_IN_MB = 32;
    public static final long cacheSize = 1024L * 1024L * Math.max(0, DatabaseDescriptor.getFileCacheSizeInMB() - RESERVED_POOL_SPACE_IN_MB);

    private static boolean enabled = cacheSize > 0;
    public static final ChunkCache instance = enabled ? new ChunkCache() : null;

    private static int UNINITIALISED = Integer.MIN_VALUE;

    private final LoadingCache<Key, Buffer> cache;
    public final CacheMissMetrics metrics;

    static class Key
    {
        final ChunkReader file;
        final String path;
        final long position;

        //Decides if the cache loading should be blocking or non-blocking.
        //Not included in hashcode or equals
        final boolean blocking;

        public Key(ChunkReader file, long position, boolean blocking)
        {
            super();
            this.file = file;
            this.position = position;
            this.path = file.channel().filePath();
            this.blocking = blocking;
        }

        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + path.hashCode();
            result = prime * result + file.getClass().hashCode();
            result = prime * result + Long.hashCode(position);
            return result;
        }

        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;

            Key other = (Key) obj;
            return (position == other.position)
                    && file.getClass() == other.file.getClass()
                    && path.equals(other.path);
        }

        public String toString()
        {
            return path + "@" + position;
        }
    }

    public static class Buffer implements Rebufferer.BufferHolder
    {
        private final Key key;
        private final CompletableFuture<ByteBuffer> futureBuffer;
        private final long offset;
        private final int capacity;
        private final AtomicInteger references;

        public Buffer(Key key ,CompletableFuture<ByteBuffer> futureBuffer, long offset, int capacity, boolean blocking)
        {
            this.key = key;
            this.offset = offset;
            this.capacity = capacity;

            // start un-referenced if non-blocking.
            // Once the async disk read is done we will reference.
            //
            // Otherwise start referenced
            if (blocking)
            {
                references = new AtomicInteger(1);
                this.futureBuffer = futureBuffer;
                this.futureBuffer.join();
            }
            else
            {

                // We need to avoid anyone trying to reference the buffer before
                // its been initialized.
                references = new AtomicInteger(UNINITIALISED);
                this.futureBuffer = futureBuffer.thenApply((buf) ->
                                                           {
                                                               boolean success = references.compareAndSet(UNINITIALISED, 1);
                                                               assert success : "Buffer was referenced before it was initialized";

                                                               return buf;
                                                           });
            }
        }

        Buffer reference()
        {
            int refCount;

            do
            {
                refCount = references.get();
                if (refCount == 0 || refCount == UNINITIALISED)
                    // Buffer was released before we managed to reference it.
                    // Or it's an async request and not ready yet
                    return null;
            } while (!references.compareAndSet(refCount, refCount + 1));

            return this;
        }

        @Override
        public ByteBuffer buffer()
        {
            assert references.get() > 0;
            return futureBuffer.join().duplicate();
        }

        /**
         * Callback handler for
         * @param onReady called when buffer is ready.
         * @param onSchedule called if the buffer isn't ready yet and will be scheduled
         * @param executor if not instantly ready the callback will happen on this executor
         */
        public void onReady(Runnable onReady, Runnable onSchedule, Executor executor)
        {
            if (futureBuffer.isDone())
            {
                onReady.run();
            }
            else
            {
                onSchedule.run();
                futureBuffer.thenRunAsync(() -> onReady.run(), executor);
            }
        }

        @Override
        public long offset()
        {
            return offset;
        }

        public int capacity()
        {
            return capacity;
        }

        @Override
        public void release()
        {
            if (references.decrementAndGet() == 0)
            {
                BufferPool.put(futureBuffer.join());
            }
        }

        public String toString()
        {
            return "ChunkCacheBuffer " + key;
        }
    }

    public ChunkCache()
    {
        cache = Caffeine.newBuilder()
                .maximumWeight(cacheSize)
                .executor(MoreExecutors.directExecutor())
                .weigher((key, buffer) -> ((Buffer) buffer).capacity())
                .removalListener(this)
                .build(this);
        metrics = new CacheMissMetrics("ChunkCache", this);
    }

    @Override
    public Buffer load(Key key) throws Exception
    {
        ChunkReader rebufferer = key.file;
        metrics.misses.mark();
        try (Timer.Context ctx = metrics.missLatency.time())
        {
            ByteBuffer buffer = BufferPool.get(key.file.chunkSize(), key.file.preferredBufferType());
            assert buffer != null;

            //Once the buffer has been filled we can give it an initial reference
            try
            {
                CompletableFuture<ByteBuffer> future = rebufferer.readChunk(key.position, buffer);

                return new Buffer(key, future, key.position, buffer.capacity(), key.blocking);
            }
            catch (Throwable t)
            {
                logger.error("Error loading buffer for {}: ", key, t);
                throw t;
            }
        }
    }

    @Override
    public void onRemoval(Key key, Buffer buffer, RemovalCause cause)
    {
        buffer.release();
    }

    public void close()
    {
        cache.invalidateAll();
    }

    public RebuffererFactory wrap(ChunkReader file)
    {
        return new CachingRebufferer(file);
    }

    public static RebuffererFactory maybeWrap(ChunkReader file)
    {
        if (!enabled)
            return file;

        return instance.wrap(file);
    }

    public void invalidatePosition(FileHandle dfile, long position)
    {
        if (!(dfile.rebuffererFactory() instanceof CachingRebufferer))
            return;

        ((CachingRebufferer) dfile.rebuffererFactory()).invalidate(position);
    }

    public void invalidateFile(String fileName)
    {
        cache.invalidateAll(Iterables.filter(cache.asMap().keySet(), x -> x.path.equals(fileName)));
    }

    @VisibleForTesting
    public void enable(boolean enabled)
    {
        ChunkCache.enabled = enabled;
        cache.invalidateAll();
        metrics.reset();
    }

    // TODO: Invalidate caches for obsoleted/MOVED_START tables?

    /**
     * Rebufferer providing cached chunks where data is obtained from the specified ChunkReader.
     * Thread-safe. One instance per SegmentedFile, created by ChunkCache.maybeWrap if the cache is enabled.
     */
    class CachingRebufferer implements Rebufferer, RebuffererFactory
    {
        private final ChunkReader source;
        final long alignmentMask;

        public CachingRebufferer(ChunkReader file)
        {
            source = file;
            int chunkSize = file.chunkSize();
            assert Integer.bitCount(chunkSize) == 1 : chunkSize;    // Must be power of two
            alignmentMask = -chunkSize;
        }

        @Override
        public Buffer rebuffer(long position)
        {
            try
            {
                metrics.requests.mark();
                long pageAlignedPos = position & alignmentMask;
                Buffer buf;
                Key pageKey = new Key(source, pageAlignedPos, true);

                Buffer page = null;

                //There is a small window when a released buffer/invalidated chunk
                //is still in the cache. In this case it will return null
                //so we spin loop while waiting for the cache to re-populate
                while(page == null || ((buf = page.reference()) == null))
                {
                    page = cache.get(pageKey);
                }

                return buf;
            }
            catch (Throwable t)
            {
                Throwables.propagateIfInstanceOf(t.getCause(), CorruptSSTableException.class);
                throw Throwables.propagate(t);
            }
        }

        @Override
        public Buffer rebuffer(long position, ReaderConstraint rc)
        {
            if (rc != ReaderConstraint.IN_CACHE_ONLY || !enabled)
                return rebuffer(position);

            metrics.requests.mark();
            long pageAlignedPos = position & alignmentMask;
            Key key = new Key(source, pageAlignedPos, false);


            Buffer buf = cache.getIfPresent(key);
            if (buf != null && (buf = buf.reference()) != null)
                return buf;

            metrics.misses.mark();

            /**
             * Notify the caller this page isn't ready
             * but give them the Buffer container so they can register a callback
             *
             */
            throw new NotInCacheException(buf != null ? buf : cache.get(key));
        }

        public void invalidate(long position)
        {
            long pageAlignedPos = position & alignmentMask;
            cache.invalidate(new Key(source, pageAlignedPos, true));
        }

        @Override
        public Rebufferer instantiateRebufferer()
        {
            return this;
        }

        @Override
        public void close()
        {
            source.close();
        }

        @Override
        public void closeReader()
        {
            // Instance is shared among readers. Nothing to release.
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
        public double getCrcCheckChance()
        {
            return source.getCrcCheckChance();
        }

        @Override
        public String toString()
        {
            return "CachingRebufferer:" + source;
        }
    }

    @Override
    public long capacity()
    {
        return cacheSize;
    }

    @Override
    public void setCapacity(long capacity)
    {
        throw new UnsupportedOperationException("Chunk cache size cannot be changed.");
    }

    @Override
    public int size()
    {
        return cache.asMap().size();
    }

    @Override
    public long weightedSize()
    {
        return cache.policy().eviction()
                .map(policy -> policy.weightedSize().orElseGet(cache::estimatedSize))
                .orElseGet(cache::estimatedSize);
    }
}
