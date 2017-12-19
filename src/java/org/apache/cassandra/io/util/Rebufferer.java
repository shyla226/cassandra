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
import java.util.function.Function;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCScheduler;

/**
 * Rebufferer for reading data by a RandomAccessReader.
 */
public interface Rebufferer extends ReaderFileProxy
{
    /**
     * Rebuffer (move on or seek to) a given position, and return a buffer that can be used there.
     * The only guarantee about the size of the returned data is that unless rebuffering at the end of the file,
     * the buffer will not be empty and will contain the requested position, i.e.
     * {@code offset <= position < offset + bh.buffer().limit()}, but the buffer will not be positioned there.
     */
    default BufferHolder rebuffer(long position)
    {
        return rebuffer(position, ReaderConstraint.NONE);
    }

    /**
     * Same as {@link #rebuffer(long)} but asynchronous. All observations for that method still hold.
     *
     * @param position - the position to which to rebuffer to.
     *
     * @return a future that upon completion has a valid {@link BufferHolder}.
     */
    CompletableFuture<BufferHolder> rebufferAsync(long position);

    /**
     * @return - the size of the buffers that will be returned.
     */
    int rebufferSize();

    /**
     * Called when a reader is closed. Should clean up reader-specific data.
     */
    void closeReader();

    // Extensions for TPC/optimistic read below.

    /**
     * Rebuffer to the given position, applying any long-running operation according to the specified reader
     * constrained:
     * - if NONE, long-running reads will block and return when the data is received
     * - if ASYNC, the call should return successfully only if the data is already present in memory (e.g. in cache),
     *   otherwise a NotInCacheException should be thrown, letting the caller attach to the async completion of the read.
     *
     * Note: Memory-mapped rebufferers ignore the ASYNC flag, assuming that either:
     * - the data is in locked memory map
     * - the data is on a non-volatile memory drive
     * and thus page faults and the resulting blocking should not occur.
     */
    BufferHolder rebuffer(long position, ReaderConstraint constraint);

    public enum ReaderConstraint
    {
        NONE,
        ASYNC
    }

    public static class NotInCacheException extends RuntimeException
    {
        private static final long serialVersionUID = 1L;

        private final CompletableFuture<Void> cacheReady;

        public NotInCacheException(CompletableFuture<Void> cacheReady, String path, long position)
        {
            super("Requested data (" + path + "@" + position +") is not in cache.");
            this.cacheReady = cacheReady;

            ChunkCache.instance.metrics.notInCacheExceptions.mark();
        }

        @Override
        public synchronized Throwable fillInStackTrace()
        {
            //Avoids generating a stack trace for every instance
            return this;
        }

        /**
         * Handles callbacks for async buffers
         * @param onReady will be run if completable future is ready
         * @param onError will be run if the buffer errored
         * @param scheduler the TPC executor to schedule on
         */
        public void accept(Runnable onReady, Function<Throwable, Void> onError, TPCScheduler scheduler)
        {
            //Registers a callback to be issued when the async buffer is ready
            assert cacheReady != null;

            if (cacheReady.isDone() && !cacheReady.isCompletedExceptionally())
            {
                onReady.run();
            }
            else
            {
                //Track the ThreadLocals
                Runnable wrappedOnReady = TPCRunnable.wrap(onReady, ExecutorLocals.create(), TPCTaskType.READ_DISK_ASYNC, scheduler);

                cacheReady.thenRunAsync(wrappedOnReady, scheduler.getExecutor())
                          .exceptionally(onError);
            }
        }

        public String toString()
        {
            return "NotInCache " + cacheReady;
        }
    }

    interface BufferHolder
    {
        /**
         * Returns a useable buffer (i.e. one whose position and limit can be freely modified). Its limit will be set
         * to the size of the available data in the buffer.
         * The buffer must be treated as read-only.
         */
        ByteBuffer buffer();

        /**
         * Position in the file of the start of the buffer.
         */
        long offset();

        /**
         * To be called when this buffer is no longer in use. Must be called for all BufferHolders, or ChunkCache
         * will not be able to free blocks.
         */
        void release();
    }

    BufferHolder EMPTY = new BufferHolder()
    {
        final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

        @Override
        public ByteBuffer buffer()
        {
            return EMPTY_BUFFER;
        }

        @Override
        public long offset()
        {
            return 0;
        }

        @Override
        public void release()
        {
            // nothing to do
        }
    };
}