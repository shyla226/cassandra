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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.Meter;
import org.apache.cassandra.metrics.MetricNameFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * A rebufferer that prefetches the next N buffers in sequential order.
 */
public class PrefetchingRebufferer extends WrappingRebufferer
{
    private static final Logger logger = LoggerFactory.getLogger(PrefetchingRebufferer.class);

    /**
     * How much to read-ahead for sequential scans, in kilobytes. The read-ahead algorithm ensures that at least
     * window * read-ahead-size have already been requested, if that's not the case, it reads up to read-ahead-size-kb.
     */
    static final String READ_AHEAD_SIZE_KB_FROM_OPERATOR = System.getProperty("dse.read_ahead_size_kb", "");
    public static final int READ_AHEAD_SIZE_KB;

    public static final double READ_AHEAD_WINDOW = Double.parseDouble(System.getProperty("dse.read_ahead_window", "0.5"));
    public static final boolean READ_AHEAD_VECTORED = Boolean.parseBoolean(System.getProperty("dse.read_ahead_vectored", "true"));

    static
    {
        DiskOptimizationStrategy diskOptimizationStrategy = DatabaseDescriptor.getDiskOptimizationStrategy();

        READ_AHEAD_SIZE_KB = READ_AHEAD_SIZE_KB_FROM_OPERATOR.isEmpty()
                             ? diskOptimizationStrategy.readAheadSizeKb()
                             : Integer.parseInt(READ_AHEAD_SIZE_KB_FROM_OPERATOR);

        logger.info("Read ahead for sequential reads (e.g. range queries, compactions) is {} k-bytes, window: {}, vectored: {}",
                    READ_AHEAD_SIZE_KB, READ_AHEAD_WINDOW, READ_AHEAD_VECTORED);
    }

    /** A dedicated channel, optionally capable of batching requests */
    private AsynchronousChannelProxy channel;

    /** The buffers that have been prefetched but not yet requested. */
    private final Deque<PrefetchedEntry> queue;

    /** The number of buffers to prefetch */
    private final int prefetchSize;

    /** The minimum number of buffers to prefetch, if at least one buffer in the window is missing, then
     * we fetch up to prefetchSize. */
    private final int windowSize;

    /** We expect the buffer size to be a power of 2, this is the mask for aligning to the buffer size */
    private final int alignmentMask;


    public PrefetchingRebufferer(Rebufferer source, AsynchronousChannelProxy channel)
    {
        this(source, channel, READ_AHEAD_SIZE_KB * 1024, READ_AHEAD_WINDOW);
    }

    PrefetchingRebufferer(Rebufferer source, AsynchronousChannelProxy channel, int readHeadSize, double window)
    {
        super(source);

        if (readHeadSize <= 0)
            throw new IllegalArgumentException("Invalid read-ahead size: " + readHeadSize);

        if (window < 0 || window > 1)
            throw new IllegalArgumentException("Invalid window size, should be >= 0 and <=1: " + window);

        this.channel = channel;
        this.prefetchSize = (int)Math.ceil((double)readHeadSize / source.rebufferSize());
        this.windowSize = (int)Math.ceil(window * prefetchSize);
        this.queue = new ArrayDeque<>(prefetchSize);
        // Must be power of two, this is already required by the chunk cache, so we enforce it here as well to avoid
        // a division when rebuffering, see rebufferAsync
        assert Integer.bitCount(source.rebufferSize()) == 1 : String.format("%d must be a power of two", source.rebufferSize());
        this.alignmentMask = -source.rebufferSize();

        if (logger.isTraceEnabled())
            logger.trace("{}: prefetch {}, window {}", channel.filePath(), prefetchSize, windowSize);
    }

    @Override
    public BufferHolder rebuffer(long position, ReaderConstraint rc)
    {
        CompletableFuture<BufferHolder> fut = rebufferAsync(position);
        if (rc == ReaderConstraint.NONE || fut.isDone())
        {
            try
            {
                return fut.join();
            }
            catch (Throwable t)
            {
                Throwables.propagateIfInstanceOf(t.getCause(), CorruptSSTableException.class);
                throw Throwables.propagate(t);
            }
        }

        throw new NotInCacheException(fut.thenAccept(bufferHolder -> bufferHolder.release()), channel().filePath, position);
    }

    @Override
    public CompletableFuture<BufferHolder> rebufferAsync(long position)
    {
        long pageAlignedPos = position & alignmentMask;

        PrefetchedEntry entry = queue.poll();
        while (entry != null && entry.position < pageAlignedPos)
        { // release any prefetched buffers that are before the requested position as they
          // probably won't be needed and they are still in the cache if they are needed again
            entry.release();
            entry = queue.poll();
        }

        CompletableFuture<BufferHolder> ret;
        if (entry != null && entry.position == pageAlignedPos)
        { // if the position match use this entry
            ret = entry.future;
            if (!entry.future.isDone())
                metrics.notReady.mark();
        }
        else
        {  // otherwise, if the position is later in the file, put it back in the queue and
           // request this buffer to the cache, it's a jump back for which we should have already prefetched probably
            if (entry != null)
                queue.addFirst(entry); // add it back, it may be required in future

            ret = super.rebufferAsync(pageAlignedPos);

            if (!ret.isDone())
                channel.submitBatch(); // principal buffer goes asap
        }

        // do no prefetch if the queue has entries later in the file as this is a jump-back for which we
        // assume buffers were already prefetched. If we did prefetch in this case, we'd have to scan the queue
        // to avoid gaps or duplicate entries caused by multiple jumps, which is not efficient and at the moment
        // we only prefetch data files that are only read forward
        if (entry == null || entry.position == pageAlignedPos)
            prefetch(pageAlignedPos + source.rebufferSize());
        else
            metrics.skipped.mark();

        return ret;
    }

    /**
     * Prefetch the next N buffers unless they are already in the queue
     * @param pageAlignedPosition
     */
    private void prefetch(long pageAlignedPosition)
    {
        // see caller, prefetch only done with empty queue or if requested buffer is exactly
        // at the beginning of the queue after purging all older entries
        assert queue.isEmpty() || pageAlignedPosition == queue.peekFirst().position
          : String.format("Unexpected read-ahead position %d, first: %s, last: %s",
                          pageAlignedPosition, queue.peekFirst(), queue.peekLast());

        long firstPositionToPrefetch = queue.isEmpty() ? pageAlignedPosition : queue.peekLast().position + source.rebufferSize();
        int toPrefetch = prefetchSize - queue.size();
        if (toPrefetch < windowSize)
            return;

        for (int i = 0; i < toPrefetch; i++)
        {
            long prefetchPosition = firstPositionToPrefetch + i * source.rebufferSize();
            if (prefetchPosition >= source.fileLength())
                break; // nothing else to read

            queue.addLast(new PrefetchedEntry(prefetchPosition, super.rebufferAsync(prefetchPosition)));
        }

        if (toPrefetch > 0)
            channel.submitBatch(); // fire the read-head requests!!!
    }

    @Override
    public void close()
    {
        releaseBuffers();

        try
        {
            channel.close();
        }
        finally
        {
            super.close();
        }
    }

    @Override
    public void closeReader()
    {
        releaseBuffers();

        try
        {
            channel.close();
        }
        finally
        {
            super.closeReader();
        }
    }

    /**
     * Release any prefetched buffers that were not used.
     */
    private void releaseBuffers()
    {
        queue.forEach(PrefetchedEntry::release);
    }

    /**
     * A simple wrapper of a future of a buffer so that we can access
     * the position even if the future hasn't completed yet.
     */
    private static final class PrefetchedEntry
    {
        private final long position;
        private final CompletableFuture<BufferHolder> future;
        private boolean released;

        PrefetchedEntry(long position, CompletableFuture<BufferHolder> future)
        {
            this.position = position;
            this.future = future;

            metrics.prefetched.mark();
        }

        public void release()
        {
            if (released)
                return;

            released = true;
            future.whenComplete((buffer, error) -> {
                try
                {
                    if (buffer != null)
                    {
                        metrics.unused.mark();
                        buffer.release();
                    }

                    if (error != null)
                        logger.debug("Failed to prefetch buffer due to {}", error);
                }
                catch (Throwable t)
                {
                    logger.debug("Failed to release prefetched buffer due to {}", t);
                }
            });
        }

        @Override
        public String toString()
        {
            return String.format("Position: %d, Status: %s", position, future.isDone());
        }
    }

    @Override
    public String toString()
    {
        return String.format("Prefetching rebufferer: (%d/%d) buffers read-ahead, %d buffer size",
                             prefetchSize, windowSize, source.rebufferSize());
    }

    @VisibleForTesting
    public static final PrefetchingMetrics metrics = new PrefetchingMetrics();

    @VisibleForTesting
    public static class PrefetchingMetrics
    {
        private final MetricNameFactory factory = new DefaultNameFactory("Prefetching");

        /** Total number of buffers that were prefetched */
        final Meter prefetched;

        /** Total number of buffers that were requested but generated no prefetch due to a non-sequential backwards access */
        final Meter skipped;

        /** Total number of buffers that were prefetched and not used by the rebufferer that
         * requested them, so these buffers could still be used by a different rebufferer (request)
         * if they were kept by the cache. */
        final Meter unused;

        /** The total number of buffers that were prefetched but requested too soon, so the client
         * had to wait anyway for example by receiving a NotInCacheException */
        final Meter notReady;

        PrefetchingMetrics()
        {
            prefetched = Metrics.meter(factory.createMetricName("Prefetched"));
            skipped = Metrics.meter(factory.createMetricName("Skipped"));
            unused = Metrics.meter(factory.createMetricName("Unused"));
            notReady = Metrics.meter(factory.createMetricName("NotReady"));
        }

        @Override
        public String toString()
        {
            if (prefetched.getCount() == 0)
                return "No read-ahead yet";

            return String.format("Prefetched: [%s], Skipped: [%s], Unused: [%s] (%.2f), Not ready: [%s] (%.2f)",
                                 prefetched,
                                 skipped,
                                 unused, (double)unused.getCount() / prefetched.getCount(),
                                 notReady, (double)notReady.getCount() / prefetched.getCount());
        }

        @VisibleForTesting
        void reset()
        {
            prefetched.mark(-prefetched.getCount());
            skipped.mark(-skipped.getCount());
            unused.mark(-unused.getCount());
        }
    }
}
