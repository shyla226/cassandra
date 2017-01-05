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

package org.apache.cassandra.cql3.continuous.paging;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import org.apache.cassandra.config.ContinuousPagingConfig;
import org.apache.cassandra.exceptions.ClientWriteException;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Writes pages to the client connection, by scheduling events on the Netty loop that read from a queue.
 *
 * Notify the client when the last page, or error, has been written by completing a future.
 */
class ContinuousPageWriter
{
    private static final Logger logger = LoggerFactory.getLogger(ContinuousPageWriter.class);

    // byteman tests rely on the exact name and type of pages, see ContinuousPagingErrorsTest
    private final ArrayBlockingQueue<Frame> pages;
    private final Writer writer;
    private final ContinuousPagingConfig config;

    /**
     * Create a continuous page writer.
     *
     * @param connection - the connection to which pages should be written to.
     * @param maxPagesPerSecond - the maximum pages to send per second.
     * @param config - the yaml configuration for continuous paging.
     */
    ContinuousPageWriter(Connection connection,
                         int maxPagesPerSecond,
                         ContinuousPagingConfig config)
    {
        this.pages = new ArrayBlockingQueue<>(config.max_session_pages);
        this.writer = new Writer(connection.channel(), pages, maxPagesPerSecond);
        this.config = config;
    }

    /**
     * A future on the completion (normal or abnormal) of this writer.
     *
     * A writer commpletes if either {@link #sendPage} is called with {@code hasMorePages == false}, or
     * {@link #sendError} is called, or the writer is canceled (through {@link #cancel}).
     *
     * @return a future on the completion of this writer.
     */
    CompletableFuture<Void> completionFuture()
    {
        return writer.completionFuture;
    }

    /**
     * Add a page to the queue, potentially blocking up to {@link ContinuousPagingConfig#max_client_wait_time_ms}
     * if there is no space.
     *
     * This method is a target of byteman tests, see ContinuousPagingErrorsTest,
     * if it is renamed then the tests should also be modified.
     *
     * @param frame, the page
     * @param hasMorePages, whether there are more pages to follow
     */
    void sendPage(Frame frame, boolean hasMorePages)
    {
        if (writer.canceled)
        {
            frame.release();
            return;
        }

        assert !writer.completed() : "Received unexpected page when writer was already completed";

        try
        {
            // byteman tests rely on pages.offer() to inject exceptions in this method, see ContinuousPagingErrorsTest
            if (!pages.offer(frame))
            { // if we cannot add the page immediately, try again with a timeout and update the metrics to keep track
              // of this fact
                long start = System.nanoTime();
                ContinuousPagingService.metrics.serverBlocked.inc();

                if (!pages.offer(frame, config.max_client_wait_time_ms, TimeUnit.MILLISECONDS))
                    throw new ClientWriteException("Timed out adding page to output queue");

                ContinuousPagingService.metrics.serverBlockedLatency.addNano(System.nanoTime() - start);
            }
        }
        catch (InterruptedException ex)
        {
            frame.release();
            throw new ClientWriteException("Interrupted whilst adding page to output queue");
        }
        catch (Throwable t)
        {
            frame.release();
            throw t;
        }

        if (!hasMorePages)
        {
            if (logger.isTraceEnabled())
                logger.trace("Completing writer");
            // this will call schedule internally, it's important to only
            // call complete after having added all the pages to the queue
            writer.complete();
        }
        else
        {
            writer.schedule();
        }
    }

    /**
     * @return - the number of pending pages, which are ready to be sent on the wire.
     */
    int pendingPages()
    {
        return pages.size();
    }

    /**
     * Send an error to the client, this will also discard any pending pages.
     */
    void sendError(Frame error)
    {
       writer.setError(error);
    }

    /**
     * Cancel this writer.
     *
     * As soon as this method is called, any page that hasn't been already sent
     * will be discarded. This include both pages that are in the internal
     * queue of the writer, as well as any new page submitted through
     * {@link #sendPage}. As such, cancelling twice is a no-op.
     */
    public void cancel()
    {
        writer.cancel();
    }

    public boolean completed()
    {
        return writer.completed();
    }

    /**
     * A runnable that can be submitted to the Netty event loop
     * in order to write and flush synchronously. If it cannot
     * keep up then the producer will be blocked by the pages queue.
     */
    private final static class Writer implements Runnable
    {
        private final Channel channel;
        private final ArrayBlockingQueue<Frame> queue;
        private final AtomicBoolean completed;
        private final AtomicReference<Frame> error;
        private final RateLimiter limiter;
        private final CompletableFuture<Void> completionFuture;

        private volatile boolean canceled;

        public Writer(Channel channel,
                      ArrayBlockingQueue<Frame> queue,
                      int maxPagesPerSecond)
        {
            this.channel = channel;
            this.queue = queue;
            this.completed = new AtomicBoolean(false);
            this.error = new AtomicReference<>(null);
            this.limiter = RateLimiter.create(maxPagesPerSecond > 0 ? maxPagesPerSecond : Double.MAX_VALUE);
            this.completionFuture = new CompletableFuture<>();

            channel.closeFuture().addListener((ChannelFutureListener) future ->
            {   // stop trying to send pages if the socket is closed by the client, this
                // will avoid time-out exceptions in the producer thread
                cancel();
            });
        }

        public void cancel()
        {
            this.canceled = true;
            complete();
        }

        public void complete()
        {
            completed.compareAndSet(false, true);
            schedule();
        }

        public boolean completed()
        {
            return completed.get();
        }

        /**
         * This will add the writer at the back of the task queue of Netty's event loop.
         */
        public void schedule()
        {
            channel.eventLoop().execute(this);
        }

        /**
         * Set an error that will be sent at the next invocation, discarding any pending pages.
         *
         * @param error - the error to send
         */
        public void setError(Frame error)
        {
            if (!completed())
            {
                if (this.error.compareAndSet(null, error))
                    complete();
            }
            else
            {
                // an exception thrown after sending the last page but before completing the iteration
                // could in theory mean that we end up in this case, I don't think there is anything that currently
                // throws after sending the last page, but to be on the safe side I prefer to log a warning rather
                // than end up with an assertion failing or silently ignoring it.
                logger.warn("Got continuous paging error for client but writer was already completed, so could not pass it to the client");
            }
        }

        public boolean aborted()
        {
            return canceled || error.get() != null;
        }

        public void run()
        {
            try
            {
                processPendingPages();

                // completed is set either after the last message has been added to the queue, or when
                // canceled it set to true or an error is added, in both cases aborted() will return true
                // and no more page will be sent, so if the queue is empty it's OK to send any error and
                // complete the future.
                if (completed() && !completionFuture.isDone())
                {
                    // it's important to check that the queue is empty after checking completed since
                    // the producer thread only calls completed after adding the last message to the queue,
                    if (queue.isEmpty())
                    {
                        if (error.get() != null)
                            sendError();

                        completionFuture.complete(null);
                    }
                }
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.error("Error processing pages in Netty event loop: {}", t);
            }
        }

        private void sendError()
        {
            if (logger.isTraceEnabled())
                logger.trace("Sending continuous paging error to client");

            channel.write(error.get());
            channel.flush();
        }

        /**
         * Process any pending pages. This method processes any pending pages as long as the channel is writable
         * and we have permits, or aborted() returns true, in which case pages will be simply discarded.
         * <p>
         * If the queue is not empty after finishing the iteration, then the task should be rescheduled.
         * <p>
         * byteman tests inject exceptions in this method, if it is renamed then the tests should be
         * updated as well, see ContinuousPagingErrorsTest
         */
        private void processPendingPages()
        {
            while(aborted() || (channel.isWritable() && limiter.tryAcquire()))
            {
                // byteman tests rely on pages.poll() to inject exceptions in this method, see ContinuousPagingErrorsTest
                Frame page = queue.poll();
                if (page == null)
                    break;

                processPage(page);
            }

            if (!queue.isEmpty())
            {
                if (logger.isTraceEnabled())
                    logger.trace("Rescheduling since queue is not empty, either channel was not writable or permit not available");

                schedule();
            }
        }

        private void processPage(Frame frame)
        {
            if (aborted())
            {
                frame.release();
            }
            else
            {
                channel.write(frame);
                channel.flush();
            }
        }
    }
}
