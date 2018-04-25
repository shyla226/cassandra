/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.cql3.continuous.paging;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
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

    private final int queueSizeInPages;
    // byteman tests rely on the exact name and type of pages, see ContinuousPagingErrorsTest
    private final ArrayBlockingQueue<Frame> pages;
    private final Writer writer;

    /**
     * Create a continuous page writer.
     *
     * @param channel - the supplier for the channel to which pages should be written to.
     * @param maxPagesPerSecond - the maximum pages to send per second.
     * @param queueSizeInPages - the desired size of the queue in pages
     */
    ContinuousPageWriter(Supplier<Channel> channel,
                         int maxPagesPerSecond,
                         int queueSizeInPages)
    {
        assert queueSizeInPages >= 1 : "queue size must be at least one";
        this.queueSizeInPages = queueSizeInPages;
        this.pages = new ArrayBlockingQueue<>(queueSizeInPages);
        this.writer = new Writer(channel.get(), pages, maxPagesPerSecond);
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
     * Add a page to the queue, unless the writer has been cancelled. Check that the writer has not been
     * completed, fire an assertion if the writer is completed.
     *
     * @param frame, the page to add
     * @param hasMorePages, whether there are more pages to follow, if no more pages the writer will be completed.
     */
    void sendPage(Frame frame, boolean hasMorePages)
    {
        if (writer.canceled)
        {
            logger.trace("Discarding page because writer was cancelled");
            frame.release();
            return;
        }

        assert !writer.completed() : "Received unexpected page when writer was already completed";

        try
        {
            pages.add(frame);
        }
        catch (Throwable t)
        {
            logger.warn("Failed to add continuous paging result to queue: {}", t.getMessage());
            frame.release();
            throw t;
        }

        if (!hasMorePages)
        {
            // this will call schedule internally, it's important to only
            // call sendError after having added all the pages to the queue
            writer.complete();
        }
        else
        {
            writer.schedule(0);
        }
    }

    boolean hasSpace()
    {
        return pages.size() < queueSizeInPages;
    }

    boolean halfQueueAvailable()
    {
        return pages.size() < (queueSizeInPages/2 + 1);
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
     *
     * The error specified as the parameter will be sent as the final message, if
     * the socket is still open and no other error was set in the meantime.
     */
    public void cancel(Frame error)
    {
        writer.cancel(error);
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
                if (logger.isTraceEnabled())
                    logger.trace("Socket {} closed by the client", channel);
                cancel(null);
            });
        }

        /**
         * Cancel this session by completing it in such a way that will discard all outstanding
         * messages. An optional final error can be sent to the client though.
         *
         * @param error - an optional error to send to the client as a final message
         */
        public void cancel(@Nullable Frame error)
        {
            if (canceled)
                return;

            logger.trace("Cancelling continuous page writer");
            canceled = true; // an additional boolean is required to ensure we discard all messages

            if (error != null && !this.error.compareAndSet(null, error))
                logger.debug("Failed to set final error when cancelling session, another error was already there");

            complete();
        }

        public void complete()
        {
            completed.compareAndSet(false, true);
            schedule(0);
        }

        public boolean completed()
        {
            return completed.get();
        }

        /**
         * This will add the writer at the back of the task queue of Netty's event loop,
         * either immediately of after the specified pause, in microseconds.
         *
         * @param pauseMicros - how long to wait before rescheduling the task, in microseconds
         */
        public void schedule(long pauseMicros)
        {
            if (pauseMicros > 0)
                channel.eventLoop().schedule(this, pauseMicros, TimeUnit.MICROSECONDS);
            else if (channel.eventLoop().inEventLoop())
                run();
            else
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
                // sendError the future.
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
         * If the queue is not empty after finishing the iteration, then the task should be rescheduled. If the
         * limiter did not release a permit, then the reschedule will only happen after a pause in microseconds.
         * The limiter does not tell how much longer till the next permit is available, so we pause by one tenth
         * of the whole interval between permits, so that we'll never be more than 10% slower (can we do better?).
         * If there is no pause, then the CPU will be spinning for too long if the rate is really small,
         * see DB-316.
         * <p>
         * byteman tests inject exceptions in this method, if it is renamed then the tests should be
         * updated as well, see ContinuousPagingErrorsTest
         */
        private void processPendingPages()
        {
            long pauseMicros = 1;
            boolean aborted = aborted();
            while (aborted || channel.isWritable())
            {
                if (!aborted && !limiter.tryAcquire())
                {
                    long intervalMicros = (long)(TimeUnit.SECONDS.toMicros(1L) / limiter.getRate());
                    pauseMicros = intervalMicros / 10;
                    break;
                }

                // byteman tests rely on pages.poll() to inject exceptions in this method, see ContinuousPagingErrorsTest
                Frame page = queue.poll();
                if (page == null)
                    break;

                processPage(page);
                aborted = aborted();
            }

            if (!queue.isEmpty())
            {
                schedule(pauseMicros);
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
