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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.annotations.NonNull;
import io.reactivex.internal.disposables.EmptyDisposable;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.ContinuousPagingConfig;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.PagingResult;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.selection.ResultBuilder;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.exceptions.ClientWriteException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.metrics.ContinuousPagingMetrics;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * A collection of classes that send query results to a client asynchronously, that is as soon as a page
 * is available without the client requesting it, by using sessions
 * uniquely identified by the client state and the frame id of the initial request sent by the client.
 *
 * See APOLLO-3/CASSANDRA-11521 for more details.
 */
public class ContinuousPagingService
{
    private static final Logger logger = LoggerFactory.getLogger(ContinuousPagingService.class);

    /**
     * The metrics for continuous paging.
     */
    public static final ContinuousPagingMetrics metrics = new ContinuousPagingMetrics();

    /**
     * A class that wraps the client address and the initial request stream id.
     * This is required as a key in the Sessions map just below.
     */
    private static final class SessionKey
    {
        private final InetSocketAddress address;
        private final int streamId;

        SessionKey(InetSocketAddress address, int streamId)
        {
            this.address = address;
            this.streamId = streamId;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
                return true;

            if (!(other instanceof SessionKey))
                return false;

            SessionKey that = (SessionKey) other;

            return Objects.equals(address, that.address) && streamId == that.streamId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(address, streamId);
        }

        @Override
        public String toString()
        {
            return String.format("%s/%d", address, streamId);
        }
    }

    /** Any ongoing continuous paging sessions will be stored in this map. */
    private static final ConcurrentHashMap<SessionKey, ContinuousPagingSession> sessions = new ConcurrentHashMap<>();

    /** Atomically keep track of how many sessions are stored in {@link #sessions} */
    private static final AtomicInteger numSessions = new AtomicInteger(0);

    public static ResultBuilder createSession(Selection.Selectors selectors,
                                              GroupMaker groupMaker,
                                              ResultSet.ResultMetadata resultMetadata,
                                              ContinuousPagingState continuousPagingState,
                                              QueryState queryState,
                                              QueryOptions options)
    throws RequestValidationException, RequestExecutionException
    {
        assert options.continuousPagesRequested();

        SessionKey key = new SessionKey(queryState.getClientState().getRemoteAddress(), queryState.getStreamId());

        if (!canCreateSession(continuousPagingState.config.max_concurrent_sessions))
        {
            metrics.creationFailures.mark();
            metrics.tooManySessions.mark();
            if (logger.isDebugEnabled())
                logger.debug("Too many continuous paging sessions are already running: {}", sessions.size());
            throw RequestValidations.invalidRequest("Invalid request, too many continuous paging sessions are already running: %d", numSessions.get());
        }

        if (logger.isTraceEnabled())
            logger.trace("Starting continuous paging session {} with paging options {}, local {}, total number of sessions running: {}",
                         key, options.getPagingOptions(), continuousPagingState.executor.isLocalQuery(), sessions.size());

        ContinuousPagingSession session = new ContinuousPagingSession(selectors,
                                                                      groupMaker,
                                                                      resultMetadata,
                                                                      queryState,
                                                                      options,
                                                                      continuousPagingState,
                                                                      key);


        if (sessions.putIfAbsent(key, session) != null)
        {
            session.release();
            numSessions.decrementAndGet();

            metrics.creationFailures.mark();
            logger.error("Continuous paging session {} already exists", key);
            throw RequestValidations.invalidRequest("Invalid request, already executing continuous paging session %s", key);
        }

        return new ContinuousPagingSession.Builder(session, selectors, groupMaker);
    }

    /**
     * Check {@link #numSessions} and see if we can add one more session without exceeding the maximum
     * number of sessions specified by {@code maxSessions}.
     *
     * @param maxSessions - the maximum number of sessions
     * @return true if the session can be created, false if there are too many sessions
     */
    private static boolean canCreateSession(int maxSessions)
    {
        while(true)
        {
            int current = numSessions.get();
            if (current >= maxSessions)
                return false; // too many sessions

            if (numSessions.compareAndSet(current, current+1))
                return true; // if we succeeded there are at most maxSessions because current < maxSessions
        }
    }

    private static ContinuousPagingSession getSession(SessionKey key)
    {
        return sessions.get(key);
    }

    private static ContinuousPagingSession removeSession(SessionKey key)
    {
        ContinuousPagingSession ret = sessions.remove(key);
        if (ret != null)
        {
            numSessions.decrementAndGet();
            if (logger.isTraceEnabled())
                logger.trace("Removed continuous paging session {}, {} sessions still running", key, numSessions.get());
        }
        return ret;
    }
    /**
     * @return - the number of continuous paging sessions currently executing.
     */
    public static long liveSessions()
    {
        return numSessions.get();
    }

    /**
     * @return - the number of pages currently pending, that is waiting to be sent on the wire.
     */
    public static long pendingPages()
    {
        return sessions.values().stream().map(ContinuousPagingSession::pendingPages).reduce(Integer::sum).orElseGet(() -> 0);
    }

    /**
     * Asynchronously cancel an ongoing continuous paging session.
     *
     * @param queryState - the queryState
     * @param streamId - the initial frame id of the continuous paging request message
     * @return true if the session was cancelled, false if the session was not found
     */
    public static Single<Boolean> cancel(QueryState queryState, int streamId)
    {
        SessionKey key = new SessionKey(queryState.getClientState().getRemoteAddress(), streamId);
        ContinuousPagingSession session = removeSession(key);
        if (session == null)
        {
            if (logger.isTraceEnabled())
                logger.trace("Cannot cancel continuous paging session {}: not found", key);
            return Single.just(false);
        }

        if (logger.isTraceEnabled())
            logger.trace("Cancelling continuous paging session {}", key);

        return TPCUtils.toSingle(session.cancel()
                                        .thenCompose(ret -> TPCUtils.completedFuture(true)))
                       .timeout(session.config().cancel_timeout_sec, TimeUnit.SECONDS);
    }

    /**
     * Update the backpressure for an ongoing continuous paging session by increasing the number of pages
     * requested by the client. If the session was suspended because all previously requested pages were sent,
     * then it will be resumed, see also {@link ContinuousPagingSession#updateBackpressure(int)}.
     *
     * @param state - the query state
     * @param streamId - the initial stream id of the continuous paging request message
     * @param nextPages - the number of additional pages requested, must be positive.
     *
     * @return true if backpressure was updated, false if the session was not found, or was not running,
     *              or already had numPagesRequested set to the max
     * @throws RequestValidationException if nextPages is <= 0
     */
    public static boolean updateBackpressure(QueryState state, int streamId, int nextPages)
    {
        SessionKey key = new SessionKey(state.getClientState().getRemoteAddress(), streamId);

        if (nextPages <= 0)
            throw RequestValidations.invalidRequest(String.format("Cannot update next_pages for continuous paging session %s, expected positive value but got %d", key, nextPages));

        ContinuousPagingSession session = getSession(key);
        if (session == null || session.state.get() != ContinuousPagingSession.State.RUNNING)
        {
            if (logger.isTraceEnabled())
                logger.trace("Cannot update next_pages for continuous paging session {}: not found or no longer running", key);
            return false;
        }

        if (session.numPagesRequested == Integer.MAX_VALUE)
        {
            logger.warn("Cannot update next_pages for continuous paging session {}, numPagesRequested already set to maximum", key);
            return false;
        }

        return session.updateBackpressure(nextPages);
    }

    /**
     * Build pages of CQL rows by implementing the abstract methods of {@link ResultBuilder}.
     * Unlike {@link ResultSet.Builder}, rows are written to a buffer as soon as they are available,
     * so as to avoid the cost of an additional List and more importantly of calling encodedSize for each one
     * of them. This is possible because we do not support sorting across partitions when paging is enabled
     * (CASSANDRA-6722) and we can enforce user limits on rows directly in the query pager.
     *
     * The initial size of the buffer depends on the paging unit. If the page size is in bytes, then we use the page
     * size specified by the user. Otherwise we estimate an average row size and we use the page size in rows
     * times this estimate. In both cases we add a safe margin equal to 2 average row sizes.
     *
     * Before appending a row, we check if we can send the current page. If the page unit is rows, we simply
     * compare the current number of rows in the page with the number requested by the user. If the page unit is
     * in bytes, we see if the current buffer capacity is less than one average row size from the size requested
     * by the user.
     *
     * The average row size is updated every time a page is sent by diving the buffer size by the number of rows in the page.
     *
     * If we are not ready to send a page, we append the row to the current page buffer. If we cannot append the row
     * because we've run out of space, we either double the buffer size (up to a maximum) or at least add enough space
     * for the current row. There is a slight risk here that an abnormally large row may cause us to use a very large buffer
     * from this point onwards, but it is a small risk that we are currently taking.
     *
     * The buffer is therefore always reused, unless we require a larger one.
     */
    @VisibleForTesting
    final static class ContinuousPagingSession
    {
        /**
         * The encoded page to be sent to a client. This wraps the byte buffer that contains
         * the rows already encoded (according to the ResultMessage.Rows specifications), the
         * number of rows and the result set metadata.
         *
         * The codec will encode a page in an identical format to what is used
         * by ResultMessage.ROWS, using the same message kind and metadata.
         *
         * Decoding is not supported.
         */
        private  static class EncodedPage extends ResultMessage
        {
            final ResultSet.ResultMetadata metadata;
            final int numRows;
            final ByteBuf buff;

            private EncodedPage(ResultSet.ResultMetadata metadata, int numRows, ByteBuf buff)
            {
                super(Kind.ROWS);
                this.metadata = metadata;
                this.numRows = numRows;
                this.buff = buff;
            }

            @Override
            public String toString()
            {
                return String.format("ENCODED PAGE (%d ROWS)", numRows);
            }

            public static final Message.Codec<EncodedPage> codec = new Message.Codec<EncodedPage>()
            {
                public EncodedPage decode(ByteBuf body, ProtocolVersion version)
                {
                   assert false : "should never be called";
                    return null;
                }

                public void encode(EncodedPage msg, ByteBuf dest, ProtocolVersion version)
                {
                    dest.writeInt(msg.kind.id);
                    ResultSet.codec.encodeHeader(msg.metadata, dest, msg.numRows, version);
                    dest.writeBytes(msg.buff);
                }

                public int encodedSize(EncodedPage msg, ProtocolVersion version)
                {
                    return 4 + ResultSet.codec.encodedHeaderSize(msg.metadata, version) + msg.buff.readableBytes();
                }
            };
        }

        /**
         * A page: this wraps a buffer and keeps useful information
         * such as metadata, the number of rows and whether this is the last page.
         * It can then create a Frame using this information, at which point
         * it is free to reuse the buffer, if possible, or to release it.
         */
        static class Page
        {
            // The metadata attached to the response
            final ResultSet.ResultMetadata metadata;

            // the state is needed to fix the response parameters
            final QueryState state;

            // the paging options
            final QueryOptions.PagingOptions pagingOptions;

            // The protocol version
            final ProtocolVersion version;

            /** The unique key that identifies the paging session in the sessions map */
            final SessionKey sessionKey;

            // the buffer where the rows will be written
            ByteBuf buf;

            // the number of rows already written to the buffer
            int numRows;

            // the page sequence number
            int seqNo;

            // the max page size in bytes
            int maxPageSize;

            Page(int bufferSize,
                 ResultSet.ResultMetadata metadata,
                 QueryState state,
                 QueryOptions options,
                 SessionKey sessionKey,
                 int seqNo,
                 int maxPageSize)
            {
                this.metadata = metadata;
                this.state = state;
                this.pagingOptions = options.getPagingOptions();
                this.version = options.getProtocolVersion();
                this.sessionKey = sessionKey;

                this.buf = CBUtil.allocator.buffer(bufferSize);
                this.seqNo = seqNo;
                this.maxPageSize = maxPageSize;
            }

            /**
             * Calculates the header size and creates a frame. Copies the page header and rows into it.
             * We could try avoiding the copy of the row data using Netty CompositeByteBuf but on the
             * other hand if we copy then we can reuse the buffer and so it's probably not too bad.
             *
             * @return a newly created frame, the caller takes ownership of the frame body buffer.
             */
            Frame makeFrame(PagingResult pagingResult)
            {
                metadata.setPagingResult(pagingResult);
                EncodedPage response = new EncodedPage(metadata, numRows, buf);
                response.setWarnings(ClientWarn.instance.getWarnings());
                response.setTracingId(state.getPreparedTracingSession());

                return makeFrame(response, EncodedPage.codec, version, state.getStreamId());
            }

            @SuppressWarnings("unchecked")
            static <M extends Message.Response> Frame makeFrame(M response,
                                                                Message.Codec codec,
                                                                ProtocolVersion version,
                                                                int streamId)
            {
                response.setStreamId(streamId);
                int messageSize = codec.encodedSize(response, version);
                Frame frame = Message.ProtocolEncoder.makeFrame(response, messageSize, version);
                codec.encode(response, frame.body, version);
                return frame;
            }

            /** Add a row to the buffer.
             * <p>
             * If {@link org.apache.cassandra.cql3.ResultSet.Codec#encodeRow(List, ResultSet.ResultMetadata, ByteBuf, boolean)}
             * returns false, then we've run out of space in the buffer and the row was only partially written. If this is
             * the case, roll back the buffer writer's index and allocate a larger buffer. We double the buffer size,
             * up to {@link ContinuousPagingSession#maxPageSize()}. We also make sure that at a minimum we have enough space for adding
             * the current row by calling {@link org.apache.cassandra.cql3.ResultSet.Codec#encodedRowSize(List, ResultSet.ResultMetadata)}.
             */
            void addRow(List<ByteBuffer> row)
            {
                int prevWriteIndex = buf.writerIndex();
                boolean ret = ResultSet.codec.encodeRow(row, metadata, buf, true);
                if (ret)
                {
                    numRows++;
                    return;
                }

                buf.writerIndex(prevWriteIndex);
                int rowSize = ResultSet.codec.encodedRowSize(row, metadata);
                int bufferSize = Math.max(buf.readableBytes() + rowSize, Math.min(buf.capacity() * 2, maxPageSize));
                if (logger.isTraceEnabled())
                    logger.trace("Reallocating page buffer from {}/{} to {} for row size {} - {}",
                                buf.readableBytes(), buf.capacity(), bufferSize, rowSize, sessionKey);

                ByteBuf old = buf;
                try
                {
                    buf = null;
                    buf = CBUtil.allocator.buffer(bufferSize);
                    buf.writeBytes(old);
                    ResultSet.codec.encodeRow(row, metadata, buf, false);
                    numRows++;
                }
                finally
                {
                    old.release();
                }
            }

            int size()
            {
                return buf.readableBytes();
            }

            boolean isEmpty()
            {
                return numRows == 0;
            }

            void reuse(int seqNo)
            {
                this.numRows = 0;
                this.seqNo = seqNo;
                this.buf.clear();
            }

            void release()
            {
                buf.release();
                buf = null;
            }

            int seqNo()
            {
                return seqNo;
            }

            /**
             * Return an average row size by calculating this page average and averaging it with the existing one.
             *
             * @param current - the current average row size in bytes
             * @return - the new average row size in bytes
             */
            int avgRowSize(int current)
            {
                if (buf == null || numRows == 0)
                    return current;

                int avg = buf.readableBytes() / numRows;
                return (avg + current) / 2;
            }

            @Override
            public String toString()
            {
                return String.format("[Page rows: %d]", numRows);
            }
        }


        /**
         * The page builder passed to {@link SelectStatement.ContinuousPagingExecutor}.
         *
         * We create a new builder each time the session is suspended because of backpressure.
         *
         * */
        static class Builder extends ResultBuilder
        {
            @VisibleForTesting
            final ContinuousPagingSession session;

            Builder(ContinuousPagingSession session, Selection.Selectors selectors, GroupMaker groupMaker)
            {
                super(selectors, groupMaker);
                this.session = session;
            }

            @Override
            public boolean onRowCompleted(List<ByteBuffer> row, boolean nextRowPending)
            {
                return session.onRowCompleted(row, nextRowPending);
            }

            @Override
            public boolean resultIsEmpty()
            {
                return session.resultIsEmpty();
            }

            @Override
            public void complete(Throwable error)
            {
                session.sendError(error);
            }

            @Override
            public void complete()
            {
                if (logger.isTraceEnabled())
                    logger.trace("Completing continuous paging session {}", session.key);

                // this will invoke onRowCompleted and move the session to state STOPPED after sending the last page, unless there was no data
                super.complete();

                // this takes care of sending the last page in the case that there was no data (inputRow == null in ResultBuilder)
                session.sendLastPage();

                long duration = session.continuousPagingState.timeSource.nanoTime() - session.continuousPagingState.executor.queryStartTimeInNanos();
                ContinuousPagingService.metrics.addTotalDuration(session.continuousPagingState.executor.isLocalQuery(), duration);

                if (logger.isTraceEnabled())
                    logger.trace("Completed continuous paging session {} after {} milliseconds", session.key, TimeUnit.NANOSECONDS.toMillis(duration));
            }

            @Override
            public String toString()
            {
                return String.format("Continuous paging session %s", session.key);
            }
        }

        /**
         * The state of a continuous paging session.
         */
        private enum State
        {
            /** Sending pages */
            RUNNING,
            /** A cancel request was received */
            CANCEL_REQUESTED,
            /** Error or last page sent - the last page
             * did not necessarily have the last page flag
             * set in case of a cancel request. */
            STOPPED
        }


        /** The ResultSet metadata is needed as the header in the page */
        private final ResultSet.ResultMetadata resultMetaData;

        /** A template response for creating the page frame */
        private final QueryState queryState;

        /** The select selectors and group maker */
        private final Selection.Selectors selectors;
        private final GroupMaker groupMaker;

        /** The query options contain some parameters that we need */
        private final QueryOptions options;

        /** The page writer takes care of sending pages to the client. */
        private final ContinuousPageWriter pageWriter;

        /** State related to this continuous paging session */
        private final ContinuousPagingState continuousPagingState;

        /** The paging options, including paging unit, size and max number of pages */
        private final QueryOptions.PagingOptions pagingOptions;

        /** The unique key that identifies us in the sessions map */
        private final SessionKey key;

        /** The average row size, initially estimated by the selection and then refined each time a page is sent */
        private int avgRowSize;

        /** The number of pages sent so far */
        @VisibleForTesting
        int numPagesSent;

        /** The total number of pages requested by the client */
        private int numPagesRequested;

        /** The current page being written to */
        private Page currentPage;

        /** The state of the continuous paging session, see {@link State} */
        private final AtomicReference<State> state;

        /** Set to the timestamp when the session is paused, -1 otherwise. */
        private volatile long paused;

        /** The paging state is set when a page is sent, used to resume a session. */
        @Nullable private volatile PagingState pagingState;

        ContinuousPagingSession(Selection.Selectors selectors,
                                GroupMaker groupMaker,
                                ResultSet.ResultMetadata resultMetadata,
                                QueryState queryState,
                                QueryOptions options,
                                ContinuousPagingState continuousPagingState,
                                SessionKey key)
        {
            this.selectors = selectors;
            this.groupMaker = groupMaker;
            this.resultMetaData = resultMetadata;
            this.queryState = queryState;
            this.options = options;
            this.continuousPagingState = continuousPagingState;
            this.key = key;
            this.pagingOptions = options.getPagingOptions();
            this.numPagesSent = 0;
            this.numPagesRequested = pagingOptions.nextPages() <= 0 ? Integer.MAX_VALUE : pagingOptions.nextPages();
            this.state = new AtomicReference<>(State.RUNNING);
            this.paused = -1;
            this.pageWriter = new ContinuousPageWriter(continuousPagingState.channel,
                                                       pagingOptions.maxPagesPerSecond(),
                                                       continuousPagingState.config.max_session_pages);
            this.avgRowSize = continuousPagingState.averageRowSize;

            allocatePage(1);
        }

        /**
         * @return - the number of pending pages, which are ready to be sent on the wire.
         */
        int pendingPages()
        {
            return pageWriter.pendingPages();
        }

        /**
         * Request to cancel an ongoing session. Returns a future that will be completed
         * after the last page has been sent or immediately if the session was not running.
         */
        public CompletableFuture<Void> cancel()
        {
            if (state.compareAndSet(State.RUNNING, State.CANCEL_REQUESTED))
                pageWriter.cancel();
            else if (logger.isTraceEnabled())
                logger.trace("Could not cancel continuous paging session {}, not running ({})", key, state.get());

            return pageWriter.completionFuture();
        }

        /**
         * Process a backpressure update:
         * - increase numPagesRequested by nextPages making sure it doesn't overflow
         * - schedule a check to see if the session needs resuming
         */
        boolean updateBackpressure(int nextPages)
        {
            assert nextPages > 0 : "nextPages should be positive";

            int old = numPagesRequested;
            numPagesRequested += nextPages;
            if (numPagesRequested <= old)
            { // overflow
                logger.warn("Tried to increase numPagesRequested from {} by {} but got {} for session {}, setting it to {}",
                            old, nextPages, numPagesRequested, key, Integer.MAX_VALUE);
                numPagesRequested = Integer.MAX_VALUE;
                return false;
            }

            if (logger.isTraceEnabled())
                logger.trace("Updated numPagesRequested to {} for continuous paging session {}", numPagesRequested, key);

            continuousPagingState.executor.getScheduler().scheduleDirect(this::maybeResume);
            return true;
        }

        public ContinuousPagingConfig config()
        {
            return continuousPagingState.config;
        }

        /** Allocate the initial page with the page size specified by the user and some safe margin added to it,
         * but making sure it is not too big by enforcing maxPageSize() as an upper limit. Otherwise
         * reuse an existing page.
         *
         * @param seqNo - the sequence number for the new page
         */
        private void allocatePage(int seqNo)
        {
            if (currentPage == null)
            {
                int maxPageSize = maxPageSize();
                int bufferSize = Math.min(maxPageSize, pagingOptions.pageSize().inEstimatedBytes(avgRowSize) + safePageMargin());
                if (logger.isTraceEnabled())
                    logger.trace("Allocating page with buffer size {}, avg row size {} for {}",
                                 bufferSize, avgRowSize, key);
                currentPage = new Page(bufferSize, resultMetaData, queryState, options, key, seqNo, maxPageSize);
            }
            else
            {
                if (logger.isTraceEnabled())
                    logger.trace("Reusing page with buffer size {}, avg row size {} for {}",
                                 currentPage.buf.capacity(), avgRowSize, key);
                currentPage.reuse(seqNo);
            }
        }

        /**
         * User page sizes bigger than this value will be ignored and this value will be used instead.
         *
         * @return - the max page size in bytes
         */
        int maxPageSize()
        {
            return continuousPagingState.config.max_page_size_mb * 1024 * 1024;
        }

        /**
         * Return the safe page margin, this gets added to the page buffer size in order to reduce
         * the probability of having to reallocate to fit all page rows.
         *
         * @return the safe page margin, currently 2 average row sizes
         */
        private int safePageMargin()
        {
            return 2 * avgRowSize;
        }

        /**
         * Send the page to the callback.
         *
         * It's OK to send empty pages if they are the last page, because the client needs
         * to know it has received the last page. Also update the average row size.
         *
         * If nextRowPending is true, it means the pager has alredy moved on to the next row,
         * therefore the paging state will point to the next row and hence we must set inclusive to true
         * so that a new search includes the next row.
         */
        private void processPage(boolean last, boolean nextRowPending)
        {
            // This isn't strictly necessary as if we're canceled, the pageWriter will discard the page
            // anyway and we'll stop on the next row. Kind of sensible though and better safe than sorry.
            if (state.get() == State.CANCEL_REQUESTED)
                return;

            assert !currentPage.isEmpty() || last;

            avgRowSize = currentPage.avgRowSize(avgRowSize);
            pagingState = continuousPagingState.executor.state(nextRowPending);

            if (logger.isTraceEnabled())
                logger.trace("Sending page nr. {} with {} rows, average row size: {}, last: {}, nextRowPending: {}, pagingState {} for session {}",
                             numPagesSent + 1, currentPage.numRows, avgRowSize, last, nextRowPending, pagingState, key);

            PagingResult pagingResult = new PagingResult(pagingState,
                                                         currentPage.seqNo,
                                                         last);
            pageWriter.sendPage(currentPage.makeFrame(pagingResult), !last);
            numPagesSent++;
        }

        /**
         * A row is available: add the row to the current page and see if we must send
         * the current page. Allocate a new page if the current page was sent, stop
         * if we need to do so.
         *
         * @param row - the completed row
         * @param nextRowPending - true if there is a new row ready to be processed and alredy available to the pager
         *
         * @return - true if we should continue processing more rows, false otherwise.
         */
        boolean onRowCompleted(List<ByteBuffer> row, boolean nextRowPending)
        {
            if (state.get() == State.CANCEL_REQUESTED)
            {
                stop();
                return false;
            }

            if (currentPage == null)
            {
                assert state.get() == State.STOPPED : "Invalid state with null page: " + state.get();
                return false;
            }

            currentPage.addRow(row);

            boolean mustSendPage = !nextRowPending || pageCompleted(currentPage.numRows, currentPage.size(), avgRowSize) || pageIsCloseToMax();
            if (!mustSendPage)
                return true; // process more rows for sure

            boolean isLastPage = !nextRowPending || isLastPage(currentPage.seqNo());

            // remove the session before sendiing the last page
            if (isLastPage)
                removeSession(key);

            processPage(isLastPage, nextRowPending);

            if (numPagesSent == Integer.MAX_VALUE)
            {
                sendError(new ClientWriteException(String.format("Reached maximum number of pages (%d), stopping session to avoid overflow",
                                                                 numPagesSent)));
            }
            else if (!isLastPage && state.get() == State.RUNNING)
            {
                allocatePage(currentPage.seqNo() + 1);
                maybePause();
            }
            else
            {
                stop();
            }

            return state.get() == State.RUNNING;
        }

        void maybePause()
        {
            ContinuousBackPressureException ret;

            // for local queries, the time at which the query started
            long startedAtMillis = continuousPagingState.executor.localStartTimeInMillis();

            if (!pageWriter.hasSpace())
                ret = new ContinuousBackPressureException(String.format("Continuous paging queue is full (%d pages in the queue)",
                                                                        pageWriter.pendingPages()));
            else if (numPagesSent >= numPagesRequested)
                ret = new ContinuousBackPressureException(String.format("Continuous paging backpressure was triggered, requested %d sent %d",
                                                                        numPagesRequested,
                                                                        numPagesSent));
            else if (startedAtMillis > 0 && ((continuousPagingState.timeSource.currentTimeMillis() - startedAtMillis) >= continuousPagingState.config.max_local_query_time_ms))
                ret = new ContinuousBackPressureException(String.format("Locally optimized query running longer than %d milliseconds",
                                                                        continuousPagingState.config.max_local_query_time_ms));
            else
                return;

            if (logger.isTraceEnabled())
                logger.trace("Pausing session {}: {}", key, ret.getMessage());

            this.paused = continuousPagingState.timeSource.nanoTime();
            ContinuousPagingService.metrics.serverBlocked.inc();
            continuousPagingState.executor.getScheduler().scheduleDirect(this::maybeResume,
                                                                         continuousPagingState.config.paused_check_interval_ms,
                                                                         TimeUnit.MILLISECONDS);

            throw ret;
        }

        // this must always execute on the same thread by scheduling on pagingExecutor.getScheduler()
        private void maybeResume()
        {
            if (paused == -1 || state.get() == State.STOPPED)
                return;

            long now = continuousPagingState.timeSource.nanoTime();
            boolean canResume = numPagesRequested == Integer.MAX_VALUE
                                ? pageWriter.halfQueueAvailable()
                                : pageWriter.hasSpace() && numPagesSent < numPagesRequested;

            if (canResume && logger.isTraceEnabled())
                logger.trace("Resuming session {}, pages requested: {}, pages sent: {}, queue size: {}, paging state {}",
                             key, numPagesRequested, numPagesSent, pageWriter.pendingPages(), pagingState);

            if (!canResume)
            {
                if ((now - paused) >= TimeUnit.SECONDS.toNanos(continuousPagingState.config.client_timeout_sec))
                {
                    ContinuousPagingService.metrics.serverBlockedLatency.addNano(now - paused);
                    sendError(new ClientWriteException(String.format("Paused for longer than %d seconds and unable to write pages to client", continuousPagingState.config.client_timeout_sec)));
                }
                else
                {
                    continuousPagingState.executor.getScheduler().scheduleDirect(this::maybeResume, continuousPagingState.config.paused_check_interval_ms, TimeUnit.MILLISECONDS);
                }
            }
            else
            {
                ContinuousPagingService.metrics.serverBlockedLatency.addNano(now - paused);
                paused = -1;
                continuousPagingState.executor.schedule(pagingState, new Builder(this, selectors, groupMaker));
            }
        }

        /**
         * Return true if we are ready to send a page. If the page unit is rows, simply compare the number
         * of rows with the page size, if it is in bytes see if there is less than avg row size left in
         * the page.
         *
         * @param numRows - the number of rows written so far
         * @param size - the size written so far in bytes
         * @param rowSize - the average row size in bytes
         * @return - true if the page can be considered completed
         */
        private boolean pageCompleted(int numRows, int size, int rowSize)
        {
            return pagingOptions.pageSize().isComplete(numRows, size + rowSize - 1);
        }

        private boolean isLastPage(int pageNo)
        {
            return pageNo >= pagingOptions.maxPages();
        }

        private boolean pageIsCloseToMax()
        {
            return currentPage != null && (maxPageSize() - currentPage.size()) < safePageMargin();
        }

        private void stop()
        {
            State current = state.get();
            assert current == State.RUNNING || current == State.CANCEL_REQUESTED : "Invalid state when stopping: " + current;
            if (state.compareAndSet(current, State.STOPPED))
            {
                if (logger.isTraceEnabled())
                    logger.trace("Continuous paging session {} stopped, previously in state {}", key, current);

                release();
            }
            else
            {
                logger.error("Failed to stop session {} ({})", key, state.get());
            }
        }

        private void release()
        {
            if (currentPage != null)
            {
                currentPage.release();
                currentPage = null;
            }
        }

        /**
         * This is called by ResultBuilder.sendError() to determine if we should send one final unique
         * aggregate result row, so it should return true only if we haven't sent a single row so far,
         * but also if we are ready to receive a row (currentPage not null), that is we have not been
         * stopped.
         *
         * @return - true if no row was processed so far and if we can accept a new row.
         */
        boolean resultIsEmpty()
        {
            return numPagesSent == 0 && currentPage != null && currentPage.isEmpty();
        }

        void sendError(Throwable error)
        {
            if (logger.isTraceEnabled())
                logger.trace("Sending error {}/{} for session {}", error.getClass(), error.getMessage(), key);

            if (error instanceof ClientWriteException)
                metrics.clientWriteExceptions.mark();
            else
                metrics.failures.mark();

            if (currentPage != null)
                stop();

            // remove the session before sending the final error
            removeSession(key);

            Message.Response err = ErrorMessage.fromException(error);
            pageWriter.sendError(Page.makeFrame(err, err.type.codec, options.getProtocolVersion(), queryState.getStreamId()));
        }

        void sendLastPage()
        {
            if (state.get() != State.STOPPED)
            {
                removeSession(key);

                processPage(true, false);
                stop();
            }
        }
    }
}
