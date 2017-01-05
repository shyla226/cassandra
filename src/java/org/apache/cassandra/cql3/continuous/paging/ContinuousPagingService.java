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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.config.ContinuousPagingConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.PagingResult;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.selection.ResultBuilder;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.exceptions.ClientWriteException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.metrics.ContinuousPagingMetrics;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.ServerError;
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

    /** Any ongoing continuous paging sessions willl be stored in this map. */
    private static final Map<SessionKey, PageBuilder> sessions = new HashMap<>();

    public static ResultBuilder makeBuilder(SelectStatement statement,
                                            SelectStatement.ContinuousPagingExecutor pagingExecutor,
                                            QueryState state,
                                            QueryOptions options,
                                            ContinuousPagingConfig config)
    throws RequestValidationException, RequestExecutionException
    {
        assert options.continuousPagesRequested();

        SessionKey key = new SessionKey(state.getClientState().getRemoteAddress(), state.getStreamId());

        synchronized (sessions)
        {
            if (sessions.containsKey(key))
            {
                metrics.creationFailures.mark();
                logger.error("Continuous paging session {} already exists", key);
                throw RequestValidations.invalidRequest("Invalid request, already executing continuous paging session %s", key);
            }

            if (sessions.size() >= config.max_concurrent_sessions)
            {
                metrics.creationFailures.mark();
                metrics.tooManySessions.mark();
                if (logger.isTraceEnabled())
                    logger.trace("Too many continuous paging sessions are already running: {}", sessions.size());
                throw RequestValidations.invalidRequest("Invalid request, too many continuous paging sessions are already running: %d", sessions.size());
            }

            PageBuilder ret = new PageBuilder(statement,
                                              state,
                                              options,
                                              pagingExecutor,
                                              key,
                                              config);


            sessions.put(key, ret);

            if (logger.isTraceEnabled())
                logger.trace("Starting continuous paging session {} with paging options {}, total number of sessions running: {}",
                             key, options.getPagingOptions(), sessions.size());
            return ret;
        }
    }

    private static PageBuilder removeBuilder(SessionKey key)
    {
        synchronized (sessions)
        {
            PageBuilder ret = sessions.remove(key);
            if (ret != null)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Removed continuous paging session {}, {} sessions still running", key, sessions.size());
            }
            return ret;
        }
    }

    /**
     * @return - the number of continuous paging sessions currently executing.
     */
    public static long liveSessions()
    {
        return sessions.size();
    }

    /**
     * @return - the number of pages currently pending, that is waiting to be sent on the wire.
     */
    public static long pendingPages()
    {
        return sessions.values().stream().map(PageBuilder::pendingPages).reduce(Integer::sum).orElseGet(() -> 0);
    }

    /**
     * Cancel an ongoing continuous paging session.
     *
     * @param queryState - the queryState
     * @param streamId - the initial frame id of the continuous paging request message
     * @return true if the session was cancelled, false if the session was not found
     */
    public static boolean cancel(QueryState queryState, int streamId)
    {
        SessionKey key = new SessionKey(queryState.getClientState().getRemoteAddress(), streamId);
        PageBuilder builder = removeBuilder(key);
        if (builder == null)
        {
            if (logger.isTraceEnabled())
                logger.trace("Cannot cancel continuous paging session {}: not found", key);
            return false;
        }

        if (logger.isTraceEnabled())
            logger.trace("Cancelling continuous paging session {}", key);
        ContinuousPagingConfig config = builder.config();

        try
        {
            builder.cancel().get(config.max_client_wait_time_ms, TimeUnit.MILLISECONDS);
            return true;
        }
        catch (InterruptedException | CancellationException | TimeoutException | ExecutionException ex)
        {
            logger.warn("Failed to wait for last page to be delivered when cancelling continuous " +
                        "paging session {}, waited for {} milliseconds", key, config.max_client_wait_time_ms);

            throw new ServerError("Failed to wait for last page to be delivered when cancelling continuous paging session");
        }
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
    private final static class PageBuilder extends ResultBuilder
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
            public final ResultSet.ResultMetadata metadata;
            public final int numRows;
            public final ByteBuf buff;

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

            Page(int bufferSize, ResultSet.ResultMetadata metadata, QueryState state, QueryOptions options, SessionKey sessionKey, int seqNo)
            {
                this.metadata = metadata;
                this.state = state;
                this.pagingOptions = options.getPagingOptions();
                this.version = options.getProtocolVersion();
                this.sessionKey = sessionKey;

                this.buf = CBUtil.allocator.buffer(bufferSize);
                this.seqNo = seqNo;
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
                if (state.getPreparedTracingSession() != null)
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
             * up to {@link PageBuilder#maxPageSize()}. We also make sure that at a minimum we have enough space for adding
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
                int bufferSize = Math.max(buf.readableBytes() + rowSize, Math.min(buf.capacity() * 2, maxPageSize()));
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

        /** The ResultSet metadata is needed as the header in the page */
        private final ResultSet.ResultMetadata resultMetaData;

        /** A template response for creating the page frame */
        private final QueryState state;

        /** The query options contain some parameters that we need */
        private final QueryOptions options;

        /** The page writer takes care of sending pages to the client. */
        private final ContinuousPageWriter pageWriter;

        /** The paging executor is responsible for the iteration, we need it to retrieve the paging state */
        private final SelectStatement.ContinuousPagingExecutor pagingExecutor;

        /** The options from the YAML file */
        private final ContinuousPagingConfig config;

        /** The paging options, including paging unit, size and max number of pages */
        private final QueryOptions.PagingOptions pagingOptions;

        /** The unique key that identifies us in the sessions map */
        private final SessionKey key;

        /** The average row size, initially estimated by the selection and then refined each time a page is sent */
        private int avgRowSize;

        /** The number of pages sent so far */
        private int numSentPages;

        /** The current page being written to */
        private Page currentPage;

        /** Set to true when a cancel request has been received */
        private volatile boolean canceled;

        /** Set to true when the session has been stopped, either because of limits or a cancel request. */
        private volatile boolean stopped;

        PageBuilder(SelectStatement statement,
                    QueryState state,
                    QueryOptions options,
                    SelectStatement.ContinuousPagingExecutor pagingExecutor,
                    SessionKey key,
                    ContinuousPagingConfig config)
        {
            super(options,
                  statement.parameters.isJson,
                  statement.aggregationSpec == null ? null: statement.aggregationSpec.newGroupMaker(),
                  statement.getSelection());
            this.resultMetaData = selection.getResultMetadata(isJson).copy();
            this.state = state;
            this.options = options;
            this.pagingExecutor = pagingExecutor;
            this.key = key;
            this.config = config;
            this.pagingOptions = options.getPagingOptions();
            this.pageWriter = new ContinuousPageWriter(state.getConnection(),
                                                       pagingOptions.maxPagesPerSecond(),
                                                       config);
            this.avgRowSize = ResultSet.estimatedRowSize(statement.cfm, selection.getColumns());

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
         * Request to stop an ongoing session. Returns a future that will be completed
         * after the last page has been sent.
         */
        public CompletableFuture<Void> cancel()
        {
            pageWriter.cancel();
            canceled = true;
            return pageWriter.completionFuture();
        }

        public ContinuousPagingConfig config()
        {
            return config;
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
                int bufferSize = Math.min(maxPageSize(), pagingOptions.pageSize().inEstimatedBytes(avgRowSize) + safePageMargin());
                if (logger.isTraceEnabled())
                    logger.trace("Allocating page with buffer size {}, avg row size {} for {}",
                                 bufferSize, avgRowSize, key);
                currentPage = new Page(bufferSize, resultMetaData, state, options, key, seqNo);
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
        static int maxPageSize()
        {
            return DatabaseDescriptor.getNativeTransportMaxFrameSize() / 2;
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
            if (canceled)
                return;

            assert !currentPage.isEmpty() || last;

            avgRowSize = currentPage.avgRowSize(avgRowSize);

            boolean hasMorePages = !last && !stopped;
            if (logger.isTraceEnabled())
                logger.trace("Sending page with {} rows, average row size: {}, last: {}, hasMorePages: {}",
                             currentPage.numRows, avgRowSize, last, hasMorePages);

            PagingResult pagingResult = new PagingResult(pagingExecutor.state(nextRowPending),
                                                         currentPage.seqNo,
                                                         last);
            pageWriter.sendPage(currentPage.makeFrame(pagingResult), hasMorePages);
            numSentPages++;
        }

        /**
         * A row is available: add the row to the current page and see if we must send
         * the current page. Allocate a new page if the current page was sent, stop
         * if we need to do so.
         *
         * @param row - the completed row
         * @return - true if we should continue processing more rows, false otherwise.
         */
        public boolean onRowCompleted(List<ByteBuffer> row, boolean nextRowPending)
        {
            if (canceled)
            {
                stop();
                return false;
            }

            if (currentPage == null)
            {
                assert stopped;
                return false;
            }

            currentPage.addRow(row);

            boolean mustSendPage = pageCompleted(currentPage.numRows, currentPage.size(), avgRowSize)
                                   || pageIsCloseToMax();
            if (mustSendPage)
            {
                boolean isLastPage = isLastPage(currentPage.seqNo());
                processPage(isLastPage, nextRowPending);
                if (!isLastPage)
                    allocatePage(currentPage.seqNo() + 1);
                else
                    stop();
            }

            return !stopped;
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
            if (!stopped)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Stopping continuous paging session {} early", key);
                stopped = true;

                release();
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
         * This is called by ResultBuilder.complete() to determine if we should send one final unique
         * aggregate result row, so it should return true only if we haven't sent a single row so far,
         * but also if we are ready to receive a row (currentPage not null), that is we have not been
         * stopped.
         *
         * @return - true if no row was processed so far and if we can accept a new row.
         */
        public boolean resultIsEmpty()
        {
            return numSentPages == 0 && currentPage != null && currentPage.isEmpty();
        }

        @Override
        public void complete(Throwable error)
        {
            if (error instanceof ClientWriteException)
                metrics.clientWriteExceptions.mark();
            else
                metrics.failures.mark();

            if (currentPage != null)
                stop();

            Message.Response err = ErrorMessage.fromException(error);
            pageWriter.sendError(Page.makeFrame(err, err.type.codec, options.getProtocolVersion(), state.getStreamId()));

            complete();
        }

        @Override
        public void complete()
        {
            super.complete();

            // no need to allow cancelling this session if we are already completing it
            // Removing it before sending any last page avoids races in case the
            // same client sends a new continuous paging request with the same stream id before we had
            // a chance ot remove it
            removeBuilder(key);

            if (currentPage != null)
            {
                processPage(true, false);
                release();
            }
        }
    }
}
