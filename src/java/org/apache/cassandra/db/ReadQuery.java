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
package org.apache.cassandra.db;

import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.monitoring.Monitorable;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Generic abstraction for read queries.
 * <p>
 * The main implementation of this is {@link ReadCommand} but we have this interface because
 * {@link SinglePartitionReadCommand.Group} is also consider as a "read query" but is not a
 * {@code ReadCommand}.
 */
public interface ReadQuery
{
    ReadQuery EMPTY = new EmptyQuery();

    final static class EmptyQuery implements ReadQuery
    {
        public ReadExecutionController executionController()
        {
            return ReadExecutionController.empty();
        }

        public PartitionIterator execute(ConsistencyLevel consistency,
                                         ClientState clientState,
                                         long queryStartNanoTime,
                                         boolean forContinuousPaging) throws RequestExecutionException
        {
            return EmptyIterators.partition();
        }

        public PartitionIterator executeInternal(ReadExecutionController controller)
        {
            return EmptyIterators.partition();
        }

        public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController)
        {
            return EmptyIterators.unfilteredPartition(executionController.metaData());
        }

        public DataLimits limits()
        {
            // What we return here doesn't matter much in practice. However, returning DataLimits.NONE means
            // "no particular limit", which makes SelectStatement.execute() take the slightly more complex "paging"
            // path. Not a big deal but it's easy enough to return a limit of 0 rows which avoids this.
            return DataLimits.cqlLimits(0);
        }

        public QueryPager getPager(PagingState state, ProtocolVersion protocolVersion)
        {
            return QueryPager.EMPTY;
        }

        public boolean selectsKey(DecoratedKey key)
        {
            return false;
        }

        public boolean selectsClustering(DecoratedKey key, Clustering clustering)
        {
            return false;
        }

        public int nowInSec()
        {
            return FBUtilities.nowInSeconds();
        }

        public void monitor(long constructionTime, long timeout, long slowQueryTimeout, boolean isCrossNode) { }

        public void monitorLocal(long startTime) { }

        public boolean complete()
        {
            return true;
        }

        public boolean queriesOnlyLocalData()
        {
            return true;
        }
    }

    /**
     * Starts a new read operation.
     * <p>
     * This must be called before {@link this#executeInternal(ReadExecutionController)} and passed to it to protect the read.
     * The returned object <b>must</b> be closed on all path and it is thus strongly advised to
     * use it in a try-with-ressource construction.
     *
     * @return a newly started execution controller for this {@code ReadQuery}.
     */
    public ReadExecutionController executionController();

    /**
     * Executes the query for external client requests, at the provided consistency level.
     *
     * @param consistency the consistency level to achieve for the query.
     * @param clientState the {@code ClientState} for the query. In practice, this can be null unless
     * {@code consistency} is a serial consistency.
     * @param queryStartNanoTime the time at which we have received the client request, in nano seconds
     * @param forContinuousPaging indicates that this a query for continuous paging. When this is
     * provided, queries that are fully local (and are CL.ONE or CL.LOCAL_ONE) are optimized by
     * returning a direct iterator to the data (as from calling executeInternal() but with the additional
     * bits needed for a full user query), which 1) leave to the caller the responsibility of not holding
     * the iterator open too long (as it holds a {@code executionController} open) and 2) this bypass
     * the dynamic snitch, read repair and speculative retries. This flag is also used to record
     * different metrics than for non-continuous queries.
     *
     * @return the result of the query.
     */
    public PartitionIterator execute(ConsistencyLevel consistency,
                                     ClientState clientState,
                                     long queryStartNanoTime,
                                     boolean forContinuousPaging) throws RequestExecutionException;

    /**
     * Execute the query for internal queries.
     *
     * This return an iterator that directly query the local underlying storage.
     *
     * @param controller the {@code ReadExecutionController} protecting the read.
     * @return the result of the query.
     */
    public PartitionIterator executeInternal(ReadExecutionController controller);

    /**
     * Execute the query locally. This is where the reading actually happens, typically this method
     * would be invoked by the read verb handlers, {@link org.apache.cassandra.service.StorageProxy.LocalReadRunnable}
     * and {@link ReadQuery#executeInternal(ReadExecutionController)}, or whenever we need to read local data
     * and we need an unfiltered partition iterator, rather than a filtered one. The main difference with
     * {@link ReadQuery#executeInternal(ReadExecutionController)} is the filtering, only unfiltered iterators can
     * be merged later on.
     *
     * @param executionController the {@code ReadExecutionController} protecting the read.
     * @return the result of the read query.
     */
    public UnfilteredPartitionIterator executeLocally(ReadExecutionController controller);

    /**
     * Returns a pager for the query.
     *
     * @param pagingState the {@code PagingState} to start from if this is a paging continuation. This can be
     * {@code null} if this is the start of paging.
     * @param protocolVersion the protocol version to use for the paging state of that pager.
     *
     * @return a pager for the query.
     */
    public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion);

    /**
     * The limits for the query.
     *
     * @return The limits for the query.
     */
    public DataLimits limits();

    /**
     * @return true if the read query would select the given key, including checks against the row filter, if
     * checkRowFilter is true
     */
    public boolean selectsKey(DecoratedKey key);

    /**
     * @return true if the read query would select the given clustering, including checks against the row filter, if
     * checkRowFilter is true
     */
    public boolean selectsClustering(DecoratedKey key, Clustering clustering);

    /**
     * The time in seconds to use as "now" for this query.
     * <p>
     * We use the same time as "now" for the whole query to avoid considering different
     * values as expired during the query, which would be buggy (would throw of counting amongst other
     * things).
     *
     * @return the time (in seconds) to use as "now".
     */
    public int nowInSec();

    /**
     * Monitor a normal query, either originating cross node or not. Report this query as failed
     * or slow, if timeout or slowQueryTimeout milliseconds elapse.
     * <p>
     * Note that normal queries may be running locally too, via
     * {@link org.apache.cassandra.service.StorageProxy.LocalReadRunnable}. The difference with local
     * queries monitored by {@link ReadQuery#monitorLocal(long)}, is that these local queries run <b>only</b>
     * locally, and tend to take much longer as they usually retrieve more data.
     *
     * @param constructionTime - the approximate time at which the query message was received, if cross node, or
     *                         the query was started, if running locally.
     * @param timeout - the timeout after which the query should be aborted and reported as failed.
     * @param slowQueryTimeout - the timeout after which the query should be reported slow.
     * @param isCrossNode - true when the query originated in another node.
     */
    public void monitor(long constructionTime, long timeout, long slowQueryTimeout, boolean isCrossNode);

    /**
     * Monitor a local query. These are queries that never go cross node, and tend to be long running queries
     * (continuous paging). Therefore, they should not be reported as failed or slow, but they should
     * be aborted after {@link org.apache.cassandra.config.ContinuousPagingConfig#max_local_query_time_ms}
     * in order to release resources. They then get restarted later on.
     *
     * @param startTime - the approximate time at which the query was started.
     */
    public void monitorLocal(long startTime);

    /**
     * @return true if there was no monitoring, otherwise return {@link Monitorable#complete()}.
     */
    public boolean complete();

    /**
     * Check if this query can be performed with local data only.
     *
     * @return true if we can perform this query only with local data, false otherwise.
     */
    public boolean queriesOnlyLocalData();
}
