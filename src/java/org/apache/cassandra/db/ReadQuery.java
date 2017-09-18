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

import javax.annotation.Nullable;

import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.db.monitoring.Monitorable;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.flow.Flow;

/**
 * Generic abstraction for read queries.
 * <p>
 * The main implementation of this is {@link ReadCommand} but we have this interface because
 * {@link SinglePartitionReadCommand.Group} is also consider as a "read query" but is not a
 * {@code ReadCommand}.
 */
public interface ReadQuery extends Monitorable
{
    public static ReadQuery empty(final TableMetadata metadata)
    {
        return new ReadQuery()
        {
            public TableMetadata metadata()
            {
                return metadata;
            }

            public Flow<FlowablePartition> execute(ReadContext ctx) throws RequestExecutionException
            {
                return Flow.empty();
            }

            public Flow<FlowablePartition> executeInternal(Monitor monitor)
            {
                return Flow.empty();
            }

            public Flow<FlowableUnfilteredPartition> executeLocally(Monitor monitor)
            {
                return Flow.empty();
            }

            @Override
            public boolean isEmpty()
            {
                return true;
            }

            public ReadExecutionController executionController()
            {
                return ReadExecutionController.empty();
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


            @Override
            public boolean selectsFullPartition()
            {
                return false;
            }

            public boolean queriesOnlyLocalData()
            {
                return true;
            }

            public String toCQLString()
            {
                return "<EMPTY>";
            }
        };
    }

    /**
     * The metadata for the table this is a query on.
     *
     * @return the metadata for the table this is a query on.
     */
    public TableMetadata metadata();

    /**
     * Starts a new read operation.
     * <p>
     * This must be called before {@link this#executeInternal()} and passed to it to protect the read.
     * The returned object <b>must</b> be closed on all path and it is thus strongly advised to
     * use it in a try-with-resource construction.
     *
     * @return a newly started execution controller for this {@code ReadQuery}.
     */
    public ReadExecutionController executionController();

    /**
     * Executes the query for external client requests, at the provided consistency level.
     *
     * @param ctx the read context.
     * @return the result of the query as as asynchronous flow of {@link FlowablePartition}
     */
    public Flow<FlowablePartition> execute(ReadContext ctx) throws RequestExecutionException;

    /**
     * Execute the query for internal queries.
     *
     * This return an iterator that directly query the local underlying storage.
     *
     * @return the result of the query.
     */
    public Flow<FlowablePartition> executeInternal(@Nullable Monitor monitor);

    public default Flow<FlowablePartition> executeInternal()
    {
        return executeInternal(null);
    }

    /**
     * Execute the query locally. This is where the reading actually happens, typically this method
     * would be invoked by the read verb handlers, {@link ReadVerbs}
     * and {@link ReadQuery#executeInternal()}, or whenever we need to read local data
     * and we need an unfiltered partition iterator, rather than a filtered one. The main difference with
     * {@link ReadQuery#executeInternal()} is the filtering, only unfiltered iterators can
     * be merged later on.
     *
     * @return the result of the read query.
     */
    public Flow<FlowableUnfilteredPartition> executeLocally(@Nullable Monitor monitor);

    default public Flow<FlowableUnfilteredPartition> executeLocally()
    {
        return executeLocally(null);
    }

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
     * Check if this query can be performed with local data only.
     *
     * @return true if we can perform this query only with local data, false otherwise.
     */
    public boolean queriesOnlyLocalData();

    /**
     * Recreate a rough CQL string corresponding to this query.
     * <p>
     * Note that this is meant for debugging purpose and the goal is mainly to provide a user-understandable representation
     * of the operation. There is absolutely not guarantee the string will be valid CQL (and it won't be in some case).
     */
    public String toCQLString();

    // Monitorable interface
    default public String name()
    {
        return toCQLString();
    }

    /**
     * Checks if this {@code ReadQuery} selects full partitions, that is it has no filtering on clustering or regular columns.
     * @return {@code true} if this {@code ReadQuery} selects full partitions, {@code false} otherwise.
     */
    public boolean selectsFullPartition();

    /**
     * Test-only helper method.
     */
    default public UnfilteredPartitionIterator executeForTests()
    {
        return FlowablePartitions.toPartitions(FlowablePartitions.skipEmptyUnfilteredPartitions(executeLocally()), metadata());
    }

    /**
     * Test-only helper method.
     */
    default public PartitionIterator executeInternalForTests()
    {
        return FlowablePartitions.toPartitionsFiltered(executeInternal());
    }

    /**
     * Whether this query is known to return nothing upfront.
     * <p>
     * This is overridden by the {@code ReadQuery} created through {@link #empty(TableMetadata)}, and that's probably the
     * only place that should override it (we should be creating true {@code ReadCommand} otherwise).
     *
     * @return if this method is guaranteed to return no results whatsoever.
     */
    public default boolean isEmpty()
    {
        return false;
    }

    /**
     * Apply potential defaults for this type of query to the provided context.
     * <p>
     * Note that this is only defaults and can be overwritten for specific reads through the {@link ReadContext.Builder}
     * as appropriate.
     *
     * @param ctx the read context (builder) to apply the default to.
     * @return {@code ctx} (for chaining purposes).
     */
    public default ReadContext.Builder applyDefaults(ReadContext.Builder ctx)
    {
        return ctx;
    }
}
