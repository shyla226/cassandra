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
package org.apache.cassandra.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Completable;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.concurrent.Accumulator;
import org.apache.cassandra.utils.flow.Flow;

public abstract class ResponseResolver<T>
{
    protected static final Logger logger = LoggerFactory.getLogger(ResponseResolver.class);

    final ReadCommand command;
    final ReadContext ctx;

    // Accumulator gives us non-blocking thread-safety with optimal algorithmic constraints
    protected final Accumulator<Response<ReadResponse>> responses;

    ResponseResolver(ReadCommand command, ReadContext ctx, int maxResponseCount)
    {
        // It's a programming error to pass a command here created through ReadCommand.createDigestCommand(). Those digest
        // commands should be created late when sending request, so only to be passed to Verb#newRequest,
        // but no resolver applies _only_ to digest queries so keeping the raw command here is logical and more intuitive.
        // The reason this matter is that with speculative retries and read repairs, the command passed here might be
        // used to generate new requests, and the assumption when we do so is that we won't get digest queries that way:
        // if we want digest query then, we can simply call createDigestCommand() then, but getting digest queries
        // silently is error prone.
        assert !command.isDigestQuery() : "Shouldn't create a response resolver with a digest command; cmd=" + command;
        this.command = command;
        this.ctx = ctx;
        this.responses = new Accumulator<>(maxResponseCount);
    }

    ConsistencyLevel consistency()
    {
        return ctx.consistencyLevel;
    }

    public abstract Flow<T> getData();
    public abstract Flow<T> resolve() throws DigestMismatchException;

    /**
     * Compares received responses, potentially triggering a digest mismatch (for a digest resolver) and read-repairs
     * (for a data resolver).
     * <p>
     * This is functionally equivalent to calling {@link #resolve()} and consuming the result, but can be slightly more
     * efficient in some case due to the fact that we don't care about the result itself. This is used when doing
     * asynchronous read-repairs.
     *
     * @throws DigestMismatchException if it's a digest resolver and the responses don't match.
     */
    public abstract Completable compareResponses() throws DigestMismatchException;

    public abstract boolean isDataPresent();

    public void preprocess(Response<ReadResponse> message)
    {
        responses.add(message);
    }

    public Iterable<Response<ReadResponse>> getMessages()
    {
        return responses;
    }

    /**
     * Simple utility that returns the iterator corresponding to a single response, for use when no resolving has to
     * happen.
     * <p>
     * The main purpose of this method is to properly call the {@link ReadReconciliationObserver} methods if an observer
     * is set.
     *
     * @param response the response to return.
     * @return the content of {@code response} as a {@code PartitionIterator}.
     */
    protected Flow<FlowableUnfilteredPartition> fromSingleResponse(ReadResponse response)
    {
        Flow<FlowableUnfilteredPartition> result = response.data(command);
        return ctx.readObserver == null
               ? result
               : result.map(p ->
                            {
                                ctx.readObserver.onPartition(p.partitionKey());

                                if (!p.partitionLevelDeletion().isLive())
                                    ctx.readObserver.onPartitionDeletion(p.partitionLevelDeletion(), true);

                                if (!p.staticRow.isEmpty())
                                    ctx.readObserver.onRow(p.staticRow, true);

                                return p.mapContent(u -> {
                                    if (u.isRow())
                                        ctx.readObserver.onRow((Row)u, true);
                                    else
                                        ctx.readObserver.onRangeTombstoneMarker((RangeTombstoneMarker)u, true);
                                    return u;
                                });
                            });
    }

    protected Flow<FlowablePartition> fromSingleResponseFiltered(ReadResponse response)
    {
        return FlowablePartitions.filterAndSkipEmpty(fromSingleResponse(response), command.nowInSec());
    }
}
