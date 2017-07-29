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
package org.apache.cassandra.cql3;

import javax.annotation.Nullable;

import io.reactivex.Scheduler;
import io.reactivex.Single;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.concurrent.TPCUtils.WouldBlockException;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public interface CQLStatement
{
    /**
     * Returns the number of bound terms in this statement.
     */
    public int getBoundTerms();

    /**
     * Perform any access verification necessary for the statement.
     * <p>
     * <b>Important:</b> implementation of this method may have to block under some circumstances (typically, some
     * permissions are not in cache and must be queried). If implementations of this method need to block, they should
     * first check if they are running on a TPC thread, and if that's the case, they must throw {@link WouldBlockException},
     * in which case the access will be checked again on a non-TPC thread (on which it is ok to block). This behavior
     * can be simplified by using the methods in {@link TPCUtils}.
     * <p>
     * TODO: we should probably change this to be explicitly (potentially) asynchronous so it's less error-prone.
     *
     * @param state the current client state
     */
    public void checkAccess(QueryState state) throws UnauthorizedException, InvalidRequestException;

    /**
     * Perform additional validation required by the statement.
     * To be overridden by subclasses if needed.
     * <p>
     * <b>Important:</b> this method is meant for simple validation that should never block (internally it may be
     * executed on TPC threads regardless of the scheduler returned by {@link CQLStatement#getScheduler()}).
     *
     * @param state the current client state
     */
    public void validate(QueryState state) throws RequestValidationException;

    /**
     * Execute the statement and return the resulting result or null if there is no result.
     *  @param state the current query state
     * @param options options for this query (consistency, variables, pageSize, ...)
     * @param queryStartNanoTime the timestamp returned by System.nanoTime() when this statement was received
     */
    public Single<? extends ResultMessage> execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException;

    /**
     * Variant of execute used for internal query against the system tables, and thus only query the local node.
     *
     * @param state the current query state
     */
    public Single<? extends ResultMessage> executeInternal(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException;

    /**
     * Return an Iterable over all of the functions (both native and user-defined) used by any component
     * of the statement
     * @return functions all functions found (may contain duplicates)
     */
    public Iterable<Function> getFunctions();

    /**
     * Return the scheduler that should be used to execute this statement, this includes
     * {@link CQLStatement#checkAccess(QueryState)} and {@link CQLStatement#execute(QueryState, QueryOptions, long)}.
     *
     * If no specific scheduler is required, then return null. If returning null then it must be guaranteed that
     * {@link CQLStatement#execute(QueryState, QueryOptions, long)} doesn't block.
     * {@link CQLStatement#checkAccess(QueryState)} may block only in rare cases, such as security cache misses, but in that
     * case {@link WouldBlockException} must be thrown so that {@link QueryProcessor}
     * may retry the operation on a different scheduler.
     * {@link CQLStatement#validate(QueryState)} should never block as it is not necessarily executed on this scheduler.
     *
     * @return the scheduler for this statement, or null, if no specific scheduler is required because the operations are non blocking.
     */
    @Nullable public Scheduler getScheduler();
}
