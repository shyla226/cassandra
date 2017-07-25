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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.continuous.paging.ContinuousPagingService;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.ExternalRestriction;
import org.apache.cassandra.cql3.restrictions.Restrictions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.ResultBuilder;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.selection.Selectable.WithFunction;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.selection.Selection.Selectors;
import org.apache.cassandra.cql3.selection.Selector;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.monitoring.AbortedOperationException;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.ClientWriteException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.AggregationQueryPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.flow.Flow;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.db.aggregation.AggregationSpecification.aggregatePkPrefixFactory;
import static org.apache.cassandra.db.aggregation.AggregationSpecification.aggregatePkPrefixFactoryWithSelector;
import static org.apache.cassandra.utils.ByteBufferUtil.UNSET_BYTE_BUFFER;

/*
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 * A number of public methods here are only used internally. However,
 * many of these are made accessible for the benefit of custom
 * QueryHandler implementations, so before reducing their accessibility
 * due consideration should be given.
 */
public class SelectStatement implements CQLStatement
{
    private static final Logger logger = LoggerFactory.getLogger(SelectStatement.class);

    private static final int DEFAULT_PAGE_SIZE = 10000;

    private final int boundTerms;
    public final TableMetadata table;
    public final Parameters parameters;
    private final Selection selection;
    private final Term limit;
    private final Term perPartitionLimit;

    private final StatementRestrictions restrictions;

    private final boolean isReversed;

    /**
     * The {@code Factory} used to create the {@code AggregationSpecification}.
     */
    private final AggregationSpecification.Factory aggregationSpecFactory;

    /**
     * The comparator used to orders results when multiple keys are selected (using IN).
     */
    private final Comparator<List<ByteBuffer>> orderingComparator;

    // Used by forSelection below
    private static final Parameters defaultParameters = new Parameters(Collections.emptyMap(),
                                                                       Collections.emptyList(),
                                                                       false,
                                                                       false,
                                                                       false);

    public SelectStatement(TableMetadata table,
                           int boundTerms,
                           Parameters parameters,
                           Selection selection,
                           StatementRestrictions restrictions,
                           boolean isReversed,
                           AggregationSpecification.Factory aggregationSpecFactory,
                           Comparator<List<ByteBuffer>> orderingComparator,
                           Term limit,
                           Term perPartitionLimit)
    {
        this.table = table;
        this.boundTerms = boundTerms;
        this.selection = selection;
        this.restrictions = restrictions;
        this.isReversed = isReversed;
        this.aggregationSpecFactory = aggregationSpecFactory;
        this.orderingComparator = orderingComparator;
        this.parameters = parameters;
        this.limit = limit;
        this.perPartitionLimit = perPartitionLimit;
    }

    /**
     * Adds the specified restrictions to the index restrictions.
     *
     * @param indexRestrictions the index restrictions to add
     * @return a new {@code SelectStatement} instance with the added index restrictions
     */
    public SelectStatement addIndexRestrictions(Restrictions indexRestrictions)
    {
        return new SelectStatement(table,
                                   boundTerms,
                                   parameters,
                                   selection,
                                   restrictions.addIndexRestrictions(indexRestrictions),
                                   isReversed,
                                   aggregationSpecFactory,
                                   orderingComparator,
                                   limit,
                                   perPartitionLimit);
    }

    /**
     * Adds the specified external restrictions to the index restrictions.
     *
     * @param indexRestrictions the index restrictions to add
     * @return a new {@code SelectStatement} instance with the added index restrictions
     */
    public SelectStatement addIndexRestrictions(Iterable<ExternalRestriction> indexRestrictions)
    {
        return new SelectStatement(table,
                                   boundTerms,
                                   parameters,
                                   selection,
                                   restrictions.addExternalRestrictions(indexRestrictions),
                                   isReversed,
                                   aggregationSpecFactory,
                                   orderingComparator,
                                   limit,
                                   perPartitionLimit);
    }

    /**
     * Returns the columns requested by the user
     * @return the columns requested by the user
     */
    public final List<ColumnSpecification> getSelectedColumns()
    {
        return selection.getSelectedColumns();
    }

    public Iterable<Function> getFunctions()
    {
        List<Function> functions = new ArrayList<>();
        addFunctionsTo(functions);
        return functions;
    }

    private void addFunctionsTo(List<Function> functions)
    {
        selection.addFunctionsTo(functions);
        restrictions.addFunctionsTo(functions);

        if (aggregationSpecFactory != null)
            aggregationSpecFactory.addFunctionsTo(functions);

        if (limit != null)
            limit.addFunctionsTo(functions);

        if (perPartitionLimit != null)
            perPartitionLimit.addFunctionsTo(functions);
    }

    /**
     * The columns to fetch internally for this SELECT statement (which can be more than the one selected by the
     * user as it also include any restricted column in particular).
     */
    public ColumnFilter queriedColumns()
    {
        return selection.newSelectors(QueryOptions.DEFAULT).getColumnFilter();
    }

    // Creates a simple select based on the given selection.
    // Note that the results select statement should not be used for actual queries, but only for processing already
    // queried data through processColumnFamily.
    static SelectStatement forSelection(TableMetadata table, Selection selection)
    {
        return new SelectStatement(table,
                                   0,
                                   defaultParameters,
                                   selection,
                                   StatementRestrictions.empty(StatementType.SELECT, table),
                                   false,
                                   null,
                                   null,
                                   null,
                                   null);
    }

    public ResultSet.ResultMetadata getResultMetadata()
    {
        return selection.getResultMetadata();
    }

    public int getBoundTerms()
    {
        return boundTerms;
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        if (table.isView())
        {
            TableMetadataRef baseTable = View.findBaseTable(keyspace(), columnFamily());
            if (baseTable != null)
                state.hasColumnFamilyAccess(baseTable, CorePermission.SELECT);
        }
        else
        {
            state.hasColumnFamilyAccess(table, CorePermission.SELECT);
        }

        for (Function function : getFunctions())
            state.ensureHasPermission(CorePermission.EXECUTE, function);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        // Nothing to do, all validation has been done by RawStatement.prepare()
    }

    public Single<ResultMessage.Rows> execute(QueryState state, QueryOptions options, long queryStartNanoTime)
    {
        ConsistencyLevel cl = options.getConsistency();
        checkNotNull(cl, "Invalid empty consistency level");
        cl.validateForRead(keyspace());

        if (options.continuousPagesRequested())
        {
            checkNotNull(state.getConnection(), "Continuous paging should only be used for external queries");
            checkFalse(cl.isSerialConsistency(), "Continuous paging does not support serial reads");
            return executeContinuous(state, options, FBUtilities.nowInSeconds(), cl, queryStartNanoTime);
        }

        return execute(state, options, FBUtilities.nowInSeconds(), cl, queryStartNanoTime);
    }

    public Scheduler getScheduler()
    {
        return null;
    }

    /**
     * Return the page size to be used for a query. If async paging is requested, then it overrides
     * the legacy page size set in the options. Note that if the page unit is bytes, then the estimated
     * page size in rows is only used when sending queries to replicas, it is not the page size returned
     * to the user, which is always controlled by
     * {@link ContinuousPagingService.PageBuilder} when async
     * paging is used.
     *
     * @param options - the query options
     *
     * @return the page size requested by the user or an estimate if paging in bytes
     */
    private int getPageSize(QueryOptions options)
    {
        QueryOptions.PagingOptions pagingOptions = options.getPagingOptions();
        if (pagingOptions == null)
            return -1;

        PageSize size = pagingOptions.pageSize();

        // We know the size can only be in rows currently if continuous paging
        // is not used, so don't bother computing the average row size.
        return pagingOptions.isContinuous()
             ? size.inEstimatedRows(ResultSet.estimatedRowSize(table, selection.getColumnMapping()))
             : size.inRows();
    }

    public AggregationSpecification getAggregationSpec(QueryOptions options)
    {
        return aggregationSpecFactory == null ? null : aggregationSpecFactory.newInstance(options);
    }

    public ReadQuery getQuery(QueryState queryState, QueryOptions options, int nowInSec) throws RequestValidationException
    {
        Selectors selectors = selection.newSelectors(options);
        DataLimits limit = getDataLimits(getLimit(options),
                                         getPerPartitionLimit(options),
                                         getPageSize(options),
                                         getAggregationSpec(options));

        return getQuery(queryState, options, selectors.getColumnFilter(), nowInSec, limit);
    }

    public ReadQuery getQuery(QueryState queryState,
                              QueryOptions options,
                              ColumnFilter columnFilter,
                              int nowInSec,
                              DataLimits limit)
    {
        boolean isPartitionRangeQuery = restrictions.isKeyRange() || restrictions.usesSecondaryIndexing();

        if (isPartitionRangeQuery)
            return getRangeCommand(queryState, options, columnFilter, limit, nowInSec);

        return getSliceCommands(options, columnFilter, limit, nowInSec);
    }

    private Single<ResultMessage.Rows> execute(ReadQuery query,
                                               QueryOptions options,
                                               QueryState state,
                                               Selectors selectors,
                                               int nowInSec,
                                               int userLimit,
                                               long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
    {
        Flow<FlowablePartition> data = query.execute(options.getConsistency(), state.getClientState(), queryStartNanoTime, false);
        return processResults(data, options, selectors, nowInSec, userLimit, null);
    }

    /**
     * A wrapper class for interfacing to the real pagers. Because we need to invoke a different method
     * depending on the query execution type (internal, local or distributed), this abstraction saves
     * code duplication.
     */
    private static abstract class Pager
    {
        protected QueryPager pager;

        protected Pager(QueryPager pager)
        {
            this.pager = pager;
        }

        public static Pager forInternalQuery(QueryPager pager)
        {
            return new InternalPager(pager);
        }

        public static Pager forNormalQuery(QueryPager pager, ConsistencyLevel consistency, ClientState clientState, boolean forContinuousPaging)
        {
            return new NormalPager(pager, consistency, clientState, forContinuousPaging);
        }

        public boolean isExhausted()
        {
            return pager.isExhausted();
        }

        public PagingState state(boolean inclusive)
        {
            return pager.state(inclusive);
        }

        public int maxRemaining()
        {
            return pager.maxRemaining();
        }

        public abstract Flow<FlowablePartition> fetchPage(int pageSize, long queryStartNanoTime);

        /**
         * The pager for ordinary queries.
         */
        public static class NormalPager extends Pager
        {
            private final ConsistencyLevel consistency;
            private final ClientState clientState;
            private final boolean forContinuousPaging;

            private NormalPager(QueryPager pager, ConsistencyLevel consistency, ClientState clientState, boolean forContinuousPaging)
            {
                super(pager);
                this.consistency = consistency;
                this.clientState = clientState;
                this.forContinuousPaging = forContinuousPaging;
            }

            public Flow<FlowablePartition> fetchPage(int pageSize, long queryStartNanoTime)
            {
                return pager.fetchPage(pageSize, consistency, clientState, queryStartNanoTime, forContinuousPaging);
            }
        }

        /**
         * A pager for internal queries.
         */
        public static class InternalPager extends Pager
        {
            private InternalPager(QueryPager pager)
            {
                super(pager);
            }

            public Flow<FlowablePartition> fetchPage(int pageSize, long queryStartNanoTime)
            {
                return pager.fetchPageInternal(pageSize);
            }
        }
    }

    private Single<ResultMessage.Rows> execute(Pager pager,
                                               QueryOptions options,
                                               Selectors selectors,
                                               int pageSize,
                                               int nowInSec,
                                               int userLimit,
                                               AggregationSpecification aggregationSpec,
                                               long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
    {
        if (aggregationSpecFactory != null)
        {
            if (!restrictions.hasPartitionKeyRestrictions())
            {
                warn("Aggregation query used without partition key");
            }
            else if (restrictions.keyIsInRelation())
            {
                warn("Aggregation query used on multiple partition keys (IN restriction)");
            }
        }

        // We can't properly do post-query ordering if we page (see #6722)
        // For GROUP BY or aggregation queries we always page internally even if the user has turned paging off
        checkFalse(pageSize > 0 && needsPostQueryOrdering(),
                   "Cannot page queries with both ORDER BY and a IN restriction on the partition key;"
                   + " you must either remove the ORDER BY or the IN and sort client side, or disable paging for this query");

        final Single<ResultMessage.Rows> msg;
        final Flow<FlowablePartition> page = pager.fetchPage(pageSize, queryStartNanoTime);

        // Please note that the isExhausted state of the pager only gets updated when we've closed the page, so this
        // shouldn't be moved inside the 'try' above.
        msg = processResults(page, options, selectors, nowInSec, userLimit, aggregationSpec).map(r -> {
            if (!pager.isExhausted())
                r.result.metadata.setPagingResult(new PagingResult(pager.state(false)));

            return r;
        });

        return msg;
    }

    private void warn(String msg)
    {
        logger.warn(msg);
        ClientWarn.instance.warn(msg);
    }

    private Single<ResultMessage.Rows> processResults(Flow<FlowablePartition> partitions,
                                                      QueryOptions options,
                                                      Selectors selectors,
                                                      int nowInSec,
                                                      int userLimit,
                                                      AggregationSpecification aggregationSpec) throws RequestValidationException
    {
        return process(partitions, options, selectors, nowInSec, userLimit, aggregationSpec).map(ResultMessage.Rows::new);
    }

    public Single<ResultMessage.Rows> executeInternal(QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        return executeInternal(state, options, FBUtilities.nowInSeconds(), System.nanoTime());
    }

    public Single<ResultMessage.Rows> executeInternal(QueryState state, QueryOptions options, int nowInSec, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
    {
        Selectors selectors = selection.newSelectors(options);
        AggregationSpecification aggregationSpec = getAggregationSpec(options);

        int userLimit = getLimit(options);
        int userPerPartitionLimit = getPerPartitionLimit(options);
        int pageSize = getPageSize(options);
        DataLimits limit = getDataLimits(userLimit, userPerPartitionLimit, pageSize, aggregationSpec);

        ReadQuery query = getQuery(state, options, selectors.getColumnFilter(), nowInSec, limit);

        if (aggregationSpec == null && (pageSize <= 0 || (query.limits().count() <= pageSize)))
            return processResults(query.executeInternal(), options, selectors, nowInSec, userLimit, null);

        QueryPager pager = getPager(query, options);
        return execute(Pager.forInternalQuery(pager),
                       options,
                       selectors,
                       pageSize,
                       nowInSec,
                       userLimit,
                       aggregationSpec,
                       queryStartNanoTime);
    }

    /**
     * Execute the query synchronously, typically we only retrieve one page.
     * @param state - the query state
     * @param options - the query options
     * @param queryStartNanoTime - the timestamp returned by System.nanoTime() when this statement was received
     * @return - a message containing the result rows
     * @throws RequestExecutionException
     * @throws RequestValidationException
     */
    private Single<ResultMessage.Rows> execute(QueryState state, QueryOptions options, int nowInSec, ConsistencyLevel cl, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        Selectors selectors = selection.newSelectors(options);
        AggregationSpecification aggregationSpec = getAggregationSpec(options);

        int userLimit = getLimit(options);
        int userPerPartitionLimit = getPerPartitionLimit(options);
        int pageSize = getPageSize(options);
        DataLimits limit = getDataLimits(userLimit, userPerPartitionLimit, pageSize, aggregationSpec);

        ReadQuery query = getQuery(state, options, selectors.getColumnFilter(), nowInSec, limit);

        if (aggregationSpec == null && (pageSize <= 0 || (query.limits().count() <= pageSize)))
            return execute(query, options, state, selectors, nowInSec, userLimit, queryStartNanoTime);

        QueryPager pager = getPager(query, options);

        return execute(Pager.forNormalQuery(pager, cl, state.getClientState(), false),
                       options,
                       selectors,
                       pageSize,
                       nowInSec,
                       userLimit,
                       aggregationSpec,
                       queryStartNanoTime);
    }

    /**
     * Execute the query by pushing multiple pages to the client continuously, as soon as they become available.
     *
     * @param state - the query state
     * @param options - the query options
     * @param queryStartNanoTime - the timestamp returned by System.nanoTime() when this statement was received
     * @return - a void message, the results will be sent asynchronously by the async paging service
     * @throws RequestExecutionException
     * @throws RequestValidationException
     */
    private Single<ResultMessage.Rows> executeContinuous(QueryState state, QueryOptions options, int nowInSec, ConsistencyLevel cl, long queryStartNanoTime)
    throws RequestValidationException, RequestExecutionException
    {
        ContinuousPagingService.metrics.requests.mark();

        checkFalse(needsPostQueryOrdering(),
                   "Cannot page queries with both ORDER BY and a IN restriction on the partition key;"
                   + " you must either remove the ORDER BY or the IN and sort client side, or avoid async paging for this query");

        Selectors selectors = selection.newSelectors(options);
        AggregationSpecification aggregationSpec = getAggregationSpec(options);

        int userLimit = getLimit(options);
        int userPerPartitionLimit = getPerPartitionLimit(options);
        int pageSize = getPageSize(options);
        DataLimits limit = getDataLimits(userLimit, userPerPartitionLimit, pageSize, aggregationSpec);

        ReadQuery query = getQuery(state, options, selectors.getColumnFilter(), nowInSec, limit);
        ContinuousPagingExecutor executor = new ContinuousPagingExecutor(this, options, state, cl, query, queryStartNanoTime, pageSize);
        ResultBuilder builder = ContinuousPagingService.makeBuilder(this, executor, state, options, DatabaseDescriptor.getContinuousPaging());

        executor.schedule(options.getPagingOptions().state(), builder);
        return Single.just(new ResultMessage.Rows(new ResultSet(getResultMetadata(), Collections.emptyList()), false));
    }

    /**
     * A class for executing queries with continuous paging.
     */
    public final static class ContinuousPagingExecutor
    {
        final SelectStatement statement;
        final QueryOptions options;
        final QueryState state;
        final ConsistencyLevel consistencyLevel;
        final int pageSize;
        final ReadQuery query;
        final boolean isLocalQuery;
        final long queryStartNanoTime;

        // Not final because it is recreated every time we reschedule
        Pager pager;

        // Not final because it is updated every time we schedule a task
        long schedulingTimeNano;

        private ContinuousPagingExecutor(SelectStatement statement,
                                         QueryOptions options,
                                         QueryState state,
                                         ConsistencyLevel consistencyLevel,
                                         ReadQuery query,
                                         long queryStartNanoTime,
                                         int pageSize)
        {
            this.statement = statement;
            this.options = options;
            this.state = state;
            this.consistencyLevel = consistencyLevel;
            this.pageSize = pageSize;
            this.query = query;
            this.isLocalQuery = consistencyLevel.isSingleNode() && query.queriesOnlyLocalData();
            this.queryStartNanoTime = queryStartNanoTime;
        }

        public PagingState state(boolean inclusive)
        {
            return pager == null || pager.isExhausted() ? null : pager.state(inclusive);
        }

        public void retrieveMultiplePages(PagingState pagingState, ResultBuilder builder)
        {
            // update the metrics with how long we were waiting since scheduling this task
            ContinuousPagingService.metrics.waitingTime.addNano(System.nanoTime() - schedulingTimeNano);

            if (logger.isTraceEnabled())
                logger.trace("{} - retrieving multiple pages with paging state {}",
                             statement.table, pagingState);

            assert pager == null;
            assert !builder.isCompleted();

            pager = Pager.forNormalQuery(statement.getPager(query, pagingState, options.getProtocolVersion()),
                                         consistencyLevel,
                                         state.getClientState(),
                                         true);


            // non-local queries span only one page at a time in SP, and each page is monitored starting from
            // queryStartNanoTime and will fail once RPC read timeout has elapsed, so we must use System.nanoTime()
            // instead of queryStartNanoTime for distributed queries
            // local queries, on the other hand, are not monitored against the RPC timeout and we want to record
            // the entire query duration in the metrics, so we should not reset queryStartNanoTime, further we
            // should query all available data, not just page size rows
            int pageSize = isLocalQuery ? pager.maxRemaining() : this.pageSize;
            long queryStart = isLocalQuery ? queryStartNanoTime : System.nanoTime();

            Flow<FlowablePartition> page = pager.fetchPage(pageSize, queryStart);

            page.takeUntil(builder::isCompleted)
                .flatMap(partition -> statement.processPartition(partition, options, builder, query.nowInSec()))
                .reduceToFuture(builder, (b, v) -> b)
                .whenComplete((bldr, error) ->
                        {
                            if (error == null)
                                maybeReschedule(bldr);
                            else
                                handleError(error, builder); // bldr is null!
                        });
        }

        private void handleError(Throwable error, ResultBuilder builder)
        {
            if (error instanceof AbortedOperationException)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Continuous paging aborted, rescheduling? {}", !builder.isCompleted());

                // An aborted exception will only reach here in the case of local queries (otherwise it stays inside
                // MessagingService) and it means we should re-schedule the query (we use the monitor to ensure we
                // don't hold OpOrder for too long).
                if (!builder.isCompleted())
                {
                    schedule(pager.state(false), builder);
                    return;
                }

            }
            else if (error instanceof ClientWriteException)
            {
                logger.debug("Continuous paging client did not keep up: {}", error.getMessage());
                builder.complete(error);
            }
            else
            {
                JVMStabilityInspector.inspectThrowable(error);
                logger.error("Continuous paging failed with unexpected error: {}", error.getMessage(), error);

                builder.complete(error);
            }

            ContinuousPagingService.metrics.addTotalDuration(isLocalQuery, System.nanoTime() - queryStartNanoTime);
        }

        /**
         * This method is called when the iteration in retrieveMultiplePages() terminates.
         *
         * If the pager is exhausted, then we've run out of data, in this case we check
         * if we need to complete the builder and then we are done.
         *
         * Otherwise, if there is still data, either we're in the distributed case and have read a full page of data
         * or the builder has interrupted iteration (continuous paging was cancelled or has reached the maximum
         * number of pages). In the first case, builder not completed, we reschedule, in the second case we're done.
         *
         * @param builder - the result builder
         */
        void maybeReschedule(ResultBuilder builder)
        {
            assert pager != null;

            if (pager.isExhausted())
            {
                builder.complete();
                ContinuousPagingService.metrics.addTotalDuration(isLocalQuery, System.nanoTime() - queryStartNanoTime);
            }
            else
            {
                if (!builder.isCompleted())
                    schedule(pager.state(false), builder);
                else
                    ContinuousPagingService.metrics.addTotalDuration(isLocalQuery, System.nanoTime() - queryStartNanoTime);
            }
        }

        private void schedule(PagingState pagingState, ResultBuilder builder)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - scheduling retrieving of multiple pages with paging state {}",
                             statement.table, pagingState);

            // the pager will be recreated when retrieveMultiplePages executes, set it to null because in the local
            // case the pager depends on the execution controller, which will be released when this method returns
            pager = null;

            schedulingTimeNano = System.nanoTime();
            StageManager.getStage(Stage.CONTINUOUS_PAGING).submit(() -> retrieveMultiplePages(pagingState, builder));
        }
    }

    private QueryPager getPager(ReadQuery query, QueryOptions options)
    {
        PagingState pagingState = options.getPagingOptions() == null
                                  ? null
                                  : options.getPagingOptions().state();
        return getPager(query, pagingState, options.getProtocolVersion());
    }

    private QueryPager getPager(ReadQuery query, PagingState pagingState, ProtocolVersion protocolVersion)
    {
        QueryPager pager = query.getPager(pagingState, protocolVersion);

        if (aggregationSpecFactory == null || query.isEmpty())
            return pager;

        return new AggregationQueryPager(pager, query.limits());
    }

    /**
     * Convert the iterator into a {@link ResultSet}. The iterator will be closed by this method.
     *
     * @param partitions - the partitions iterator
     * @param nowInSec - the current time in seconds
     *
     * @return - a result set with the iterator results
     */
    public ResultSet process(PartitionIterator partitions, int nowInSec)
    {
        return process(FlowablePartitions.fromPartitions(partitions, Schedulers.io()),
                       nowInSec).blockingGet();
    }

    /**
     * Convert the partitions into a {@link ResultSet}.
     *
     * @param partitions - the partitions flow
     * @param nowInSec - the current time in seconds
     *
     * @return - a single of a result set with the results
     */
    public Single<ResultSet> process(Flow<FlowablePartition> partitions, int nowInSec)
    {
        return process(partitions,
                       QueryOptions.DEFAULT,
                       selection.newSelectors(QueryOptions.DEFAULT),
                       nowInSec,
                       getLimit(QueryOptions.DEFAULT),
                       getAggregationSpec(QueryOptions.DEFAULT));
    }

    public String keyspace()
    {
        return table.keyspace;
    }

    public String columnFamily()
    {
        return table.name;
    }

    /**
     * May be used by custom QueryHandler implementations
     */
    public Selection getSelection()
    {
        return selection;
    }

    /**
     * May be used by custom QueryHandler implementations
     */
    public StatementRestrictions getRestrictions()
    {
        return restrictions;
    }

    private ReadQuery getSliceCommands(QueryOptions options, ColumnFilter columnFilter, DataLimits limit, int nowInSec)
    {
        Collection<ByteBuffer> keys = restrictions.getPartitionKeys(options);
        if (keys.isEmpty())
            return new ReadQuery.EmptyQuery(table);

        ClusteringIndexFilter filter = makeClusteringIndexFilter(options, columnFilter);
        if (filter == null)
            return new ReadQuery.EmptyQuery(table);

        RowFilter rowFilter = getRowFilter(options);

        // Note that we use the total limit for every key, which is potentially inefficient.
        // However, IN + LIMIT is not a very sensible choice.
        List<SinglePartitionReadCommand> commands = new ArrayList<>(keys.size());
        for (ByteBuffer key : keys)
        {
            QueryProcessor.validateKey(key);
            DecoratedKey dk = table.partitioner.decorateKey(ByteBufferUtil.clone(key));
            commands.add(SinglePartitionReadCommand.create(table, nowInSec, columnFilter, rowFilter, limit, dk, filter));
        }

        return new SinglePartitionReadCommand.Group(commands, limit);
    }

    /**
     * Returns the slices fetched by this SELECT, assuming an internal call (no bound values in particular).
     * <p>
     * Note that if the SELECT intrinsically selects rows by names, we convert them into equivalent slices for
     * the purpose of this method. This is used for MVs to restrict what needs to be read when we want to read
     * everything that could be affected by a given view (and so, if the view SELECT statement has restrictions
     * on the clustering columns, we can restrict what we read).
     */
    public Slices clusteringIndexFilterAsSlices()
    {
        QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
        ColumnFilter columnFilter = selection.newSelectors(options).getColumnFilter();
        ClusteringIndexFilter filter = makeClusteringIndexFilter(options, columnFilter);
        if (filter instanceof ClusteringIndexSliceFilter)
            return ((ClusteringIndexSliceFilter)filter).requestedSlices();

        Slices.Builder builder = new Slices.Builder(table.comparator);
        for (Clustering clustering: ((ClusteringIndexNamesFilter)filter).requestedRows())
            builder.add(Slice.make(clustering));
        return builder.build();
    }

    /**
     * Returns a read command that can be used internally to query all the rows queried by this SELECT for a
     * give key (used for materialized views).
     */
    public SinglePartitionReadCommand internalReadForView(DecoratedKey key, int nowInSec)
    {
        QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
        ColumnFilter columnFilter = selection.newSelectors(options).getColumnFilter();
        ClusteringIndexFilter filter = makeClusteringIndexFilter(options, columnFilter);
        RowFilter rowFilter = getRowFilter(options);
        return SinglePartitionReadCommand.create(table, nowInSec, columnFilter, rowFilter, DataLimits.NONE, key, filter);
    }

    /**
     * The {@code RowFilter} for this SELECT, assuming an internal call (no bound values in particular).
     */
    public RowFilter rowFilterForInternalCalls()
    {
        return getRowFilter(QueryOptions.forInternalCalls(Collections.emptyList()));
    }

    private ReadQuery getRangeCommand(QueryState queryState,
                                      QueryOptions options,
                                      ColumnFilter columnFilter,
                                      DataLimits limit,
                                      int nowInSec)
    {
        ClusteringIndexFilter clusteringIndexFilter = makeClusteringIndexFilter(options, columnFilter);
        if (clusteringIndexFilter == null)
            return new ReadQuery.EmptyQuery(table);

        RowFilter rowFilter = getRowFilter(options);

        // The LIMIT provided by the user is the number of CQL row he wants returned.
        // We want to have getRangeSlice to count the number of columns, not the number of keys.
        AbstractBounds<PartitionPosition> keyBounds = restrictions.getPartitionKeyBounds(options);
        if (keyBounds == null)
            return new ReadQuery.EmptyQuery(table);

        PartitionRangeReadCommand command = new PartitionRangeReadCommand(table,
                                                                          nowInSec,
                                                                          columnFilter,
                                                                          rowFilter,
                                                                          limit,
                                                                          new DataRange(keyBounds, clusteringIndexFilter),
                                                                          Optional.empty());
        // If there's a secondary index that the command can use, have it validate
        // the request parameters. Note that as a side effect, if a viable Index is
        // identified by the CFS's index manager, it will be cached in the command
        // and serialized during distribution to replicas in order to avoid performing
        // further lookups.
        command.maybeValidateIndex();

        ClientState clientState = queryState.getClientState();
        // Warn about SASI usage.
        if (!clientState.isInternal && !clientState.isSASIWarningIssued() &&
            command.getIndex(Keyspace.open(table.keyspace).getColumnFamilyStore(table.name)) instanceof SASIIndex)
        {
            warn(String.format(SASIIndex.USAGE_WARNING, table.keyspace, table.name));
            clientState.setSASIWarningIssued();
        }

        return command;
    }

    private ClusteringIndexFilter makeClusteringIndexFilter(QueryOptions options, ColumnFilter columnFilter)
    {
        if (parameters.isDistinct)
        {
            // We need to be able to distinguish between partition having live rows and those that don't. But
            // doing so is not trivial since "having a live row" depends potentially on
            //   1) when the query is performed, due to TTLs
            //   2) how thing reconcile together between different nodes
            // so that it's hard to really optimize properly internally. So to keep it simple, we simply query
            // for the first row of the partition and hence uses Slices.ALL. We'll limit it to the first live
            // row however in getLimit().
            return new ClusteringIndexSliceFilter(Slices.ALL, false);
        }

        if (restrictions.isColumnRange())
        {
            Slices slices = makeSlices(options);
            if (slices == Slices.NONE && !selection.containsStaticColumns())
                return null;

            return new ClusteringIndexSliceFilter(slices, isReversed);
        }

        NavigableSet<Clustering> clusterings = getRequestedRows(options);
        // We can have no clusterings if either we're only selecting the static columns, or if we have
        // a 'IN ()' for clusterings. In that case, we still want to query if some static columns are
        // queried. But we're fine otherwise.
        if (clusterings.isEmpty() && columnFilter.fetchedColumns().statics.isEmpty())
            return null;

        return new ClusteringIndexNamesFilter(clusterings, isReversed);
    }

    private Slices makeSlices(QueryOptions options)
    throws InvalidRequestException
    {
        SortedSet<ClusteringBound> startBounds = restrictions.getClusteringColumnsBounds(Bound.START, options);
        SortedSet<ClusteringBound> endBounds = restrictions.getClusteringColumnsBounds(Bound.END, options);
        assert startBounds.size() == endBounds.size();

        // The case where startBounds == 1 is common enough that it's worth optimizing
        if (startBounds.size() == 1)
        {
            ClusteringBound start = startBounds.first();
            ClusteringBound end = endBounds.first();
            return table.comparator.compare(start, end) > 0
                 ? Slices.NONE
                 : Slices.with(table.comparator, Slice.make(start, end));
        }

        Slices.Builder builder = new Slices.Builder(table.comparator, startBounds.size());
        Iterator<ClusteringBound> startIter = startBounds.iterator();
        Iterator<ClusteringBound> endIter = endBounds.iterator();
        while (startIter.hasNext() && endIter.hasNext())
        {
            ClusteringBound start = startIter.next();
            ClusteringBound end = endIter.next();

            // Ignore slices that are nonsensical
            if (table.comparator.compare(start, end) > 0)
                continue;

            builder.add(start, end);
        }

        return builder.build();
    }

    private DataLimits getDataLimits(int userLimit,
                                     int perPartitionLimit,
                                     int pageSize,
                                     AggregationSpecification aggregationSpec)
    {
        int cqlRowLimit = DataLimits.NO_LIMIT;
        int cqlPerPartitionLimit = DataLimits.NO_LIMIT;

        // If we do post ordering we need to get all the results sorted before we can trim them.
        if (aggregationSpec != AggregationSpecification.AGGREGATE_EVERYTHING)
        {
            if (!needsPostQueryOrdering())
                cqlRowLimit = userLimit;
            cqlPerPartitionLimit = perPartitionLimit;
        }

        // Group by and aggregation queries will always be paged internally to avoid OOM.
        // If the user provided a pageSize we'll use that to page internally (because why not), otherwise we use our default
        if (pageSize <= 0)
            pageSize = DEFAULT_PAGE_SIZE;

        // Aggregation queries work fine on top of the group by paging but to maintain
        // backward compatibility we need to use the old way.
        if (aggregationSpec != null && aggregationSpec != AggregationSpecification.AGGREGATE_EVERYTHING)
        {
            if (parameters.isDistinct)
                return DataLimits.distinctLimits(cqlRowLimit);

            return DataLimits.groupByLimits(cqlRowLimit,
                                            cqlPerPartitionLimit,
                                            pageSize,
                                            aggregationSpec);
        }

        if (parameters.isDistinct)
            return cqlRowLimit == DataLimits.NO_LIMIT ? DataLimits.DISTINCT_NONE : DataLimits.distinctLimits(cqlRowLimit);

        return DataLimits.cqlLimits(cqlRowLimit, cqlPerPartitionLimit);
    }

    /**
     * Returns the limit specified by the user.
     * May be used by custom QueryHandler implementations
     *
     * @return the limit specified by the user or <code>DataLimits.NO_LIMIT</code> if no value
     * as been specified.
     */
    public int getLimit(QueryOptions options)
    {
        return getLimit(limit, options);
    }

    /**
     * Returns the per partition limit specified by the user.
     * May be used by custom QueryHandler implementations
     *
     * @return the per partition limit specified by the user or <code>DataLimits.NO_LIMIT</code> if no value
     * as been specified.
     */
    public int getPerPartitionLimit(QueryOptions options)
    {
        return getLimit(perPartitionLimit, options);
    }

    private int getLimit(Term limit, QueryOptions options)
    {
        int userLimit = DataLimits.NO_LIMIT;

        if (limit != null)
        {
            ByteBuffer b = checkNotNull(limit.bindAndGet(options), "Invalid null value of limit");
            // treat UNSET limit value as 'unlimited'
            if (b != UNSET_BYTE_BUFFER)
            {
                try
                {
                    Int32Type.instance.validate(b);
                    userLimit = Int32Type.instance.compose(b);
                    checkTrue(userLimit > 0, "LIMIT must be strictly positive");
                }
                catch (MarshalException e)
                {
                    throw new InvalidRequestException("Invalid limit value");
                }
            }
        }
        return userLimit;
    }

    private NavigableSet<Clustering> getRequestedRows(QueryOptions options) throws InvalidRequestException
    {
        // Note: getRequestedColumns don't handle static columns, but due to CASSANDRA-5762
        // we always do a slice for CQL3 tables, so it's ok to ignore them here
        assert !restrictions.isColumnRange();
        return restrictions.getClusteringColumns(options);
    }

    /**
     * May be used by custom QueryHandler implementations
     */
    public RowFilter getRowFilter(QueryOptions options) throws InvalidRequestException
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(columnFamily());
        SecondaryIndexManager secondaryIndexManager = cfs.indexManager;
        RowFilter filter = restrictions.getRowFilter(secondaryIndexManager, options);
        return filter;
    }

    private Single<ResultSet> process(Flow<FlowablePartition> partitions,
                                      QueryOptions options,
                                      Selectors selectors,
                                      int nowInSec,
                                      int userLimit,
                                      AggregationSpecification aggregationSpec)
    {
        ResultSet.Builder result = ResultSet.makeBuilder(getResultMetadata(), selectors, aggregationSpec);
        return partitions.flatProcess(partition -> processPartition(partition, options, result, nowInSec))
                         .mapToRxSingle(VOID -> postQueryProcessing(result, userLimit));
    }

    private ResultSet postQueryProcessing(ResultSet.Builder result, int userLimit)
    {
        ResultSet cqlRows = result.build();

        orderResults(cqlRows);

        cqlRows.trim(userLimit);

        return cqlRows;
    }

    public static ByteBuffer[] getComponents(TableMetadata metadata, DecoratedKey dk)
    {
        ByteBuffer key = dk.getKey();
        if (metadata.partitionKeyType instanceof CompositeType)
            return ((CompositeType)metadata.partitionKeyType).split(key);

        return new ByteBuffer[]{ key };
    }

    // Determines whether, when we have a partition result with not rows, we still return the static content (as a
    // result set row with null for all other regular columns.)
    private boolean returnStaticContentOnPartitionWithNoRows()
    {
        // The general rational is that if some rows are specifically selected by the query (have clustering or
        // regular columns restrictions), we ignore partitions that are empty outside of static content, but if it's a full partition
        // query, then we include that content.
        // We make an exception for "static compact" table are from a CQL standpoint we always want to show their static
        // content for backward compatibility.
        return queriesFullPartitions() || table.isStaticCompactTable();
    }

    // Used by ModificationStatement for CAS operations
    <T extends ResultBuilder> Flow<T> processPartition(FlowablePartition partition, QueryOptions options, T result, int nowInSec)
    throws InvalidRequestException
    {
        final ProtocolVersion protocolVersion = options.getProtocolVersion();
        final ByteBuffer[] keyComponents = getComponents(table, partition.header.partitionKey);

        return partition.content
               .takeUntil(result::isCompleted)
               .reduce(false, (hasContent, row) -> {
                    result.newRow(partition.header.partitionKey, row.clustering());
                    // Respect selection order
                    for (ColumnMetadata def : selection.getColumns())
                    {
                        switch (def.kind)
                        {
                            case PARTITION_KEY:
                                result.add(keyComponents[def.position()]);
                                break;
                            case CLUSTERING:
                                result.add(row.clustering().get(def.position()));
                                break;
                            case REGULAR:
                                addValue(result, def, row, nowInSec, protocolVersion);
                                break;
                            case STATIC:
                                addValue(result, def, partition.staticRow, nowInSec, protocolVersion);
                                break;
                        }
                    }
                    return true; // return true if any rows were included
               })
               .map((hasContent) -> {
                    if (!hasContent && !result.isCompleted() && !partition.staticRow.isEmpty() && returnStaticContentOnPartitionWithNoRows())
                    { //if there were no rows but there is a static row then process it
                        result.newRow(partition.header.partitionKey, partition.staticRow.clustering());
                        for (ColumnMetadata def : selection.getColumns())
                        {
                            switch (def.kind)
                            {
                                case PARTITION_KEY:
                                    result.add(keyComponents[def.position()]);
                                    break;
                                case STATIC:
                                    addValue(result, def, partition.staticRow, nowInSec, protocolVersion);
                                    break;
                                default:
                                    result.add(null);
                            }
                        }
                    }

                    return result;
        });
    }

    /**
     * Checks if the query is a full partitions selection.
     * @return {@code true} if the query is a full partitions selection, {@code false} otherwise.
     */
    private boolean queriesFullPartitions()
    {
        return !restrictions.hasClusteringColumnsRestrictions() && !restrictions.hasRegularColumnsRestrictions();
    }

    private static void addValue(ResultBuilder result, ColumnMetadata def, Row row, int nowInSec, ProtocolVersion protocolVersion)
    {
        if (def.isComplex())
        {
            assert def.type.isMultiCell();
            ComplexColumnData complexData = row.getComplexColumnData(def);
            if (complexData == null)
                result.add(null);
            else if (def.type.isCollection())
                result.add(((CollectionType<?>) def.type).serializeForNativeProtocol(complexData.iterator(), protocolVersion));
            else
                result.add(((UserType) def.type).serializeForNativeProtocol(complexData.iterator(), protocolVersion));
        }
        else
        {
            result.add(row.getCell(def), nowInSec);
        }
    }

    private boolean needsPostQueryOrdering()
    {
        // We need post-query ordering only for queries with IN on the partition key and an ORDER BY.
        return restrictions.keyIsInRelation() && !parameters.orderings.isEmpty();
    }

    /**
     * Orders results when multiple keys are selected (using IN)
     */
    private void orderResults(ResultSet cqlRows)
    {
        if (cqlRows.size() == 0 || !needsPostQueryOrdering())
            return;

        Collections.sort(cqlRows.rows, orderingComparator);
    }

    public static class RawStatement extends CFStatement
    {
        public final Parameters parameters;
        public final List<RawSelector> selectClause;
        public final WhereClause whereClause;
        public final Term.Raw limit;
        public final Term.Raw perPartitionLimit;

        public RawStatement(CFName cfName, Parameters parameters,
                            List<RawSelector> selectClause,
                            WhereClause whereClause,
                            Term.Raw limit,
                            Term.Raw perPartitionLimit)
        {
            super(cfName);
            this.parameters = parameters;
            this.selectClause = selectClause;
            this.whereClause = whereClause;
            this.limit = limit;
            this.perPartitionLimit = perPartitionLimit;
        }

        public ParsedStatement.Prepared prepare() throws InvalidRequestException
        {
            return prepare(false);
        }

        public ParsedStatement.Prepared prepare(boolean forView) throws InvalidRequestException
        {
            TableMetadata table = Schema.instance.validateTable(keyspace(), columnFamily());
            VariableSpecifications boundNames = getBoundVariables();

            List<Selectable> selectables = RawSelector.toSelectables(selectClause, table);
            boolean containsOnlyStaticColumns = selectOnlyStaticColumns(table, selectables);

            StatementRestrictions restrictions = prepareRestrictions(table, boundNames, containsOnlyStaticColumns, forView);

            // If we order post-query, the sorted column needs to be in the ResultSet for sorting,
            // even if we don't ultimately ship them to the client (CASSANDRA-4911).
            Map<ColumnMetadata, Boolean> orderingColumns = getOrderingColumns(table);
            Set<ColumnMetadata> resultSetOrderingColumns = restrictions.keyIsInRelation() ? orderingColumns.keySet()
                                                                                          : Collections.emptySet();

            Selection selection = selectables.isEmpty()
                    ? Selection.wildcard(table, parameters.isJson)
                    : Selection.fromSelectors(table,
                                              selectables,
                                              boundNames,
                                              resultSetOrderingColumns,
                                              restrictions.nonPKRestrictedColumns(false),
                                              !parameters.groups.isEmpty(),
                                              parameters.isJson);

            if (parameters.isDistinct)
            {
                checkNull(perPartitionLimit, "PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries");
                validateDistinctSelection(table, selection, restrictions);
            }

            AggregationSpecification.Factory aggregationSpecFactory = getAggregationSpecFactory(table,
                                                                                                boundNames,
                                                                                                selection,
                                                                                                restrictions,
                                                                                                parameters.isDistinct);

            checkFalse(aggregationSpecFactory == AggregationSpecification.AGGREGATE_EVERYTHING_FACTORY
                        && perPartitionLimit != null,
                           "PER PARTITION LIMIT is not allowed with aggregate queries.");

            Comparator<List<ByteBuffer>> orderingComparator = null;
            boolean isReversed = false;

            if (!orderingColumns.isEmpty())
            {
                assert !forView;
                verifyOrderingIsAllowed(restrictions);
                orderingComparator = getOrderingComparator(table, selection, restrictions, orderingColumns);
                isReversed = isReversed(table, orderingColumns, restrictions);
                if (isReversed)
                    orderingComparator = Collections.reverseOrder(orderingComparator);
            }

            checkNeedsFiltering(restrictions);

            SelectStatement stmt = new SelectStatement(table,
                                                       boundNames.size(),
                                                       parameters,
                                                       selection,
                                                       restrictions,
                                                       isReversed,
                                                       aggregationSpecFactory,
                                                       orderingComparator,
                                                       prepareLimit(boundNames, limit, keyspace(), limitReceiver()),
                                                       prepareLimit(boundNames, perPartitionLimit, keyspace(), perPartitionLimitReceiver()));

            return new ParsedStatement.Prepared(stmt, boundNames, boundNames.getPartitionKeyBindIndexes(table));
        }

        /**
         * Checks if the specified selectables select only partition key columns or static columns
         *
         * @param table the table metadata
         * @param selectables the selectables to check
         * @return {@code true} if the specified selectables select only partition key columns or static columns,
         * {@code false} otherwise.
         */
        private boolean selectOnlyStaticColumns(TableMetadata table, List<Selectable> selectables)
        {
            if (table.isStaticCompactTable() || !table.hasStaticColumns() || selectables.isEmpty())
                return false;

            return Selectable.selectColumns(selectables, (column) -> column.isStatic())
                    && !Selectable.selectColumns(selectables, (column) -> !column.isPartitionKey() && !column.isStatic());
        }

        /**
         * Returns the columns used to order the data.
         * @return the columns used to order the data.
         */
        private Map<ColumnMetadata, Boolean> getOrderingColumns(TableMetadata table)
        {
            if (parameters.orderings.isEmpty())
                return Collections.emptyMap();

            Map<ColumnMetadata, Boolean> orderingColumns = new LinkedHashMap<>();
            for (Map.Entry<ColumnMetadata.Raw, Boolean> entry : parameters.orderings.entrySet())
            {
                orderingColumns.put(entry.getKey().prepare(table), entry.getValue());
            }
            return orderingColumns;
        }

        /**
         * Prepares the restrictions.
         *
         * @param metadata the column family meta data
         * @param boundNames the variable specifications
         * @param selectsOnlyStaticColumns {@code true} if the query select only static columns, {@code false} otherwise.
         * @return the restrictions
         * @throws InvalidRequestException if a problem occurs while building the restrictions
         */
        private StatementRestrictions prepareRestrictions(TableMetadata metadata,
                                                          VariableSpecifications boundNames,
                                                          boolean selectsOnlyStaticColumns,
                                                          boolean forView) throws InvalidRequestException
        {
            return new StatementRestrictions(StatementType.SELECT,
                                             metadata,
                                             whereClause,
                                             boundNames,
                                             selectsOnlyStaticColumns,
                                             parameters.allowFiltering,
                                             forView);
        }

        /** Returns a Term for the limit or null if no limit is set */
        private Term prepareLimit(VariableSpecifications boundNames, Term.Raw limit,
                                  String keyspace, ColumnSpecification limitReceiver) throws InvalidRequestException
        {
            if (limit == null)
                return null;

            Term prepLimit = limit.prepare(keyspace, limitReceiver);
            prepLimit.collectMarkerSpecification(boundNames);
            return prepLimit;
        }

        private static void verifyOrderingIsAllowed(StatementRestrictions restrictions) throws InvalidRequestException
        {
            checkFalse(restrictions.usesSecondaryIndexing(), "ORDER BY with 2ndary indexes is not supported.");
            checkFalse(restrictions.isKeyRange(), "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
        }

        private static void validateDistinctSelection(TableMetadata metadata,
                                                      Selection selection,
                                                      StatementRestrictions restrictions)
                                                      throws InvalidRequestException
        {
            checkFalse(restrictions.hasClusteringColumnsRestrictions() ||
                       (restrictions.hasNonPrimaryKeyRestrictions() && !restrictions.nonPKRestrictedColumns(true).stream().allMatch(ColumnMetadata::isStatic)),
                       "SELECT DISTINCT with WHERE clause only supports restriction by partition key and/or static columns.");

            Collection<ColumnMetadata> requestedColumns = selection.getColumns();
            for (ColumnMetadata def : requestedColumns)
                checkFalse(!def.isPartitionKey() && !def.isStatic(),
                           "SELECT DISTINCT queries must only request partition key columns and/or static columns (not %s)",
                           def.name);

            // If it's a key range, we require that all partition key columns are selected so we don't have to bother
            // with post-query grouping.
            if (!restrictions.isKeyRange())
                return;

            for (ColumnMetadata def : metadata.partitionKeyColumns())
                checkTrue(requestedColumns.contains(def),
                          "SELECT DISTINCT queries must request all the partition key columns (missing %s)", def.name);
        }

        /**
         * Creates the {@code AggregationSpecification.Factory} used to make the aggregates.
         *
         * @param metadata the table metadata
         * @param selection the selection
         * @param restrictions the restrictions
         * @param isDistinct <code>true</code> if the query is a DISTINCT one.
         * @return the {@code AggregationSpecification.Factory} used to make the aggregates
         */
        private AggregationSpecification.Factory getAggregationSpecFactory(TableMetadata metadata,
                                                                             VariableSpecifications boundNames,
                                                                             Selection selection,
                                                                             StatementRestrictions restrictions,
                                                                             boolean isDistinct)
        {
            if (parameters.groups.isEmpty())
                return selection.isAggregate() ? AggregationSpecification.AGGREGATE_EVERYTHING_FACTORY
                                               : null;

            int clusteringPrefixSize = 0;

            Iterator<ColumnMetadata> pkColumns = metadata.primaryKeyColumns().iterator();
            Selector.Factory selectorFactory = null;
            for (Selectable.Raw raw : parameters.groups)
            {
                Selectable selectable = raw.prepare(metadata);
                ColumnMetadata def = null;

                // For GROUP BY we only allow column names or functions at the higher level.
                if (selectable instanceof WithFunction)
                {
                    WithFunction withFunction = (WithFunction) selectable;
                    validateGroupByFunction(withFunction);
                    List<ColumnMetadata> columns = new ArrayList<ColumnMetadata>();
                    selectorFactory = selectable.newSelectorFactory(metadata, null, columns, boundNames);
                    checkFalse(columns.isEmpty(), "GROUP BY functions must have one clustering column name as parameter");
                    if (columns.size() > 1)
                        throw invalidRequest("GROUP BY functions accept only one clustering column as parameter, got: %s",
                                             columns.stream().map(c -> c.name.toCQLString()).collect(Collectors.joining(",")));

                    def = columns.get(0);
                    checkTrue(def.isClusteringColumn(),
                              "Group by functions are only supported on clustering columns, got %s", def.name);
                }
                else
                {
                    def = (ColumnMetadata) selectable;
                    checkTrue(def.isPartitionKey() || def.isClusteringColumn(),
                              "Group by is currently only supported on the columns of the PRIMARY KEY, got %s", def.name);
                    checkNull(selectorFactory, "Functions are only supported on the last element of the GROUP BY clause");
                }

                while (true)
                {
                    checkTrue(pkColumns.hasNext(),
                              "Group by currently only support groups of columns following their declared order in the PRIMARY KEY");

                    ColumnMetadata pkColumn = pkColumns.next();

                    if (pkColumn.isClusteringColumn())
                        clusteringPrefixSize++;

                    // As we do not support grouping on only part of the partition key, we only need to know
                    // which clustering columns need to be used to build the groups
                    if (pkColumn.equals(def))
                        break;

                    checkTrue(restrictions.isColumnRestrictedByEq(pkColumn),
                              "Group by currently only support groups of columns following their declared order in the PRIMARY KEY");
                }
            }

            checkFalse(pkColumns.hasNext() && pkColumns.next().isPartitionKey(),
                       "Group by is not supported on only a part of the partition key");

            checkFalse(clusteringPrefixSize > 0 && isDistinct,
                       "Grouping on clustering columns is not allowed for SELECT DISTINCT queries");

            return selectorFactory == null ? aggregatePkPrefixFactory(metadata.comparator, clusteringPrefixSize)
                                           : aggregatePkPrefixFactoryWithSelector(metadata.comparator,
                                                                                  clusteringPrefixSize,
                                                                                  selectorFactory);
        }

        /**
         * Checks that the function used is a valid one for the GROUP BY clause.
         *
         * @param withFunction the {@code Selectable} from which the function must be retrieved.
         * @return the monotonic scalar function that must be used for determining the groups.
         */
        private void validateGroupByFunction(WithFunction withFunction)
        {
            Function f = withFunction.getFunction();
            checkFalse(f.isAggregate(), "Aggregate functions are not supported within the GROUP BY clause, got: %s", f.name());
        }

        private Comparator<List<ByteBuffer>> getOrderingComparator(TableMetadata metadata,
                                                                   Selection selection,
                                                                   StatementRestrictions restrictions,
                                                                   Map<ColumnMetadata, Boolean> orderingColumns)
                                                                   throws InvalidRequestException
        {
            if (!restrictions.keyIsInRelation())
                return null;

            Map<ColumnIdentifier, Integer> orderingIndexes = getOrderingIndex(metadata, selection, orderingColumns);

            List<Integer> idToSort = new ArrayList<Integer>();
            List<Comparator<ByteBuffer>> sorters = new ArrayList<Comparator<ByteBuffer>>();

            for (ColumnMetadata orderingColumn : orderingColumns.keySet())
            {
                idToSort.add(orderingIndexes.get(orderingColumn.name));
                sorters.add(orderingColumn.type);
            }
            return idToSort.size() == 1 ? new SingleColumnComparator(idToSort.get(0), sorters.get(0))
                    : new CompositeComparator(sorters, idToSort);
        }

        private Map<ColumnIdentifier, Integer> getOrderingIndex(TableMetadata table,
                                                                Selection selection,
                                                                Map<ColumnMetadata, Boolean> orderingColumns)
        {
            Map<ColumnIdentifier, Integer> orderingIndexes = new HashMap<>();
            for (ColumnMetadata def : orderingColumns.keySet())
            {
                int index = selection.getResultSetIndex(def);
                orderingIndexes.put(def.name, index);
            }
            return orderingIndexes;
        }

        private boolean isReversed(TableMetadata table, Map<ColumnMetadata, Boolean> orderingColumns, StatementRestrictions restrictions) throws InvalidRequestException
        {
            Boolean[] reversedMap = new Boolean[table.clusteringColumns().size()];
            int i = 0;
            for (Map.Entry<ColumnMetadata, Boolean> entry : orderingColumns.entrySet())
            {
                ColumnMetadata def = entry.getKey();
                boolean reversed = entry.getValue();

                checkTrue(def.isClusteringColumn(),
                          "Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", def.name);

                while (i != def.position())
                {
                    checkTrue(restrictions.isColumnRestrictedByEq(table.clusteringColumns().get(i++)),
                              "Order by currently only supports the ordering of columns following their declared order in the PRIMARY KEY");
                }
                i++;
                reversedMap[def.position()] = (reversed != def.isReversedType());
            }

            // Check that all boolean in reversedMap, if set, agrees
            Boolean isReversed = null;
            for (Boolean b : reversedMap)
            {
                // Column on which order is specified can be in any order
                if (b == null)
                    continue;

                if (isReversed == null)
                {
                    isReversed = b;
                    continue;
                }
                checkTrue(isReversed.equals(b), "Unsupported order by relation");
            }
            assert isReversed != null;
            return isReversed;
        }

        /** If ALLOW FILTERING was not specified, this verifies that it is not needed */
        private void checkNeedsFiltering(StatementRestrictions restrictions) throws InvalidRequestException
        {
            // non-key-range non-indexed queries cannot involve filtering underneath
            if (!parameters.allowFiltering && (restrictions.isKeyRange() || restrictions.usesSecondaryIndexing()))
            {
                // We will potentially filter data if either:
                //  - Have more than one IndexExpression
                //  - Have no index expression and the row filter is not the identity
                checkFalse(restrictions.needFiltering(), StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
            }
        }

        private ColumnSpecification limitReceiver()
        {
            return new ColumnSpecification(keyspace(), columnFamily(), new ColumnIdentifier("[limit]", true), Int32Type.instance);
        }

        private ColumnSpecification perPartitionLimitReceiver()
        {
            return new ColumnSpecification(keyspace(), columnFamily(), new ColumnIdentifier("[per_partition_limit]", true), Int32Type.instance);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                              .add("name", cfName)
                              .add("selectClause", selectClause)
                              .add("whereClause", whereClause)
                              .add("isDistinct", parameters.isDistinct)
                              .toString();
        }
    }

    public static class Parameters
    {
        // Public because CASSANDRA-9858
        public final Map<ColumnMetadata.Raw, Boolean> orderings;
        public final List<Selectable.Raw> groups;
        public final boolean isDistinct;
        public final boolean allowFiltering;
        public final boolean isJson;

        public Parameters(Map<ColumnMetadata.Raw, Boolean> orderings,
                          List<Selectable.Raw> groups,
                          boolean isDistinct,
                          boolean allowFiltering,
                          boolean isJson)
        {
            this.orderings = orderings;
            this.groups = groups;
            this.isDistinct = isDistinct;
            this.allowFiltering = allowFiltering;
            this.isJson = isJson;
        }
    }

    private static abstract class ColumnComparator<T> implements Comparator<T>
    {
        protected final int compare(Comparator<ByteBuffer> comparator, ByteBuffer aValue, ByteBuffer bValue)
        {
            if (aValue == null)
                return bValue == null ? 0 : -1;

            return bValue == null ? 1 : comparator.compare(aValue, bValue);
        }
    }

    /**
     * Used in orderResults(...) method when single 'ORDER BY' condition where given
     */
    private static class SingleColumnComparator extends ColumnComparator<List<ByteBuffer>>
    {
        private final int index;
        private final Comparator<ByteBuffer> comparator;

        public SingleColumnComparator(int columnIndex, Comparator<ByteBuffer> orderer)
        {
            index = columnIndex;
            comparator = orderer;
        }

        public int compare(List<ByteBuffer> a, List<ByteBuffer> b)
        {
            return compare(comparator, a.get(index), b.get(index));
        }
    }

    /**
     * Used in orderResults(...) method when multiple 'ORDER BY' conditions where given
     */
    private static class CompositeComparator extends ColumnComparator<List<ByteBuffer>>
    {
        private final List<Comparator<ByteBuffer>> orderTypes;
        private final List<Integer> positions;

        private CompositeComparator(List<Comparator<ByteBuffer>> orderTypes, List<Integer> positions)
        {
            this.orderTypes = orderTypes;
            this.positions = positions;
        }

        public int compare(List<ByteBuffer> a, List<ByteBuffer> b)
        {
            for (int i = 0; i < positions.size(); i++)
            {
                Comparator<ByteBuffer> type = orderTypes.get(i);
                int columnPos = positions.get(i);

                int comparison = compare(type, a.get(columnPos), b.get(columnPos));

                if (comparison != 0)
                    return comparison;
            }

            return 0;
        }
    }
}
