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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.db.audit.AuditableEvent;
import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.cql3.CqlAuditLogger;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.reactivex.Completable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.metrics.CQLMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.flow.RxThreads;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

public class QueryProcessor implements QueryHandler
{
    public static final CassandraVersion CQL_VERSION = new CassandraVersion("3.4.5");

    public static final QueryProcessor instance = new QueryProcessor();

    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

    private static final Cache<MD5Digest, ParsedStatement.Prepared> preparedStatements;

    // A map for prepared statements used internally (which we don't want to mix with user statement, in particular we don't
    // bother with expiration on those.
    private static final ConcurrentMap<String, ParsedStatement.Prepared> internalStatements = new ConcurrentHashMap<>();

    // Direct calls to processStatement do not increment the preparedStatementsExecuted/regularStatementsExecuted
    // counters. Callers of processStatement are responsible for correctly notifying metrics
    public static final CQLMetrics metrics = new CQLMetrics();

    private static final AtomicInteger lastMinuteEvictionsCount = new AtomicInteger(0);
    public static CqlAuditLogger auditLogger = new CqlAuditLogger();

    static
    {
        preparedStatements = Caffeine.newBuilder()
                             .executor(MoreExecutors.directExecutor())
                             .maximumWeight(capacityToBytes(DatabaseDescriptor.getPreparedStatementsCacheSizeMB()))
                             .weigher(QueryProcessor::measure)
                             .removalListener((key, prepared, cause) -> {
                                 MD5Digest md5Digest = (MD5Digest) key;
                                 if (cause.wasEvicted())
                                 {
                                     metrics.preparedStatementsEvicted.inc();
                                     lastMinuteEvictionsCount.incrementAndGet();
                                     SystemKeyspace.removePreparedStatement(md5Digest); // fut not waited on
                                 }
                             }).build();

        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(() -> {
            long count = lastMinuteEvictionsCount.getAndSet(0);
            if (count > 0)
                logger.warn("{} prepared statements discarded in the last minute because cache limit reached ({} MB)",
                            count,
                            DatabaseDescriptor.getPreparedStatementsCacheSizeMB());
        }, 1, 1, TimeUnit.MINUTES);

        logger.info("Initialized prepared statement caches with {} MB",
                    DatabaseDescriptor.getPreparedStatementsCacheSizeMB());
    }

    private static long capacityToBytes(long cacheSizeMB)
    {
        return cacheSizeMB * 1024 * 1024;
    }

    public static int preparedStatementsCount()
    {
        return preparedStatements.asMap().size();
    }

    // Work around initialization dependency
    private enum InternalStateInstance
    {
        INSTANCE;

        private final QueryState queryState;

        InternalStateInstance()
        {
            ClientState state = ClientState.forInternalCalls();
            state.setKeyspace(SchemaConstants.SYSTEM_KEYSPACE_NAME);
            this.queryState = new QueryState(state);
        }
    }

    public static void preloadPreparedStatementBlocking()
    {
        final ClientState clientState = ClientState.forInternalCalls();
        TPCUtils.blockingAwait(SystemKeyspace.loadPreparedStatements().thenAccept(results -> {
            int count = 0;
            for (Pair<String, String> useKeyspaceAndCQL : results)
            {
                try
                {
                    clientState.setKeyspace(useKeyspaceAndCQL.left);
                    prepare(useKeyspaceAndCQL.right, clientState);
                    count++;
                }
                catch (RequestValidationException e)
                {
                    logger.warn("prepared statement recreation error: {}", useKeyspaceAndCQL.right, e);
                }
            }
            logger.info("Preloaded {} prepared statements", count);
       }));
    }

    /**
     * Clears the prepared statement cache.
     * @param memoryOnly {@code true} if only the in memory caches must be cleared, {@code false} otherwise.
     */
    @VisibleForTesting
    public static void clearPreparedStatements(boolean memoryOnly)
    {
        preparedStatements.invalidateAll();
        if (!memoryOnly)
            SystemKeyspace.resetPreparedStatementsBlocking();
    }

    private static QueryState internalQueryState()
    {
        return InternalStateInstance.INSTANCE.queryState;
    }

    private QueryProcessor()
    {
        Schema.instance.registerListener(new StatementInvalidatingListener());
    }

    public ParsedStatement.Prepared getPrepared(MD5Digest id)
    {
        return preparedStatements.getIfPresent(id);
    }

    public static void validateKey(ByteBuffer key) throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new InvalidRequestException("Key may not be empty");
        }
        if (key == ByteBufferUtil.UNSET_BYTE_BUFFER)
            throw new InvalidRequestException("Key may not be unset");

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
        }
    }

    public Single<ResultMessage> processStatement(CQLStatement statement, QueryState queryState, QueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        if (logger.isTraceEnabled())
            logger.trace("Process {} @CL.{}", statement, options.getConsistency());

        final ClientState clientState = queryState.getClientState();
        final Scheduler scheduler = statement.getScheduler();

        Single<ResultMessage> ret = Single.defer(() -> {
            try
            {
                statement.checkAccess(clientState);
                statement.validate(clientState);
                return statement.execute(queryState, options, queryStartNanoTime);
            }
            catch (RequestValidationException|RequestExecutionException e)
            {
                AuditableEvent.Builder builder = AuditableEvent.Builder.fromClientState(clientState);
                builder.type(AuditableEventType.REQUEST_FAILURE);
                builder.operation(statement.toString());
                auditLogger.logFailedQuery(Lists.newArrayList(builder.build()), e);
                logger.trace("Unexpected exception when trying to process a statement: " + e.getMessage(), e);
                throw e;
            }
            catch (RuntimeException ex)
            {
                if (!TPCUtils.isWouldBlockException(ex))
                    throw ex;

                if (logger.isTraceEnabled())
                    logger.trace("Failed to execute blocking operation, retrying on io schedulers");

                Single<ResultMessage> single = Single.defer(() ->
                                                                     {
                                                                         statement.checkAccess(clientState);
                                                                         statement.validate(clientState);
                                                                         return statement.execute(queryState,
                                                                                                  options,
                                                                                                  queryStartNanoTime);
                                                                     });
                return RxThreads.subscribeOnIo(single, TPCTaskType.EXECUTE_STATEMENT);
            }
        });

        return scheduler == null ? ret : RxThreads.subscribeOn(ret, scheduler, TPCTaskType.EXECUTE_STATEMENT);
    }

    public static Single<ResultMessage> process(String queryString, ConsistencyLevel cl, QueryState queryState, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        return instance.process(queryString, queryState, QueryOptions.forInternalCalls(cl, Collections.<ByteBuffer>emptyList()), queryStartNanoTime);
    }

    public Single<ResultMessage> process(String query,
                                         QueryState state,
                                         QueryOptions options,
                                         Map<String, ByteBuffer> customPayload,
                                         long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
    {
        return process(query, state, options, queryStartNanoTime);
    }

    public Single<ResultMessage> process(String queryString, QueryState queryState, QueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        try
        {
            ParsedStatement.Prepared p = getStatement(queryString, queryState.getClientState().cloneWithKeyspaceIfSet(options.getKeyspace()));
            options.prepare(p.boundNames);
            CQLStatement prepared = p.statement;
            if (prepared.getBoundTerms() != options.getValues().size())
                throw new InvalidRequestException("Invalid amount of bind variables");

            if (!queryState.getClientState().isInternal)
                metrics.regularStatementsExecuted.inc();

            List<AuditableEvent> events = null;

            if (isAuditEnabled())
            {
                QueryOptions.PagingOptions pagingOptions = options.getPagingOptions();
                if (pagingOptions == null || pagingOptions.state() == null)
                { // it's not a paging query, get events to log
                    events = auditLogger.getEvents(prepared, queryString, queryState,
                                                   options, p.boundNames);
                }
            }

            Single<ResultMessage> executeAndMaybeAuditLogErrors = processStatement(prepared,
                                                                                   queryState,
                                                                                   options,
                                                                                   queryStartNanoTime)
                                                                  .onErrorResumeNext(maybeAuditLogErrors(events));
            return maybeAuditLog(events).andThen(executeAndMaybeAuditLogErrors);
        }
        catch (RequestValidationException e)
        {
            AuditableEvent.Builder builder = AuditableEvent.Builder.fromClientState(queryState.getClientState());
            builder.type(AuditableEventType.REQUEST_FAILURE);
            builder.operation(queryString);
            auditLogger.logFailedQuery(Lists.newArrayList(builder.build()), e);
            logger.trace("Unexpected exception when trying to process a statement: " + e.getMessage(), e);
            throw e;
        }
    }

    public static ParsedStatement.Prepared parseStatement(String queryStr, ClientState clientState) throws RequestValidationException
    {
        return getStatement(queryStr, clientState);
    }

    public static UntypedResultSet processBlocking(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        return TPCUtils.blockingGet(process(query, cl));
    }

    private static Single<UntypedResultSet> process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        return process(query, cl, Collections.<ByteBuffer>emptyList());
    }

    public static Single<UntypedResultSet> process(String query, ConsistencyLevel cl, List<ByteBuffer> values) throws RequestExecutionException
    {
        Single<? extends ResultMessage> obs = instance.process(query, QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, values), System.nanoTime());

        return obs.map(result -> {
            if (result instanceof ResultMessage.Rows)
                return UntypedResultSet.create(((ResultMessage.Rows) result).result);
            else
                return UntypedResultSet.EMPTY;
        });
    }

    private static QueryOptions makeInternalOptions(ParsedStatement.Prepared prepared, Object[] values)
    {
        return makeInternalOptions(prepared, values, ConsistencyLevel.ONE);
    }

    private static QueryOptions makeInternalOptions(ParsedStatement.Prepared prepared, Object[] values, ConsistencyLevel cl)
    {
        if (prepared.boundNames.size() != values.length)
            throw new IllegalArgumentException(String.format("Invalid number of values. Expecting %d but got %d", prepared.boundNames.size(), values.length));

        List<ByteBuffer> boundValues = createBoundValues(prepared, values);
        return QueryOptions.forInternalCalls(cl, boundValues);
    }

    private static ParsedStatement.Prepared prepareInternal(String query) throws RequestValidationException
    {
        ParsedStatement.Prepared existing = internalStatements.get(query);
        if (existing != null)
            return existing;

        ParsedStatement.Prepared prepared = parseStatement(query, internalQueryState().getClientState());
        prepared.statement.validate(internalQueryState().getClientState());
        internalStatements.putIfAbsent(query, prepared);
        return prepared;
    }

    public static Single<UntypedResultSet> executeInternalAsync(String query, Object... values)
    {
        ParsedStatement.Prepared prepared = prepareInternal(query);

        return prepared.statement.executeInternal(internalQueryState(), makeInternalOptions(prepared, values))
                                 .map(result -> {
                                     if (result instanceof ResultMessage.Rows)
                                         return UntypedResultSet.create(((ResultMessage.Rows) result).result);
                                     else
                                         return UntypedResultSet.EMPTY;
                                 });
    }

    /**
     * Executes the query internally.
     *
     * @param query - the query to execute
     * @param values - the query values
     *
     * @return the query result
     * @throws org.apache.cassandra.concurrent.TPCUtils.WouldBlockException when called from a core thread
     */
    public static UntypedResultSet executeInternal(String query, Object... values)
    {
        return TPCUtils.blockingGet(executeInternalAsync(query, values));
    }

    public static UntypedResultSet execute(String query, ConsistencyLevel cl, Object... values)
    throws RequestExecutionException
    {
        return execute(query, cl, internalQueryState(), values);
    }

    /**
     * Executes the query.
     *
     * @param query - the query to execute
     * @param cl - the consistency level
     * @param state - the query state
     * @param values - the query values
     *
     * @return the query result
     *
     * @throws RequestExecutionException
     * @throws org.apache.cassandra.concurrent.TPCUtils.WouldBlockException when called from a core thread
     */
    public static UntypedResultSet execute(String query, ConsistencyLevel cl, QueryState state, Object... values)
    throws RequestExecutionException
    {
        try
        {
            ParsedStatement.Prepared prepared = prepareInternal(query);
            ResultMessage result = TPCUtils.blockingGet(prepared.statement.execute(state, makeInternalOptions(prepared, values, cl), System.nanoTime()));
            if (result instanceof ResultMessage.Rows)
                return UntypedResultSet.create(((ResultMessage.Rows)result).result);
            else
                return UntypedResultSet.EMPTY;
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException("Error validating " + query, e);
        }
    }

    public static UntypedResultSet executeInternalWithPaging(String query, PageSize pageSize, Object... values)
    {
        ParsedStatement.Prepared prepared = prepareInternal(query);
        if (!(prepared.statement instanceof SelectStatement))
            throw new IllegalArgumentException("Only SELECTs can be paged");

        SelectStatement select = (SelectStatement)prepared.statement;
        QueryPager pager = select.getQuery(QueryState.forInternalCalls(), makeInternalOptions(prepared, values), FBUtilities.nowInSeconds()).getPager(null, ProtocolVersion.CURRENT);
        return UntypedResultSet.create(select, pager, pageSize);
    }

    private static List<ByteBuffer> createBoundValues(ParsedStatement.Prepared prepared, Object ... values)
    {
        List<ByteBuffer> boundValues = new ArrayList<>(values.length);
        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            AbstractType type = prepared.boundNames.get(i).type;
            boundValues.add(value instanceof ByteBuffer || value == null ? (ByteBuffer)value : type.decompose(value));
        }
        return boundValues;
    }

    /**
     * Same than executeInternal, but to use for queries we know are only executed once so that the
     * created statement object is not cached.
     */
    public static Single<UntypedResultSet> executeOnceInternal(String query, Object... values)
    {
        final ParsedStatement.Prepared prepared = parseStatement(query, internalQueryState().getClientState());
        final Scheduler scheduler = prepared.statement.getScheduler();

        Single<? extends ResultMessage> observable = Single.defer(() -> {
            prepared.statement.validate(internalQueryState().getClientState());
            return prepared.statement.executeInternal(internalQueryState(), makeInternalOptions(prepared, values));
        });

        if (scheduler != null)
            observable = RxThreads.subscribeOn(observable, scheduler, TPCTaskType.EXECUTE_STATEMENT_INTERNAL);

        return observable.map(result -> {
            if (result instanceof ResultMessage.Rows)
                return UntypedResultSet.create(((ResultMessage.Rows) result).result);
            else
                return UntypedResultSet.EMPTY;
        });
    }

    /**
     * A special version of executeInternal that takes the time used as "now" for the query in argument.
     * Note that this only make sense for Selects so this only accept SELECT statements and is only useful in rare
     * cases.
     */
    public static Single<UntypedResultSet> executeInternalWithNow(int nowInSec, long queryStartNanoTime, String query, Object... values)
    {
        ParsedStatement.Prepared prepared = prepareInternal(query);
        assert prepared.statement instanceof SelectStatement;
        SelectStatement select = (SelectStatement)prepared.statement;

        return select.executeInternal(internalQueryState(), makeInternalOptions(prepared, values), nowInSec, queryStartNanoTime)
                     .map(result -> UntypedResultSet.create(result.result));
    }

    public static UntypedResultSet resultify(String query, RowIterator partition)
    {
        return resultify(query, PartitionIterators.singletonIterator(partition));
    }

    public static UntypedResultSet resultify(String query, PartitionIterator partitions)
    {
        SelectStatement ss = (SelectStatement) getStatement(query, null).statement;
        ResultSet cqlRows = ss.process(partitions, FBUtilities.nowInSeconds()); // iterator will be closed by ss.process
        return UntypedResultSet.create(cqlRows);
    }

    public Single<ResultMessage.Prepared> prepare(String query,
                                                  ClientState clientState,
                                                  Map<String, ByteBuffer> customPayload) throws RequestValidationException
    {
        return prepare(query, clientState);
    }

    public Single<ResultMessage.Prepared> prepare(String queryString, QueryState queryState)
    {
        ClientState cState = queryState.getClientState();
        return prepare(queryString, cState);
    }

    public static Single<ResultMessage.Prepared> prepare(String queryString, ClientState clientState)
    {
        List<AuditableEvent> events = null;
        try
        {
            ResultMessage.Prepared existing = getStoredPreparedStatement(queryString, clientState.getRawKeyspace());

            if (existing != null)
                return Single.just(existing);

            ParsedStatement.Prepared prepared = getStatement(queryString, clientState);
            prepared.rawCQLStatement = queryString;
            validateBindingMarkers(prepared);

            if (isAuditEnabled())
                events = auditLogger.getEventsForPrepare(prepared.statement,
                                                         queryString,
                                                         clientState);

            Single<ResultMessage.Prepared> executeAndMaybeAuditLogErrors = storePreparedStatement(queryString,
                                                                                            clientState.getRawKeyspace(),
                                                                                            prepared)
                                                                     .onErrorResumeNext(maybeAuditLogErrors(events));

            return maybeAuditLog(events).andThen(executeAndMaybeAuditLogErrors);
        }
        catch (RequestValidationException e)
        {
            // RequestValidationException can come out before the Single gets created, so we need the try/catch still.
            AuditableEvent.Builder builder = AuditableEvent.Builder.fromClientState(clientState);
            builder.type(AuditableEventType.REQUEST_FAILURE);
            builder.operation(queryString);
            auditLogger.logFailedQuery(Lists.newArrayList(builder.build()), e);
            logger.trace("Unexpected exception when trying to prepare a statement: " + e.getMessage(), e);
            throw e;
        }
    }


    public static void validateBindingMarkers(ParsedStatement.Prepared prepared)
    {
        int boundTerms = prepared.statement.getBoundTerms();
        if (boundTerms > FBUtilities.MAX_UNSIGNED_SHORT)
            throw new InvalidRequestException(String.format("Too many markers(?). %d markers exceed the allowed maximum of %d", boundTerms, FBUtilities.MAX_UNSIGNED_SHORT));
        assert boundTerms == prepared.boundNames.size();
    }

    private static MD5Digest computeId(String queryString, String keyspace)
    {
        String toHash = keyspace == null ? queryString : keyspace + queryString;
        return MD5Digest.compute(toHash);
    }

    public static ResultMessage.Prepared getStoredPreparedStatement(String queryString, String keyspace)
    throws InvalidRequestException
    {
        MD5Digest statementId = computeId(queryString, keyspace);
        ParsedStatement.Prepared existing = preparedStatements.getIfPresent(statementId);
        if (existing == null)
            return null;

        checkTrue(queryString.equals(existing.rawCQLStatement),
                String.format("MD5 hash collision: query with the same MD5 hash was already prepared. \n Existing: '%s'", existing.rawCQLStatement));

        return new ResultMessage.Prepared(statementId, existing.resultMetadataId, existing);
    }

    public static Single<ResultMessage.Prepared> storePreparedStatement(String queryString, String keyspace, ParsedStatement.Prepared prepared)
    throws InvalidRequestException
    {
        // Concatenate the current keyspace so we don't mix prepared statements between keyspace (#5352).
        // (if the keyspace is null, queryString has to have a fully-qualified keyspace so it's fine.
        long statementSize = ObjectSizes.measureDeep(prepared.statement);
        // don't execute the statement if it's bigger than the allowed threshold
        if (statementSize > capacityToBytes(DatabaseDescriptor.getPreparedStatementsCacheSizeMB()))
            throw new InvalidRequestException(String.format("Prepared statement of size %d bytes is larger than allowed maximum of %d MB: %s...",
                                                            statementSize,
                                                            DatabaseDescriptor.getPreparedStatementsCacheSizeMB(),
                                                            queryString.substring(0, 200)));
        MD5Digest statementId = computeId(queryString, keyspace);
        preparedStatements.put(statementId, prepared);

        ResultSet.ResultMetadata resultMetadata = ResultSet.ResultMetadata.fromPrepared(prepared);
        Single<UntypedResultSet> observable = SystemKeyspace.writePreparedStatement(keyspace, statementId, queryString);
        return observable.map(resultSet -> new ResultMessage.Prepared(statementId, resultMetadata.getResultMetadataId(), prepared));
    }

    public Single<ResultMessage> processPrepared(CQLStatement statement,
                                                           QueryState state,
                                                           QueryOptions options,
                                                           Map<String, ByteBuffer> customPayload,
                                                           long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        return processPrepared(statement, state, options, queryStartNanoTime);
    }

    public Single<ResultMessage> processPrepared(CQLStatement statement, QueryState queryState, QueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> variables = options.getValues();

        // Check to see if there are any bound variables to verify
        if (!(variables.isEmpty() && (statement.getBoundTerms() == 0)))
        {
            if (variables.size() != statement.getBoundTerms())
                throw new InvalidRequestException(String.format("there were %d markers(?) in CQL but %d bound variables",
                                                                statement.getBoundTerms(),
                                                                variables.size()));

            // at this point there is a match in count between markers and variables that is non-zero

            if (logger.isTraceEnabled())
                for (int i = 0; i < variables.size(); i++)
                    logger.trace("[{}] '{}'", i + 1, variables.get(i));
        }

        metrics.preparedStatementsExecuted.inc();

        ParsedStatement.Prepared preparedStatement = (ParsedStatement.Prepared) statement;

        List<AuditableEvent> events = null;
        if (isAuditEnabled())
            events = auditLogger.getEvents(statement,
                                           preparedStatement.rawCQLStatement,
                                           queryState,
                                           options,
                                           preparedStatement.boundNames);

        Single<ResultMessage> executeAndMaybeAuditLogErrors = processStatement(statement,
                                                                               queryState,
                                                                               options,
                                                                               queryStartNanoTime)
                                                              .onErrorResumeNext(maybeAuditLogErrors(events));

        return maybeAuditLog(events).andThen(executeAndMaybeAuditLogErrors);
    }

    public Single<ResultMessage> processBatch(BatchStatement statement,
                                              QueryState state,
                                              BatchQueryOptions options,
                                              Map<String, ByteBuffer> customPayload,
                                              long queryStartNanoTime)
    {
        return processBatch(statement, state, options, queryStartNanoTime);
    }

    public Single<ResultMessage> processBatch(BatchStatement batch, QueryState queryState, BatchQueryOptions options, long queryStartNanoTime)
    {
        final ClientState clientState = queryState.getClientState().cloneWithKeyspaceIfSet(options.getKeyspace());
        return Single.defer(() -> {
            List<AuditableEvent> events = null;

            if (isAuditEnabled())
                events = auditLogger.getEvents(batch, queryState, options);

            try
            {
                batch.checkAccess(clientState);
                batch.validate();
                batch.validate(clientState);
                return auditLogger.logEvents(events).andThen(batch.execute(queryState, options, queryStartNanoTime)
                                                                  .onErrorResumeNext(maybeAuditLogErrors(events)));
            }
            catch (UnauthorizedException e)
            {
                auditLogger.logUnauthorizedAttempt(events, e);
                throw e;
            }
            catch (InvalidRequestException | RequestExecutionException e)
            {
                auditLogger.logFailedQuery(events, e);
                throw e;
            }
            catch (RuntimeException ex)
            {
                if (!TPCUtils.isWouldBlockException(ex))
                    throw ex;

                return RxThreads.subscribeOnIo(
                Single.defer(() -> {
                    batch.checkAccess(clientState);
                    batch.validate();
                    batch.validate(clientState);
                    return batch.execute(queryState, options, queryStartNanoTime);
                }),
                TPCTaskType.EXECUTE_STATEMENT
                );
            }
        });
    }

    public static ParsedStatement.Prepared getStatement(String queryStr, ClientState clientState)
    throws RequestValidationException
    {
        Tracing.trace("Parsing {}", queryStr);
        ParsedStatement statement = parseStatement(queryStr);

        // Set keyspace for statement that require login
        if (statement instanceof CFStatement)
            ((CFStatement) statement).prepareKeyspace(clientState);

        Tracing.trace("Preparing statement");
        return statement.prepare();
    }

    public static <T extends ParsedStatement> T parseStatement(String queryStr, Class<T> klass, String type) throws SyntaxException
    {
        try
        {
            ParsedStatement stmt = parseStatement(queryStr);

            if (!klass.isAssignableFrom(stmt.getClass()))
                throw new IllegalArgumentException("Invalid query, must be a " + type + " statement but was: " + stmt.getClass());

            return klass.cast(stmt);
        }
        catch (RequestValidationException e)
        {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    public static ParsedStatement parseStatement(String queryStr) throws SyntaxException
    {
        try
        {
            return CQLFragmentParser.parseAnyUnhandled(CqlParser::query, queryStr);
        }
        catch (CassandraException ce)
        {
            throw ce;
        }
        catch (RuntimeException re)
        {
            logger.error(String.format("The statement: [%s] could not be parsed.", queryStr), re);
            throw new SyntaxException(String.format("Failed parsing statement: [%s] reason: %s %s",
                                                    queryStr,
                                                    re.getClass().getSimpleName(),
                                                    re.getMessage()));
        }
        catch (RecognitionException e)
        {
            throw new SyntaxException("Invalid or malformed CQL query string: " + e.getMessage());
        }
    }

    private static int measure(Object key, ParsedStatement.Prepared value)
    {
        return Ints.checkedCast(ObjectSizes.measureDeep(key) + ObjectSizes.measureDeep(value));
    }

    /**
     * Clear our internal statmeent cache for test purposes.
     */
    @VisibleForTesting
    public static void clearInternalStatementsCache()
    {
        internalStatements.clear();
    }

    private static class StatementInvalidatingListener extends SchemaChangeListener
    {
        private static void removeInvalidPreparedStatements(String ksName, String cfName)
        {
            removeInvalidPreparedStatements(internalStatements.values().iterator(), ksName, cfName);
            removeInvalidPersistentPreparedStatements(preparedStatements.asMap().entrySet().iterator(), ksName, cfName);
        }

        private static void removePreparedStatementBlocking(MD5Digest key)
        {
            TPCUtils.blockingAwait(SystemKeyspace.removePreparedStatement(key));
        }

        private static void removeInvalidPreparedStatementsForFunction(String ksName, String functionName)
        {
            Predicate<Function> matchesFunction = f -> ksName.equals(f.name().keyspace) && functionName.equals(f.name().name);

            for (Iterator<Map.Entry<MD5Digest, ParsedStatement.Prepared>> iter = preparedStatements.asMap().entrySet().iterator();
                 iter.hasNext();)
            {
                Map.Entry<MD5Digest, ParsedStatement.Prepared> pstmt = iter.next();
                if (Iterables.any(pstmt.getValue().statement.getFunctions(), matchesFunction))
                {
                    removePreparedStatementBlocking(pstmt.getKey());
                    iter.remove();
                }
            }


            Iterators.removeIf(internalStatements.values().iterator(),
                               statement -> Iterables.any(statement.statement.getFunctions(), matchesFunction));
        }

        private static void removeInvalidPersistentPreparedStatements(Iterator<Map.Entry<MD5Digest, ParsedStatement.Prepared>> iterator,
                                                                      String ksName, String cfName)
        {
            while (iterator.hasNext())
            {
                Map.Entry<MD5Digest, ParsedStatement.Prepared> entry = iterator.next();
                if (shouldInvalidate(ksName, cfName, entry.getValue().statement))
                {
                    removePreparedStatementBlocking(entry.getKey());
                    iterator.remove();
                }
            }
        }

        private static void removeInvalidPreparedStatements(Iterator<ParsedStatement.Prepared> iterator, String ksName, String cfName)
        {
            while (iterator.hasNext())
            {
                if (shouldInvalidate(ksName, cfName, iterator.next().statement))
                    iterator.remove();
            }
        }

        private static boolean shouldInvalidate(String ksName, String cfName, CQLStatement statement)
        {
            String statementKsName;
            String statementCfName;

            if (statement instanceof ModificationStatement)
            {
                ModificationStatement modificationStatement = ((ModificationStatement) statement);
                statementKsName = modificationStatement.keyspace();
                statementCfName = modificationStatement.columnFamily();
            }
            else if (statement instanceof SelectStatement)
            {
                SelectStatement selectStatement = ((SelectStatement) statement);
                statementKsName = selectStatement.keyspace();
                statementCfName = selectStatement.columnFamily();
            }
            else if (statement instanceof BatchStatement)
            {
                BatchStatement batchStatement = ((BatchStatement) statement);
                for (ModificationStatement stmt : batchStatement.getStatements())
                {
                    if (shouldInvalidate(ksName, cfName, stmt))
                        return true;
                }
                return false;
            }
            else
            {
                return false;
            }

            return ksName.equals(statementKsName) && (cfName == null || cfName.equals(statementCfName));
        }

        public void onCreateFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            onCreateFunctionInternal(ksName, functionName, argTypes);
        }

        public void onCreateAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
        {
            onCreateFunctionInternal(ksName, aggregateName, argTypes);
        }

        private static void onCreateFunctionInternal(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            // in case there are other overloads, we have to remove all overloads since argument type
            // matching may change (due to type casting)
            if (Schema.instance.getKeyspaceMetadata(ksName).functions.get(new FunctionName(ksName, functionName)).size() > 1)
                removeInvalidPreparedStatementsForFunction(ksName, functionName);
        }

        public void onAlterTable(String ksName, String cfName, boolean affectsStatements)
        {
            logger.trace("Column definitions for {}.{} changed, invalidating related prepared statements", ksName, cfName);
            if (affectsStatements)
                removeInvalidPreparedStatements(ksName, cfName);
        }

        public void onAlterFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            // Updating a function may imply we've changed the body of the function, so we need to invalid statements so that
            // the new definition is picked (the function is resolved at preparation time).
            // TODO: if the function has multiple overload, we could invalidate only the statement refering to the overload
            // that was updated. This requires a few changes however and probably doesn't matter much in practice.
            removeInvalidPreparedStatementsForFunction(ksName, functionName);
        }

        public void onAlterAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
        {
            // Updating a function may imply we've changed the body of the function, so we need to invalid statements so that
            // the new definition is picked (the function is resolved at preparation time).
            // TODO: if the function has multiple overload, we could invalidate only the statement refering to the overload
            // that was updated. This requires a few changes however and probably doesn't matter much in practice.
            removeInvalidPreparedStatementsForFunction(ksName, aggregateName);
        }

        public void onDropKeyspace(String ksName)
        {
            logger.trace("Keyspace {} was dropped, invalidating related prepared statements", ksName);
            removeInvalidPreparedStatements(ksName, null);
        }

        public void onDropTable(String ksName, String cfName)
        {
            logger.trace("Table {}.{} was dropped, invalidating related prepared statements", ksName, cfName);
            removeInvalidPreparedStatements(ksName, cfName);
        }

        public void onDropFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            removeInvalidPreparedStatementsForFunction(ksName, functionName);
        }

        public void onDropAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
        {
            removeInvalidPreparedStatementsForFunction(ksName, aggregateName);
        }
    }

    public static boolean isAuditEnabled()
    {
        return DatabaseDescriptor.getAuditLogger().isEnabled() || DatabaseDescriptor.getAuditLogger().forceAuditLogging();
    }

    public static <T extends ResultMessage> io.reactivex.functions.Function<Throwable, SingleSource<T>> maybeAuditLogErrors(List<AuditableEvent> events)
    {
        if (events == null)
            return Single::error;

        return e ->
        {
            if (e instanceof UnauthorizedException)
            {
                auditLogger.logUnauthorizedAttempt(events, (UnauthorizedException) e);
            }
            else if (e instanceof RequestExecutionException |
                     e instanceof RequestValidationException |
                     e instanceof UnavailableException)
            {
                auditLogger.logFailedQuery(events, (CassandraException) e);
                logger.trace("Unexpected exception when trying to execute a statement: " + e.getMessage(), e);
            }
            return Single.error(e);
        };
    }

    public static Completable maybeAuditLog(List<AuditableEvent> events)
    {
        if (events == null)
            return Completable.complete();

        return auditLogger.logEvents(events);
    }
}
