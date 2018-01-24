/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.db.audit.cql3.BatchStatementUtils;

import io.reactivex.Completable;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;

final class AuditLogger implements IAuditLogger
{
    private static final Logger logger = LoggerFactory.getLogger(AuditLogger.class);
    private static final Pattern obfuscatePasswordPattern = Pattern.compile("(?i)(PASSWORD\\s+(=\\s+)?)'[^']*'");

    /**
     * The writer used to log the events
     */
    private final IAuditWriter writer;

    /**
     * The filter used to determine which events must be logged
     */
    private final IAuditFilter filter;

    public AuditLogger(IAuditWriter writer, IAuditFilter filter)
    {
        this.writer = writer;
        this.filter = filter;
    }

    /**
     * Creates the {@code DefaultAuditLogger} corresponding to the configuration.
     * @param config the configuration
     * @return the {@code DefaultAuditLogger} corresponding to the configuration
     */
    public static AuditLogger fromConfiguration(AuditLoggingOptions auditLoggingOptions)
    {
        IAuditWriter writer = getWriterInstance(auditLoggingOptions);
        IAuditFilter filter = AuditFilters.fromConfiguration(auditLoggingOptions);

        return new AuditLogger(writer, filter);
    }

    /**
     * Creates the {@code IAuditWriter} corresponding to the configuration.
     * @return IAuditWriter configured in the cassandra.yaml
     */
    private static IAuditWriter getWriterInstance(AuditLoggingOptions auditLoggingOptions)
    {
        String className = System.getProperty("cassandra.audit_writer", auditLoggingOptions.logger);
        if (!className.contains("."))
            className = "com.datastax.bdp.db.audit." + className;
        return FBUtilities.construct(className, "audit writer");
    }

    /**
     * Records the specified event if it is accepted by the filters.
     * @param event the event being processed
     * @return the {@code Completable} tracking the logging operation
     */
    private Completable recordEvent(AuditableEvent event)
    {
        if (!isEnabled() || !filter.accept(event))
            return Completable.complete();

        return writer.recordEvent(event);
    }

    @Override
    public boolean isEnabled()
    {
        return writer.isSetUpComplete();
    }

    @Override
    public void setup()
    {
        writer.setUp();
        logger.info("Audit logging is enabled with " + writer.getClass().getName());
    }

    public Completable logEvents(List<AuditableEvent> events)
    {
        if (events.isEmpty())
            return Completable.complete();


        Completable result = null;
        for (AuditableEvent event : events)
        {
            result = chain(result, recordEvent(event));
        }

        // The Completable returned by the AuditLogger might not be scheduled on the expected scheduler.
        // By consequence, we might need to set back the scheduler for the rest of the pipeline.

        // We only need to change the scheduler if we are on an IO thread while building the pipeline.
        if (TPC.isTPCThread())
            return result;

        return result.observeOn(TPC.ioScheduler());
    }

    private Completable chain(Completable completable, Completable nextCompletable)
    {
        return completable == null ? nextCompletable : completable.andThen(nextCompletable);
    }

    @Override
    public Completable logFailedQuery(String queryString, QueryState state, Throwable e)
    {
        if (state.isSystem() && !isEnabled())
            return Completable.complete();

        AuditableEventType type = e instanceof UnauthorizedException ? CoreAuditableEventType.UNAUTHORIZED_ATTEMPT
                                                                     : CoreAuditableEventType.REQUEST_FAILURE;

        String operation = getOperationFrom(queryString, e);

        return recordEvent(new AuditableEvent(state, type, operation));
    }

    @Override
    public Completable logFailedQuery(List<AuditableEvent> events, Throwable e)
    {
        if (!isEnabled() || events.isEmpty())
            return Completable.complete();

        AuditableEventType type = e instanceof UnauthorizedException ? CoreAuditableEventType.UNAUTHORIZED_ATTEMPT
                                                                     : CoreAuditableEventType.REQUEST_FAILURE;
        Completable result = null;

        for (AuditableEvent event : events)
        {
            String operation = getOperationFrom(event.getOperation(), e);
            AuditableEvent auditable = new AuditableEvent(event, type, operation);
            result = chain(result, recordEvent(auditable));
        }

        return result;
    }

    private String getOperationFrom(String query, Throwable e)
    {
        if (query == null)
            return e.getLocalizedMessage();

        return new StringBuilder(e.getLocalizedMessage()).append(' ').append(query).toString();
    }

    @Override
    public List<AuditableEvent> getEvents(CQLStatement statement,
                                          String queryString,
                                          QueryState queryState,
                                          QueryOptions queryOptions,
                                          List<ColumnSpecification> boundNames)
    {
        if (queryState.isSystem() || !isEnabled() || isPagingQuery(queryOptions))
            return Collections.emptyList();

        if (statement instanceof BatchStatement)
        {
            UUID batchID = UUID.randomUUID();
            List<BatchStatementUtils.Meta> batchStatements = BatchStatementUtils.decomposeBatchStatement(queryString);
            List<ModificationStatement> statements = ((BatchStatement) statement).getStatements();
            List<AuditableEvent> events = new ArrayList<>(batchStatements.size());
            for (int i = 0, m = batchStatements.size(); i < m; i++)
            {
                ModificationStatement stmt = statements.get(i);
                BatchStatementUtils.Meta stmtMeta = batchStatements.get(i);
                appendEvent(events,
                            queryState,
                            stmt,
                            stmtMeta.query,
                            batchID,
                            stmtMeta.getSubList(queryOptions.getValues()),
                            stmtMeta.getSubList(boundNames),
                            queryOptions.getConsistency());
            }
            return events;
        }

        return appendEvent(new ArrayList<>(1),
                           queryState,
                           statement,
                           queryString,
                           null,
                           queryOptions.getValues(),
                           boundNames,
                           queryOptions.getConsistency());
    }

    private List<AuditableEvent> appendEvent(List<AuditableEvent> events,
                                             QueryState queryState,
                                             CQLStatement statement,
                                             String queryString,
                                             UUID batchID,
                                             List<ByteBuffer> variables,
                                             List<ColumnSpecification> boundNames,
                                             ConsistencyLevel consistencyLevel)
    {
        AuditableEventType type = getAuditEventType(statement, queryString);
        String keyspace = getKeyspace(statement);

        events.add(new AuditableEvent(queryState,
                                      type,
                                      batchID,
                                      keyspace,
                                      getTable(statement),
                                      getOperation(statement, queryString, variables, boundNames),
                                      consistencyLevel));

        return events;
    }

    @Override
    public List<AuditableEvent> getEvents(BatchStatement batch,
                                          QueryState queryState,
                                          BatchQueryOptions queryOptions)
    {
        if (queryState.isSystem() || !isEnabled())
            return Collections.emptyList();

        UUID batchId = UUID.randomUUID();
        List<Object> queryOrIdList = queryOptions.getQueryOrIdList();
        List<AuditableEvent> events = Lists.newArrayList();
        for (int i=0; i<queryOrIdList.size(); i++)
        {
            Object queryOrId = queryOrIdList.get(i);
            if (queryOrId instanceof String)
            {
                // regular statement
                // column specs for bind vars are not available, so we pass the
                // variables + an empty list of specs so we log the fact that
                // they exit, but can't be logged
                appendEvent(events,
                            queryState,
                            batch.getStatements().get(i),
                            (String) queryOrId,
                            batchId,
                            queryOptions.forStatement(i).getValues(),
                            queryOptions.forStatement(i).getColumnSpecifications(),
                            queryOptions.getConsistency());
            }
            else if (queryOrId instanceof MD5Digest)
            {
                // prepared statement
                // lookup the original prepared stmt from QP's cache
                // then use it as the key to fetch CQL string & column
                // specs for bind vars from our own cache
                ParsedStatement.Prepared preparedStatement = queryState.getClientState()
                                                                        .getCQLQueryHandler()
                                                                        .getPrepared((MD5Digest) queryOrId);
                if (preparedStatement == null)
                {
                    logger.warn(String.format("Prepared Statement [id=%s] is null! " +
                                    "This usually happens because the KS or CF was dropped between the " +
                                    "prepared statement creation and its retrieval from cache",
                                    queryOrId.toString()));
                    continue;
                }

                appendEvent(events,
                            queryState,
                            preparedStatement.statement,
                            preparedStatement.rawCQLStatement,
                            batchId,
                            queryOptions.forStatement(i).getValues(),
                            preparedStatement.boundNames,
                            queryOptions.getConsistency());
            }
            else
                throw new IllegalArgumentException("Got unexpected " + queryOrId);
        }
        return events;
    }

    @Override
    public List<AuditableEvent> getEventsForPrepare(CQLStatement statement,
                                                    String queryString,
                                                    QueryState queryState)
    {
        if (queryState.isSystem() || !isEnabled())
            return Collections.emptyList();

        if (statement instanceof BatchStatement)
        {
            UUID batchID = UUID.randomUUID();
            List<BatchStatementUtils.Meta> batchStatements = BatchStatementUtils.decomposeBatchStatement(queryString);
            List<ModificationStatement> statements = ((BatchStatement) statement).getStatements();
            List<AuditableEvent> events = new ArrayList<>(batchStatements.size());
            for (int i = 0, m = batchStatements.size(); i < m; i++)
            {
                appendPrepareEvent(events, queryState, statements.get(i), batchStatements.get(i).query, batchID);
            }
            return events;
        }

        return appendPrepareEvent(new ArrayList<>(1), queryState, statement, queryString, null);
    }

    private List<AuditableEvent> appendPrepareEvent(List<AuditableEvent> events,
                                                    QueryState queryState,
                                                    CQLStatement statement,
                                                    String queryString,
                                                    UUID batchID)
    {
        String keyspace = getKeyspace(statement);

        events.add(new AuditableEvent(queryState,
                                      CoreAuditableEventType.CQL_PREPARE_STATEMENT,
                                      batchID,
                                      keyspace,
                                      getTable(statement),
                                      queryString,
                                      AuditableEvent.NO_CL));

        return events;
    }

    private boolean isPagingQuery(QueryOptions options)
    {
        QueryOptions.PagingOptions pagingOptions = options.getPagingOptions();
        boolean pagingQuery = pagingOptions != null && pagingOptions.state() != null;
        return pagingQuery;
    }

    private static String obfuscatePasswordsIfNeeded(CQLStatement stmt, String queryString)
    {
        if (stmt instanceof CreateRoleStatement || stmt instanceof AlterRoleStatement)
            queryString = obfuscatePasswordPattern.matcher(queryString)
                                                  .replaceAll("$1'*****'");
        return queryString;
    }

    private static String getOperation(CQLStatement stmt,
                                       String queryString,
                                       List<ByteBuffer> variables,
                                       List<ColumnSpecification> boundNames)
    {
        if (null == variables || variables.isEmpty())
            return obfuscatePasswordsIfNeeded(stmt, queryString);

        StringBuilder builder = new StringBuilder(queryString).append(' ');
        appendBindVariables(builder, boundNames, variables);
        return obfuscatePasswordsIfNeeded(stmt, builder.toString());
    }

    private static AuditableEventType getAuditEventType(CQLStatement stmt, String queryString)
    {
        AuditableEventType type = stmt.getAuditEventType();
        if (null != type)
            return type;

        logger.warn("Encountered a CQL statement I don't know how to log : " + stmt.getClass().getName() + " ( " + queryString + ")");
        return CoreAuditableEventType.UNKNOWN;
    }

    private static StringBuilder appendBindVariables(StringBuilder builder,
                                                     List<ColumnSpecification> boundNames,
                                                     List<ByteBuffer> variables)
    {
        if (null == boundNames || boundNames.isEmpty())
            return builder.append("[bind variable values unavailable]");

        builder.append('[');

        for (int i = 0, m = variables.size(); i < m; i++)
        {
            if (i != 0)
                builder.append(',');

            ColumnSpecification spec = boundNames.get(i);
            ByteBuffer var = variables.get(i);

            builder.append(spec.name)
                   .append('=')
                   .append(spec.type.getString(var));
        }
        return builder.append(']');
    }

    private static String getKeyspace(CQLStatement stmt)
    {
        return stmt instanceof KeyspaceStatement ? ((KeyspaceStatement) stmt).keyspace() : null;
    }

    private static String getTable(CQLStatement stmt)
    {
        return stmt instanceof TableStatement ? ((TableStatement) stmt).columnFamily() : null;
    }
}
