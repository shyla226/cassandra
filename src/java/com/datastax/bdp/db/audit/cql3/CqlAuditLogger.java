/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit.cql3;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.db.audit.AuditLogger;
import com.datastax.bdp.db.audit.AuditableEvent;
import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;

import io.reactivex.Completable;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.MD5Digest;

public class CqlAuditLogger
{
    private static final Logger logger = LoggerFactory.getLogger(CqlAuditLogger.class);

    private final AuditableEventGenerator eventGenerator = new AuditableEventGenerator();
    private AuditLogger auditLogger = DatabaseDescriptor.getAuditLogger();

    public boolean isAuditEnabled()
    {
        return auditLogger.isEnabled() || auditLogger.forceAuditLogging();
    }

    public Completable logEvents(List<AuditableEvent> events)
    {
        if (events.isEmpty())
            return Completable.complete();


        Completable result = null;
        for (AuditableEvent event : events)
        {
            Completable recordEvent = auditLogger.recordEvent(event);

            if (result == null)
                result = recordEvent;
            else
                result = result.andThen(recordEvent);
        }

        // The Completable returned by the AuditLogger might not be scheduled on the expected scheduler.
        // By consequence, we might need to set back the scheduler for the rest of the pipeline.

        // We only need to change the scheduler if we are on an IO thread while building the pipeline.
        if (TPC.isTPCThread())
            return result;

        return result.observeOn(TPC.ioScheduler());
    }

    public Completable logFailedQuery(String queryString, QueryState state, Throwable e)
    {
        if (state.isSystem() && !isAuditEnabled())
            return Completable.complete();

        AuditableEventType type = e instanceof UnauthorizedException ? CoreAuditableEventType.UNAUTHORIZED_ATTEMPT
                                                                     : CoreAuditableEventType.REQUEST_FAILURE;

        String operation = getOperationForm(queryString, e);

        AuditableEvent auditable = AuditableEvent.Builder.fromQueryState(state)
                                                         .type(type)
                                                         .operation(operation)
                                                         .build();
        return auditLogger.recordEvent(auditable);
    }

    public Completable logFailedQuery(List<AuditableEvent> events, Throwable e)
    {
        if (!isAuditEnabled() || events.isEmpty())
            return Completable.complete();

        AuditableEventType type = e instanceof UnauthorizedException ? CoreAuditableEventType.UNAUTHORIZED_ATTEMPT
                                                                     : CoreAuditableEventType.REQUEST_FAILURE;
        Completable result = null;

        for (AuditableEvent event: events)
        {
            String operation = getOperationForm(event.getOperation(), e);
            AuditableEvent auditable = AuditableEvent.Builder.fromEvent(event)
                                                             .type(type)
                                                             .operation(operation)
                                                             .build();

            Completable recordEvent = auditLogger.recordEvent(auditable);

            if (result == null)
                result = recordEvent;
            else
                result = result.andThen(recordEvent);
        }

        return result;
    }

    private String getOperationForm(String query, Throwable e)
    {
        if (query == null)
            return e.getLocalizedMessage();

        return new StringBuilder(e.getLocalizedMessage()).append(' ').append(query).toString();
    }

    /**
     * Get AuditableEvents for a CQL statement
     *
     * @param statement
     * @param queryString
     * @param queryState
     * @param queryOptions
     * @param boundNames
     * @return
     */
    public List<AuditableEvent> getEvents(CQLStatement statement,
                                          String queryString,
                                          QueryState queryState,
                                          QueryOptions queryOptions,
                                          List<ColumnSpecification> boundNames)
    {
        if (queryState.isSystem() || !isAuditEnabled() || isPagingQuery(queryOptions))
            return Collections.emptyList();

        return eventGenerator.getEvents(statement,
                                        queryState,
                                        queryString,
                                        queryOptions.getValues(),
                                        boundNames,
                                        queryOptions.getConsistency());
    }

    /**
     * Get AuditableEvents for a CQL BatchStatement
     *
     * @param batch
     * @param queryState
     * @param queryOptions
     * @return
     */
    public List<AuditableEvent> getEvents(BatchStatement batch,
                                          QueryState queryState,
                                          BatchQueryOptions queryOptions)
    {
        if (queryState.isSystem() || !isAuditEnabled())
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
                events.addAll(eventGenerator.getEvents(batch.getStatements().get(i),
                                                       queryState,
                                                       (String) queryOrId,
                                                       queryOptions.forStatement(i).getValues(),
                                                       Collections.<ColumnSpecification>emptyList(),
                                                       batchId,
                                                       queryOptions.getConsistency()));
            }
            else if (queryOrId instanceof MD5Digest)
            {
                // prepared statement
                // lookup the original prepared stmt from QP's cache
                // then use it as the key to fetch CQL string & column
                // specs for bind vars from our own cache
                CQLStatement prepared = null;
                ParsedStatement.Prepared preparedStatement = queryState.getClientState()
                                                                        .getCQLQueryHandler()
                                                                        .getPrepared((MD5Digest) queryOrId);
                if (preparedStatement != null)
                {
                    prepared = preparedStatement.statement;
                }
                else
                {
                    logger.warn(String.format("Prepared Statement [id=%s] is null! " +
                                    "This usually happens because the KS or CF was dropped between the " +
                                    "prepared statement creation and its retrieval from cache",
                                    queryOrId.toString()));
                }

                events.addAll(eventGenerator.getEvents(prepared,
                                                       queryState,
                                                       preparedStatement.rawCQLStatement,
                                                       queryOptions.forStatement(i).getValues(),
                                                       preparedStatement.boundNames,
                                                       batchId,
                                                       queryOptions.getConsistency()));
            }
        }
        return events;
    }

    /**
     * Get AuditableEvents for the act of preparing a statement.
     * Also caches metadata about the prepared statement for retrieval
     * at execution time. This consists of the original CQL string and
     * bind variable definitions required for audit logging at execution
     * time
     *
     * @param statement
     * @param queryString
     * @param queryState
     * @return
     */
    public List<AuditableEvent> getEventsForPrepare(CQLStatement statement,
                                                    String queryString,
                                                    QueryState queryState)
    {
        if (queryState.isSystem() || !isAuditEnabled())
            return Collections.emptyList();

        return eventGenerator.getEventsForPrepare(statement, queryState, queryString);
    }

    private boolean isPagingQuery(QueryOptions options)
    {
        QueryOptions.PagingOptions pagingOptions = options.getPagingOptions();
        boolean pagingQuery = pagingOptions != null && pagingOptions.state() != null;
        return pagingQuery;
    }
}
