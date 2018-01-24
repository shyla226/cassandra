/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.util.Collections;
import java.util.List;

import io.reactivex.Completable;

import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.service.QueryState;

/**
 * An {@code IAuditLogger} that does nothing.
 * <p>This implementation is the one being used when audit logging is not enabled.</p>
 */
final class NoopAuditLogger implements IAuditLogger
{
    @Override
    public List<AuditableEvent> getEvents(CQLStatement statement,
                                          String queryString,
                                          QueryState queryState,
                                          QueryOptions queryOptions,
                                          List<ColumnSpecification> boundNames)
    {
        return Collections.emptyList();
    }

    @Override
    public boolean isEnabled()
    {
        return false;
    }

    @Override
    public List<AuditableEvent> getEvents(BatchStatement batch, QueryState queryState, BatchQueryOptions queryOptions)
    {
        return Collections.emptyList();
    }

    @Override
    public List<AuditableEvent> getEventsForPrepare(CQLStatement statement, String queryString, QueryState queryState)
    {
        return Collections.emptyList();
    }

    @Override
    public Completable logEvents(List<AuditableEvent> events)
    {
        return Completable.complete();
    }

    @Override
    public Completable logFailedQuery(String queryString, QueryState state, Throwable e)
    {
        return Completable.complete();
    }

    @Override
    public Completable logFailedQuery(List<AuditableEvent> events, Throwable e)
    {
        return Completable.complete();
    }
}
