/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.util.List;

import io.reactivex.Completable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.service.QueryState;

public interface IAuditLogger
{
    public static final Logger logger = LoggerFactory.getLogger(IAuditLogger.class);

    /**
     * Set up this {@code IAuditLogger}
     */
    default void setup()
    {
    }

    /**
     * Checks if this logger is enabled.
     * @return {@code true} if this logger is enabled {@code false} otherwise.
     */
    boolean isEnabled();

    /**
     * Creates the auditable events associated to the CQL statement.
     *
     * @param statement the statement
     * @param queryString the raw CQL used to create the statement
     * @param queryState the query state
     * @param queryOptions the query options
     * @param boundNames the bound names
     * @return the auditable events associated to the CQL statement.
     */
    List<AuditableEvent> getEvents(CQLStatement statement,
                                   String queryString,
                                   QueryState queryState,
                                   QueryOptions queryOptions,
                                   List<ColumnSpecification> boundNames);

    /**
     * Creates the auditable events associated to the batch statement.
     *
     * @param batch the batch statement
     * @param queryState the query state
     * @param queryOptions the query options
     * @return the auditable events associated to the batch statement.
     */
    List<AuditableEvent> getEvents(BatchStatement batch,
                                   QueryState queryState,
                                   BatchQueryOptions queryOptions);

    /**
     * Creates the auditable events associated to the preparation of the CQL statement.
     *
     * @param statement the statement
     * @param queryString the raw CQL used to create the statement
     * @param queryState the query state
     * @return the auditable events associated to the preparation of the CQL statement
     */
    List<AuditableEvent> getEventsForPrepare(CQLStatement statement,
                                             String queryString,
                                             QueryState queryState);

    /**
     * Logs the specified events if needed.
     *
     * @param events the events to log
     * @return a completable
     */
    Completable logEvents(List<AuditableEvent> events);

    /**
     * Logs the specified event if needed.
     *
     * @param event the event to log
     * @return a completable
     */
    Completable logEvent(AuditableEvent event);

    /**
     * Logs a failed query for the specified query if needed.
     *
     * @param queryString the failed query
     * @param state the query state
     * @param e the exception
     * @return a completable
     */
    Completable logFailedQuery(String queryString, QueryState state, Throwable e);

    /**
     * Logs a failed query for the specified events if needed.
     *
     * @param events the failed events
     * @return a completable
     */
    Completable logFailedQuery(List<AuditableEvent> events, Throwable e);

    /**
     * Creates an {@code IAuditLogger} that use the specified writer and filter.
     * @param writer the audit writer used to log the events
     * @param filter the audit filter used to select the events to log
     * @return an {@code IAuditLogger} that use the specified writer and filter
     */
    public static IAuditLogger newInstance(IAuditWriter writer, IAuditFilter filter)
    {
        return new AuditLogger(writer, filter);
    }

    /**
     * Creates the {@code IAuditLogger} corresponding to the configuration.
     * @param config the configuration
     * @return the {@code IAuditLogger} corresponding to the configuration
     */
    public static IAuditLogger fromConfiguration(Config config)
    {
        AuditLoggingOptions auditLoggingOptions = config.audit_logging_options;
        if (!auditLoggingOptions.enabled && System.getProperty("cassandra.audit_writer") == null)
        {
            logger.info("Audit logging is disabled");
            return new NoopAuditLogger();
        }

        return AuditLogger.fromConfiguration(auditLoggingOptions);
    }
}
