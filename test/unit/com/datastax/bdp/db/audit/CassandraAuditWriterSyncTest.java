/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import org.junit.BeforeClass;

import com.datastax.bdp.db.audit.CassandraAuditWriter.BatchingOptions;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.schema.SchemaConstants;

public class CassandraAuditWriterSyncTest extends CassandraAuditWriterTester
{
    @BeforeClass
    public static void setup() throws Throwable
    {
        requireAuthentication();
        DatabaseDescriptor.setPermissionsValidity(9999);
        DatabaseDescriptor.setPermissionsUpdateInterval(9999);
        CassandraAuditWriter writer = new CassandraAuditWriter(0, ConsistencyLevel.ONE, BatchingOptions.SYNC);
        IAuditFilter filter = AuditFilters.excludeKeyspace(SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                                           SchemaConstants.SCHEMA_KEYSPACE_NAME);
        IAuditLogger auditLogger = IAuditLogger.newInstance(writer, filter);
        DatabaseDescriptor.setAuditLoggerUnsafe(auditLogger);
        requireNetwork();
        auditLogger.setup();
    }

    /**
     * Waits for the specific amount of queries to be logged.
     * @param numberOfQueries the number of queries to be logged.
     */
    protected void waitForLogging(int numberOfQueries)
    {

    }
}
