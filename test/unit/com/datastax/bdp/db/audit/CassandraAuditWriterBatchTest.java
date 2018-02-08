/*
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

import static org.junit.Assert.assertEquals;

public class CassandraAuditWriterBatchTest extends CassandraAuditWriterTester
{
    private static final long TIMEOUT = 2000;

    @BeforeClass
    public static void setup()
    {
        requireAuthentication();
        DatabaseDescriptor.setPermissionsValidity(9999);
        DatabaseDescriptor.setPermissionsUpdateInterval(9999);

        final int flushPeriod = 500;
        final int batchSize = 2;
        BatchingOptions options = new BatchingOptions(batchSize, flushPeriod, 100);
        CassandraAuditWriter writer = new CassandraAuditWriter(0, ConsistencyLevel.ONE, options);

        IAuditFilter filter = AuditFilters.excludeKeyspaces(SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                                            SchemaConstants.SCHEMA_KEYSPACE_NAME);
        IAuditLogger auditLogger = IAuditLogger.newInstance(writer, filter);
        DatabaseDescriptor.setAuditLoggerUnsafe(auditLogger);
        requireNetwork();
        auditLogger.setup();
    }

    @Override
    protected void waitForLogging(int numberOfQueries) throws Throwable
    {
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < TIMEOUT)
        {
            if (getLoggedEventCount() == numberOfQueries)
                break;
            Thread.yield();
        }
        assertEquals(String.format("Expected %s events to be written, but condition not met after %s ms", numberOfQueries, TIMEOUT),
                     numberOfQueries, getLoggedEventCount());
    }

    private int getLoggedEventCount() throws Throwable
    {
        return fetchAuditEvents().size();
    }
}
