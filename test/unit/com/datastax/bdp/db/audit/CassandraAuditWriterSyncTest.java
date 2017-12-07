/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.bdp.db.audit.CassandraAuditWriter.BatchingOptions;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.FunctionExecutionException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.exceptions.UnauthorizedException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CassandraAuditWriterSyncTest extends CassandraAuditWriterTester
{
    @BeforeClass
    public static void setup() throws Throwable
    {
        requireAuthentication();
        DatabaseDescriptor.setPermissionsValidity(9999);
        DatabaseDescriptor.setPermissionsUpdateInterval(9999);
        CassandraAuditWriter writer = new CassandraAuditWriter(0, ConsistencyLevel.ONE, BatchingOptions.SYNC);
        AuditFilter filter = AuditFilter.builder().excludeKeyspace(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                                                  .excludeKeyspace(SchemaConstants.SCHEMA_KEYSPACE_NAME)
                                                  .build();
        AuditLogger auditLogger = new AuditLogger(writer, filter);
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
