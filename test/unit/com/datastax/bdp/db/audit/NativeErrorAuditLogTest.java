/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.MD5Digest;

/**
 * Test the query failure logging of the DseQueryHandler. In most cases, failures are created
 * by using a CL of QUORUM on DSE Node
 */

public class NativeErrorAuditLogTest extends CQLTester
{
    private static String select = "SELECT * FROM test.test;";
    private static String insert = "INSERT INTO test.test (k, v) VALUES (1, 2);";
    private static QueryOptions quorumOpts = QueryOptions.forInternalCalls(ConsistencyLevel.QUORUM,
                                                                           Collections.<ByteBuffer>emptyList());
    @BeforeClass
    public static void setUpClass()
    {
        AuditLogger auditLogger = new AuditLogger(new InProcTestAuditWriter(),
                                                  new AuditFilter.Builder().fromConfig().build());

        // We don't need to set the AL to its old state as we'll be resetting
        // the in-memory writer after every test.
        DatabaseDescriptor.setAuditLoggerUnsafe(auditLogger);

        CQLTester.setUpClass();
    }

    @Before
    public void setUp()
    {
        if (Schema.instance.getKeyspaceMetadata("test") != null)
            QueryProcessor.processBlocking("DROP KEYSPACE test;", ConsistencyLevel.ONE);
        QueryProcessor.processBlocking("CREATE KEYSPACE test WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':'3'};",
                               ConsistencyLevel.ONE);
        QueryProcessor.processBlocking("CREATE TABLE test.test (k int PRIMARY KEY, v int);", ConsistencyLevel.ONE);

        InProcTestAuditWriter.reset();
    }

    private static ClientState getClientState() throws AuthenticationException
    {
        ClientState clientState = ClientState.forExternalCalls(new InetSocketAddress(9042), null);
        clientState.login(AuthenticatedUser.ANONYMOUS_USER);
        return clientState;
    }

    private static QueryState getQueryState() throws AuthenticationException
    {
        return new QueryState(getClientState());
    }

    private static void checkQueryFailureAuditEntry()
    {
        Assert.assertTrue(InProcTestAuditWriter.hasEvents());
        Assert.assertEquals(CoreAuditableEventType.REQUEST_FAILURE, InProcTestAuditWriter.lastEvent().getType());
    }

    @Test
    public void checkProcess() throws Exception
    {
        QueryHandler queryHandler = QueryProcessor.instance;

        try
        {
            TPCUtils.blockingGet(queryHandler.process(select, getQueryState(), quorumOpts, null, System.nanoTime()));
            Assert.fail("Expecting RequestExecutionException or RequestValidationException");
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            checkQueryFailureAuditEntry();
        }
    }

    @Test
    public void checkProcessBadQuery() throws Exception
    {
        QueryHandler queryHandler = QueryProcessor.instance;

        try
        {
            TPCUtils.blockingGet(queryHandler.process("SELECT * FROM nothing.nothing;", getQueryState(), quorumOpts, null, System.nanoTime()));
            Assert.fail("Expecting RequestExecutionException or RequestValidationException");
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            checkQueryFailureAuditEntry();
        }
    }

    @Test
    public void checkPrepared() throws Exception
    {
        QueryHandler queryHandler = QueryProcessor.instance;
        MD5Digest preparedId = TPCUtils.blockingGet(queryHandler.prepare(select, getClientState(), null)).statementId;
        ParsedStatement.Prepared prepared = queryHandler.getPrepared(preparedId);

        try
        {
            TPCUtils.blockingGet(queryHandler.processPrepared(prepared, getQueryState(), quorumOpts, null, System.nanoTime()));
            Assert.fail("Expecting RequestExecutionException or RequestValidationException");
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            checkQueryFailureAuditEntry();
        }
    }

    @Test
    public void checkProcessBatch() throws Exception
    {
        QueryHandler queryHandler = QueryProcessor.instance;


        ModificationStatement stmt = (ModificationStatement) QueryProcessor.getStatement(insert, getClientState()).statement;
        List<ModificationStatement> statements = Lists.newArrayList(stmt);
        BatchStatement batch = new BatchStatement(0, BatchStatement.Type.UNLOGGED, statements, Attributes.none());
        List<List<ByteBuffer>> values = Lists.newArrayList();
        values.add(Collections.<ByteBuffer>emptyList());
        List<Object> queriesOrIds = Lists.<Object>newArrayList(select);
        BatchQueryOptions options = BatchQueryOptions.withPerStatementVariables(
                                        QueryOptions.forInternalCalls(ConsistencyLevel.QUORUM,
                                                                      Collections.<ByteBuffer>emptyList()),
                                        values,
                                        queriesOrIds);

        try
        {
            TPCUtils.blockingGet(queryHandler.processBatch(batch, getQueryState(), options, null, System.nanoTime()));
            Assert.fail("Expecting RequestExecutionException or RequestValidationException");
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            checkQueryFailureAuditEntry();
        }
    }

    @Test
    public void checkPrepare() throws Exception
    {
        QueryHandler queryHandler = QueryProcessor.instance;

        try
        {
            TPCUtils.blockingGet(queryHandler.prepare("SELECT * FROM nothing.nothing;", getClientState(), null));
            Assert.fail("Expecting RequestExecutionException or RequestValidationException");
        }
        catch (RequestValidationException e)
        {
            checkQueryFailureAuditEntry();
        }
    }
}
