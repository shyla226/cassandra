/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.bdp.db.audit.cql3.AuditableEventGenerator;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class Cql3AuditLoggerTest extends CQLTester
{

    protected static final List<ByteBuffer> NONE = Collections.emptyList();
    private static boolean keyspaceInitialized = false;
    static String cf = "test_cf";
    static String counter_cf = "test_counter_cf";
    static String ks = "test_ks";
    ClientState clientState;
    IAuditWriter writer;
    AuditLogger auditLogger;
    AuthenticatedUser user;
    InetAddress eventSource;

    String getCreateTableStatement(String name)
    {
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                " id int, " +
                " field0 text," +
                " PRIMARY KEY(id));",
                name);
    }

    @Before
    public void setupKeyspaceAndLogger() throws Exception
    {
        AuditLogger.setForceAuditLogging(false);
        if (!keyspaceInitialized)
        {
            QueryProcessor.executeInternal("CREATE KEYSPACE " + ks + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
            QueryProcessor.executeInternal("CREATE TABLE " + ks + ".test_cf (id int, field0 text, field1 text, PRIMARY KEY(id));");
            QueryProcessor.executeInternal("CREATE TABLE " + ks + ".test_counter_cf (id int, field0 counter, field1 counter, PRIMARY KEY(id));");
            keyspaceInitialized = true;
        }

        user = new AuthenticatedUser("user" + System.currentTimeMillis());
        eventSource = InetAddress.getLocalHost();
        SocketAddress address = new InetSocketAddress(eventSource, 9999);
        clientState = new ClientState((InetSocketAddress)address, null){
            @Override
            public AuthenticatedUser getUser()
            {
                return user;
            }
        };
        clientState.setKeyspace(ks);

        writer = new InProcTestAuditWriter();
        auditLogger = new AuditLogger(writer, AuditFilter.builder().build());
        InProcTestAuditWriter.reset();
        AuditLogger.setForceAuditLogging(true);
    }

    @After
    public void clearSystemProperty()
    {
        AuditLogger.setForceAuditLogging(false);
    }

    protected void assertLastEventProperties(AuditableEventType type, String keyspace, String columnFamily, String operation, ConsistencyLevel cl)
    {
        AuditableEvent event = InProcTestAuditWriter.lastEvent();
        assertEquals(type, event.getType());
        assertEquals(user.getName(), event.getUser());
        assertEquals(eventSource.toString(), event.getSource());

        if (null != keyspace)
            assertEquals(keyspace, event.getKeyspace());

        if (null != columnFamily)
            assertEquals(columnFamily, event.getColumnFamily());

        if (null != operation)
            assertEquals(operation, event.getOperation());

        assertEquals(cl, event.getConsistencyLevel());
    }

    protected void assertAllEventsInSameBatch(Stack<AuditableEvent> events)
    {
        UUID batchId = events.get(0).getBatchId();
        assertNotNull(batchId);
        for (AuditableEvent event : events )
        {
            assertEquals(batchId, event.getBatchId());
        }
    }

    protected void assertEventProperties(AuditableEvent event,
                                         String keyspace,
                                         String columnFamily,
                                         AuditableEventType type,
                                         String operation,
                                         ConsistencyLevel cl) throws Exception
    {
        assertEquals(columnFamily, event.getColumnFamily());
        assertEquals(keyspace, event.getKeyspace());
        assertEquals(type, event.getType());
        // whitespace in batch cql strings are normalised by CQL handler impls, so this test
        // is sensitive. If the assertion fails, check the whitespace in the tested string
        // as what is logged may not be exactly the same as the statement in the case of
        // BATCH statements
        assertEquals(operation, event.getOperation());
        assertEquals(cl, event.getConsistencyLevel());
    }

    protected void logStatement(ClientState clientState, String queryString, ConsistencyLevel cl) throws Exception
    {
        logStatement(clientState, queryString, Collections.emptyList(), cl);
    }

    protected void logStatement(ClientState clientState, String queryString, List<ByteBuffer> variables, ConsistencyLevel cl)
            throws Exception
    {
        QueryHandler handler = QueryProcessor.instance;
        QueryState queryState = new QueryState(clientState);
        AuditableEventGenerator logger = new AuditableEventGenerator();

        ResultMessage.Prepared prepared = TPCUtils.blockingGet(handler.prepare(queryString, clientState, null));
        CQLStatement stmt = handler.getPrepared(prepared.statementId).statement;

        ParsedStatement.Prepared preparedStatement = QueryProcessor.instance.getPrepared(prepared.statementId);
        List<ColumnSpecification> boundNames = preparedStatement.boundNames;
        for (AuditableEvent event : logger.getEvents(stmt, queryState, queryString, variables, boundNames, cl))
        {
            auditLogger.recordEvent(event);
        }
    }

    protected void logPrepareStatement(ClientState clientState, String queryString) throws Exception
    {
        QueryHandler handler = QueryProcessor.instance;
        AuditableEventGenerator logger = new AuditableEventGenerator();
        ResultMessage.Prepared  prepared = TPCUtils.blockingGet(handler.prepare(queryString, clientState, null));
        CQLStatement preparedStmt = handler.getPrepared(prepared.statementId).statement;
        for (AuditableEvent event : logger.getEventsForPrepare(preparedStmt, clientState, queryString))
        {
            auditLogger.recordEvent(event);
        }
    }

    @Test
    public void listRolesIsLogged() throws Exception
    {
        String stmt = "LIST ROLES";
        logStatement(clientState, stmt, null);
        assertLastEventProperties(CoreAuditableEventType.LIST_ROLES, null, null, stmt, null);
    }

    @Test
    public void createRoleIsLogged() throws Exception
    {
        String stmt = "CREATE ROLE foo WITH PASSWORD = 'bar' AND LOGIN = true";
        logStatement(clientState, stmt, null);
        String obfuscated = "CREATE ROLE foo WITH PASSWORD = '*****' AND LOGIN = true";
        assertLastEventProperties(CoreAuditableEventType.CREATE_ROLE, null, null, obfuscated, null);
    }

    @Test
    public void alterRoleIsLogged() throws Exception
    {
        String stmt = "ALTER ROLE foo WITH PASSWORD = 'baz' AND LOGIN = true";
        logStatement(clientState, stmt, null);
        String obfuscated = "ALTER ROLE foo WITH PASSWORD = '*****' AND LOGIN = true";
        assertLastEventProperties(CoreAuditableEventType.ALTER_ROLE, null, null, obfuscated, null);
    }

    @Test
    public void createRoleWithExtraWhitespaceIsLogged() throws Exception
    {
        String stmt = "CREATE ROLE foo WITH PASSWORD  =  'bar' AND LOGIN = true";
        logStatement(clientState, stmt, null);
        String obfuscated = "CREATE ROLE foo WITH PASSWORD = '*****' AND LOGIN = true";
        assertLastEventProperties(CoreAuditableEventType.CREATE_ROLE, null, null, obfuscated, null);
    }

    @Test
    public void alterRoleWithExtraWhitespaceIsLogged() throws Exception
    {
        String stmt = "ALTER ROLE foo WITH PASSWORD  =  'baz' AND LOGIN = true";
        logStatement(clientState, stmt, null);
        String obfuscated = "ALTER ROLE foo WITH PASSWORD = '*****' AND LOGIN = true";
        assertLastEventProperties(CoreAuditableEventType.ALTER_ROLE, null, null, obfuscated, null);
    }

    @Test
    public void createUserWithExtraWhitespaceIsLogged() throws Exception
    {
        String stmt = "CREATE USER 'foo' WITH  PASSWORD  'bar'";
        logStatement(clientState, stmt, null);
        String obfuscated = "CREATE USER 'foo' WITH  PASSWORD '*****'";
        assertLastEventProperties(CoreAuditableEventType.CREATE_ROLE, null, null, obfuscated, null);
    }

    @Test
    public void alterUserWithExtraWhitespaceIsLogged() throws Exception
    {
        String stmt = "ALTER USER 'foo' WITH  PASSWORD  'baz'";
        logStatement(clientState, stmt, null);
        String obfuscated = "ALTER USER 'foo' WITH  PASSWORD '*****'";
        assertLastEventProperties(CoreAuditableEventType.ALTER_ROLE, null, null, obfuscated, null);
    }

    @Test
    public void dropRoleIsLogged() throws Exception
    {
        String stmt = "DROP ROLE foo";
        logStatement(clientState, stmt, null);
        assertLastEventProperties(CoreAuditableEventType.DROP_ROLE, null, null, stmt, null);
    }

    @Test
    public void listPermissionsIsLogged() throws Exception
    {
        String stmt = "LIST ALL PERMISSIONS OF user";
        logStatement(clientState, stmt, null);
        assertLastEventProperties(CoreAuditableEventType.LIST_PERMISSIONS, null, null, stmt, null);
    }

    @Test
    public void grantStatementIsLogged() throws Exception
    {
        String stmt = "GRANT ALL PERMISSIONS ON ks.cf TO user";
        logStatement(clientState, stmt, null);
        assertLastEventProperties(CoreAuditableEventType.GRANT, null, null, stmt, null);
    }

    @Test
    public void revokeStatementIsLogged() throws Exception
    {
        String stmt = "REVOKE ALL PERMISSIONS ON ks.cf FROM user";
        logStatement(clientState, stmt, null);
        assertLastEventProperties(CoreAuditableEventType.REVOKE, null, null, stmt, null);
    }

    @Test
    public void createKeyspaceIsLogged() throws Exception
    {
        String stmt =
                "CREATE KEYSPACE ks_1 " +
                " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
        logStatement(clientState, stmt, NONE, null);
        assertLastEventProperties(CoreAuditableEventType.ADD_KS, "ks_1", null, stmt, null);
    }

    @Test
    public void logBatchSpecifyingConsistencyLevel() throws Exception
    {
        String stmt =
          "BEGIN BATCH " + //USING CONSISTENCY IS NOT WORKING IN CQL3
          "   INSERT INTO test_cf (id, field0) VALUES (0, 'INSERT') ;" +
          "   DELETE field0 FROM test_cf WHERE id = 1 ;" +
          "APPLY BATCH;";

        logStatement(clientState, stmt, NONE, ConsistencyLevel.ONE);
        Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
        assertEquals(2, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), ks, cf, CoreAuditableEventType.CQL_DELETE,
                "DELETE field0 FROM test_cf WHERE id = 1 ;", ConsistencyLevel.ONE);
        assertEventProperties(events.pop(), ks, cf, CoreAuditableEventType.CQL_UPDATE,
                "INSERT INTO test_cf ( id , field0 ) VALUES ( 0 , 'INSERT' ) ;", ConsistencyLevel.ONE);
    }

    @Test
    public void eachStatementInCounterBatchIsLogged() throws Exception
    {
        // 4 stmts, 3 distinct keys
        String stmt =
                "BEGIN COUNTER BATCH " +
                        "   UPDATE test_counter_cf SET field0 = field0 + 1 WHERE id = 0;" +
                        "   UPDATE test_counter_cf SET field0 = field0 + 1 WHERE id = 0;" +
                        "   UPDATE test_counter_cf SET field0 = field0 + 1 WHERE id = 1;" +
                        "   UPDATE test_counter_cf SET field0 = field0 + 1 WHERE id = 2;" +
                        "APPLY BATCH;";
        logStatement(clientState, stmt, NONE, ConsistencyLevel.ONE);
        Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
        assertEquals(4, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), ks, counter_cf, CoreAuditableEventType.CQL_UPDATE,
                "UPDATE test_counter_cf SET field0 = field0 + 1 WHERE id = 2 ;", ConsistencyLevel.ONE);
        assertEventProperties(events.pop(), ks, counter_cf, CoreAuditableEventType.CQL_UPDATE,
                "UPDATE test_counter_cf SET field0 = field0 + 1 WHERE id = 1 ;", ConsistencyLevel.ONE);
        assertEventProperties(events.pop(), ks, counter_cf, CoreAuditableEventType.CQL_UPDATE,
                "UPDATE test_counter_cf SET field0 = field0 + 1 WHERE id = 0 ;", ConsistencyLevel.ONE);
        assertEventProperties(events.pop(), ks, counter_cf, CoreAuditableEventType.CQL_UPDATE,
                "UPDATE test_counter_cf SET field0 = field0 + 1 WHERE id = 0 ;", ConsistencyLevel.ONE);
    }


    @Test
    public void createTriggerIsLogged() throws Exception
    {
        String stmt = "CREATE TRIGGER test_trigger ON test_cf USING 'some_class'";
        logStatement(clientState, stmt, NONE, null);
        assertLastEventProperties(CoreAuditableEventType.CREATE_TRIGGER, null, null, stmt, null);
    }

    @Test
    public void dropTriggerIsLogged() throws Exception
    {
        String stmt = "DROP TRIGGER test_trigger ON test_cf";
        logStatement(clientState, stmt, NONE, null);
        assertLastEventProperties(CoreAuditableEventType.DROP_TRIGGER, null, null, stmt, null);
    }

    @Test
    public void singleInsertIsLogged() throws Exception
    {
        String stmt = "INSERT INTO test_cf (id, field0) VALUES (0, 'some value');";
        logStatement(clientState, stmt, NONE, ConsistencyLevel.ONE);
        assertLastEventProperties(CoreAuditableEventType.CQL_UPDATE, ks, cf, stmt, ConsistencyLevel.ONE);
    }

    @Test
    public void insertWithVariablesIsLogged() throws Exception
    {
        String stmt = "INSERT INTO test_cf (id, field0) VALUES (?, ?)";
        List<ByteBuffer> values = new ArrayList<>();
        values.add(ByteBufferUtil.bytes(0));
        values.add(ByteBufferUtil.bytes("'some value'"));
        logStatement(clientState, stmt, values, ConsistencyLevel.ONE);

        String expected = "INSERT INTO test_cf (id, field0) VALUES (?, ?) [id=0,field0='some value']";
        assertLastEventProperties(CoreAuditableEventType.CQL_UPDATE, ks, cf, expected, ConsistencyLevel.ONE);
    }

    @Test
    public void singleUpdateIsLogged() throws Exception
    {
        String stmt = "UPDATE test_cf SET field0 = 'some value' WHERE id = 0;";
        logStatement(clientState, stmt, NONE, ConsistencyLevel.ONE);
        assertLastEventProperties(CoreAuditableEventType.CQL_UPDATE, ks, cf, stmt, ConsistencyLevel.ONE);
    }

    @Test
    public void updateWithVariablesIsLogged() throws Exception
    {
        String stmt = "UPDATE test_cf SET field0=? WHERE id=?";
        List<ByteBuffer> values = new ArrayList<ByteBuffer>();
        values.add(ByteBufferUtil.bytes("'some value'"));
        values.add(ByteBufferUtil.bytes(0));
        logStatement(clientState, stmt, values, ConsistencyLevel.ONE);

        String expected = "UPDATE test_cf SET field0=? WHERE id=? [field0='some value',id=0]";
        assertLastEventProperties(CoreAuditableEventType.CQL_UPDATE, ks, cf, expected, ConsistencyLevel.ONE);
    }

    @Test
    public void selectStatementIsLogged() throws Exception
    {
        String stmt = "SELECT * FROM test_cf;";
        logStatement(clientState, stmt, NONE, ConsistencyLevel.ONE);
        assertLastEventProperties(CoreAuditableEventType.CQL_SELECT, ks, cf, stmt, ConsistencyLevel.ONE);
    }

    @Test
    public void selectWithVariablesIsLogged() throws Exception
    {
        String stmt = "SELECT * FROM test_cf WHERE id=?";
        List<ByteBuffer> values = new ArrayList<ByteBuffer>();
        values.add(ByteBufferUtil.bytes(0));
        logStatement(clientState, stmt, values, ConsistencyLevel.ONE);

        String expected = "SELECT * FROM test_cf WHERE id=? [id=0]";
        assertLastEventProperties(CoreAuditableEventType.CQL_SELECT, ks, cf, expected, ConsistencyLevel.ONE);
    }

    @Test
    public void singleDeleteIsLogged() throws Exception
    {
        String stmt = "DELETE field0 FROM test_cf WHERE id = 0;";
        logStatement(clientState, stmt, NONE, ConsistencyLevel.ONE);
        assertLastEventProperties(CoreAuditableEventType.CQL_DELETE, ks, cf, stmt, ConsistencyLevel.ONE);
    }

    @Test
    public void deleteWithVariablesIsLogged() throws Exception
    {
        String stmt = "DELETE FROM test_cf WHERE id=?";
        List<ByteBuffer> values = new ArrayList<ByteBuffer>();
        values.add(ByteBufferUtil.bytes(0));
        logStatement(clientState, stmt, values, ConsistencyLevel.ONE);

        String expected = "DELETE FROM test_cf WHERE id=? [id=0]";
        assertLastEventProperties(CoreAuditableEventType.CQL_DELETE, ks, cf, expected, ConsistencyLevel.ONE);
    }

    @Test
    public void eachStatementInBatchIsLogged() throws Exception
    {
        // 4 stmts, 3 distinct keys
        String stmt =
          "BEGIN BATCH " +
            "   INSERT INTO test_cf (id, field0) VALUES (0, 'INSERT') ;" +
            "   UPDATE test_cf SET field1 = 'some other value' WHERE id = 0 ;" +
            "   INSERT INTO test_cf (id, field0) VALUES (1, 'yet another value') ;" +
            "   DELETE field0 FROM test_cf WHERE id = 2 ;" +
          "APPLY BATCH;";
        logStatement(clientState, stmt, NONE, ConsistencyLevel.QUORUM);
        Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
        assertEquals(4, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), ks, cf, CoreAuditableEventType.CQL_DELETE,
                "DELETE field0 FROM test_cf WHERE id = 2 ;", ConsistencyLevel.QUORUM);
        assertEventProperties(events.pop(), ks, cf, CoreAuditableEventType.CQL_UPDATE,
                "INSERT INTO test_cf ( id , field0 ) VALUES ( 1 , 'yet another value' ) ;", ConsistencyLevel.QUORUM);
        assertEventProperties(events.pop(), ks, cf, CoreAuditableEventType.CQL_UPDATE,
                "UPDATE test_cf SET field1 = 'some other value' WHERE id = 0 ;", ConsistencyLevel.QUORUM);
        assertEventProperties(events.pop(), ks, cf, CoreAuditableEventType.CQL_UPDATE,
                "INSERT INTO test_cf ( id , field0 ) VALUES ( 0 , 'INSERT' ) ;", ConsistencyLevel.QUORUM);
    }

    @Test
    public void logBatchContainingMultipleColumnFamilies() throws Exception
    {
        // Add a second cf
        QueryProcessor.executeInternal("create table " + ks + ".other_cf (id int, field0 text, field1 text, primary key(id));");
        InProcTestAuditWriter.reset();

        // 4 stmts, 1 distinct key, 2 CFs
        String stmt =
                "BEGIN BATCH " +
            "   INSERT INTO test_cf (id, field0) VALUES (0, 'some value') ;" +
            "   UPDATE test_cf SET field0 = 'some other value' WHERE id = 0 ;" +
            "   INSERT INTO other_cf (id, field0) VALUES (0, 'some value') ;" +
            "   UPDATE other_cf SET field0 = 'some other value' WHERE id = 0 ;" +
          "APPLY BATCH;";
        logStatement(clientState, stmt, NONE, ConsistencyLevel.QUORUM);
        Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
        assertEquals(4, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), ks, "other_cf", CoreAuditableEventType.CQL_UPDATE,
                "UPDATE other_cf SET field0 = 'some other value' WHERE id = 0 ;", ConsistencyLevel.QUORUM);
        assertEventProperties(events.pop(), ks, "other_cf", CoreAuditableEventType.CQL_UPDATE,
                "INSERT INTO other_cf ( id , field0 ) VALUES ( 0 , 'some value' ) ;", ConsistencyLevel.QUORUM);
        assertEventProperties(events.pop(), ks, cf, CoreAuditableEventType.CQL_UPDATE,
                "UPDATE test_cf SET field0 = 'some other value' WHERE id = 0 ;", ConsistencyLevel.QUORUM);
        assertEventProperties(events.pop(), ks, cf, CoreAuditableEventType.CQL_UPDATE,
                "INSERT INTO test_cf ( id , field0 ) VALUES ( 0 , 'some value' ) ;", ConsistencyLevel.QUORUM);
    }

    @Test
    public void logBatchContainingMultipleKeyspaces() throws Exception
    {
        // Add another ks & cf
        QueryProcessor.executeInternal("create keyspace other_ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        QueryProcessor.executeInternal("create table other_ks.other_cf (id int, field0 text, field1 text, primary key(id));");
        InProcTestAuditWriter.reset();

        // 2 stmts, 1 distinct key, 2 CFs in different Keyspaces
        String stmt =
          "BEGIN BATCH " +
            "   INSERT INTO test_ks.test_cf (id, field0) VALUES (0, 'some value') ;" +
            "   INSERT INTO other_ks.other_cf (id, field0) VALUES (0, 'some value') ;" +
          "APPLY BATCH;";

        logStatement(clientState, stmt, NONE, ConsistencyLevel.ONE);
        Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
        assertEquals(2, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), "other_ks", "other_cf", CoreAuditableEventType.CQL_UPDATE,
                "INSERT INTO other_ks.other_cf ( id , field0 ) VALUES ( 0 , 'some value' ) ;", ConsistencyLevel.ONE);
        assertEventProperties(events.pop(), ks, cf, CoreAuditableEventType.CQL_UPDATE,
                "INSERT INTO test_ks.test_cf ( id , field0 ) VALUES ( 0 , 'some value' ) ;", ConsistencyLevel.ONE);
    }

    @Test
    public void truncateWithoutExplicitKeyspaceIsLogged()throws Exception
    {
        String stmt = "TRUNCATE test_cf;";
        logStatement(clientState, stmt, NONE, null);
        assertLastEventProperties(CoreAuditableEventType.TRUNCATE, ks, cf, stmt, null);
    }

    @Test
    public void truncateWithExplicitKeyspaceIsLogged()throws Exception
    {
        String stmt = "TRUNCATE test_ks.test_cf;";
        logStatement(clientState, stmt, NONE, null);
        assertLastEventProperties(CoreAuditableEventType.TRUNCATE, ks, cf, stmt, null);
    }

    @Test
    public void dropKeyspaceIsLogged() throws Exception
    {
        String stmt = "DROP KEYSPACE ks_1 ;";
        logStatement(clientState, stmt, NONE, null);
        assertLastEventProperties(CoreAuditableEventType.DROP_KS, "ks_1", null, stmt, null);
    }

    @Test
    public void createTableIsLogged() throws Exception
    {
        String stmt = getCreateTableStatement("table_1");
        logStatement(clientState, stmt, NONE, null);
        assertLastEventProperties(CoreAuditableEventType.ADD_CF, ks, "table_1", stmt, null);
    }

    @Test
    public void dropTableIsLogged() throws Exception
    {
        String stmt = "DROP TABLE table_1 ;";
        logStatement(clientState, stmt, NONE, null);
        assertLastEventProperties(CoreAuditableEventType.DROP_CF, ks, "table_1", stmt, null);
    }

    @Test
    public void alterTableIsLogged() throws Exception
    {
        String stmt = "ALTER TABLE test_cf ALTER field0 TYPE blob;";
        logStatement(clientState, stmt, NONE, null);
        assertLastEventProperties(CoreAuditableEventType.UPDATE_CF, ks, cf, stmt, null);
    }

    @Test
    public void createIndexIsLogged() throws Exception
    {
        String stmt = "CREATE INDEX test_idx ON test_cf (field0);";
        logStatement(clientState, stmt, NONE, null);
        assertLastEventProperties(CoreAuditableEventType.CREATE_INDEX, ks, cf, stmt, null);
    }

    @Test
    public void dropIndexIsLogged() throws Exception
    {
        QueryProcessor.executeInternal("CREATE INDEX IF NOT EXISTS test_idx ON " + ks + ".test_cf (field0);");
        String stmt = "DROP INDEX test_idx;";
        logStatement(clientState, stmt, NONE, null);
        // Column family name for index isn't accessible
        assertLastEventProperties(CoreAuditableEventType.DROP_INDEX, ks, null, stmt, null);
    }

    @Test
    public void useIsLogged() throws Exception
    {
        String stmt = "USE test_ks";
        logStatement(clientState, stmt, NONE, null);
        assertLastEventProperties(CoreAuditableEventType.SET_KS, ks, null, stmt, null);
    }

    @Test
    public void prepareSingleStatementIsLogged() throws Exception
    {
        String stmt = "SELECT * FROM test_cf WHERE id=?";
        logPrepareStatement(clientState, stmt);
        assertLastEventProperties(CoreAuditableEventType.CQL_PREPARE_STATEMENT, ks, cf, stmt, null);
    }

    @Test
    public void prepareBatchIsLogged() throws Exception
    {
        String stmt =
            "BEGIN BATCH " +
            "   INSERT INTO test_cf (id, field0) VALUES (0, 'INSERT') ;" +
            "   UPDATE test_cf SET field1 = 'some other value' WHERE id = 0 ;" +
            "   INSERT INTO test_cf (id, field0) VALUES (1, 'yet another value') ;" +
            "   DELETE field0 FROM test_cf WHERE id = 2 ;" +
            "APPLY BATCH;";
        logPrepareStatement(clientState, stmt);
        Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
        assertEquals(4, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), ks, cf, CoreAuditableEventType.CQL_PREPARE_STATEMENT,
                "DELETE field0 FROM test_cf WHERE id = 2 ;", null);
        assertEventProperties(events.pop(), ks, cf, CoreAuditableEventType.CQL_PREPARE_STATEMENT,
                "INSERT INTO test_cf ( id , field0 ) VALUES ( 1 , 'yet another value' ) ;", null);
        assertEventProperties(events.pop(), ks, cf, CoreAuditableEventType.CQL_PREPARE_STATEMENT,
                "UPDATE test_cf SET field1 = 'some other value' WHERE id = 0 ;", null);
        assertEventProperties(events.pop(), ks, cf, CoreAuditableEventType.CQL_PREPARE_STATEMENT,
                "INSERT INTO test_cf ( id , field0 ) VALUES ( 0 , 'INSERT' ) ;", null);
    }
}
