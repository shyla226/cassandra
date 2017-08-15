/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.audit;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.apollo.audit.AuditFilter;
import com.datastax.apollo.audit.AuditLogger;
import com.datastax.apollo.audit.AuditableEvent;
import com.datastax.apollo.audit.AuditableEventType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.utils.ByteBufferUtil;

import static com.datastax.apollo.audit.AuditLoggingTestSupport.assertAllEventsInSameBatch;
import static com.datastax.apollo.audit.AuditLoggingTestSupport.assertEventProperties;
import static com.datastax.apollo.audit.AuditLoggingTestSupport.assertMatchingEventInList;
import static org.junit.Assert.assertEquals;

public abstract class AbstractCql3AuditLoggingTest extends CQLTester
{
    // Black box tests of audit logging for CQL
    // Tests here only cover CQL3

    String ks = "keyspace1";
    String cf = "Standard1"; // mixed case to test cql3 quoting
    String ccf = "Counters";
    ByteBuffer key = ByteBufferUtil.bytes(0);


    @BeforeClass
    public static void setUpClass()
    {
        AuditFilter filter = new AuditFilter.Builder().excludeKeyspace("system.*")
                                                      .build();
        AuditLogger auditLogger = new AuditLogger(new InProcTestAuditWriter(),
                                                  filter);

        // We don't need to set the AL to its old state as we'll be resetting
        // the in-memory writer after every test.
        DatabaseDescriptor.setAuditLoggerUnsafe(auditLogger);

        CQLTester.setUpClass();
    }

    @Before
    public void setUp() throws Exception
    {
        String createKs = String.format("CREATE KEYSPACE %s WITH replication = {" +
                                        "  'class': 'SimpleStrategy', " +
                                        "  'replication_factor': '1'" +
                                        "}", ks);
        String regularCfSchema = String.format("CREATE TABLE %s.\"%s\" (" +
                                               "  k int," +
                                               "  v int, " +
                                               "  PRIMARY KEY(k))", ks, cf);
        String counterCfSchema = String.format("CREATE TABLE %s.\"%s\" (column1 text, column2 text, " +
                                               "count counter, PRIMARY KEY(column1, column2))",
                                               ks, ccf);
        String indexStatement = String.format("CREATE INDEX ON %s.\"%s\"(v)", ks, cf);
        try
        {
            QueryProcessor.processBlocking(createKs, ConsistencyLevel.ONE);
            QueryProcessor.processBlocking(regularCfSchema, ConsistencyLevel.ONE);
            QueryProcessor.processBlocking(counterCfSchema, ConsistencyLevel.ONE);
            QueryProcessor.processBlocking(indexStatement, ConsistencyLevel.ONE);
        }
        catch (AlreadyExistsException e)
        {
            // ignore
        }
        InProcTestAuditWriter.reset();
    }

    abstract void closeConnection();
    abstract void executeCql3Query(String cql, boolean compression);
    abstract PreparedStmtHandle prepareCql3Query(String cql, boolean compression);
    abstract void executePreparedCql3Query(PreparedStmtHandle handle, List<Object> variables);

    @Test
    public void execute_cql3_queryWithoutCompressionIsLogged() throws Exception
    {
        String cql = "SELECT * FROM \"Standard1\"";
        executeCql3Query(cql, false);
        assertMatchingEventInList(getEvents(), AuditableEventType.CQL_SELECT, ks, cf, cql);
    }

    @Test
    public void execute_cql3_BatchIsLogged() throws Exception
    {
        String cql = "BEGIN BATCH " +
                "  INSERT INTO \"Standard1\" (k, v) VALUES (0, 24) ;" +
                "  UPDATE \"Standard1\" SET v = 48 WHERE k = 1 ;" +
                "  DELETE FROM \"Standard1\" WHERE k = 2 ;" +
                "APPLY BATCH";
        executeCql3Query(cql, false);
        Stack<AuditableEvent> events = getEvents();
        assertEquals(3, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), AuditableEventType.CQL_DELETE, ks, cf, "DELETE FROM Standard1 WHERE k = 2 ;");
        assertEventProperties(events.pop(), AuditableEventType.CQL_UPDATE, ks, cf, "UPDATE Standard1 SET v = 48 WHERE k = 1 ;");
        assertEventProperties(events.pop(), AuditableEventType.CQL_UPDATE, ks, cf, "INSERT INTO Standard1 ( k , v ) VALUES ( 0 , 24 ) ;");
    }

    @Test
    public void prepare_cql3_queryWithoutCompressionIsLogged() throws Exception
    {
        String cql = "SELECT * FROM \"Standard1\"";
        prepareCql3Query(cql, false);
        assertMatchingEventInList(getEvents(), AuditableEventType.CQL_PREPARE_STATEMENT, ks, cf, cql);
    }

    @Test
    public void execute_prepared_cql3_SelectWithoutBindVariablesIsLogged() throws Exception
    {
        String cql = "SELECT * FROM \"Standard1\"";
        PreparedStmtHandle handle = prepareCql3Query(cql, false);
        executePreparedCql3Query(handle, Collections.<Object>emptyList());
        assertMatchingEventInList(getEvents(), AuditableEventType.CQL_SELECT, ks, cf, cql);
    }

    @Test
    public void execute_prepared_cql3_SelectWithBindVariablesIsLogged() throws Exception
    {
        String cql = "SELECT * FROM \"Standard1\" WHERE k = ?";
        List<Object> variables = getVarsList(0);

        PreparedStmtHandle handle = prepareCql3Query(cql, false);
        executePreparedCql3Query(handle, variables);
        String expected = cql + " [k=0]";
        assertMatchingEventInList(getEvents(), AuditableEventType.CQL_SELECT, ks, cf, expected);
    }

    @Ignore("APOLLO-903")
    @Test
    public void execute_prepared_cql3_InsertWithoutBindVariablesIsLogged() throws Exception
    {
        String cql = "INSERT INTO \"Standard1\" (k, v) VALUES (1, 1) USING TIMESTAMP 0 AND TTL 100";
        List<Object> variables = Collections.EMPTY_LIST;

        PreparedStmtHandle handle = prepareCql3Query(cql, false);
        executePreparedCql3Query(handle, variables);
        assertMatchingEventInList(getEvents(), AuditableEventType.CQL_UPDATE, ks, cf, cql);
    }

    @Test
    public void execute_prepared_cql3_InsertWithBindVariablesIsLogged() throws Exception
    {
        String cql = "INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) USING TIMESTAMP ? AND TTL ?";
        List<Object> variables = getVarsList(0, 24, 999l, 1);

        PreparedStmtHandle handle = prepareCql3Query(cql, false);
        executePreparedCql3Query(handle, variables);
        String expected = cql + " [k=0,v=24,[timestamp]=999,[ttl]=1]";
        assertMatchingEventInList(getEvents(), AuditableEventType.CQL_UPDATE, ks, cf, expected);
    }

    @Test
    public void execute_prepared_cql3_DeleteWithoutBindVariablesIsLogged() throws Exception
    {
        String cql = "DELETE FROM \"Standard1\" WHERE k = 1";
        List<Object> variables = Collections.EMPTY_LIST;

        PreparedStmtHandle handle = prepareCql3Query(cql, false);
        executePreparedCql3Query(handle, variables);
        assertMatchingEventInList(getEvents(), AuditableEventType.CQL_DELETE, ks, cf, cql);
    }

    @Test
    public void execute_prepared_cql3_DeleteWithBindVariablesIsLogged() throws Exception
    {
        String cql = "DELETE FROM \"Standard1\" WHERE k = ?";
        List<Object> variables = getVarsList(0);

        PreparedStmtHandle handle = prepareCql3Query(cql, false);
        executePreparedCql3Query(handle, variables);
        String expected = cql + " [k=0]";
        assertMatchingEventInList(getEvents(), AuditableEventType.CQL_DELETE, ks, cf, expected);
    }

    @Test
    public void execute_prepared_cql3_DeleteWithKeyRangeAndBindVariablesIsLogged() throws Exception
    {
        String cql = "DELETE FROM \"Standard1\" WHERE k in (?, ?)";
        List<Object> variables = getVarsList(0, 1);

        PreparedStmtHandle handle = prepareCql3Query(cql, false);
        executePreparedCql3Query(handle, variables);
        String expected = cql + " [k=0,k=1]";
        assertMatchingEventInList(getEvents(), AuditableEventType.CQL_DELETE, ks, cf, expected);
    }

    @Test
    public void execute_prepared_cql3_BatchWithBindVariablesIsLogged() throws Exception
    {
        String cql = "BEGIN BATCH " +
                     "  USING TIMESTAMP ?" +
                     "  INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) USING TTL ? ;" +
                     "  UPDATE \"Standard1\" SET v = ? WHERE k = ? ;" +
                     "  DELETE FROM \"Standard1\" WHERE k = ? ;" +
                     "APPLY BATCH";
        List<Object> variables = getVarsList(999l, 0, 24, 3600, 48, 1, 2);

        PreparedStmtHandle handle = prepareCql3Query(cql, false);
        executePreparedCql3Query(handle, variables);
        Stack<AuditableEvent> events = getEvents();
        assertEquals(3, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), AuditableEventType.CQL_DELETE, ks, cf, "DELETE FROM Standard1 WHERE k = ? ; [k=2]");
        assertEventProperties(events.pop(), AuditableEventType.CQL_UPDATE, ks, cf, "UPDATE Standard1 SET v = ? WHERE k = ? ; [v=48,k=1]");
        assertEventProperties(events.pop(), AuditableEventType.CQL_UPDATE, ks, cf, "INSERT INTO Standard1 ( k , v ) VALUES ( ? , ? ) USING TTL ? ; [k=0,v=24,[ttl]=3600]");
    }

    @Test
    public void execute_cql3_createKeyspaceIsLogged() throws Exception
    {
        String cql = "CREATE KEYSPACE create_test WITH replication = {" +
                "  'class': 'SimpleStrategy', 'replication_factor': '1'}";
        executeCql3Query(cql, false);
        // altering schema causes java driver to refresh its view, so we
        // don't care about all the queries it issues after that
        assertMatchingEventInList(getEvents(), AuditableEventType.ADD_KS, "create_test", null, cql);
    }

    @Test
    public void execute_cql3_alterKeyspaceIsLogged() throws Exception
    {
        String cql = "CREATE KEYSPACE alter_test WITH replication = {" +
                "  'class': 'SimpleStrategy', 'replication_factor': '1'}";
        executeCql3Query(cql, false);
        cql = "ALTER KEYSPACE alter_test WITH replication = {" +
                "  'class': 'NetworkTopologyStrategy', 'datacenter1': '1'}";
        executeCql3Query(cql, false);
        // altering schema causes java driver to refresh its view, so we
        // don't care about all the queries it issues after that
        assertMatchingEventInList(getEvents(), AuditableEventType.UPDATE_KS, "alter_test", null, cql);
    }

    @Test
    public void execute_cql3_dropKeyspaceIsLogged() throws Exception
    {
        String cql = "CREATE KEYSPACE drop_test WITH replication = {" +
                "  'class': 'SimpleStrategy', 'replication_factor': '1'}";
        executeCql3Query(cql, false);
        cql = "DROP KEYSPACE drop_test";
        executeCql3Query(cql, false);
        // altering schema causes java driver to refresh its view, so we
        // don't care about all the queries it issues after that
        assertMatchingEventInList(getEvents(), AuditableEventType.DROP_KS, "drop_test", null, cql);
    }

    @Test
    public void execute_cql3_createTableIsLogged() throws Exception
    {
        String cql = "CREATE TABLE keyspace1.create_table (" +
                "  k int, v int, PRIMARY KEY(k))";
        executeCql3Query(cql, false);
        // altering schema causes java driver to refresh its view, so we
        // don't care about all the queries it issues after that
        assertMatchingEventInList(getEvents(), AuditableEventType.ADD_CF, "keyspace1", "create_table", cql);
    }

    @Test
    public void execute_cql3_alterTableIsLogged() throws Exception
    {
        String cql = "CREATE TABLE keyspace1.alter_table (" +
                "  k int, v int, PRIMARY KEY(k))";
        executeCql3Query(cql, false);
        cql = "ALTER TABLE keyspace1.alter_table ADD v2 int";
        executeCql3Query(cql, false);
        // altering schema causes java driver to refresh its view, so we
        // don't care about all the queries it issues after that
        assertMatchingEventInList(getEvents(), AuditableEventType.UPDATE_CF, "keyspace1", "alter_table", cql);
    }

    @Test
    public void execute_cql3_dropTableIsLogged() throws Exception
    {
        String cql = "CREATE TABLE keyspace1.drop_table (" +
                "  k int, v int, PRIMARY KEY(k))";
        executeCql3Query(cql, false);
        cql = "DROP TABLE keyspace1.drop_table";
        executeCql3Query(cql, false);
        // altering schema causes java driver to refresh its view, so we
        // don't care about all the queries it issues after that
        assertMatchingEventInList(getEvents(), AuditableEventType.DROP_CF, "keyspace1", "drop_table", cql);
    }

    /**
     * Noop trigger for audit log testing
     */
    public static class TestTrigger implements ITrigger
    {
        public Collection<Mutation> augmentNonBlocking(Partition update)
        {
            return null;
        }
    }

    @Test
    public void execute_cql3_createDropTriggerIsLogged() throws Exception
    {
        String create_cql = String.format("CREATE TRIGGER test_trigger ON %s.\"%s\" USING '%s'", ks, cf, TestTrigger.class.getName());
        executeCql3Query(create_cql, false);

        // check create logging
        assertMatchingEventInList(getEvents(), AuditableEventType.CREATE_TRIGGER, ks, cf, create_cql);

        String drop_cql = String.format("DROP TRIGGER test_trigger ON %s.\"%s\"", ks, cf);
        executeCql3Query(drop_cql, false);

        // check drop logging
        assertMatchingEventInList(getEvents(), AuditableEventType.DROP_TRIGGER, ks, cf, drop_cql);
    }

    protected void assertLastEventProperties(AuditableEventType type, String keyspace, String columnFamily) throws Exception
    {
        AuditableEvent event = getEvents().peek();
        assertEquals(type, event.getType());

        if (null != keyspace)
            assertEquals(keyspace, event.getKeyspace());

        if (null != columnFamily)
            assertEquals(columnFamily, event.getColumnFamily());
    }

    private List<Object> getVarsList(Object...vars)
    {
        return Arrays.asList(vars);
    }

    public Stack<AuditableEvent> getEvents() throws Exception
    {
        return InProcTestAuditWriter.getEvents();
    }

    public static interface PreparedStmtHandle
    {
        // used to encapsulate identifiers returned by CQL3 statement preparation
        // i.e. an MD5Digest object for native protocol & and int for Thrift
    }
}

