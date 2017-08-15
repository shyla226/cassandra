/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.net.InetAddress;
import java.util.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import static com.datastax.bdp.db.audit.CoreAuditableEventType.*;

public class NativeProtocolAuditLoggingTest extends CQLTester
{
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

        InetAddress host = FBUtilities.getBroadcastAddress();
        Gossiper.instance.initializeNodeUnsafe(host, UUID.randomUUID(), 1);
        StorageService.instance.setNativeTransportReady(true);
    }

    @Before
    public void setUp() throws Exception
    {
        InProcTestAuditWriter.reset();
    }

    @Test
    public void testLoggingForSelectExecution() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        executeNet("SELECT * FROM %s");
        assertMatchingEventInList(getEvents(), CQL_SELECT, KEYSPACE, currentTable(), formatQuery("SELECT * FROM %s"));
    }

    @Test
    public void testLoggingForBatchExecution() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        String insertQuery = formatQuery("INSERT INTO %s ( k , v ) VALUES ( 0 , 24 ) ;");
        String updateQuery = formatQuery("UPDATE %s SET v = 48 WHERE k = 1 ;");
        String deleteQuery = formatQuery("DELETE FROM %s WHERE k = 2 ;");

        String cql = "BEGIN BATCH " +
                insertQuery +
                updateQuery +
                deleteQuery +
                "APPLY BATCH";

        executeNet(new SimpleStatement(cql));
        Stack<AuditableEvent> events = getEvents();
        assertEquals(3, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), CQL_DELETE, KEYSPACE, currentTable(), deleteQuery);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(), updateQuery);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(), insertQuery);
    }

    @Test
    public void testLoggingForSelectPreparation() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        String cql = formatQuery("SELECT * FROM %s");
        prepareNet(cql);
        assertMatchingEventInList(getEvents(), CQL_PREPARE_STATEMENT, KEYSPACE, currentTable(), cql);
    }

    @Test
    public void testLoggingForSelectPreparationAndExecution() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        String cql = formatQuery("SELECT * FROM %s");
        PreparedStatement ps = prepareNet(cql);
        executeNet(ps.bind());
        assertMatchingEventInList(getEvents(), CQL_SELECT, KEYSPACE, currentTable(), cql);
    }

    @Test
    public void testLoggingForSelectWithBindVariablesPreparationAndExecution() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        String cql = formatQuery("SELECT * FROM %s WHERE k = ?");
        PreparedStatement ps = prepareNet(cql);
        executeNet(ps.bind(0));

        String expected = cql + " [k=0]";
        assertMatchingEventInList(getEvents(), CQL_SELECT, KEYSPACE, currentTable(), expected);
    }

    @Test
    public void testLoggingForInsertPreparationAndExecution() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        String cql = formatQuery("INSERT INTO %s (k, v) VALUES (1, 1) USING TIMESTAMP 0 AND TTL 100");
        PreparedStatement ps = prepareNet(cql);
        executeNet(ps.bind());
        assertMatchingEventInList(getEvents(), CQL_UPDATE, KEYSPACE, currentTable(), cql);
    }

    @Test
    public void testLoggingForInsertWithBindVariablesPreparationAndExecution() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        String cql = formatQuery("INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP ? AND TTL ?");
        PreparedStatement ps = prepareNet(cql);
        executeNet(ps.bind(0, 24, 999l, 1));
        String expected = cql + " [k=0,v=24,[timestamp]=999,[ttl]=1]";
        assertMatchingEventInList(getEvents(), CQL_UPDATE, KEYSPACE, currentTable(), expected);
    }

    @Test
    public void testLoggingForDeletePreparationAndExecution() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        String cql = formatQuery("DELETE FROM %s WHERE k = 1");
        PreparedStatement ps = prepareNet(cql);
        executeNet(ps.bind());
        assertMatchingEventInList(getEvents(), CQL_DELETE, KEYSPACE, currentTable(), cql);
    }

    @Test
    public void testLoggingForDeleteWithBindVariablesPreparationAndExecution() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        String cql = formatQuery("DELETE FROM %s WHERE k = ?");
        PreparedStatement ps = prepareNet(cql);
        executeNet(ps.bind(0));
        String expected = cql + " [k=0]";
        assertMatchingEventInList(getEvents(), CQL_DELETE, KEYSPACE, currentTable(), expected);
    }

    @Test
    public void testLoggingForDeleteWithKeyRangeAndBindVariablesPreparationAndExecution() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        String cql = formatQuery("DELETE FROM %s WHERE k in (?, ?)");
        PreparedStatement ps = prepareNet(cql);
        executeNet(ps.bind(0, 1));
        String expected = cql + " [k=0,k=1]";
        assertMatchingEventInList(getEvents(), CQL_DELETE, KEYSPACE, currentTable(), expected);
    }

    @Test
    public void testLoggingForBatchWithBindVariablesPreparationAndExecution() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        String insertQuery = formatQuery("INSERT INTO %s ( k , v ) VALUES ( ? , ? ) USING TTL ? ;");
        String updateQuery = formatQuery("UPDATE %s SET v = ? WHERE k = ? ;");
        String deleteQuery = formatQuery("DELETE FROM %s WHERE k = ? ;");

        String cql = "BEGIN BATCH " +
                     "  USING TIMESTAMP ?" +
                     insertQuery +
                     updateQuery +
                     deleteQuery +
                     "APPLY BATCH";

        PreparedStatement ps = prepareNet(cql);
        executeNet(ps.bind(999l, 0, 24, 3600, 48, 1, 2));

        Stack<AuditableEvent> events = getEvents();
        assertEquals(6, events.size());
        assertAllEventsInSameBatch(events.subList(3, 5));
        assertEventProperties(events.pop(), CQL_DELETE, KEYSPACE, currentTable(), deleteQuery + " [k=2]");
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(), updateQuery + " [v=48,k=1]");
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(), insertQuery + " [k=0,v=24,[ttl]=3600]");
        assertEventProperties(events.pop(), CQL_PREPARE_STATEMENT, KEYSPACE, currentTable(), deleteQuery);
        assertEventProperties(events.pop(), CQL_PREPARE_STATEMENT, KEYSPACE, currentTable(), updateQuery);
        assertEventProperties(events.pop(), CQL_PREPARE_STATEMENT, KEYSPACE, currentTable(), insertQuery);
    }

    @Test
    public void testLoggingForCreateKeyspace() throws Throwable
    {
        try
        {
            String cql = "CREATE KEYSPACE create_test WITH replication = {" +
                    "  'class': 'SimpleStrategy', 'replication_factor': '1'}";

            executeNet(new SimpleStatement(cql));
            // altering schema causes java driver to refresh its view, so we
            // don't care about all the queries it issues after that
            assertMatchingEventInList(getEvents(), ADD_KS, "create_test", null, cql);
        }
        finally
        {
            schemaChange("DROP KEYSPACE IF EXISTS create_test");
        }
    }

    @Test
    public void testLoggingForAlterKeyspace() throws Throwable
    {
        try
        {
            String cql = "CREATE KEYSPACE alter_test WITH replication = {" +
                    "  'class': 'SimpleStrategy', 'replication_factor': '1'}";
            executeNet(new SimpleStatement(cql));

            cql = "ALTER KEYSPACE alter_test WITH replication = {" +
                    "  'class': 'NetworkTopologyStrategy', 'datacenter1': '1'}";
            executeNet(new SimpleStatement(cql));

            // altering schema causes java driver to refresh its view, so we
            // don't care about all the queries it issues after that
            assertMatchingEventInList(getEvents(), UPDATE_KS, "alter_test", null, cql);
        }
        finally
        {
            schemaChange("DROP KEYSPACE IF EXISTS create_test");
        }
    }

    @Test
    public void testLoggingForDropKeyspace() throws Exception
    {
        String cql = "CREATE KEYSPACE drop_test WITH replication = {" +
                "  'class': 'SimpleStrategy', 'replication_factor': '1'}";
        executeNet(new SimpleStatement(cql));

        cql = "DROP KEYSPACE drop_test";
        executeNet(new SimpleStatement(cql));
        // altering schema causes java driver to refresh its view, so we
        // don't care about all the queries it issues after that
        assertMatchingEventInList(getEvents(), DROP_KS, "drop_test", null, cql);
    }

    @Test
    public void testLoggingForCreateTable() throws Exception
    {
        try
        {
            String cql = String.format("CREATE TABLE %s.create_table (k int, v int, PRIMARY KEY(k))", KEYSPACE);
            executeNet(new SimpleStatement(cql));
            // altering schema causes java driver to refresh its view, so we
            // don't care about all the queries it issues after that
            assertMatchingEventInList(getEvents(), ADD_CF, KEYSPACE, "create_table", cql);
        }
        finally
        {
            schemaChange(String.format("DROP TABLE IF EXISTS %s.create_table", KEYSPACE));
        }
    }

    @Test
    public void testLoggingForAlterTable() throws Exception
    {
        try
        {
            String cql = String.format("CREATE TABLE %s.alter_table (k int, v int, PRIMARY KEY(k))", KEYSPACE);
            executeNet(new SimpleStatement(cql));
            cql = String.format("ALTER TABLE %s.alter_table ADD v2 int", KEYSPACE);
            executeNet(new SimpleStatement(cql));
            // altering schema causes java driver to refresh its view, so we
            // don't care about all the queries it issues after that
            assertMatchingEventInList(getEvents(), UPDATE_CF, KEYSPACE, "alter_table", cql);
        }
        finally
        {
            schemaChange(String.format("DROP TABLE IF EXISTS %s.alter_table", KEYSPACE));
        }
    }

    @Test
    public void testLoggingForDropTable() throws Exception
    {
        String cql = String.format("CREATE TABLE %s.drop_table (k int, v int, PRIMARY KEY(k))", KEYSPACE);
        executeNet(new SimpleStatement(cql));
        cql = String.format("DROP TABLE %s.drop_table", KEYSPACE);
        executeNet(new SimpleStatement(cql));
        // altering schema causes java driver to refresh its view, so we
        // don't care about all the queries it issues after that
        assertMatchingEventInList(getEvents(), DROP_CF, KEYSPACE, "drop_table", cql);
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
    public void testLoggingForCreateAndDropTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        String create_cql = "CREATE TRIGGER test_trigger ON %s USING '" + TestTrigger.class.getName() + "'";
        executeNet(create_cql);

        // check create logging
        assertMatchingEventInList(getEvents(), CREATE_TRIGGER, KEYSPACE, currentTable(), formatQuery(create_cql));

        String drop_cql = "DROP TRIGGER test_trigger ON %s";
        executeNet(drop_cql);

        // check drop logging
        assertMatchingEventInList(getEvents(), DROP_TRIGGER, KEYSPACE, currentTable(), formatQuery(drop_cql));
    }

    @Test
    public void testLoggingForCreateAndDropUserType() throws Throwable
    {
        String createType = String.format("CREATE TYPE %s.test (a int , b int) ;", KEYSPACE);
        executeNet(createType);

        // check create logging
        assertMatchingEventInList(getEvents(), CREATE_TYPE, KEYSPACE, currentTable(), createType);

        String dropType = String.format("DROP TYPE %s.test ;", KEYSPACE);
        executeNet(dropType);

        // check drop logging
        assertMatchingEventInList(getEvents(), DROP_TYPE, KEYSPACE, currentTable(), dropType);
    }

    @Test
    public void testLoggingForCreateAndDropFunction() throws Throwable
    {
        String createFunction = String.format("CREATE FUNCTION %s.test() RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE JAVA AS 'return 1L;' ;", KEYSPACE);
        executeNet(createFunction);

        // check create logging
        assertMatchingEventInList(getEvents(), CREATE_FUNCTION, KEYSPACE, null, createFunction);

        String dropFunction = String.format("DROP FUNCTION %s.test ;", KEYSPACE);
        executeNet(dropFunction);

        // check drop logging
        assertMatchingEventInList(getEvents(), DROP_FUNCTION, KEYSPACE, null, dropFunction);
    }

    @Test
    public void testLoggingForCreateAndDropAggregate() throws Throwable
    {
        String createFunction = String.format("CREATE FUNCTION %s.test(left bigint, right bigint) CALLED ON NULL INPUT  RETURNS bigint LANGUAGE JAVA AS 'return 1L;' ;", KEYSPACE);
        executeNet(createFunction);
        String createAggregate = String.format("CREATE AGGREGATE %s.testAggregate(bigint) SFUNC test STYPE bigint", KEYSPACE);
        executeNet(createAggregate);

        // check create logging
        assertMatchingEventInList(getEvents(), CREATE_AGGREGATE, KEYSPACE, null, createAggregate);

        String dropAggregate = String.format("DROP AGGREGATE %s.testAggregate ;", KEYSPACE);
        executeNet(dropAggregate);

        // check drop logging
        assertMatchingEventInList(getEvents(), DROP_AGGREGATE, KEYSPACE, null, dropAggregate);
    }

    @Test
    public void testLoggingForCreateAndDropView() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v int, PRIMARY KEY(pk, c))");
        String createView = String.format("CREATE MATERIALIZED VIEW %s.myview AS SELECT v, c, pk FROM %s WHERE pk IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, c, pk);", KEYSPACE, currentTable());
        executeNet(createView);

        assertMatchingEventInList(getEvents(), CREATE_VIEW, KEYSPACE, "myview", createView);

        String alterView = String.format("ALTER MATERIALIZED VIEW %s.myview WITH comment = 'This my view'", KEYSPACE);
        executeNet(alterView);
        assertMatchingEventInList(getEvents(), UPDATE_VIEW, KEYSPACE, "myview", alterView);

        String dropView = String.format("DROP MATERIALIZED VIEW %s.myview ;", KEYSPACE);
        executeNet(dropView);

        // check drop logging
        assertMatchingEventInList(getEvents(), DROP_VIEW, KEYSPACE, "myview", dropView);
    }

    @Test
    public void testLoggingForRegularStatementWithVariables() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");

        executeNet("INSERT INTO %s (k, v) VALUES (?, ?) ;", 3, 72);
        Stack<AuditableEvent> events = getEvents();
        assertEquals(1, events.size());
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(), formatQuery("INSERT INTO %s (k, v) VALUES (?, ?) ; [k=3,v=72]"));
    }

    @Test
    public void testLoggingForPreparedStatementWithVariablesAndUnset() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");

        String query = formatQuery("INSERT INTO %s (k, v) VALUES (?, ?) ;");
        PreparedStatement ps1 = prepareNet(query);
        executeNet(ps1.bind(3));
        Stack<AuditableEvent> events = getEvents();
        assertEquals(2, events.size());
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(), query + " [k=3,v=UNSET]");
        assertEventProperties(events.pop(), CQL_PREPARE_STATEMENT, KEYSPACE, currentTable(), query);
    }

    @Test
    public void testLoggingForBatchOfPreparedStatements() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");

        String query = formatQuery("INSERT INTO %s (k, v) VALUES (?, ?) ;");
        PreparedStatement ps1 = prepareNet(query);
        PreparedStatement ps2 = prepareNet(query);
        BatchStatement batch = new BatchStatement();
        batch.add(ps1.bind(1, 24)).add(ps2.bind(2, 48));

        executeNet(batch);

        Stack<AuditableEvent> events = getEvents();
        assertEquals(3, events.size());
        assertAllEventsInSameBatch(events.subList(1, 2));
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(), query + " [k=2,v=48]");
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(), query + " [k=1,v=24]");
        assertEventProperties(events.pop(), CQL_PREPARE_STATEMENT, KEYSPACE, currentTable(), query);
    }

    @Test
    public void testLoggingForQueryBuilderBatch() throws Exception
    {
        createTable("CREATE TABLE %s (column1 text, column2 text, count counter, PRIMARY KEY(column1, column2))");

        List<Update> queries = new ArrayList<Update>();

        for (int i = 0; i < 2; i++)
        {
            Assignment inc = QueryBuilder.incr("count");
            Update countUpdate = QueryBuilder.update(KEYSPACE, currentTable());
            Update.Assignments incs = countUpdate.with(inc);
            Update.Where where = incs.where(QueryBuilder.eq("column1", "data1"));
            where.and(QueryBuilder.eq("column2", "data2"));
            queries.add(countUpdate);
        }

        Batch b = QueryBuilder.batch(queries.toArray(new Update[queries.size()]));
        executeNet(b);
        Stack<AuditableEvent> events = getEvents();
        assertEquals(2, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(),
                                                      formatQuery("UPDATE %s SET count = count + 1 WHERE column1 = ? AND column2 = ? ; [column1=data1,column2=data2]"));
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(),
                                                      formatQuery("UPDATE %s SET count = count + 1 WHERE column1 = ? AND column2 = ? ; [column1=data1,column2=data2]"));
    }

    @Test
    public void testLoggingForMixedBatch() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        String psQuery = formatQuery("INSERT INTO %s (k, v) VALUES (?, ?) ;");
        PreparedStatement ps1 = prepareNet(psQuery);
        PreparedStatement ps2 = prepareNet(psQuery);
        BatchStatement batch = new BatchStatement();
        batch.add(ps1.bind(1, 24)).add(ps2.bind(2, 48));
        String query = formatQuery("INSERT INTO %s (k, v) VALUES (3, 72) ;");
        batch.add(new SimpleStatement(query));
        batch.add(new SimpleStatement(psQuery, 4, 96));

        executeNet(batch);

        Stack<AuditableEvent> events = getEvents();
        assertEquals(5, events.size());
        assertAllEventsInSameBatch(events.subList(1, 4));
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(), psQuery + " [bind variable values unavailable]");
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(), query);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(), psQuery + " [k=2,v=48]");
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(), psQuery + " [k=1,v=24]");
        assertEventProperties(events.pop(), CQL_PREPARE_STATEMENT, KEYSPACE, currentTable(), psQuery);
    }

    @Test
    public void testLoggingForSelectWithMultiplePages() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");

        BatchStatement batch = new BatchStatement();
        PreparedStatement prepared = prepareNet(formatQuery("INSERT INTO %s (k, v) VALUES (?, ?) ;"));
        for (int i = 0; i < 10; i++)
        {
            batch.add(prepared.bind(i,i));
        }
        executeNet(batch);

        InProcTestAuditWriter.reset();

        String query = formatQuery("SELECT * FROM %s");
        Statement paged = new SimpleStatement(query);
        paged.setFetchSize(5);
        // iterate over the results to force the driver
        // into executing additional queries
        executeNet(paged).all();

        Stack<AuditableEvent> events = getEvents();
        assertEquals(1, events.size());
        assertEventProperties(events.pop(), CQL_SELECT, KEYSPACE, currentTable(), query);
    }

    private Stack<AuditableEvent> getEvents() throws Exception
    {
        return InProcTestAuditWriter.getEvents();
    }

    private void assertEventProperties(AuditableEvent event,
                                      AuditableEventType type,
                                      String keyspace,
                                      String table,
                                      String operation)
    {
        assertEquals(table, event.getColumnFamily());
        assertEquals(keyspace, event.getKeyspace());
        assertEquals(type, event.getType());
        // whitespace in batch cql strings are normalised by CQL handler impls, so this test
        // is sensitive. If the assertion fails, check the whitespace in the tested string
        // as what is logged may not be exactly the same as the statement in the case of
        // BATCH statements
        assertEquals(operation, event.getOperation());
    }

    private static void assertAllEventsInSameBatch(List<AuditableEvent> events)
    {
        UUID batchId = events.get(0).getBatchId();
        assertNotNull(batchId);
        for (AuditableEvent event : events )
        {
            assertEquals(batchId, event.getBatchId());
        }
    }

    public static void assertMatchingEventInList(List<AuditableEvent> events,
                                                 AuditableEventType type,
                                                 String keyspace,
                                                 String columnFamily,
                                                 String operation)
    {
        for (AuditableEvent event : events)
        {
            if (event.getType() == type)
            {
                assertEquals(operation, event.getOperation());
                assertEquals(keyspace, event.getKeyspace());
                assertEquals(columnFamily, event.getColumnFamily());
                return;
            }
        }
        fail("Didn't find matching event");
    }
}
