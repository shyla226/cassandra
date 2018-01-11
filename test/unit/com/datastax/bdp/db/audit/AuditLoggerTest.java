/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.net.InetAddress;
import java.util.*;

import com.google.common.collect.ImmutableMap;

import org.junit.*;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;
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

public class AuditLoggerTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        requireAuthentication();
        IAuditFilter filter = AuditFilters.excludeKeyspace("system.*");
        IAuditLogger auditLogger = IAuditLogger.newInstance(new InProcTestAuditWriter(), filter);

        // We don't need to set the AL to its old state as we'll be resetting
        // the in-memory writer after every test.
        DatabaseDescriptor.setAuditLoggerUnsafe(auditLogger);

        CQLTester.setUpClass();

        InetAddress host = FBUtilities.getBroadcastAddress();
        Gossiper.instance.initializeNodeUnsafe(host, UUID.randomUUID(), 1);
        StorageService.instance.setNativeTransportReady(true);
    }

    @Before
    public void setUp() throws Throwable
    {
        useSuperUser();

        executeNet("CREATE USER test WITH PASSWORD 'test'");
        executeNet("GRANT ALL PERMISSIONS ON KEYSPACE " + KEYSPACE + " TO test");
        executeNet("GRANT ALL PERMISSIONS ON ALL FUNCTIONS IN KEYSPACE " + KEYSPACE + " TO test");

        useUser("test", "test");

        InProcTestAuditWriter.reset();
    }

    @After
    public void tearDown() throws Throwable
    {
        useSuperUser();
        executeNet("DROP USER test");
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
    public void testLoggingForInsertWithNamedVariables() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        String cql = formatQuery("INSERT INTO %s (k, v) VALUES (:k_value, :v_value) ;");
        executeNet(new SimpleStatement(cql, ImmutableMap.<String, Object>of("k_value", 5, "v_value", 124)));
        String expected = cql + " [k_value=5,v_value=124]";
        assertMatchingEventInList(getEvents(), CQL_UPDATE, KEYSPACE, currentTable(), expected);
    }

    @Test
    public void testLoggingForInsertWithNamedVariableUsedTwice() throws Exception
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        String cql = formatQuery("INSERT INTO %s (k, v) VALUES (:value, :value) ;");
        executeNet(new SimpleStatement(cql, ImmutableMap.<String, Object>of("value", 5)));
        String expected = cql + " [value=5,value=5]";
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
        useSuperUser();

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
        useSuperUser();

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
        useSuperUser();

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
    public void testLoggingForCreateAndDropTrigger() throws Throwable
    {
        useSuperUser();

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
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(), query + " [k=3,v=unset]");
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
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(), psQuery + " [k=4,v=96]");
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

    @Test
    public void testLoggingForCreateAlterAndDropRole() throws Throwable
    {
        useSuperUser();

        executeNet("CREATE ROLE foo WITH PASSWORD = 'bar' AND LOGIN = true");
        assertMatchingEventInList(getEvents(), CREATE_ROLE, null, null, formatQuery("CREATE ROLE foo WITH PASSWORD = '*****' AND LOGIN = true"));

        executeNet("ALTER ROLE foo WITH PASSWORD = 'baz' AND LOGIN = true");
        assertMatchingEventInList(getEvents(), ALTER_ROLE, null, null, formatQuery("ALTER ROLE foo WITH PASSWORD = '*****' AND LOGIN = true"));

        executeNet("DROP ROLE foo");
        assertMatchingEventInList(getEvents(), DROP_ROLE, null, null, formatQuery("DROP ROLE foo"));
    }

    @Test
    public void testLoggingForCreateAlterAndDropRoleWithExtraWhitespace() throws Throwable
    {
        useSuperUser();

        executeNet("CREATE ROLE foo WITH PASSWORD  =  'bar' AND LOGIN = true");
        assertMatchingEventInList(getEvents(), CREATE_ROLE, null, null, formatQuery("CREATE ROLE foo WITH PASSWORD = '*****' AND LOGIN = true"));

        executeNet("ALTER ROLE foo WITH PASSWORD  =  'baz' AND LOGIN = true");
        assertMatchingEventInList(getEvents(), ALTER_ROLE, null, null, formatQuery("ALTER ROLE foo WITH PASSWORD = '*****' AND LOGIN = true"));

        executeNet("DROP ROLE foo");
        assertMatchingEventInList(getEvents(), DROP_ROLE, null, null, formatQuery("DROP ROLE foo"));
    }

    @Test
    public void testLoggingForCreateAlterAndDropUserWithExtraWhitespace() throws Throwable
    {
        useSuperUser();

        executeNet("CREATE USER 'foo' WITH  PASSWORD  'bar'");
        assertMatchingEventInList(getEvents(), CREATE_ROLE, null, null, formatQuery("CREATE USER 'foo' WITH  PASSWORD '*****'"));

        executeNet("ALTER USER 'foo' WITH  PASSWORD  'baz'");
        assertMatchingEventInList(getEvents(), ALTER_ROLE, null, null, formatQuery("ALTER USER 'foo' WITH  PASSWORD '*****'"));

        executeNet("DROP USER foo");
        assertMatchingEventInList(getEvents(), DROP_ROLE, null, null, formatQuery("DROP USER foo"));
    }

    @Test
    public void testLoggingForListPermissions() throws Throwable
    {
        useSuperUser();

        executeNet("LIST ALL PERMISSIONS OF test");
        assertMatchingEventInList(getEvents(), LIST_PERMISSIONS, null, null, formatQuery("LIST ALL PERMISSIONS OF test"));
    }

    @Test
    public void testLoggingForGrantAndRevokeStatementsIsLogged() throws Throwable
    {
        useSuperUser();

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");

        executeNet("GRANT ALL PERMISSIONS ON %s TO test");
        assertMatchingEventInList(getEvents(), GRANT, null, null, formatQuery("GRANT ALL PERMISSIONS ON %s TO test"));

        executeNet("LIST ALL PERMISSIONS OF test");
        assertMatchingEventInList(getEvents(), LIST_PERMISSIONS, null, null, formatQuery("LIST ALL PERMISSIONS OF test"));

        executeNet("REVOKE ALL PERMISSIONS ON %s FROM test");
        assertMatchingEventInList(getEvents(), REVOKE, null, null, formatQuery("REVOKE ALL PERMISSIONS ON %s FROM test"));
    }

    @Test
    public void testLoggingForBatchWithConsistencyLevel() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        String stmt =
                "BEGIN BATCH " + //USING CONSISTENCY IS NOT WORKING IN CQL3
                "   INSERT INTO " + KEYSPACE + "." + currentTable() + " (pk, v) VALUES (0, 'INSERT') ;" +
                "   DELETE v FROM " + KEYSPACE + "." + currentTable() + " WHERE pk = 1 ;" +
                "APPLY BATCH;";

        executeNet(new SimpleStatement(stmt).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE));
        Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
        assertEquals(2, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(),
                              CQL_DELETE,
                              KEYSPACE,
                              currentTable(),
                              "DELETE v FROM " + KEYSPACE + "." + currentTable() + " WHERE pk = 1 ;",
                              ConsistencyLevel.ONE);
        assertEventProperties(events.pop(),
                              CQL_UPDATE,
                              KEYSPACE,
                              currentTable(),
                              "INSERT INTO " + KEYSPACE + "." + currentTable()
                                      + " ( pk , v ) VALUES ( 0 , 'INSERT' ) ;",
                              ConsistencyLevel.ONE);
    }

    @Test
    public void testLoggingForCounterBatch() throws Exception
    {
        createTable("CREATE TABLE %s (id int, field0 counter, field1 counter, PRIMARY KEY(id));");
        // 4 stmts, 3 distinct keys
        String stmt =
                "BEGIN COUNTER BATCH " +
                        "   UPDATE  " + KEYSPACE + "." + currentTable() + " SET field0 = field0 + 1 WHERE id = 0;" +
                        "   UPDATE  " + KEYSPACE + "." + currentTable() + " SET field0 = field0 + 1 WHERE id = 0;" +
                        "   UPDATE  " + KEYSPACE + "." + currentTable() + " SET field0 = field0 + 1 WHERE id = 1;" +
                        "   UPDATE  " + KEYSPACE + "." + currentTable() + " SET field0 = field0 + 1 WHERE id = 2;" +
                        "APPLY BATCH;";

        executeNet(new SimpleStatement(stmt).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE));
        Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
        assertEquals(4, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(),
                "UPDATE " + KEYSPACE + "." + currentTable() + " SET field0 = field0 + 1 WHERE id = 2 ;", ConsistencyLevel.ONE);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(),
                "UPDATE " + KEYSPACE + "." + currentTable() + " SET field0 = field0 + 1 WHERE id = 1 ;", ConsistencyLevel.ONE);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(),
                "UPDATE " + KEYSPACE + "." + currentTable() + " SET field0 = field0 + 1 WHERE id = 0 ;", ConsistencyLevel.ONE);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(),
                "UPDATE " + KEYSPACE + "." + currentTable() + " SET field0 = field0 + 1 WHERE id = 0 ;", ConsistencyLevel.ONE);
    }

    @Test
    public void testLoggingForBatchWithQuorumConsistencyLevel() throws Exception
    {
        createTable("CREATE TABLE %s (id int, field0 text, field1 text, PRIMARY KEY(id));");
        // 4 stmts, 3 distinct keys
        String stmt =
          "BEGIN BATCH " +
            "   INSERT INTO " + KEYSPACE + "." + currentTable() + " (id, field0) VALUES (0, 'INSERT') ;" +
            "   UPDATE " + KEYSPACE + "." + currentTable() + " SET field1 = 'some other value' WHERE id = 0 ;" +
            "   INSERT INTO " + KEYSPACE + "." + currentTable() + " (id, field0) VALUES (1, 'yet another value') ;" +
            "   DELETE field0 FROM " + KEYSPACE + "." + currentTable() + " WHERE id = 2 ;" +
          "APPLY BATCH;";
        executeNet(new SimpleStatement(stmt).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM));
        Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
        assertEquals(4, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), CQL_DELETE, KEYSPACE, currentTable(),
                "DELETE field0 FROM " + KEYSPACE + "." + currentTable() + " WHERE id = 2 ;", ConsistencyLevel.QUORUM);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(),
                "INSERT INTO " + KEYSPACE + "." + currentTable() + " ( id , field0 ) VALUES ( 1 , 'yet another value' ) ;", ConsistencyLevel.QUORUM);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(),
                "UPDATE " + KEYSPACE + "." + currentTable() + " SET field1 = 'some other value' WHERE id = 0 ;", ConsistencyLevel.QUORUM);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, currentTable(),
                "INSERT INTO " + KEYSPACE + "." + currentTable() + " ( id , field0 ) VALUES ( 0 , 'INSERT' ) ;", ConsistencyLevel.QUORUM);
    }

    @Test
    public void testLoggingForBatchContainingTables() throws Exception
    {
        String table1 = createTable("CREATE TABLE %s (id int, field0 text, field1 text, PRIMARY KEY(id));");
        String table2 = createTable("CREATE TABLE %s (id int, field0 text, field1 text, PRIMARY KEY(id));");
        InProcTestAuditWriter.reset();

        // 4 stmts, 1 distinct key, 2 CFs
        String stmt =
                "BEGIN BATCH " +
            "   INSERT INTO " + KEYSPACE + "." + table1 + " (id, field0) VALUES (0, 'some value') ;" +
            "   UPDATE " + KEYSPACE + "." + table1 + " SET field0 = 'some other value' WHERE id = 0 ;" +
            "   INSERT INTO " + KEYSPACE + "." + table2 + " (id, field0) VALUES (0, 'some value') ;" +
            "   UPDATE " + KEYSPACE + "." + table2 + " SET field0 = 'some other value' WHERE id = 0 ;" +
          "APPLY BATCH;";
        executeNet(new SimpleStatement(stmt).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM));
        Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
        assertEquals(4, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, table2,
                "UPDATE " + KEYSPACE + "." + table2 + " SET field0 = 'some other value' WHERE id = 0 ;", ConsistencyLevel.QUORUM);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, table2,
                "INSERT INTO " + KEYSPACE + "." + table2 + " ( id , field0 ) VALUES ( 0 , 'some value' ) ;", ConsistencyLevel.QUORUM);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, table1,
                "UPDATE " + KEYSPACE + "." + table1 + " SET field0 = 'some other value' WHERE id = 0 ;", ConsistencyLevel.QUORUM);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, table1,
                "INSERT INTO " + KEYSPACE + "." + table1 + " ( id , field0 ) VALUES ( 0 , 'some value' ) ;", ConsistencyLevel.QUORUM);
    }

    @Test
    public void testLoggingForBatchContainingMultipleKeyspaces() throws Exception
    {
        useSuperUser();

        // Add another ks & cf
        String table1 = createTable("CREATE TABLE %s (id int, field0 text, field1 text, PRIMARY KEY(id));");
        String keyspace2 = createKeyspace("create keyspace %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        String table2 = createTable(keyspace2, "CREATE TABLE %s (id int, field0 text, field1 text, PRIMARY KEY(id));");
        InProcTestAuditWriter.reset();

        // 2 stmts, 1 distinct key, 2 CFs in different Keyspaces
        String stmt =
          "BEGIN BATCH " +
            "   INSERT INTO " + KEYSPACE + "." + table1 + " (id, field0) VALUES (0, 'some value') ;" +
            "   INSERT INTO " + keyspace2 + "." + table2 + " (id, field0) VALUES (0, 'some value') ;" +
          "APPLY BATCH;";

        executeNet(new SimpleStatement(stmt).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE));
        Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
        assertEquals(2, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), CQL_UPDATE, keyspace2, table2,
                "INSERT INTO " + keyspace2 + "." + table2 + " ( id , field0 ) VALUES ( 0 , 'some value' ) ;", ConsistencyLevel.ONE);
        assertEventProperties(events.pop(), CQL_UPDATE, KEYSPACE, table1,
                "INSERT INTO " + KEYSPACE + "." + table1 + " ( id , field0 ) VALUES ( 0 , 'some value' ) ;", ConsistencyLevel.ONE);
    }

    @Test
    public void testLoggingForTruncate() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        executeNet("TRUNCATE %s;");
        assertMatchingEventInList(getEvents(), TRUNCATE, KEYSPACE, currentTable(), formatQuery("TRUNCATE %s;"));
    }

    @Test
    public void testLoggingForCreateAndDropIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        executeNet("CREATE INDEX test_idx ON %s (v);");
        assertMatchingEventInList(getEvents(), CREATE_INDEX, KEYSPACE, currentTable(), formatQuery("CREATE INDEX test_idx ON %s (v);"));
        executeNet("DROP INDEX " + KEYSPACE + ".test_idx;");
        assertMatchingEventInList(getEvents(), DROP_INDEX, KEYSPACE, currentTable(), formatQuery("DROP INDEX " + KEYSPACE + ".test_idx;"));
    }

    @Test
    public void testLoggingForUseStatement() throws Throwable
    {
        executeNet("USE " + KEYSPACE);
        assertMatchingEventInList(getEvents(), SET_KS, KEYSPACE, null, formatQuery("USE " + KEYSPACE));
    }

    @Test
    public void testLoggingForConsistencyFailures() throws Throwable
    {
        useSuperUser();

        executeNet(new SimpleStatement("CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
                   .setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE));
        executeNet(new SimpleStatement("CREATE TABLE test.test (pk int PRIMARY KEY, v int)")
                   .setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE));

        try
        {
            executeNet(new SimpleStatement("SELECT * FROM test.test").setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM));
            Assert.fail("Expecting NoHostAvailableException");
        }
        catch (NoHostAvailableException e)
        {
            Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
            assertEventProperties(events.pop(), REQUEST_FAILURE, "test", "test",
                                  "Cannot achieve consistency level QUORUM SELECT * FROM test.test", ConsistencyLevel.QUORUM);
            assertEventProperties(events.pop(), CQL_SELECT, "test", "test",
                                  "SELECT * FROM test.test", ConsistencyLevel.QUORUM);
        }
        finally
        {
            execute("DROP KEYSPACE test");
        }
    }

    @Test
    public void testLoggingForConsistencyFailuresWithPreparedStatement() throws Throwable
    {
        useSuperUser();

        executeNet(new SimpleStatement("CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
                   .setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE));
        executeNet(new SimpleStatement("CREATE TABLE test.test (pk int PRIMARY KEY, v int)")
                   .setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE));

        try
        {
            PreparedStatement stmt = prepareNet("SELECT * FROM test.test");
            executeNet(stmt.bind().setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM));
            Assert.fail("Expecting NoHostAvailableException");
        }
        catch (NoHostAvailableException e)
        {
            e.printStackTrace();
            Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
            assertEventProperties(events.pop(), REQUEST_FAILURE, "test", "test",
                                  "Cannot achieve consistency level QUORUM SELECT * FROM test.test", ConsistencyLevel.QUORUM);
            assertEventProperties(events.pop(), CQL_SELECT, "test", "test",
                                  "SELECT * FROM test.test", ConsistencyLevel.QUORUM);
        }
        finally
        {
            execute("DROP KEYSPACE test");
        }
    }

    @Test
    public void testLoggingForInvalidQueries() throws Exception
    {
        try
        {
            executeNet(new SimpleStatement("SELECT * FROM test.test").setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM));
            Assert.fail("Expecting InvalidQueryException");
        }
        catch (InvalidQueryException e)
        {
            Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
            assertEventProperties(events.pop(), REQUEST_FAILURE, null, null,
                                  "keyspace test does not exist SELECT * FROM test.test", null);
        }
    }

    @Test
    public void testLoggingForPreparingInvalidQueries() throws Exception
    {
        try
        {
            prepareNet("SELECT * FROM test.test");
            Assert.fail("Expecting InvalidQueryException");
        }
        catch (InvalidQueryException e)
        {
            Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
            assertEventProperties(events.pop(), REQUEST_FAILURE, null, null,
                                  "keyspace test does not exist SELECT * FROM test.test", null);
        }
    }

    @Test
    public void checkProcessBatch() throws Throwable
    {
        useSuperUser();

        executeNet(new SimpleStatement("CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
                   .setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE));
        executeNet(new SimpleStatement("CREATE TABLE test.test (pk int PRIMARY KEY, v int)")
                   .setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE));

        try
        {
            BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            batch.add(new SimpleStatement("INSERT INTO test.test (pk, v) VALUES (1, 2);"));
            executeNet(batch.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM));
            Assert.fail("Expecting NoHostAvailableException");
        }
        catch (NoHostAvailableException e)
        {
            Stack<AuditableEvent> events = InProcTestAuditWriter.getEvents();
            assertEventProperties(events.pop(), REQUEST_FAILURE, "test", "test",
                                  "Cannot achieve consistency level QUORUM INSERT INTO test.test (pk, v) VALUES (1, 2);", ConsistencyLevel.QUORUM);
            assertEventProperties(events.pop(), CQL_UPDATE, "test", "test",
                                  "INSERT INTO test.test (pk, v) VALUES (1, 2);", ConsistencyLevel.QUORUM);
        }
        finally
        {
            execute("DROP KEYSPACE test");
        }
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

    private void assertEventProperties(AuditableEvent event,
                                       AuditableEventType type,
                                       String keyspace,
                                       String table,
                                       String operation,
                                       org.apache.cassandra.db.ConsistencyLevel cl)
     {
        assertEventProperties(event, type, keyspace, table, operation);
        assertEquals(cl, event.getConsistencyLevel());
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
