/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.net.InetAddress;
import java.util.Stack;
import java.util.UUID;

import org.junit.*;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnauthorizedException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.datastax.bdp.db.audit.CoreAuditableEventType.*;

public class AuditLoggerFilteringTest extends CQLTester
{
    private static ForwardingAuditFilter filter = new ForwardingAuditFilter();

    @BeforeClass
    public static void setUpClass()
    {
        requireAuthentication();

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

        executeNet("CREATE USER user1 WITH PASSWORD 'user1'");
        executeNet("CREATE USER user2 WITH PASSWORD 'user2'");
        executeNet("CREATE ROLE audited");
        executeNet("GRANT audited TO user1");
        executeNet("GRANT ALL PERMISSIONS ON KEYSPACE " + KEYSPACE + " TO user1");
        executeNet("GRANT ALL PERMISSIONS ON KEYSPACE " + KEYSPACE + " TO user2");

        InProcTestAuditWriter.reset();
    }

    @After
    public void tearDown() throws Throwable
    {
        useSuperUser();
        executeNet("DROP USER user1");
        executeNet("DROP USER user2");
        executeNet("DROP ROLE audited");
    }

    @Test
    public void testFilteringForDmlCategory() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");

        // Test filtering for DML category
        AuditLoggingOptions options = new AuditLoggingOptions();
        options.excluded_keyspaces = "system.*";
        options.included_categories = AuditableEventCategory.DML + ", " + AuditableEventCategory.ERROR;
        options.included_roles = "audited";

        filter.delegate = AuditFilters.fromConfiguration(options);

        // With audited user
        useUser("user1", "user1");

        executeSimpleQueries();

        assertEventProperties(getEvents().pop(), CQL_PREPARE_STATEMENT, KEYSPACE, currentTable(), formatQuery("SELECT * FROM %s WHERE k = ?"));
        assertEventProperties(getEvents().pop(), CQL_UPDATE, KEYSPACE, currentTable(), formatQuery("INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP ? AND TTL ? [k=0,v=24,[timestamp]=999,[ttl]=1]"));
        assertEventProperties(getEvents().pop(), CQL_PREPARE_STATEMENT, KEYSPACE, currentTable(), formatQuery("INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP ? AND TTL ?"));
        assertTrue(getEvents().isEmpty());

        // With non audited user
        useUser("user2", "user2");

        executeSimpleQueries();
        assertTrue(getEvents().isEmpty());
    }

    @Test
    public void testFilteringForQueryCategory() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");

        // Test filtering for DML category
        AuditLoggingOptions options = new AuditLoggingOptions();
        options.excluded_keyspaces = "system.*";
        options.included_categories = AuditableEventCategory.QUERY + ", " + AuditableEventCategory.ERROR;
        options.included_roles = "audited";

        filter.delegate = AuditFilters.fromConfiguration(options);

        // With audited user
        useUser("user1", "user1");

        executeSimpleQueries();

        assertEventProperties(getEvents().pop(), CQL_SELECT, KEYSPACE, currentTable(), formatQuery("SELECT * FROM %s WHERE k = ? [k=0]"));
        assertEventProperties(getEvents().pop(), CQL_SELECT, KEYSPACE, currentTable(), formatQuery("SELECT * FROM %s"));
        assertTrue(getEvents().isEmpty());

        // With non audited user
        useUser("user2", "user2");

        executeSimpleQueries();
        assertTrue(getEvents().isEmpty());
    }

    private void executeSimpleQueries() throws Throwable
    {
        // Insert preparation and execution
        PreparedStatement ps = prepareNet(formatQuery("INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP ? AND TTL ?"));
        executeNet(ps.bind(0, 24, 999l, 1));

        // Simple query execution
        executeNet("SELECT * FROM %s");

        // Query preparation and execution
        executeNet(prepareNet(formatQuery("SELECT * FROM %s WHERE k = ?")).bind(0));
    }

    @Test
    public void testFilteringForConsistencyFailures() throws Throwable
    {
        executeNet(new SimpleStatement("CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
                   .setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE));
        executeNet(new SimpleStatement("CREATE TABLE test.test (pk int PRIMARY KEY, v int)")
                   .setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE));
        executeNet("GRANT ALL PERMISSIONS ON KEYSPACE test TO user1");
        executeNet("GRANT ALL PERMISSIONS ON KEYSPACE test TO user2");

        InProcTestAuditWriter.reset();

        try
        {
            // Test filtering for ERROR category
            AuditLoggingOptions options = new AuditLoggingOptions();
            options.excluded_keyspaces = "system.*";
            options.included_categories = AuditableEventCategory.DML + ", " + AuditableEventCategory.ERROR;
            options.included_roles = "audited";

            filter.delegate = AuditFilters.fromConfiguration(options);

            useUser("user1", "user1");

            triggerConsistencyFailure();

            assertEventProperties(getEvents().pop(), REQUEST_FAILURE, "test", "test",
                                  "Cannot achieve consistency level QUORUM SELECT * FROM test.test", ConsistencyLevel.QUORUM);
            assertEventProperties(getEvents().pop(), CQL_PREPARE_STATEMENT, "test", "test",
                                  "SELECT * FROM test.test", null); // prepared statement belongs to the DML category
            assertTrue(getEvents().isEmpty());

            useUser("user2", "user2");

            triggerConsistencyFailure();

            assertTrue(getEvents().isEmpty());

            // Test filtering without ERROR category
            options.included_categories = AuditableEventCategory.QUERY.toString();

            filter.delegate = AuditFilters.fromConfiguration(options);

            useUser("user1", "user1");

            triggerConsistencyFailure();

            assertEventProperties(getEvents().pop(), CQL_SELECT, "test", "test", "SELECT * FROM test.test", ConsistencyLevel.QUORUM);
            assertTrue(getEvents().isEmpty());

            useUser("user2", "user2");

            triggerConsistencyFailure();

            assertTrue(getEvents().isEmpty());
        }
        finally
        {
            execute("DROP KEYSPACE test");
        }
    }

    private void triggerConsistencyFailure()
    {
        try
        {
            PreparedStatement stmt = prepareNet("SELECT * FROM test.test");
            executeNet(stmt.bind().setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM));
            fail("Expecting NoHostAvailableException");
        }
        catch (NoHostAvailableException e)
        {
            assertTrue(true);
        }
    }

    @Test
    public void testFilteringForInvalidQueries() throws Exception
    {
        // Test filtering for ERROR category
        AuditLoggingOptions options = new AuditLoggingOptions();
        options.excluded_keyspaces = "system.*";
        options.included_categories = AuditableEventCategory.DML + ", " + AuditableEventCategory.ERROR;
        options.included_roles = "audited";

        filter.delegate = AuditFilters.fromConfiguration(options);

        useUser("user1", "user1");

        executeInvalidQuery();

        assertEventProperties(getEvents().pop(), REQUEST_FAILURE, null, null, "keyspace test does not exist SELECT * FROM test.test", null);
        assertTrue(getEvents().isEmpty());

        useUser("user2", "user2");

        executeInvalidQuery();

        assertTrue(getEvents().isEmpty());
    }

    private void executeInvalidQuery()
    {
        try
        {
            executeNet(new SimpleStatement("SELECT * FROM test.test").setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM));
            fail("Expecting InvalidQueryException");
        }
        catch (InvalidQueryException e)
        {
            assertTrue(true);
        }
    }

    @Test
    public void testLoggingForUnauthorizedQueries() throws Throwable
    {
        executeNet(new SimpleStatement("CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
                   .setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE));
        executeNet(new SimpleStatement("CREATE TABLE test.test (pk int PRIMARY KEY, v int)")
                   .setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE));

        InProcTestAuditWriter.reset();

        try
        {
            // Test filtering for ERROR category
            AuditLoggingOptions options = new AuditLoggingOptions();
            options.excluded_keyspaces = "system.*";
            options.included_categories = AuditableEventCategory.AUTH.toString();
            options.included_roles = "audited";

            filter.delegate = AuditFilters.fromConfiguration(options);

            useUser("user1", "user1");

            executeUnauthorizedQuery();

            assertEventProperties(getEvents().pop(), UNAUTHORIZED_ATTEMPT, "test", "test",
                                  "User user1 has no SELECT permission on <table test.test> or any of its parents SELECT * FROM test.test", ConsistencyLevel.LOCAL_ONE);
            assertTrue(getEvents().isEmpty());

            useUser("user2", "user2");

            assertTrue(getEvents().isEmpty());
        }
        finally
        {
            execute("DROP KEYSPACE test");
        }
    }

    private void executeUnauthorizedQuery()
    {
        try
        {
            PreparedStatement stmt = prepareNet("SELECT * FROM test.test");
            executeNet(stmt.bind());
            Assert.fail("Expecting UnauthorizedException");
        }
        catch (UnauthorizedException e)
        {
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

    private static class ForwardingAuditFilter implements IAuditFilter
    {
        public volatile IAuditFilter delegate = AuditFilters.acceptEverything();

        @Override
        public boolean accept(AuditableEvent event)
        {
            return delegate.accept(event);
        }
    }
}
