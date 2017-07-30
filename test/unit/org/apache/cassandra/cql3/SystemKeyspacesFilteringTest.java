/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.UnauthorizedException;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.Resources;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SizeEstimatesRecorder;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.utils.Streams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.apache.cassandra.schema.SchemaKeyspace.*;

public class SystemKeyspacesFilteringTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();

        DatabaseDescriptor.enableSystemKeyspaceFiltering();
        DatabaseDescriptor.setRolesValidity(9999);
        DatabaseDescriptor.setRolesUpdateInterval(9999);
        DatabaseDescriptor.setPermissionsValidity(9999);
        DatabaseDescriptor.setPermissionsUpdateInterval(9999);
        requireAuthentication();
        requireNetwork();
    }

    @Before
    public void beforeTest() throws Throwable
    {
        String query = String.format("SELECT * FROM %s.%s WHERE role = '%s'",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.ROLES,
                                     CassandraRoleManager.DEFAULT_SUPERUSER_NAME);

        // Wait until the 'cassandra' use is in system_auth.roles (i.e. auth has been setup)
        while (execute(query).isEmpty())
            Thread.yield();
    }

    @After
    public void afterTest() throws Throwable
    {
        useSuperUser();
        executeNet("DROP ROLE IF EXISTS one");
        executeNet("DROP ROLE IF EXISTS two");

        executeNet("DROP KEYSPACE IF EXISTS ks_one");
        executeNet("DROP KEYSPACE IF EXISTS  ks_two");

        super.afterTest();
    }

    @Test
    public void testKeyspacesAndTablesFiltering() throws Throwable
    {
        useSuperUser();

        // create ks "ks_generic"
        String ksGeneric = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        // create tab t_gen_one
        createTable(ksGeneric, "CREATE TABLE %s (id int PRIMARY KEY, val text)");
        // create tab t_gen_two
        createTable(ksGeneric, "CREATE TABLE %s (id int PRIMARY KEY, val text)");

        // Create roles and grant permissions
        executeNet("CREATE ROLE one WITH LOGIN = true AND PASSWORD = 'one'");
        executeNet("CREATE ROLE two WITH LOGIN = true AND PASSWORD = 'two'");
        executeNet("GRANT CREATE ON ALL KEYSPACES TO one");
        executeNet("GRANT CREATE ON ALL KEYSPACES TO two");

        SizeEstimatesRecorder.instance.run();

        // Switch to user "one"
        useUser("one", "one");

        Metadata meta = getMetadata();

        checkMetadata(meta, new TableSetBuilder().addSystemTables()
                                                 .build());

        executeNet("CREATE KEYSPACE ks_one WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

        checkMetadata(meta, new TableSetBuilder().addSystemTables()
                                                 .addEmptyKeyspace("ks_one")
                                                 .build());

        executeNet("USE ks_one");

        // create a bunch of schema objects to verify later
        executeNet("CREATE TABLE ks_one.t_one (id int PRIMARY KEY, val text, drop_one text)");
        executeNet("ALTER TABLE ks_one.t_one DROP drop_one");
        executeNet("CREATE INDEX ks_one_t_one ON ks_one.t_one (val)");
        executeNet("CREATE TABLE ks_one.t_two (id int PRIMARY KEY, val text)");
        executeNet("CREATE INDEX ks_one_t_two ON ks_one.t_two (val)");
        executeNet("CREATE FUNCTION ks_one.f_one(a int, b int) " +
                          "CALLED ON NULL INPUT " +
                          "RETURNS int " +
                          "LANGUAGE java " +
                          "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");
        executeNet("CREATE AGGREGATE ks_one.a_one(int) " +
                          "SFUNC f_one " +
                          "STYPE int ");
        executeNet("CREATE TYPE ks_one.udt_one(a int, b int)");
        executeNet("CREATE OR REPLACE FUNCTION ks_one.f_two() " +
                          "RETURNS NULL ON NULL INPUT " +
                          "RETURNS bigint " +
                          "LANGUAGE JAVA\n" +
                          "AS 'return 1L;'");
        executeNet("GRANT DESCRIBE ON KEYSPACE ks_one TO two");

        checkMetadata(meta, new TableSetBuilder().addSystemTables()
                                                 .addTable("ks_one", "t_one")
                                                 .addTable("ks_one", "t_two")
                                                 .build());

        // Switch to user "two"
        useUser("two", "two");

        meta = getMetadata();

        checkMetadata(meta, new TableSetBuilder().addSystemTables()
                                                 .addTable("ks_one", "t_one")
                                                 .addTable("ks_one", "t_two")
                                                 .build());

        executeNet("CREATE KEYSPACE ks_two WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

        checkMetadata(meta, new TableSetBuilder().addSystemTables()
                                                 .addTable("ks_one", "t_one")
                                                 .addTable("ks_one", "t_two")
                                                 .addEmptyKeyspace("ks_two")
                                                 .build());

        // create a bunch of schema objects to verify later
        executeNet("CREATE TABLE ks_two.t_one (id int PRIMARY KEY, val text, drop_one text)");
        executeNet("ALTER TABLE ks_two.t_one DROP drop_one");
        executeNet("CREATE INDEX ks_two_t_one ON ks_two.t_one (val)");
        executeNet("CREATE TABLE ks_two.t_two (id int PRIMARY KEY, val text)");
        executeNet("CREATE INDEX ks_two_t_two ON ks_two.t_two (val)");
        executeNet("CREATE FUNCTION ks_two.f_one(a int, b int) " +
                          "CALLED ON NULL INPUT " +
                          "RETURNS int " +
                          "LANGUAGE java " +
                          "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");
        executeNet("CREATE AGGREGATE ks_two.a_one(int) " +
                          "SFUNC f_one " +
                          "STYPE int ");
        executeNet("CREATE TYPE ks_two.udt_one(a int, b int)");
        executeNet("CREATE OR REPLACE FUNCTION ks_two.f_two() " +
                          "RETURNS NULL ON NULL INPUT " +
                          "RETURNS bigint " +
                          "LANGUAGE JAVA\n" +
                          "AS 'return 1L;'");

        Multimap<String, String> tables = new TableSetBuilder().addSystemTables()
                                                               .addTable("ks_one", "t_one")
                                                               .addTable("ks_one", "t_two")
                                                               .addTable("ks_two", "t_one")
                                                               .addTable("ks_two", "t_two")
                                                               .build();
        checkMetadata(meta, tables);
        checkAccess(tables);

        useUser("one", "one");

        meta = getMetadata();

        tables = new TableSetBuilder().addSystemTables()
                                      .addTable("ks_one", "t_one")
                                      .addTable("ks_one", "t_two")
                                      .build();

        executeNet("USE ks_two");

        checkMetadata(meta, tables);
        checkAccess(tables);

        useSuperUser();

        checkAccess(new TableSetBuilder().addSystemTables()
                                         .addTablesFrom("ks_one")
                                         .addTablesFrom("ks_two")
                                         .addTablesFrom(ksGeneric)
                                         .build());
    }

    private Metadata getMetadata()
    {
        return sessionNet().getCluster().getMetadata();
    }

    @Test
    public void testQueriesOnSystemTables() throws Throwable
    {
        useSuperUser();

        // Create roles and grant permissions
        executeNet("CREATE ROLE one WITH LOGIN = true AND PASSWORD = 'one'");

        assertRowsNet(executeNet("SELECT DISTINCT keyspace_name FROM system_schema.keyspaces " +
                                    "WHERE keyspace_name IN (\'system\', \'system_schema\', " +
                                    "\'system_distributed\', \'system_auth\', \'system_traces\')"),
                      row(SchemaConstants.SYSTEM_KEYSPACE_NAME),
                      row(SchemaConstants.AUTH_KEYSPACE_NAME),
                      row(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME),
                      row(SchemaConstants.SCHEMA_KEYSPACE_NAME),
                      row(SchemaConstants.TRACE_KEYSPACE_NAME));

        useUser("one", "one");

        // Verify SELECT DISTINCT works.
        // (technically only "static" columns will be filtered in
        // SystemKeyspacesFilteringRestrictions.SystemKeyspacesRestrictions.Expression.isSatisfiedBy() - i.e.
        // no clustering key for the table name)
        assertRowsNet(executeNet("SELECT DISTINCT keyspace_name FROM system_schema.keyspaces " +
                                               "WHERE keyspace_name IN ('system', 'system_schema', " +
                                               "'system_distributed', 'system_auth', 'system_traces')"),
                             row(SchemaConstants.SYSTEM_KEYSPACE_NAME),
                             row(SchemaConstants.SCHEMA_KEYSPACE_NAME));

        assertRowsNet(executeNet("SELECT DISTINCT keyspace_name FROM system_schema.tables " +
                                               "WHERE keyspace_name IN ('system', 'system_schema', " +
                                               "'system_distributed', 'system_auth', 'system_traces')"),
                             row(SchemaConstants.SYSTEM_KEYSPACE_NAME),
                             row(SchemaConstants.SCHEMA_KEYSPACE_NAME));

        // Verify schema information of "accessible" system-ks tables
        assertRowsNet(executeNet("SELECT table_name FROM system_schema.tables " +
                                               "WHERE keyspace_name = 'system'"),
                             row(SystemKeyspace.BUILT_INDEXES),
                             row(SystemKeyspace.AVAILABLE_RANGES),
                             row(SystemKeyspace.BUILT_VIEWS),
                             row(SystemKeyspace.LOCAL),
                             row(SystemKeyspace.PEERS),
                             row(SystemKeyspace.SIZE_ESTIMATES),
                             row(SystemKeyspace.SSTABLE_ACTIVITY),
                             row(SystemKeyspace.VIEWS_BUILDS_IN_PROGRESS));

        // Verify schema information of "hidden" system-ks tables
        for (String table : Arrays.asList(SystemKeyspace.BATCHES,
                                          SystemKeyspace.PAXOS,
                                          SystemKeyspace.PEER_EVENTS,
                                          SystemKeyspace.RANGE_XFERS,
                                          SystemKeyspace.COMPACTION_HISTORY,
                                          SystemKeyspace.TRANSFERRED_RANGES,
                                          SystemKeyspace.PREPARED_STATEMENTS,
                                          SystemKeyspace.REPAIRS))
        {
            assertRowsNet(executeNet("SELECT table_name FROM system_schema.tables " +
                                                   "WHERE keyspace_name = 'system' AND table_name= '" + table + '\''));
        }
    }

    @Test
    public void testInvalidQueries() throws Throwable
    {
        useSuperUser();

        executeNet("CREATE KEYSPACE ks_one WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        executeNet("CREATE TABLE ks_one.t_one (id int PRIMARY KEY, val text, drop_one text)");

        // Create role
        executeNet("CREATE ROLE one WITH LOGIN = true AND PASSWORD = 'one'");

        useUser("one", "one");

        verifyUnauthorized("SELECT", "t_one", "SELECT id, val, drop_one FROM ks_one.t_one");
        verifyUnauthorized("MODIFY", "t_one", "UPDATE ks_one.t_one SET val = ? WHERE id = ?", "foo", 1);
        verifyUnauthorized("MODIFY", "t_one", "DELETE FROM ks_one.t_one WHERE id = ?", 1);
    }

    /**
     * Checks that the metadata contains only the allowed tables
     * @param metadata the metadata
     * @param allowedTables the allowed tables per keyspaces
     */
    private void checkMetadata(Metadata metadata, Multimap<String, String> allowedTables)
    {
        Multimap<String, String> visibleTables = HashMultimap.create();
        for (KeyspaceMetadata kMeta : metadata.getKeyspaces())
        {
            for (TableMetadata tMeta : kMeta.getTables())
            {
                visibleTables.put(kMeta.getName(), tMeta.getName());
            }
        }

        if (!visibleTables.equals(allowedTables))
            fail(String.format("The metadata tables do not match the expected onces. diff: %s",
                               Maps.difference(allowedTables.asMap(), visibleTables.asMap())));
    }

    /**
     * Checks that the queries on the system keyspaces tables return only data for the allowed tables
     * @param allowed the allowed tables per keyspaces
     */
    private void checkAccess(Multimap<String, String> allowed) throws Throwable
    {
        checkKeyspacesVisibility(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.AVAILABLE_RANGES, Collections.emptySet());
        checkTablesVisibility(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SIZE_ESTIMATES, allowed);

        for (String table : Arrays.asList(TABLES, COLUMNS, DROPPED_COLUMNS, VIEWS, INDEXES))
        {
            checkTablesVisibility(SchemaConstants.SCHEMA_KEYSPACE_NAME, table, allowed);
        }

        for (String table : Arrays.asList(KEYSPACES, FUNCTIONS, AGGREGATES, TRIGGERS, TYPES))
        {
            checkKeyspacesVisibility(SchemaConstants.SCHEMA_KEYSPACE_NAME, table, allowed.keySet());
        }

        if (!isSuperUser())
        {
            checkUnauthorized(SchemaConstants.TRACE_KEYSPACE_NAME, TraceKeyspace.SESSIONS);
            checkUnauthorized(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.REPAIR_HISTORY);
            checkUnauthorized(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES);
            checkUnauthorized(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PREPARED_STATEMENTS);
        }
        else
        {
            checkAuthorized(SchemaConstants.TRACE_KEYSPACE_NAME, TraceKeyspace.SESSIONS);
            checkAuthorized(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.REPAIR_HISTORY);
            checkRolesVisibility(Sets.newHashSet("cassandra", "one", "two"));
            checkAuthorized(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PREPARED_STATEMENTS);
        }
    }

    /**
     * Checks that fetching data from the specified table is not allowed.
     * @param keyspace the table keyspace
     * @param table the table
     */
    private void checkUnauthorized(String keyspace, String table) throws Throwable
    {
        try
        {
            fetchDataFrom(keyspace, table);
            fail();
        }
        catch (UnauthorizedException e)
        {
            // ok
        }
    }

    /**
     * Checks that executing the specified cause an {@code UnauthorizedException}.
     */
    private void verifyUnauthorized(String perm, String table, String query, Object... arguments) throws Throwable
    {
        try
        {
            // Execute against a keyspace on which a user has no DESCRIBE permission should
            // fail with an UnauthorizedException
            sessionNet().execute(query, arguments);
            fail();
        }
        catch (UnauthorizedException ue)
        {
            assertEquals("For execute of DML " + query,
                         String.format("User one has no %s permission on <table ks_one.%s> or any of its parents", perm, table),
                         ue.getMessage());
        }
    }

    /**
     * Checks that fetching data from the specified table is authorized.
     * @param keyspace the table keyspace
     * @param table the table
     */
    private void checkAuthorized(String keyspace, String table) throws Throwable
    {
        fetchDataFrom(keyspace, table);
    }

    /**
     * Checks that the tables visible in the specified system table are the allowed ones.
     * @param keyspace the system keyspace
     * @param table the system table
     * @param allowedTables the allowed tables
     */
    private void checkTablesVisibility(String keyspace, String table, Multimap<String, String> allowedTables) throws Throwable
    {
        Multimap<String, String> visibleTables = fetchKeyspacesAndTables(keyspace, table);

        // For the tables and columns tables the allowedTables are in fact mandatory, so we perform a special check for those.
        if (keyspace.equals(SchemaConstants.SCHEMA_KEYSPACE_NAME) && (table.equals(TABLES) || table.equals(COLUMNS)))
        {
            if (!visibleTables.equals(allowedTables))
                fail(String.format("The visible tables in %s.%s do not match the expected onces. diff: %s",
                                     keyspace, table, Maps.difference(allowedTables.asMap(), visibleTables.asMap())));
        }
        else
        {
            for (String visibleKeyspace : visibleTables.keySet())
            {
                for (String visibleTable : visibleTables.get(visibleKeyspace))
                {
                    assertTrue(String.format("A non allowed table %s.%s has been returned.",
                                             visibleKeyspace, visibleTable),
                               allowedTables.containsEntry(visibleKeyspace, visibleTable));
                }
            }
        }
    }

    /**
     * Checks that the keyspaces visible in the specified system table are the allowed ones.
     * @param keyspace the system keyspace
     * @param table the system table
     * @param allowedKeyspaces the allowed keyspaces
     */
    private void checkKeyspacesVisibility(String keyspace, String table, Set<String> allowedKeyspaces) throws Throwable
    {
        Set<String> visibleKeyspaces = fetchKeyspaces(keyspace, table);

        for (String visibleKeyspace : visibleKeyspaces)
        {
            assertTrue(String.format("A non allowed keyspace %s has been returned.", visibleKeyspace),
                       allowedKeyspaces.contains(visibleKeyspace));
        }
    }

    private void checkRolesVisibility(Set<String> roles) throws Throwable
    {
        assertEquals(fetchRoleNames(), roles);
    }

    /**
     * Retrieves all the tables visible for the current user in the specified system table.
     *
     * @param keyspace the keyspace name of the system table to read from
     * @param table the table name of the system table to read from
     * @return all the tables visible for the current user in the specified system table
     */
    private Multimap<String, String> fetchKeyspacesAndTables(String keyspace, String table) throws Throwable
    {
        Multimap<String, String> tables = HashMultimap.create();
        for (Row row : fetchDataFrom(keyspace, table))
            tables.put(row.getString("keyspace_name"), row.getString("table_name"));
        return tables;
    }

    /**
     * Retrieves all the keyspaces visible for the current user in the specified system table.
     *
     * @param keyspace the table keyspace
     * @param table the system table
     * @return all the keyspaces visible for the current user in the specified system table
     */
    private Set<String> fetchKeyspaces(String keyspace, String table) throws Throwable
    {
        return Streams.of(fetchDataFrom(keyspace, table))
                      .map(row -> row.getString("keyspace_name"))
                      .collect(Collectors.toSet());
    }

    private Set<String> fetchRoleNames() throws Throwable
    {
        return Streams.of(fetchDataFrom(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES))
                .map(row -> row.getString(0))
                .collect(Collectors.toSet());
    }

    /**
     * Fetchs the visible data from the specified system table.
     *
     * @param keyspace the keyspace
     * @param table the system table
     * @return the visible data from the specified system table
     */
    private ResultSet fetchDataFrom(String keyspace, String table)
    {
        return sessionNet().execute(String.format("SELECT * FROM %s.%s", keyspace, table));
    }

    /**
     * Builder used to create a set of tables.
     */
    private final class TableSetBuilder
    {
        private Multimap<String, String> tables = HashMultimap.create();

        /**
         * Adds all the tables from the specified keyspace that are visible for the current user.
         * @param keyspace the keyspace name
         * @return this {@code TableSetBuilder}
         */
        public TableSetBuilder addTablesFrom(String keyspace)
        {
            for (String table : Schema.instance.getKeyspaceMetadata(keyspace).tables.tableNames())
            {
                if ((!SchemaConstants.isLocalSystemKeyspace(keyspace) && !SchemaConstants.isReplicatedSystemKeyspace(keyspace))
                        || isSuperUser() || Resources.isAlwaysReadable(DataResource.table(keyspace, table)))
                    tables.put(keyspace, table);
            }

            return this;
        }

        /**
         * Adds all the tables from the system keyspaces that are visible for the current user.
         * @return this {@code TableSetBuilder}
         */
        public TableSetBuilder addSystemTables()
        {
            for (String keyspace : SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES)
            {
                addTablesFrom(keyspace);
            }

            for (String keyspace : SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES)
            {
                addTablesFrom(keyspace);
            }
            return this;
        }

        /**
         * Adds a keyspaces containing no tables.
         * @param keyspace the keyspace name
         * @return this {@code TableSetBuilder}
         */
        public TableSetBuilder addEmptyKeyspace(String keyspace)
        {
            tables.putAll(keyspace, Collections.emptyList());
            return this;
        }

        /**
         * Adds the specified table.
         * @param keyspace the keyspace name
         * @param table the table name
         * @return this {@code TableSetBuilder}
         */
        public TableSetBuilder addTable(String keyspace, String table)
        {
            tables.put(keyspace, table);
            return this;
        }

        public Multimap<String, String> build()
        {
            addSystemTables();
            return tables;
        }
    }
}
