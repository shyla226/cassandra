/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3;

import java.util.*;
import java.util.stream.Collectors;

import org.junit.*;
import org.junit.runner.RunWith;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.db.SizeEstimatesRecorder;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.CQLTester.row;
import static org.junit.Assert.*;

@RunWith(OrderedJUnit4ClassRunner.class)
public class SystemKeyspacesFilteringTest
{
    private static CQLTester tester;

    @BeforeClass
    public static void setup() throws Throwable
    {
        System.setProperty("cassandra.config", "cassandra-systemKeyspacesFiltering.yaml");
        CQLTester.setUpClass();

        tester = new CQLTester()
        {
        };

        CQLTester.requireNetwork();

        // Wait until the 'cassandra' use is in system_auth.roles (i.e. auth has been setup)
        while (true)
        {
            UntypedResultSet rset = tester.execute(String.format("SELECT * FROM %s.%s WHERE role = '%s'",
                                                                 SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES, CassandraRoleManager.DEFAULT_SUPERUSER_NAME));
            if (!rset.isEmpty())
                break;
            Thread.sleep(100L);
        }
    }

    @AfterClass
    public static void teardown()
    {
        for (String ddl : Arrays.asList("DROP KEYSPACE ks_one",
                                        "DROP KEYSPACE ks_two",
                                        "DROP USER one",
                                        "DROP USER two"))
            try
            {
                tester.execute(ddl);
            }
            catch (Throwable throwable)
            {
                // ignore
            }

        CQLTester.tearDownClass();
    }

    static String ksGeneric;
    static String tGenOne;
    static String tGenTwo;

    @Test
    public void test_01_setupSchema() throws Throwable
    {
        // create ks "ks_generic"
        ksGeneric = tester.createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        // create tab t_gen_one
        tGenOne = tester.createTable(ksGeneric, "CREATE TABLE %s (id int PRIMARY KEY, val text)");
        // create tab t_gen_two
        tGenTwo = tester.createTable(ksGeneric, "CREATE TABLE %s (id int PRIMARY KEY, val text)");

        tester.useSuperUser();

        tester.executeNet("CREATE USER one WITH PASSWORD 'one'");
        tester.executeNet("CREATE USER two WITH PASSWORD 'two'");
        tester.executeNet("GRANT CREATE ON ALL KEYSPACES TO one");
        tester.executeNet("GRANT CREATE ON ALL KEYSPACES TO two");

        tester.assertRowsNet(tester.executeNet("SELECT DISTINCT keyspace_name FROM system_schema.keyspaces " +
                                               "WHERE keyspace_name IN (\'system\', \'system_schema\', " +
                                               "\'system_distributed\', \'system_auth\', \'system_traces\')"),
                             row("system"),
                             row("system_auth"),
                             row("system_distributed"),
                             row("system_schema"),
                             row("system_traces"));

        // Switch to user "one"

        tester.useUser("one", "one");

        Metadata meta = tester.sessionNet().getCluster().getMetadata();
        assertNull(meta.getKeyspace("ks_one"));

        tester.executeNet("CREATE KEYSPACE ks_one WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        assertNotNull(meta.getKeyspace("ks_one"));

        // create a bunch of schema objects to verify later
        tester.executeNet("CREATE TABLE ks_one.t_one (id int PRIMARY KEY, val text, drop_one text)");
        tester.executeNet("ALTER TABLE ks_one.t_one DROP drop_one");
        tester.executeNet("CREATE INDEX ks_one_t_one ON ks_one.t_one (val)");
        tester.executeNet("CREATE TABLE ks_one.t_two (id int PRIMARY KEY, val text)");
        tester.executeNet("CREATE INDEX ks_one_t_two ON ks_one.t_two (val)");
        tester.executeNet("CREATE FUNCTION ks_one.f_one(a int, b int) " +
                          "CALLED ON NULL INPUT " +
                          "RETURNS int " +
                          "LANGUAGE java " +
                          "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");
        tester.executeNet("CREATE AGGREGATE ks_one.a_one(int) " +
                          "SFUNC f_one " +
                          "STYPE int ");
        tester.executeNet("CREATE TYPE ks_one.udt_one(a int, b int)");
        tester.executeNet("CREATE OR REPLACE FUNCTION ks_one.f_two() " +
                          "RETURNS NULL ON NULL INPUT " +
                          "RETURNS bigint " +
                          "LANGUAGE JAVA\n" +
                          "AS 'return 1L;'");
        tester.executeNet("GRANT DESCRIBE ON KEYSPACE ks_one TO two");

        assertNotNull(meta.getKeyspace("ks_one").getTable("t_two"));

        // Switch to user "two"

        tester.useUser("two", "two");
        Metadata meta2 = tester.sessionNet().getCluster().getMetadata();
        assertNotNull(meta2.getKeyspace("ks_one"));
        assertNotNull(meta2.getKeyspace("ks_one").getTable("t_two"));

        assertNull(meta2.getKeyspace("ks_two"));

        tester.executeNet("CREATE KEYSPACE ks_two WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        assertNotNull(meta2.getKeyspace("ks_two"));

        // create a bunch of schema objects to verify later
        tester.executeNet("CREATE TABLE ks_two.t_one (id int PRIMARY KEY, val text, drop_one text)");
        tester.executeNet("ALTER TABLE ks_two.t_one DROP drop_one");
        tester.executeNet("CREATE INDEX ks_two_t_one ON ks_two.t_one (val)");
        tester.executeNet("CREATE TABLE ks_two.t_two (id int PRIMARY KEY, val text)");
        tester.executeNet("CREATE INDEX ks_two_t_two ON ks_two.t_two (val)");
        tester.executeNet("CREATE FUNCTION ks_two.f_one(a int, b int) " +
                          "CALLED ON NULL INPUT " +
                          "RETURNS int " +
                          "LANGUAGE java " +
                          "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'");
        tester.executeNet("CREATE AGGREGATE ks_two.a_one(int) " +
                          "SFUNC f_one " +
                          "STYPE int ");
        tester.executeNet("CREATE TYPE ks_two.udt_one(a int, b int)");
        tester.executeNet("CREATE OR REPLACE FUNCTION ks_two.f_two() " +
                          "RETURNS NULL ON NULL INPUT " +
                          "RETURNS bigint " +
                          "LANGUAGE JAVA\n" +
                          "AS 'return 1L;'");

        assertNotNull(meta2.getKeyspace("ks_two").getTable("t_two"));

        // "nested" session
        tester.useUser("one", "one");

        Metadata meta1 = tester.sessionNet().getCluster().getMetadata();

        tester.executeNet("CREATE TABLE ks_one.t_three (id int PRIMARY KEY, val text)");

        // user "one" must have metadata for ks_one
        assertNotNull(meta1.getKeyspace("ks_one").getTable("t_three"));

        // not allowed to see these keyspaces - i.e. there must be no metadata
        assertNull(meta1.getKeyspace("ks_two"));
        assertNull(meta1.getKeyspace("system_auth"));
        assertNull(meta1.getKeyspace("system_traces"));
        assertNull(meta1.getKeyspace("system_distributed"));

        // we should have gotten a schema change notification about the new 't_three' table
        assertNotNull(meta.getKeyspace("ks_one").getTable("t_three"));

        // not allowed to see these keyspaces - i.e. there must be no metadata
        assertNull(meta.getKeyspace("system_auth"));
        assertNull(meta.getKeyspace("system_traces"));
        assertNull(meta.getKeyspace("system_distributed"));

        SizeEstimatesRecorder.instance.run();
    }

    @Test
    public void test_02_withCassandraUser()
    {
        tester.useSuperUser();

        Set<Pair<String, String>> allowed = new HashSet<>();
        allowed.add(Pair.create("ks_one", "t_one"));
        allowed.add(Pair.create("ks_one", "t_two"));
        allowed.add(Pair.create("ks_one", "t_three"));
        allowed.add(Pair.create("ks_two", "t_one"));
        allowed.add(Pair.create("ks_two", "t_two"));
        Set<Pair<String, String>> schemaMandatory = new HashSet<>(allowed);
        allowed.add(Pair.create(ksGeneric, null));
        SchemaConstants.SYSTEM_KEYSPACE_NAMES.forEach(ks -> allowed.add(Pair.create(ks, null)));
        SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES.forEach(ks -> allowed.add(Pair.create(ks, null)));

        checkAccess(tester.sessionNet(), allowed, schemaMandatory, true);
    }

    @Test
    public void test_03_withUserOne() throws Throwable
    {
        tester.useUser("one", "one");
        for (String dml : Arrays.asList("SELECT yeah, doo, boo FROM ks_two.nonsense",
                                        "UPDATE ks_two.nonsense SET foo = ? WHERE x = ?",
                                        "DELETE FROM ks_two.nonsense WHERE x = ?",
                                        "SELECT yeah, doo, boo FROM ks_two.t_one",
                                        "UPDATE ks_two.t_one SET foo = ? WHERE x = ?",
                                        "DELETE FROM ks_two.t_one WHERE x = ?"))
        {
            try
            {
                // Prepare against a keyspace on which a user has no DESCRIBE permission should
                // fail with an UnauthorizedException
                tester.sessionNet().prepare(dml);
                fail();
            }
            catch (Exception ue)
            {
                assertEquals("For prepare of DML " + dml,
                             "User one has no DESCRIBE permission on <keyspace ks_two> or any of its parents",
                             ue.getMessage());
            }
            try
            {
                // Execute against a keyspace on which a user has no DESCRIBE permission should
                // fail with an UnauthorizedException
                tester.sessionNet().execute(dml);
                fail();
            }
            catch (Exception ue)
            {
                assertEquals("For execute of DML " + dml,
                             "User one has no DESCRIBE permission on <keyspace ks_two> or any of its parents",
                             ue.getMessage());
            }
        }

        // Verify SELECT DISTINCT works.
        // (technically only "static" columns will be filtered in
        // SystemKeyspacesFilteringRestrictions.SystemKeyspacesRestrictions.Expression.isSatisfiedBy() - i.e.
        // no clustering key for the table name)
        tester.assertRowsNet(tester.executeNet("SELECT DISTINCT keyspace_name FROM system_schema.keyspaces " +
                                               "WHERE keyspace_name IN ('system', 'system_schema', " +
                                               "'system_distributed', 'system_auth', 'system_traces')"),
                             row(SchemaConstants.SYSTEM_KEYSPACE_NAME),
                             row(SchemaConstants.SCHEMA_KEYSPACE_NAME));
        tester.assertRowsNet(tester.executeNet("SELECT DISTINCT keyspace_name FROM system_schema.tables " +
                                               "WHERE keyspace_name IN ('system', 'system_schema', " +
                                               "'system_distributed', 'system_auth', 'system_traces')"),
                             row(SchemaConstants.SYSTEM_KEYSPACE_NAME),
                             row(SchemaConstants.SCHEMA_KEYSPACE_NAME));

        // Verify schema information of "accessible" system-ks tables
        tester.assertRowsNet(tester.executeNet("SELECT table_name FROM system_schema.tables " +
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
            tester.assertRowsNet(tester.executeNet("SELECT table_name FROM system_schema.tables " +
                                                   "WHERE keyspace_name = 'system' AND table_name= '" + table + '\''));
        }

        Set<Pair<String, String>> allowed = new HashSet<>();
        allowed.add(Pair.create("ks_one", "t_one"));
        allowed.add(Pair.create("ks_one", "t_two"));
        allowed.add(Pair.create("ks_one", "t_three"));
        Set<Pair<String, String>> schemaMandatory = new HashSet<>(allowed);
        allowed.add(Pair.create(SchemaConstants.SYSTEM_KEYSPACE_NAME, null));
        allowed.add(Pair.create(SchemaConstants.SCHEMA_KEYSPACE_NAME, null));
        withMandatorySchema(schemaMandatory);

        checkAccess(tester.sessionNet(), allowed, schemaMandatory, false);
    }

    @Test
    public void test_04_withUserTwo()
    {
        tester.useUser("two", "two");
        Set<Pair<String, String>> allowed = new HashSet<>();
        allowed.add(Pair.create("ks_one", "t_one"));
        allowed.add(Pair.create("ks_one", "t_two"));
        allowed.add(Pair.create("ks_one", "t_three"));
        allowed.add(Pair.create("ks_two", "t_one"));
        allowed.add(Pair.create("ks_two", "t_two"));
        Set<Pair<String, String>> schemaMandatory = new HashSet<>(allowed);
        allowed.add(Pair.create(SchemaConstants.SYSTEM_KEYSPACE_NAME, null));
        allowed.add(Pair.create(SchemaConstants.SCHEMA_KEYSPACE_NAME, null));
        withMandatorySchema(schemaMandatory);

        checkAccess(tester.sessionNet(), allowed, schemaMandatory, false);
    }

    private void withMandatorySchema(Set<Pair<String, String>> schemaMandatory)
    {
        schemaMandatory.add(Pair.create(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LOCAL));
        schemaMandatory.add(Pair.create(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PEERS));
        schemaMandatory.add(Pair.create(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SSTABLE_ACTIVITY));
        schemaMandatory.add(Pair.create(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SIZE_ESTIMATES));
        schemaMandatory.add(Pair.create(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BUILT_INDEXES));
        schemaMandatory.add(Pair.create(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BUILT_VIEWS));
        schemaMandatory.add(Pair.create(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.AVAILABLE_RANGES));
        schemaMandatory.add(Pair.create(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.VIEWS_BUILDS_IN_PROGRESS));
        schemaMandatory.add(Pair.create(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TABLES));
        schemaMandatory.add(Pair.create(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.COLUMNS));
        schemaMandatory.add(Pair.create(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.DROPPED_COLUMNS));
        schemaMandatory.add(Pair.create(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.AGGREGATES));
        schemaMandatory.add(Pair.create(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.FUNCTIONS));
        schemaMandatory.add(Pair.create(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.INDEXES));
        schemaMandatory.add(Pair.create(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TRIGGERS));
        schemaMandatory.add(Pair.create(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TYPES));
        schemaMandatory.add(Pair.create(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.VIEWS));
    }

    private void checkAccess(Session s, Set<Pair<String, String>> allowed, Set<Pair<String, String>> schemaMandatory, boolean superuser)
    {
        assertEmpty(s.execute(String.format("SELECT * FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.AVAILABLE_RANGES)));

        assertKsTab(allowed, Collections.emptySet(), s.execute(String.format("SELECT * FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SIZE_ESTIMATES)));

        assertKs(allowed, s.execute(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.KEYSPACES)));
        assertKsTab(allowed, schemaMandatory, s.execute(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TABLES)));
        assertKsTab(allowed, schemaMandatory, s.execute(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.COLUMNS)));
        assertKsTab(allowed, Collections.emptySet(), s.execute(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.DROPPED_COLUMNS)));
        assertKsTab(allowed, Collections.emptySet(), s.execute(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.VIEWS)));
        assertKs(allowed, s.execute(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.FUNCTIONS)));
        assertKs(allowed, s.execute(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.AGGREGATES)));
        assertKsTab(allowed, Collections.emptySet(), s.execute(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.INDEXES)));
        assertKs(allowed, s.execute(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TRIGGERS)));
        assertKs(allowed, s.execute(String.format("SELECT * FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TYPES)));

        if (!superuser)
        {
            assertFail(s, String.format("SELECT * FROM %s.%s", SchemaConstants.TRACE_KEYSPACE_NAME, TraceKeyspace.SESSIONS));
            assertFail(s, String.format("SELECT * FROM %s.%s", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.REPAIR_HISTORY));
            assertFail(s, String.format("SELECT * FROM %s.%s", SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES));
            assertFail(s, String.format("SELECT * FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PREPARED_STATEMENTS));
        }
        else
        {
            assertNotFail(s, String.format("SELECT * FROM %s.%s", SchemaConstants.TRACE_KEYSPACE_NAME, TraceKeyspace.SESSIONS));
            assertNotFail(s, String.format("SELECT * FROM %s.%s", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.REPAIR_HISTORY));
            assertValues(s.execute(String.format("SELECT * FROM %s.%s", SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES)),
                         new HashSet<>(Arrays.asList("cassandra", "one", "two")));
            assertNotFail(s, String.format("SELECT * FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PREPARED_STATEMENTS));
        }
    }

    private void assertFail(Session s, String cql)
    {
        try
        {
            s.execute(cql);
            fail();
        }
        catch (UnauthorizedException e)
        {
            // ok
        }
    }

    private void assertNotFail(Session s, String cql)
    {
        s.execute(cql);
    }

    private void assertKsTab(Set<Pair<String, String>> allowed, Set<Pair<String, String>> schemaMandatory, ResultSet resultSet)
    {
        Set<Pair<String, String>> got = new HashSet<>();
        resultSet.forEach(row ->
                          {
                              String ks = row.getString("keyspace_name");
                              String tab = row.getString("table_name");
                              got.add(Pair.create(ks, tab));
                              boolean ok = false;
                              for (Pair<String, String> ksTab : allowed)
                              {
                                  if (!ksTab.left.equals(ks))
                                      continue;
                                  ok |= ksTab.right == null || ksTab.right.equals(tab);
                              }
                              if (!ok)
                                  fail("query returned table " + ks + '.' + tab + " but is only expected to return " + allowed);
                          });
        if (!got.containsAll(schemaMandatory))
        {
            Set<Pair<String, String>> missing = new HashSet<>(schemaMandatory);
            missing.removeAll(got);
            fail("Did not get all manadatory schema information - missing: " + missing);
        }
    }

    private void assertKs(Set<Pair<String, String>> allowed, ResultSet resultSet)
    {
        Set<String> keyspaces = allowed.stream().map(p -> p.left).collect(Collectors.toSet());

        resultSet.forEach(row ->
                          {
                              String ks = row.getString("keyspace_name");
                              if (!keyspaces.contains(ks))
                                  fail("query returned keyspace " + ks + " but is only expected to return " + allowed);
                          });
    }

    private void assertEmpty(ResultSet resultSet)
    {
        assertTrue(resultSet.all().isEmpty());
    }

    private void assertValues(ResultSet resultSet, Collection<String> values)
    {
        Set<String> returned = new HashSet<>();
        resultSet.forEach(r -> returned.add(r.getString(0)));
        assertEquals(values, returned);
    }
}
