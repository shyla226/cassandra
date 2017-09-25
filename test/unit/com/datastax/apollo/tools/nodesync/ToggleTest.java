/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.tools.nodesync;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.SchemaConstants;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@SuppressWarnings("resource")
public class ToggleTest extends CQLTester
{
    @Test
    public void testExecute() throws Throwable
    {
        // Create schema context
        String t1 = "t1";
        String t2 = "t2";
        String k1 = createKeyspace("CREATE KEYSPACE %s WITH replication={'class': 'SimpleStrategy', 'replication_factor': 1}");
        String k2 = createKeyspace("CREATE KEYSPACE %s WITH replication={'class': 'SimpleStrategy', 'replication_factor': 1}");
        execute(String.format("CREATE TABLE %s.%s (k int, c int, PRIMARY KEY (k, c))", k1, t1));
        execute(String.format("CREATE TABLE %s.%s (k int, c int, PRIMARY KEY (k, c))", k1, t2));
        execute(String.format("CREATE TABLE %s.%s (k int, c int, PRIMARY KEY (k, c))", k2, t1));
        execute(String.format("CREATE TABLE %s.%s (k int, c int, PRIMARY KEY (k, c))", k2, t2));

        String k1t1 = k1 + '.' + t1;
        String k1t2 = k1 + '.' + t2;
        String k2t1 = k2 + '.' + t1;
        String k2t2 = k2 + '.' + t2;
        Set<String> userTables = newHashSet(k1t1, k1t2, k2t1, k2t2);

        Session session = sessionNet();

        Set<String> systemAlterableTables = session.getCluster()
                                                   .getMetadata()
                                                   .getKeyspace(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME)
                                                   .getTables()
                                                   .stream()
                                                   .map(t -> t.getKeyspace().getName() + '.' + t.getName())
                                                   .collect(Collectors.toSet());

        // Test valid cases without default keyspace option
        assertValidExecution(session, null, newArrayList(k1t1), newHashSet(k1t1));
        assertValidExecution(session, null, newArrayList(k1t2), newHashSet(k1t2));
        assertValidExecution(session, null, newArrayList(k1t2, k2t1, k2t2), newHashSet(k1t2, k2t1, k2t2));
        assertValidExecution(session, null, newArrayList("*"), Sets.union(userTables, systemAlterableTables));

        // Test valid cases with default keyspace option
        assertValidExecution(session, k1, newArrayList(t1), newHashSet(k1t1));
        assertValidExecution(session, k1, newArrayList(t2), newHashSet(k1t2));
        assertValidExecution(session, k1, newArrayList("*"), newHashSet(k1t1, k1t2));
        assertValidExecution(session, k1, newArrayList(t1, k2t1), newHashSet(k1t1, k2t1));

        // Test invalid cases
        assertInvalidExecution(session, null, newArrayList(".t"), NodeSyncException.class,
                               "Cannot parse table name: .t");
        assertInvalidExecution(session, null, newArrayList(t1), NodeSyncException.class,
                               "Keyspace required for unqualified table name: t1");
        assertInvalidExecution(session, "k3", newArrayList(t1), NodeSyncException.class,
                               "Keyspace [k3] does not exist.");
        assertInvalidExecution(session, "system_schema", newArrayList("keyspaces"), NodeSyncException.class,
                               "Keyspace [system_schema] is not alterable.");
    }

    private static void assertValidExecution(Session session,
                                             String defaultKeyspace,
                                             List<String> tableSelectors,
                                             Set<String> expectedTableNames)
    {
        assertValidExecution(session, defaultKeyspace, tableSelectors, expectedTableNames, true);
        assertValidExecution(session, defaultKeyspace, tableSelectors, expectedTableNames, false);
    }

    private static void assertValidExecution(Session session,
                                             String defaultKeyspace,
                                             List<String> tableSelectors,
                                             Set<String> expectedTableNames,
                                             boolean enable)
    {
        // function to get the nodesync enabling status in each table in the cluster
        Supplier<Map<String, Boolean>> nodesyncStatus = () -> {
            Map<String, Boolean> status = new HashMap<>();
            for (Row row : session.execute("SELECT keyspace_name, table_name, nodesync FROM system_schema.tables"))
            {
                String table = row.getString("keyspace_name") + '.' + row.getString("table_name");
                String enabled = row.getMap("nodesync", String.class, String.class).get("enabled");
                status.put(table, enabled == null ? null : enabled.equals("true"));
            }
            return status;
        };

        // the expected nodesync status is the initial status modifed according to the expected table names
        Map<String, Boolean> expectedStatus = nodesyncStatus.get();
        expectedTableNames.forEach(t -> expectedStatus.put(t, enable));

        // run the toggle command
        Toggle disableCmd = enable ? new Toggle.Enable() : new Toggle.Disable();
        disableCmd.defaultKeyspace = defaultKeyspace;
        disableCmd.tableSelectors = tableSelectors;
        disableCmd.execute(session.getCluster().getMetadata(), session, null);

        // verify the new nodesync status
        Map<String, Boolean> actualStatus = nodesyncStatus.get();
        assertEquals(expectedStatus, actualStatus);
    }

    private static void assertInvalidExecution(Session session,
                                               String defaultKeyspace,
                                               List<String> tableSelectors,
                                               Class<? extends Throwable> expectedError,
                                               String expectedMessage)
    {
        Toggle cmd = new Toggle.Enable();
        cmd.defaultKeyspace = defaultKeyspace;
        cmd.tableSelectors = tableSelectors;
        try
        {
            cmd.execute(session.getCluster().getMetadata(), session, null);
        }
        catch (Throwable e)
        {
            assertSame(expectedError, e.getClass());
            assertEquals(expectedMessage, e.getMessage());
            return;
        }
        fail();
    }
}
