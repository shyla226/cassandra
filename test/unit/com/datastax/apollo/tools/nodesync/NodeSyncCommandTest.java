/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.tools.nodesync;

import java.util.regex.Matcher;

import org.junit.Test;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NodeSyncCommandTest extends CQLTester
{
    @Test
    public void testTableSelectorPattern()
    {
        assertValidTableSelector("t", null, "t");
        assertValidTableSelector("Table_1", null, "Table_1");
        assertValidTableSelector("\"Table_1\"", null, "\"Table_1\"");

        assertValidTableSelector("k.t", "k", "t");
        assertValidTableSelector("Keyspace_1.Table_1", "Keyspace_1", "Table_1");

        assertValidTableSelector("\"k\".t", "\"k\"", "t");
        assertValidTableSelector("\"Keyspace_1\".Table_1", "\"Keyspace_1\"", "Table_1");

        assertValidTableSelector("k.\"t\"", "k", "\"t\"");
        assertValidTableSelector("Keyspace_1.\"Table_1\"", "Keyspace_1", "\"Table_1\"");

        assertValidTableSelector("\"k\".\"t\"", "\"k\"", "\"t\"");
        assertValidTableSelector("\"Keyspace_1\".\"Table_1\"", "\"Keyspace_1\"", "\"Table_1\"");

        assertInvalidTableSelector("");
        assertInvalidTableSelector(".");
        assertInvalidTableSelector("*");
        assertInvalidTableSelector("k.");
        assertInvalidTableSelector(".t");
        assertInvalidTableSelector("\"k\".");
        assertInvalidTableSelector(".\"t\"");
        assertInvalidTableSelector("\"k.t\"");

        assertInvalidTableSelector("*.t");
        assertInvalidTableSelector("k.*");
        assertInvalidTableSelector("*.*");
    }

    private static void assertValidTableSelector(String name, String expectedKeyspace, String expectedTable)
    {
        Matcher matcher = NodeSyncCommand.TABLE_NAME_PATTERN.matcher(name);
        assertTrue(matcher.matches());
        assertEquals(expectedKeyspace, matcher.group("k"));
        assertEquals(expectedTable, matcher.group("t"));
    }

    private static void assertInvalidTableSelector(String name)
    {
        Matcher matcher = NodeSyncCommand.TABLE_NAME_PATTERN.matcher(name);
        assertFalse(matcher.matches());
    }

    @Test
    @SuppressWarnings("resource")
    public void testParseTable() throws Throwable
    {
        String k1 = createKeyspace("CREATE KEYSPACE %s WITH replication={'class': 'SimpleStrategy', 'replication_factor': 1}");
        String k2 = createKeyspace("CREATE KEYSPACE %s WITH replication={'class': 'SimpleStrategy', 'replication_factor': 1}");
        String t1 = "t1";
        String t2 = "t2";
        execute(String.format("CREATE TABLE %s.%s (k int, c int, PRIMARY KEY (k, c))", k1, t1));
        execute(String.format("CREATE TABLE %s.%s (k int, c int, PRIMARY KEY (k, c))", k1, t2));
        execute(String.format("CREATE TABLE %s.%s (k int, c int, PRIMARY KEY (k, c))", k2, t1));
        execute(String.format("CREATE TABLE %s.%s (k int, c int, PRIMARY KEY (k, c))", k2, t2));

        Metadata metadata = sessionNet().getCluster().getMetadata();

        // Palin table names with default keyspace
        assertValidParseTable(metadata, k1, t1, k1, t1);
        assertValidParseTable(metadata, k1, t2, k1, t2);
        assertValidParseTable(metadata, k2, t1, k2, t1);
        assertValidParseTable(metadata, k2, t2, k2, t2);

        // Qualified names without default keyspace
        assertValidParseTable(metadata, null, k1 + '.' + t1, k1, t1);
        assertValidParseTable(metadata, null, k1 + '.' + t2, k1, t2);
        assertValidParseTable(metadata, null, k2 + '.' + t1, k2, t1);
        assertValidParseTable(metadata, null, k2 + '.' + t2, k2, t2);

        // Qualified names with (ignored) default keyspace
        assertValidParseTable(metadata, k2, k1 + '.' + t1, k1, t1);
        assertValidParseTable(metadata, k2, k1 + '.' + t2, k1, t2);
        assertValidParseTable(metadata, k1, k2 + '.' + t1, k2, t1);
        assertValidParseTable(metadata, k1, k2 + '.' + t2, k2, t2);

        // Quoted names
        assertValidParseTable(metadata, quote(k1), t2, k1, t2);
        assertValidParseTable(metadata, quote(k1), quote(t2), k1, t2);
        assertValidParseTable(metadata, null, quote(k1) + '.' + t1, k1, t1);
        assertValidParseTable(metadata, null, k1 + '.' + quote(t2), k1, t2);
        assertValidParseTable(metadata, null, quote(k2) + '.' + quote(t1), k2, t1);

        // Plain table name withiut default keyspace
        assertInvalidParseTable(metadata, null, t1, NodeSyncException.class,
                                "Keyspace required for unqualified table name: t1");

        // Unknown keyspace
        assertInvalidParseTable(metadata, "k0", t1, NodeSyncException.class,
                                "Keyspace [k0] does not exist.");

        // Unknown table
        assertInvalidParseTable(metadata, k1, "t0", NodeSyncException.class,
                                "Table [" + k1 + ".t0] does not exist.");
    }

    private static String quote(String s)
    {
        return String.format("\"%s\"", s);
    }

    private static void assertValidParseTable(Metadata metadata,
                                              String defaultKeyspace,
                                              String table,
                                              String expectedKeyspace,
                                              String expectedTable)
    {
        TableMetadata tableMetadata = NodeSyncCommand.parseTable(metadata, defaultKeyspace, table);
        Matcher matcher = NodeSyncCommand.TABLE_NAME_PATTERN.matcher(table);
        assertTrue(matcher.matches());
        assertEquals(expectedKeyspace, tableMetadata.getKeyspace().getName());
        assertEquals(expectedTable, tableMetadata.getName());
    }

    private static void assertInvalidParseTable(Metadata metadata,
                                                String defaultKeyspace,
                                                String table,
                                                Class<? extends Exception> expectedException,
                                                String expectedMsg)
    {
        try
        {
            NodeSyncCommand.parseTable(metadata, defaultKeyspace, table);
        }
        catch (Exception e)
        {
            assertSame(expectedException, e.getClass());
            assertEquals(expectedMsg, e.getMessage());
            return;
        }
        fail("Expected " + expectedException.getName() + " exception with message: " + expectedMsg);
    }
}
