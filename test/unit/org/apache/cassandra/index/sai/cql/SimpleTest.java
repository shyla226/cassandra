/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.index.sai.cql;

import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;

public class SimpleTest extends SAITester
{
    @BeforeClass
    public static void setupVersionBarrier()
    {
        requireNetwork();
    }

    @Test
    public void test() throws Throwable
    {
        createTable("CREATE TABLE %s (key int PRIMARY KEY, value1 int, value2 text)");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(value1) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(value2) USING '%s'", StorageAttachedIndex.class.getName()));
        waitForIndexQueryable();

        execute("INSERT INTO %s (key, value1, value2) VALUES (0, 0, 'aaa')");
        execute("INSERT INTO %s (key, value1, value2) VALUES (1, 1, 'bbb')");

        flush();

        execute("INSERT INTO %s (key, value1, value2) VALUES (2, 0, 'aaa')");
        execute("INSERT INTO %s (key, value1, value2) VALUES (3, 1, 'bbb')");
        execute("INSERT INTO %s (key, value1, value2) VALUES (4, 1, 'ccc')");

        UntypedResultSet results = execute("SELECT * FROM %s WHERE value1 = 1 AND value2 = 'bbb'");
        Iterator<UntypedResultSet.Row> rowsIterator = results.iterator();
        while (rowsIterator.hasNext())
        {
            UntypedResultSet.Row row = rowsIterator.next();
            System.out.println("row key="+row.getInt("key")+" value1="+row.getInt("value1")+" value2=" + row.getString("value2"));
        }

        flush();

        results = execute("SELECT * FROM %s WHERE value1 = 1 AND value2 = 'bbb'");
        rowsIterator = results.iterator();
        while (rowsIterator.hasNext())
        {
            UntypedResultSet.Row row = rowsIterator.next();
            System.out.println("row key2="+row.getInt("key")+" value1="+row.getInt("value1")+" value2=" + row.getString("value2"));
        }
    }
}
