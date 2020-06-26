/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.index.cql;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompositePartitionKeyIndexTest extends CQLTester
{
    @Before
    public void createTableAndIndex()
    {
        requireNetwork();

        createTable("CREATE TABLE %s (pk1 int, pk2 text, val int, PRIMARY KEY((pk1, pk2)))");
        createIndex("CREATE CUSTOM INDEX ON %s(pk1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(pk2) USING 'StorageAttachedIndex'");

        disableCompaction();
    }

    private void insertData1() throws Throwable
    {
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (1, '1', 1)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (2, '2', 2)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (3, '3', 3)");
    }

    private void insertData2() throws Throwable
    {
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (4, '4', 4)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (5, '5', 5)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (6, '6', 6)");
    }

    @Test
    public void queryFromMemtable() throws Throwable
    {
        insertData1();
        insertData2();
        runQueries();
    }

    @Test
    public void queryFromSingleSSTable() throws Throwable
    {
        insertData1();
        insertData2();
        flush();
        runQueries();
    }

    @Test
    public void queryFromMultipleSSTables() throws Throwable
    {
        insertData1();
        flush();
        insertData2();
        flush();
        runQueries();
    }

    @Test
    public void queryFromMemtableAndSSTables() throws Throwable
    {
        insertData1();
        flush();
        insertData2();
        runQueries();
    }

    @Test
    public void queryFromCompactedSSTable() throws Throwable
    {
        insertData1();
        flush();
        insertData2();
        flush();
        compact();
        runQueries();
    }

    private Object[] expectedRow(int index) {
        return row(index, Integer.toString(index), index);
    }

    private void runQueries() throws Throwable
    {
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 = 2"),
                expectedRow(2));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 > 1"),
                expectedRow(2),
                expectedRow(3),
                expectedRow(4),
                expectedRow(5),
                expectedRow(6));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 >= 3"),
                expectedRow(3),
                expectedRow(4),
                expectedRow(5),
                expectedRow(6));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 < 3"),
                expectedRow(1),
                expectedRow(2));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 <= 3"),
                expectedRow(1),
                expectedRow(2),
                expectedRow(3));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk2 = '2'"),
                expectedRow(2));

        //Note that this should eventually not require ALLOW FILTERING. Detection of filtering requirments for indexes
        //is being handled separately in CASSANDRA-15913.
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 > 1 AND pk2 = '2' ALLOW FILTERING"),
                expectedRow(2));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 = -1 AND pk2 = '2'"));

        assertThatThrownBy(()->execute("SELECT * FROM %s WHERE pk1 = -1 AND val = 2"))
                .hasMessageContaining("use ALLOW FILTERING");

    }
}
