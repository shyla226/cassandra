/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.index.sai.cql;

import java.net.InetAddress;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

public class InetAddressIndexTest extends CQLTester
{
    @Before
    public void createTableAndIndex()
    {
        requireNetwork();

        createTable("CREATE TABLE %s (pk int, ck int, ip inet, PRIMARY KEY(pk, ck ))");

        createIndex("CREATE CUSTOM INDEX ON %s(ip) USING 'StorageAttachedIndex'");

        disableCompaction();
    }

    @Test
    public void queryFromMemtable() throws Throwable
    {
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 1, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 2, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 3, '127.0.0.2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 4, '::ffff:7f00:3')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 5, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 6, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 7, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 8, '2002:4559:1fe2::4559:1fe3')");

        runQueries();
    }

    @Test
    public void queryFromSingleSSTable() throws Throwable
    {
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 1, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 2, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 3, '127.0.0.2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 4, '::ffff:7f00:3')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 5, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 6, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 7, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 8, '2002:4559:1fe2::4559:1fe3')");

        flush();

        runQueries();
    }

    @Test
    public void queryFromMultipleSSTables() throws Throwable
    {
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 1, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 2, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 3, '127.0.0.2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 4, '::ffff:7f00:3')");

        flush();

        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 5, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 6, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 7, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 8, '2002:4559:1fe2::4559:1fe3')");

        flush();

        runQueries();
    }

    @Test
    public void queryFromMemtableAndSSTables() throws Throwable
    {
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 1, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 2, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 3, '127.0.0.2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 4, '::ffff:7f00:3')");

        flush();

        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 5, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 6, '2002:4559:1fe2::4559:1fe2')");

        flush();

        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 7, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 8, '2002:4559:1fe2::4559:1fe3')");

        runQueries();
    }

    @Test
    public void queryFromCompactedSSTable() throws Throwable
    {
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 1, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 2, '127.0.0.1')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 3, '127.0.0.2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 4, '::ffff:7f00:3')");

        flush();

        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 5, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 6, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 7, '2002:4559:1fe2::4559:1fe2')");
        execute("INSERT INTO %s (pk, ck, ip) VALUES (1, 8, '2002:4559:1fe2::4559:1fe3')");

        flush();

        compact();

        runQueries();
    }

    private void runQueries() throws Throwable
    {
        // EQ single ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip = '127.0.0.1'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")));

        // EQ mapped-ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip = '::ffff:7f00:1'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")));

        // EQ ipv6 address
        assertRows(execute("SELECT * FROM %s WHERE ip = '2002:4559:1fe2::4559:1fe2'"),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")));

        // GT ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip > '127.0.0.1'"),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("::ffff:7f00:3")),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));

        // GT mapped-ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip > '::ffff:7f00:1'"),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("::ffff:7f00:3")),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));

        // GT ipv6 address
        assertRows(execute("SELECT * FROM %s WHERE ip > '2002:4559:1fe2::4559:1fe2'"),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));

        // LT ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip < '127.0.0.3'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")));

        // LT mapped-ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip < '::ffff:7f00:3'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")));

        // LT ipv6 address
        assertRows(execute("SELECT * FROM %s WHERE ip < '2002:4559:1fe2::4559:1fe3'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("::ffff:7f00:3")),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")));

        // GE ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip >= '127.0.0.2'"),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("::ffff:7f00:3")),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));

        // GE mapped-ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip >= '::ffff:7f00:2'"),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("::ffff:7f00:3")),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));

        // GE ipv6 address
        assertRows(execute("SELECT * FROM %s WHERE ip >= '2002:4559:1fe2::4559:1fe3'"),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));

        // LE ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip <= '127.0.0.2'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")));

        // LE mapped-ipv4 address
        assertRows(execute("SELECT * FROM %s WHERE ip <= '::ffff:7f00:2'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")));

        // LE ipv6 address
        assertRows(execute("SELECT * FROM %s WHERE ip <= '2002:4559:1fe2::4559:1fe2'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("::ffff:7f00:3")),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")));

        // ipv4 range
        assertRows(execute("SELECT * FROM %s WHERE ip >= '127.0.0.1' AND ip <= '127.0.0.3'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("127.0.0.3")));

        // ipv4 and mapped ipv4 range
        assertRows(execute("SELECT * FROM %s WHERE ip >= '127.0.0.1' AND ip <= '::ffff:7f00:3'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("127.0.0.3")));

        // ipv6 range
        assertRows(execute("SELECT * FROM %s WHERE ip >= '2002:4559:1fe2::4559:1fe2' AND ip <= '2002:4559:1fe2::4559:1fe3'"),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));

        // Full ipv6 range
        assertRows(execute("SELECT * FROM %s WHERE ip >= '::' AND ip <= 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'"),
                row(1, 1, InetAddress.getByName("127.0.0.1")),
                row(1, 2, InetAddress.getByName("127.0.0.1")),
                row(1, 3, InetAddress.getByName("127.0.0.2")),
                row(1, 4, InetAddress.getByName("::ffff:7f00:3")),
                row(1, 5, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 6, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 7, InetAddress.getByName("2002:4559:1fe2::4559:1fe2")),
                row(1, 8, InetAddress.getByName("2002:4559:1fe2::4559:1fe3")));
    }
}
