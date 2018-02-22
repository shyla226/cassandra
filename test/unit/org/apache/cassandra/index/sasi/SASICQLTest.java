/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sasi;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import junit.framework.Assert;
import org.apache.cassandra.cql3.CQLTester;

public class SASICQLTest extends CQLTester
{
    @Test
    public void testPaging() throws Throwable
    {
        for (boolean forceFlush : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (pk int primary key, v int);");
            createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'org.apache.cassandra.index.sasi.SASIIndex';");

            for (int i = 0; i < 10; i++)
                execute("INSERT INTO %s (pk, v) VALUES (?, ?);", i, 1);

            flush(forceFlush);

            Session session = sessionNet();
            SimpleStatement stmt = new SimpleStatement("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE v = 1");
            stmt.setFetchSize(5);
            List<Row> rs = session.execute(stmt).all();
            Assert.assertEquals(10, rs.size());
            Assert.assertEquals(Sets.newHashSet(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                                rs.stream().map((i) -> i.getInt("pk")).collect(Collectors.toSet()));
        }
    }

    @Test
    public void testPagingWithClustering() throws Throwable
    {
        for (boolean forceFlush : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck));");
            createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'org.apache.cassandra.index.sasi.SASIIndex';");

            for (int i = 0; i < 10; i++)
            {
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?);", i, 1, 1);
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?);", i, 2, 1);
            }

            flush(forceFlush);

            Session session = sessionNet();
            SimpleStatement stmt = new SimpleStatement("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE v = 1");
            stmt.setFetchSize(5);
            List<Row> rs = session.execute(stmt).all();
            Assert.assertEquals(20, rs.size());
        }
    }

    @Test
    public void rangeFilteringTest() throws Throwable
    {
        createTable("create table %s (id1 text PRIMARY KEY, val1 text, val2 text);");

        createIndex("create custom index on %s(val1) using 'org.apache.cassandra.index.sasi.SASIIndex';");
        createIndex("create custom index on %s(val2) using 'org.apache.cassandra.index.sasi.SASIIndex';");

        execute("insert into %s(id1, val1, val2) values ('1', 'aaa', 'aaa');");
        execute("insert into %s(id1, val1, val2) values ('2', 'bbb', 'bbb');");
        execute("insert into %s(id1, val1, val2) values ('3', 'ccc', 'ccc');");

        beforeAndAfterFlush(() -> {
            assertRows(execute("select * from %s where val1 < 'bbb' AND val2 = 'aaa' ALLOW FILTERING;"),
                       row("1", "aaa", "aaa"));
            assertRows(execute("select * from %s where val1 <= 'bbb' AND val2 = 'bbb' ALLOW FILTERING;"),
                       row("2", "bbb", "bbb"));
            assertRows(execute("select * from %s where val1 >= 'aaa' AND val1 <= 'bbb' AND val2 = 'bbb' ALLOW FILTERING;"),
                       row("2", "bbb", "bbb"));
            assertRows(execute("select * from %s where val1 >= 'aaa' AND val1 <= 'bbb' AND val2 = 'bbb' ALLOW FILTERING;"),
                       row("2", "bbb", "bbb"));
            assertRows(execute("select * from %s where val1 >= 'aaa' AND val2 = 'ccc' ALLOW FILTERING;"),
                       row("3", "ccc", "ccc"));
        });
    }

    @Test
    public void testLikeOperatorWithMultipleTokens() throws Throwable
    {
        createTable("CREATE TABLE %s(id int, clustering int, weight int, val text, PRIMARY KEY((id), clustering));");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {" +
                    "'mode': 'CONTAINS'," +
                    "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer'," +
                    "'analyzed': 'true'};");

        execute("INSERT INTO %s(id, clustering, weight, val) VALUES(10, 1, 12, 'homeworker');");
        execute("INSERT INTO %s(id, clustering, weight, val) VALUES(10, 2, 12, 'homeworker');");
        execute("INSERT INTO %s(id, clustering, weight, val) VALUES(11, 1, 14, 'harhomedworker');");
        execute("INSERT INTO %s(id, clustering, weight, val) VALUES(12, 1, 14, 'casa');");

        beforeAndAfterFlush(() -> {
            assertRows(executeFormattedQuery(formatQuery("SELECT * FROM %s WHERE val LIKE '%%work home%%';")),
                       row(10, 1, "homeworker", 12),
                       row(10, 2, "homeworker", 12),
                       row(11, 1, "harhomedworker", 14));
        });
    }

    @Test
    public void testMultipleLikeOperatorsInDifferentColumnsWithMultipleTokens() throws Throwable
    {
        createTable("CREATE TABLE %s(id int, clustering int, name text, val text, PRIMARY KEY((id), clustering));");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {" +
                    "'mode': 'CONTAINS'," +
                    "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer'," +
                    "'analyzed': 'true'};");

        createIndex("CREATE CUSTOM INDEX ON %s(name) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {" +
                    "'mode': 'CONTAINS'," +
                    "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer'," +
                    "'analyzed': 'true'};");


        execute("INSERT INTO %s(id, clustering, name, val) VALUES(10, 1, 'alice', 'homeworker');");
        execute("INSERT INTO %s(id, clustering, name, val) VALUES(11, 1, 'bob', 'harhomedworker');");
        execute("INSERT INTO %s(id, clustering, name, val) VALUES(12, 1, 'paul', 'casa');");


        String query = formatQuery("SELECT * FROM %s WHERE val LIKE '%%work home%%' AND name LIKE 'alicew bob another' ALLOW FILTERING;");

        beforeAndAfterFlush(() -> {
            assertRows(executeFormattedQuery(query),
                       row(11, 1, "bob", "harhomedworker"));
        });
    }

    @Test
    public void testMultipleTokensInSamePartitions() throws Throwable
    {
        createTable("CREATE TABLE %s(id int, clustering int, val text, PRIMARY KEY((id), clustering));");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {" +
                    "'mode': 'CONTAINS', " +
                    "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer'," +
                    "'analyzed': 'true'};");

        execute("INSERT INTO %s(id, clustering , val ) VALUES(1, 1, 'aaa bbb ccc ddd');");
        execute("INSERT INTO %s(id, clustering , val ) VALUES(1, 2, 'eee aaa fff ggg');");
        execute("INSERT INTO %s(id, clustering , val ) VALUES(1, 3, 'hhh iii bbb jjj');");
        execute("INSERT INTO %s(id, clustering , val ) VALUES(1, 4, 'kkk lll mmm bbb');");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE val LIKE '%%aaa bbb%%';"),
                                    row(1, 1, "aaa bbb ccc ddd"),
                                    row(1, 2, "eee aaa fff ggg"),
                                    row(1, 3, "hhh iii bbb jjj"),
                                    row(1, 4, "kkk lll mmm bbb"));
        });
    }

    @Test
    public void testMultipleTokensInDifferentPartitions() throws Throwable
    {
        createTable("CREATE TABLE %s(id int, clustering int, val text, PRIMARY KEY((id), clustering));");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {" +
                    "'mode': 'CONTAINS', " +
                    "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer'," +
                    "'analyzed': 'true'};");

        execute("INSERT INTO %s(id, clustering , val ) VALUES(1, 1, 'aaa bbb ccc ddd');");
        execute("INSERT INTO %s(id, clustering , val ) VALUES(2, 2, 'eee aaa fff ggg');");
        execute("INSERT INTO %s(id, clustering , val ) VALUES(3, 3, 'hhh iii bbb jjj');");
        execute("INSERT INTO %s(id, clustering , val ) VALUES(4, 4, 'kkk lll mmm bbb');");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE val LIKE '%%aaa bbb%%';"),
                                    row(1, 1, "aaa bbb ccc ddd"),
                                    row(2, 2, "eee aaa fff ggg"),
                                    row(3, 3, "hhh iii bbb jjj"),
                                    row(4, 4, "kkk lll mmm bbb"));
        });
    }

    @Test
    public void testMultipleLikeStatementsSinglePartition() throws Throwable
    {
        createTable("CREATE TABLE %s(id int, clustering int, val1 text, val2 text, val3 text, PRIMARY KEY((id), clustering));");

        createIndex("CREATE CUSTOM INDEX ON %s(clustering) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
        for (int i = 1; i <= 3; i++)
        {
            createIndex("CREATE CUSTOM INDEX ON %s(val" + i + ") USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {" +
                        "'mode': 'CONTAINS', " +
                        "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer'," +
                        "'analyzed': 'true'};");
        }

        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(1, 1, 'aaa bbb ccc ddd', 'aaa bbb ccc ddd', 'aaa bbb ccc ddd');");
        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(1, 2, 'aaa bbb ccc ddd', 'aaa bbb ccc ddd', 'aaa bbb ccc ddd');");
        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(2, 2, 'eee aaa fff ggg', 'eee aaa fff ggg', 'eee aaa fff ggg');");
        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(2, 3, 'eee aaa fff ggg', 'eee aaa fff ggg', 'eee aaa fff ggg');");
        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(3, 3, 'hhh iii bbb jjj', 'hhh iii bbb jjj', 'hhh iii bbb jjj');");
        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(3, 4, 'hhh iii bbb jjj', 'hhh iii bbb jjj', 'zzz zzz zzz zzz');");
        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(4, 4, 'kkk lll mmm bbb', 'kkk lll mmm bbb', 'kkk lll mmm bbb');");
        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(4, 5, 'kkk lll mmm bbb', 'kkk lll mmm bbb', 'kkk lll mmm bbb');");
        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(5, 5, 'nnn ooo ppp qqq', 'nnn ooo ppp qqq', 'nnn ooo ppp qqq');");
        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(5, 6, 'nnn ooo ppp qqq', 'nnn ooo ppp qqq', 'nnn ooo ppp qqq');");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE val1 LIKE '%%aaa bbb%%' AND val2 LIKE '%%aaa bbb%%' AND val3 LIKE '%%aaa bbb%%' allow filtering;"),
                                    row(1, 1, "aaa bbb ccc ddd", "aaa bbb ccc ddd", "aaa bbb ccc ddd"),
                                    row(1, 2, "aaa bbb ccc ddd", "aaa bbb ccc ddd", "aaa bbb ccc ddd"),
                                    row(2, 2, "eee aaa fff ggg", "eee aaa fff ggg", "eee aaa fff ggg"),
                                    row(2, 3, "eee aaa fff ggg", "eee aaa fff ggg", "eee aaa fff ggg"),
                                    row(3, 3, "hhh iii bbb jjj", "hhh iii bbb jjj", "hhh iii bbb jjj"),
                                    row(4, 4, "kkk lll mmm bbb", "kkk lll mmm bbb", "kkk lll mmm bbb"),
                                    row(4, 5, "kkk lll mmm bbb", "kkk lll mmm bbb", "kkk lll mmm bbb"));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE clustering = 2 AND val1 LIKE '%%aaa bbb%%' AND val2 LIKE '%%aaa bbb%%' AND val3 LIKE '%%aaa bbb%%' allow filtering;"),
                                    row(1, 2, "aaa bbb ccc ddd", "aaa bbb ccc ddd", "aaa bbb ccc ddd"),
                                    row(2, 2, "eee aaa fff ggg", "eee aaa fff ggg", "eee aaa fff ggg"));
        });
    }

    @Test
    public void testMultipleLikeStatementsMultiPartition() throws Throwable
    {
        createTable("CREATE TABLE %s(id int, clustering int, val1 text, val2 text, val3 text, PRIMARY KEY((id), clustering));");

        createIndex("CREATE CUSTOM INDEX ON %s(clustering) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
        for (int i = 1; i <= 3; i++)
        {
            createIndex("CREATE CUSTOM INDEX ON %s(val" + i + ") USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {" +
                        "'mode': 'CONTAINS', " +
                        "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer'," +
                        "'analyzed': 'true'};");
        }

        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(1, 1, 'aaa bbb ccc ddd', 'aaa bbb ccc ddd', 'aaa bbb ccc ddd');");
        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(1, 2, 'eee aaa fff ggg', 'eee aaa fff ggg', 'eee aaa fff ggg');");
        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(2, 3, 'hhh iii bbb jjj', 'hhh iii bbb jjj', 'hhh iii bbb jjj');");
        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(2, 4, 'kkk lll mmm bbb', 'kkk lll mmm bbb', 'kkk lll mmm bbb');");
        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(1, 5, 'nnn ooo ppp qqq', 'nnn ooo ppp qqq', 'nnn ooo ppp qqq');");
        execute("INSERT INTO %s(id, clustering, val1, val2, val3) VALUES(2, 6, 'nnn ooo ppp qqq', 'nnn ooo ppp qqq', 'nnn ooo ppp qqq');");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE val1 LIKE '%%aaa bbb%%' AND val2 LIKE '%%aaa bbb%%' AND val3 LIKE '%%aaa bbb%%' allow filtering;"),
                                    row(1, 1, "aaa bbb ccc ddd", "aaa bbb ccc ddd", "aaa bbb ccc ddd"),
                                    row(1, 2, "eee aaa fff ggg", "eee aaa fff ggg", "eee aaa fff ggg"),
                                    row(2, 3, "hhh iii bbb jjj", "hhh iii bbb jjj", "hhh iii bbb jjj"),
                                    row(2, 4, "kkk lll mmm bbb", "kkk lll mmm bbb", "kkk lll mmm bbb"));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE clustering = 2 AND val1 LIKE '%%aaa bbb%%' AND val2 LIKE '%%aaa bbb%%' AND val3 LIKE '%%aaa bbb%%' allow filtering;"),
                                    row(1, 2, "eee aaa fff ggg", "eee aaa fff ggg", "eee aaa fff ggg"));
        });
    }
}
