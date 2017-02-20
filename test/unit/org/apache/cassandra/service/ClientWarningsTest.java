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
package org.apache.cassandra.service;

import org.apache.commons.lang3.StringUtils;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.QueryMessage;

import static org.junit.Assert.*;

public class ClientWarningsTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
        DatabaseDescriptor.setBatchSizeWarnThresholdInKB(1);
    }

    @Test
    public void testUnloggedBatchWithProtoV4() throws Exception
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, Server.VERSION_4))
        {
            client.connect(false);

            QueryMessage query = new QueryMessage(createBatchStatement2(1), QueryOptions.DEFAULT);
            Message.Response resp = client.execute(query);
            assertNull(resp.getWarnings());

            query = new QueryMessage(createBatchStatement2(DatabaseDescriptor.getBatchSizeWarnThreshold()), QueryOptions.DEFAULT);
            resp = client.execute(query);
            assertEquals(1, resp.getWarnings().size());
        }
    }

    @Test
    public void testLargeBatchWithProtoV4() throws Exception
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, Server.VERSION_4))
        {
            client.connect(false);

            QueryMessage query = new QueryMessage(createBatchStatement(DatabaseDescriptor.getBatchSizeWarnThreshold()), QueryOptions.DEFAULT);
            Message.Response resp = client.execute(query);
            assertEquals(1, resp.getWarnings().size());
        }
    }

    @Test
    public void testTombstoneWarning() throws Exception
    {
        final int iterations = 10000;
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, Server.VERSION_4))
        {
            client.connect(false);

            for (int i = 0; i < iterations; i++)
            {
                QueryMessage query = new QueryMessage(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (1, %s, 1)",
                                                                    KEYSPACE,
                                                                    currentTable(),
                                                                    i), QueryOptions.DEFAULT);
                client.execute(query);
            }
            ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
            store.forceBlockingFlush();

            for (int i = 0; i < iterations; i++)
            {
                QueryMessage query = new QueryMessage(String.format("DELETE v FROM %s.%s WHERE pk = 1 AND ck = %s",
                                                                    KEYSPACE,
                                                                    currentTable(),
                                                                    i), QueryOptions.DEFAULT);
                client.execute(query);
            }
            store.forceBlockingFlush();

            {
                QueryMessage query = new QueryMessage(String.format("SELECT * FROM %s.%s WHERE pk = 1",
                                                                    KEYSPACE,
                                                                    currentTable()), QueryOptions.DEFAULT);
                Message.Response resp = client.execute(query);
                assertEquals(1, resp.getWarnings().size());
            }
        }
    }

    @Test
    public void testDTCSDeprecationWarning() throws Exception
    {
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, Server.CURRENT_VERSION))
        {
            client.connect(false);

            String currentTable = createTableName();

            // Create a table with SizeTieredCompactionStrategy and check that no warning is present
            QueryMessage query = new QueryMessage(String.format("CREATE TABLE %s.%s (pk int, ck int, val int, PRIMARY KEY (pk,ck)) " +
                                                                "with compaction = {'class':'SizeTieredCompactionStrategy'}",
                                                                KEYSPACE,
                                                                currentTable),
                                                  QueryOptions.DEFAULT);
            Message.Response resp = client.execute(query);
            assertNull(resp.getWarnings());


            // Alter the table with DateTieredCompactionStrategy and check that a warning is present
            query = new QueryMessage(String.format("ALTER TABLE %s.%s with compaction = {'class':'DateTieredCompactionStrategy'}",
                                                   KEYSPACE,
                                                   currentTable),
                                     QueryOptions.DEFAULT);

            resp = client.execute(query);
            assertNotNull(resp.getWarnings());
            assertEquals(1, resp.getWarnings().size());
            assertTrue(resp.getWarnings().get(0).contains("DateTieredCompactionStrategy"));


            // Create a new table with DateTieredCompactionStrategy and check that a warning is present
            currentTable = createTableName();
            query = new QueryMessage(String.format("CREATE TABLE %s.%s (pk int, ck int, val int, PRIMARY KEY (pk,ck)) " +
                                                   "with compaction = {'class':'DateTieredCompactionStrategy'}",
                                                   KEYSPACE,
                                                   currentTable),
                                     QueryOptions.DEFAULT);
            resp = client.execute(query);
            assertNotNull(resp.getWarnings());
            assertEquals(1, resp.getWarnings().size());
            assertTrue(resp.getWarnings().get(0).contains("DateTieredCompactionStrategy"));


            // Alter the table with SizeTieredCompactionStrategy and check that no warning is present
            query = new QueryMessage(String.format("ALTER TABLE %s.%s with compaction = {'class':'SizeTieredCompactionStrategy'}",
                                                   KEYSPACE,
                                                   currentTable),
                                     QueryOptions.DEFAULT);

            resp = client.execute(query);
            assertNull(resp.getWarnings());


            // Create a view with SizeTieredCompactionStrategy and check that no warning is present
            query = new QueryMessage(String.format("CREATE MATERIALIZED VIEW %1$s.%2$s_view1 AS " +
                                                   "SELECT * FROM %1$s.%2$s WHERE pk IS NOT NULL AND ck IS NOT NULL AND val IS NOT NULL " +
                                                   "PRIMARY KEY (ck, pk, val) with compaction = {'class':'SizeTieredCompactionStrategy'}",
                                                   KEYSPACE,
                                                   currentTable),
                                     QueryOptions.DEFAULT);
            resp = client.execute(query);
            assertNull(resp.getWarnings());


            // Alter the view with DateTieredCompactionStrategy and check that a warning is present
            query = new QueryMessage(String.format("ALTER MATERIALIZED VIEW  %1$s.%2$s_view1 with compaction = {'class':'DateTieredCompactionStrategy'}",
                                                   KEYSPACE,
                                                   currentTable),
                                     QueryOptions.DEFAULT);

            resp = client.execute(query);
            assertNotNull(resp.getWarnings());
            assertEquals(1, resp.getWarnings().size());
            assertTrue(resp.getWarnings().get(0).contains("DateTieredCompactionStrategy"));


            // Create another view with DateTieredCompactionStrategy and check that a warning is present
            query = new QueryMessage(String.format("CREATE MATERIALIZED VIEW %1$s.%2$s_view2 AS " +
                                                   "SELECT * FROM %1$s.%2$s WHERE pk IS NOT NULL AND ck IS NOT NULL AND val IS NOT NULL " +
                                                   "PRIMARY KEY (ck, pk, val) with compaction = {'class':'DateTieredCompactionStrategy'}",
                                                   KEYSPACE,
                                                   currentTable),
                                     QueryOptions.DEFAULT);
            resp = client.execute(query);
            assertNotNull(resp.getWarnings());
            assertEquals(1, resp.getWarnings().size());
            assertTrue(resp.getWarnings().get(0).contains("DateTieredCompactionStrategy"));


            // Alter the view with SizeTieredCompactionStrategy and check that no warning is present
            query = new QueryMessage(String.format("ALTER MATERIALIZED VIEW  %1$s.%2$s_view2 with compaction = {'class':'SizeTieredCompactionStrategy'}",
                                                   KEYSPACE,
                                                   currentTable),
                                     QueryOptions.DEFAULT);

            resp = client.execute(query);
            assertNull(resp.getWarnings());
        }
    }

    @Test
    public void testLargeBatchWithProtoV2() throws Exception
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, Server.VERSION_3))
        {
            client.connect(false);

            QueryMessage query = new QueryMessage(createBatchStatement(DatabaseDescriptor.getBatchSizeWarnThreshold()), QueryOptions.DEFAULT);
            Message.Response resp = client.execute(query);
            assertNull(resp.getWarnings());
        }
    }

    private String createBatchStatement(int minSize)
    {
        return String.format("BEGIN UNLOGGED BATCH INSERT INTO %s.%s (pk, v) VALUES (1, '%s') APPLY BATCH;",
                             KEYSPACE,
                             currentTable(),
                             StringUtils.repeat('1', minSize));
    }

    private String createBatchStatement2(int minSize)
    {
        return String.format("BEGIN UNLOGGED BATCH INSERT INTO %s.%s (pk, v) VALUES (1, '%s'); INSERT INTO %s.%s (pk, v) VALUES (2, '%s'); APPLY BATCH;",
                             KEYSPACE,
                             currentTable(),
                             StringUtils.repeat('1', minSize),
                             KEYSPACE,
                             currentTable(),
                             StringUtils.repeat('1', minSize));
    }

}
