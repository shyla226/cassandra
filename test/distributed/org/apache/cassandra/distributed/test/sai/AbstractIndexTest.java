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

package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.awaitility.Awaitility;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertTrue;

@Ignore
public abstract class AbstractIndexTest extends TestBaseImpl
{
    protected static final String CREATE_KEYSPACE_TEMPLATE = "CREATE KEYSPACE IF NOT EXISTS %%s WITH replication = {'class': 'SimpleStrategy' , 'replication_factor': '%d'}";
    protected static final String CREATE_RF2_KEYSPACE_TEMPLATE = String.format(CREATE_KEYSPACE_TEMPLATE, 2);
    protected static final String ALTER_KEYSPACE_TEMPLATE_RF = "ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy' , 'replication_factor': '%d'}";

    protected static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s.%s (ID1 TEXT PRIMARY KEY, V1 INT, V2 TEXT) WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";

    protected static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX %s ON %s.%s(%s) USING 'StorageAttachedIndex'";
    protected static final String CREATE_MV_TEMPLATE = "CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s " +
                                                       " WHERE V1 IS NOT NULL AND ID1 IS NOT NULL PRIMARY KEY(V1, ID1);";

    protected Cluster cluster;

    @After
    public void cleanup()
    {
        if (cluster != null)
            cluster.close();
    }

    protected Cluster initCluster(int nodes) throws IOException
    {
        return initCluster(nodes, UnaryOperator.identity());
    }

    protected Cluster initCluster(int nodes, UnaryOperator<Cluster.Builder> transform) throws IOException
    {
        Cluster.Builder builder = Cluster.build().withConfig(config -> config.set("enable_materialized_views", true).with(NETWORK).with(GOSSIP));
        cluster = transform.apply(builder).withNodes(nodes).start();
        return cluster;
    }

    protected void prepareRf1DataAndIndexesInTwoSSTables(String table, String mv, int num, String... indexColumns)
    {
        prepareDataAndIndexesInTwoSSTables(1, table, mv, num, indexColumns);
    }

    protected void prepareDataAndIndexesInTwoSSTables(int rf, String table, String mv, int num, String... indexColumns)
    {
        cluster.schemaChange(String.format(String.format(CREATE_KEYSPACE_TEMPLATE, rf), KEYSPACE));
        cluster.schemaChange(String.format(CREATE_TABLE_TEMPLATE, KEYSPACE, table));
        if (mv != null)
            cluster.schemaChange(String.format(CREATE_MV_TEMPLATE, KEYSPACE, mv, KEYSPACE, table));
        for (String column : indexColumns)
            cluster.schemaChange(String.format(CREATE_INDEX_TEMPLATE, "", KEYSPACE, table, column));
        if (indexColumns.length != 0)
            waitForIndexQueryable(1, KEYSPACE, table);

        // create rows in 2 sstables
        for (int i = 0; i < num; i++)
        {
            if (i == num / 2)
                cluster.forEach(node -> node.flush(KEYSPACE));
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + table + "(ID1, V1, V2) VALUES ('" + i + "', 0, '0');", ConsistencyLevel.ALL);
        }
        cluster.forEach(node -> node.flush(KEYSPACE));
    }

    protected void waitForIndexQueryable(int n, String keyspace, String table)
    {
        Awaitility.await().atMost(60, TimeUnit.SECONDS)
                  .untilAsserted(() -> assertTrue(isIndexQueryable(n, keyspace, table)));
    }

    protected long getIndexedCellCount(int node, String table, String column)
    {
        return cluster.get(node).callOnInstance(() -> {
            ColumnIdentifier columnID = ColumnIdentifier.getInterned(column, true);
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
            String indexName = IndexMetadata.generateDefaultIndexName(table, columnID);
            StorageAttachedIndex index = (StorageAttachedIndex) cfs.indexManager.getIndexByName(indexName);
            return index.getContext().getCellCount();
        });
    }

    protected void verifyIndexedCellCount(int node, String table, String column, int expected)
    {
        long count = getIndexedCellCount(node, table, column);
        Assert.assertEquals(expected, count);
    }

    protected void verifyIndexQueryAll(int node, String table, int numericIndexRows, int stringIndexRows)
    {
        verifyNumericIndexQuery(node, table, numericIndexRows, ConsistencyLevel.ALL);
        verifyStringIndexQuery(node, table, stringIndexRows, ConsistencyLevel.ALL);
    }

    protected void verifyIndexQueryOne(int node, String table, int numericIndexRows, int stringIndexRows)
    {
        verifyNumericIndexQuery(node, table, numericIndexRows, ConsistencyLevel.ONE);
        verifyStringIndexQuery(node, table, stringIndexRows, ConsistencyLevel.ONE);
    }

    protected void verifyIndexQueryLocal(int node, String table, int numericIndexRows, int stringIndexRows)
    {
        verifyNumericIndexQuery(node, table, numericIndexRows, null);
        verifyStringIndexQuery(node, table, stringIndexRows, null);
    }

    private void verifyNumericIndexQuery(int node, String table, int numericIndexRows, ConsistencyLevel level)
    {
        String query = String.format("SELECT ID1 FROM %s.%s WHERE V1=0", KEYSPACE, table);

        Object[][] rows = level == null
                          ? cluster.get(node).executeInternal(query)
                          : cluster.coordinator(node).execute(query, level);

        Assert.assertEquals(numericIndexRows, rows.length);
    }

    private void verifyStringIndexQuery(int node, String table, int stringIndexRows, ConsistencyLevel level)
    {
        String query = String.format("SELECT ID1 FROM %s.%s WHERE V2='0'", KEYSPACE, table);

        Object[][] rows = level == null
                          ? cluster.get(node).executeInternal(query)
                          : cluster.coordinator(node).execute(query, level);

        Assert.assertEquals(stringIndexRows, rows.length);
    }

    protected int repair(int node)
    {
        return cluster.get(node).callOnInstance(() -> {
            SimpleCondition repairFinished = new SimpleCondition();

            StorageService.instance.addNotificationListener((notification, handback) ->
            {
                // TODO: Find a cleaner way to determine that this is a completion notification.
                if (notification.getMessage().toLowerCase().contains("repair") && notification.getMessage().toLowerCase().contains("finished"))
                {
                    repairFinished.signalAll();
                }
            }, null, null);

            RepairOption options = new RepairOption(RepairParallelism.SEQUENTIAL, false, false, false, 1, Collections.emptyList(), false, false, false, PreviewKind.NONE, false);
            int ranges = StorageService.instance.repairAsync(KEYSPACE, options.asMap());

            if (ranges < 1)
            {
                return ranges;
            }

            // If the repair doesn't finish, something has gone terribly wrong...
            try
            {
                if (repairFinished.await(5, TimeUnit.MINUTES))
                    return ranges;
            }
            catch (Throwable e)
            {
                throw Throwables.unchecked(e);
            }
            throw new IllegalStateException("Repair did not finish!");
        });
    }

    protected void decommission(int node)
    {
        cluster.get(node).runOnInstance(() -> {
            try
            {
                StorageService.instance.decommission(false);
            }
            catch (Throwable e)
            {
                // expected to have IllegalStateException becuase in-jvm dtest doesn't register a daemon
                if (e instanceof IllegalStateException && e.getMessage().contains("No configured daemon"))
                    return;
                throw Throwables.unchecked(e);
            }
        });
    }

    protected void cleanup(int node, String keyspace, String table)
    {
        cluster.get(node).runOnInstance(() -> {
            try
            {
                StorageService.instance.forceKeyspaceCleanup(keyspace, table);
            }
            catch (Throwable e)
            {
                throw Throwables.unchecked(e);
            }
        });
    }

    /**
     * Run repeated verification task concurrently in 10ms interval with target test
     */
    protected void runWithConcurrentVerification(Runnable verificationTask, Runnable targetTask)
    {
        int verificationIntervalInMs = 10;
        Future<?> future = ScheduledExecutors.nonPeriodicTasks.schedule(targetTask, verificationIntervalInMs, TimeUnit.MILLISECONDS);

        Awaitility.await().atMost(1, TimeUnit.MINUTES).pollInterval(verificationIntervalInMs, TimeUnit.MILLISECONDS)
                  .until(() -> {
                      verificationTask.run();
                      return future.isDone();
                  });
    }

    protected boolean isIndexQueryable(int n, String keyspace, String table)
    {
        return cluster.get(n).callOnInstance( () -> {
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            for (Index index : cfs.indexManager.listIndexes())
            {
                if (!cfs.indexManager.isIndexQueryable(index))
                    return false;
            }
            return true;
        });
    }
}
