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
import java.net.InetSocketAddress;
import java.util.Collections;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.IndexMetadata;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

public class IndexStatusTest extends TestBaseImpl
{
    private static final int nodes = 2;
    private static final int replicationFactor = 2;

    private Cluster cluster;

    @Before
    public void init() throws IOException
    {
        cluster = (Cluster) builder().withNodes(nodes).withConfig(config -> config.set("enable_sasi_indexes", true).with(NETWORK).with(GOSSIP)).start();

        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + replicationFactor + "};");
        cluster.schemaChange(String.format("CREATE TABLE %s.cf (k int, v1 int, v2 int, PRIMARY KEY (k))", KEYSPACE));
    }

    @After
    public void cleanup()
    {
        cluster.close();
    }

    @Test
    public void testIndexStatus()
    {
        cluster.coordinator(1).execute(String.format("INSERT INTO %s.cf (k, v1, v2) VALUES (1, 1, 1);", KEYSPACE), ConsistencyLevel.ALL);

        // initially there is no index, thus UNKNOWN
        String index = "test_index";
        verifyPeerIndexStatus(index, Index.Status.UNKNOWN, Index.Status.UNKNOWN);

        // create index
        createIndex(index, "v1");

        // after index built, expect BUILD_SUCCEEDED
        verifyPeerIndexStatus(index, Index.Status.BUILD_SUCCEEDED, Index.Status.BUILD_SUCCEEDED);
        verifyIndexReadCoordinator(ALL);

        // corrupt index on node1, node2 should stay queryable
        markIndexNonQueryable(1, index);

        // after 1st node index build failed, thus BUILD_FAILED
        verifyPeerIndexStatus(index, Index.Status.BUILD_SUCCEEDED, Index.Status.BUILD_FAILED);

        // verify local index read failed on node1
        verifyIndexReadInternalFailed(1);
        verifyIndexReadInternal(2);

        // verify distributed index query still works on consistency one
        verifyIndexReadCoordinator(ONE);
        verifyIndexReadCoordinatorFailed(ALL);

        // rebuild index on node1, it should be queryable now.
        rebuildIndex(1, index);
        verifyPeerIndexStatus(index, Index.Status.BUILD_SUCCEEDED, Index.Status.BUILD_SUCCEEDED);
        verifyIndexReadCoordinator(ALL);
    }

    @Test
    public void testMultipleIndexes()
    {
        // initially there is no index, thus UNKNOWN
        String index = "test_index_1";
        String corruptedIndex = "test_corrupted_index";

        // create two indexes
        createIndex(index, "v1");
        createIndex(corruptedIndex, "v2");

        verifyPeerIndexStatus(index, Index.Status.BUILD_SUCCEEDED, Index.Status.BUILD_SUCCEEDED);
        verifyPeerIndexStatus(corruptedIndex, Index.Status.BUILD_SUCCEEDED, Index.Status.BUILD_SUCCEEDED);

        // corrup corruptedIndex on node1, other index should work properly
        markIndexNonQueryable(1, corruptedIndex);
        verifyPeerIndexStatus(index, Index.Status.BUILD_SUCCEEDED, Index.Status.BUILD_SUCCEEDED);
        verifyPeerIndexStatus(corruptedIndex, Index.Status.BUILD_SUCCEEDED, Index.Status.BUILD_FAILED);

        // drop index, corruptedIndex remains non-queryable
        cluster.schemaChange(String.format("DROP INDEX %s.%s", KEYSPACE, index));
        verifyPeerIndexStatus(index, Index.Status.UNKNOWN, Index.Status.UNKNOWN);
        verifyPeerIndexStatus(corruptedIndex, Index.Status.BUILD_SUCCEEDED, Index.Status.BUILD_FAILED);
    }

    private void createIndex(String indexName, String column)
    {
        cluster.schemaChange(String.format("CREATE CUSTOM INDEX %s ON %s.cf (%s) USING 'org.apache.cassandra.distributed.test.IndexStatusTest$TestIndex';", indexName, KEYSPACE, column));
    }

    private void verifyPeerIndexStatus(String index, Index.Status node2Status, Index.Status node1Status)
    {
        verifyIndexStatus(1, 2, index, node2Status);
        verifyIndexStatus(2, 1, index, node1Status);
    }

    private void rebuildIndex(int host, String index)
    {
        cluster.get(host).runOnInstance(() -> {
            SecondaryIndexManager sim = Keyspace.open(KEYSPACE).getColumnFamilyStore("cf").indexManager;
            sim.rebuildIndexesBlocking(Collections.singleton(index));
        });
    }

    private void markIndexNonQueryable(int host, String index)
    {
        cluster.get(host).runOnInstance(() -> {
            SecondaryIndexManager sim = Keyspace.open(KEYSPACE).getColumnFamilyStore("cf").indexManager;
            sim.makeIndexNonQueryable(sim.getIndexByName(index), Index.Status.BUILD_FAILED);
        });
    }

    private void verifyIndexStatus(int hostNode, int peerNode, String indexName, Index.Status status)
    {
        InetSocketAddress address = cluster.get(peerNode).broadcastAddress();
        cluster.get(hostNode).runOnInstance(() -> {
            InetAddressAndPort peer = InetAddressAndPort.getByAddress(address.getAddress());
            // wait for gossip propagation
            Util.spinAssertEquals(status.toString(),
                                  () -> SecondaryIndexManager.getIndexStatus(peer, KEYSPACE, indexName).toString(),
                                  15);
        });
    }

    private void verifyIndexReadCoordinator(ConsistencyLevel cl)
    {
        // try different coordinator
        for (int n = 1; n <= cluster.size(); n++)
            verifyIndexRead(n, cl, false);
    }

    private void verifyIndexReadCoordinatorFailed(ConsistencyLevel cl)
    {
        // try different coordinator
        for (int n = 1; n <= cluster.size(); n++)
            verifyIndexRead(n, cl, true);
    }

    private void verifyIndexReadInternal(int host)
    {
        verifyIndexRead(host, null, false);
    }

    private void verifyIndexReadInternalFailed(int host)
    {
        verifyIndexRead(host, null, true);
    }

    private void verifyIndexRead(int coordinator, ConsistencyLevel cl, boolean fail)
    {
        // test range read
        String query = String.format("SELECT COUNT(*) FROM %s.cf WHERE v1 >= 0 ALLOW FILTERING", KEYSPACE);
        assertSingleRow(coordinator, query, cl, fail);

        // test single partition read
        query = String.format("SELECT COUNT(*) FROM %s.cf WHERE k = 1 AND v1 >= 0 ALLOW FILTERING", KEYSPACE);
        assertSingleRow(coordinator, query, cl, fail);
    }

    private void assertSingleRow(int host, String query, ConsistencyLevel cl, boolean fail)
    {
        if (cl != null)
            assertSingleRowCoordinator(host, query, cl, fail);
        else
            assertSingleRowInternal(host, query, fail);
    }

    private void assertSingleRowCoordinator(int host, String query, ConsistencyLevel cl, boolean fail)
    {
        if (fail)
        {
            Throwable thrown = Assertions.catchThrowable(() -> assertSingleRowCoordinator(host, query, cl, false));
            assertThat(thrown).hasMessageContaining("INDEX_NOT_AVAILABLE");
            return;
        }

        Object[][] results = cluster.coordinator(host).execute(query, cl);
        Assert.assertEquals(1, (long) results[0][0]);
    }

    private void assertSingleRowInternal(int host, String query, boolean fail)
    {
        if (fail)
        {
            Throwable thrown = Assertions.catchThrowable(() -> assertSingleRowInternal(host, query, false));
            assertThat(thrown).hasMessageContaining("'test_index' is not yet available");
            return;
        }
        Object[][] results = cluster.get(host).executeInternal(query);
        Assert.assertEquals(1, (long) results[0][0]);
    }

    /**
     * Use this TestIndex to simulate Storage-Attached-Index behavior
     */
    public static class TestIndex extends SASIIndex
    {

        public TestIndex(ColumnFamilyStore baseCfs, IndexMetadata config)
        {
            super(baseCfs, config);
        }

        @Override
        public boolean isQueryable(Status status)
        {
            return status == Status.UNKNOWN || status == Status.BUILD_SUCCEEDED;
        }
    }
}
