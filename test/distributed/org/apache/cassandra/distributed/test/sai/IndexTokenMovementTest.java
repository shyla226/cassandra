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

import org.junit.Test;

import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexTokenMovementTest extends AbstractIndexTest
{
    @Test
    public void verifyIndexWithBootstrapWithMV() throws Exception
    {
        verifyIndexWithBootstrap(true);
    }

    @Test
    public void verifyIndexWithBootstrapWithoutMV() throws Exception
    {
        verifyIndexWithBootstrap(false);
    }

    private void verifyIndexWithBootstrap(boolean withMV) throws Exception
    {
        this.cluster = initCluster(1,
                                   builder -> builder.withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2))
                                                     .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0")));

        // prepare schema ks rf=1 with 2 indexes
        String table = "verify_ndi_during_bootstrap_test";
        String mv = withMV ? "create_index_files_during_repair_mv" : null;
        int num = 100;
        prepareRf1DataAndIndexesInTwoSSTables(table, mv, num, "v1", "v2");

        cluster.forEach(node -> node.flush(KEYSPACE));

        // verify 1st node contains all data
        long indexedRowsNumeric = getIndexedCellCount(1, table, "v1");
        long indexedRowsString = getIndexedCellCount(1, table, "v2");
        assertEquals(indexedRowsNumeric, indexedRowsString);
        assertEquals(num, indexedRowsNumeric);

        // boostrap 2nd node
        IInstanceConfig config = cluster.newInstanceConfig();
        config.set("auto_bootstrap", true);
        cluster.bootstrap(config).startup();

        // verify 1st node still contains all data after bootstrap before cleanup
        indexedRowsNumeric = getIndexedCellCount(1, table, "v1");
        indexedRowsString = getIndexedCellCount(1, table, "v2");
        assertEquals(indexedRowsNumeric, indexedRowsString);
        assertEquals(num, indexedRowsNumeric);

        // verify 2nd node contains part of data after bootstrap
        long indexedRowsNumeric2 = getIndexedCellCount(2, table, "v1");
        long indexedRowsString2 = getIndexedCellCount(2, table, "v2");
        assertEquals(indexedRowsNumeric2, indexedRowsString2);
        assertTrue(indexedRowsNumeric2 < num);

        // verify data with concurrent nodetool cleanup
        runWithConcurrentVerification(() -> verifyIndexQueryAll(1, table, num, num), () -> cleanup(1, KEYSPACE, table));

        // after cleanup on 1st node, it should contains part of data
        indexedRowsNumeric = getIndexedCellCount(1, table, "v1");
        indexedRowsString = getIndexedCellCount(1, table, "v2");
        assertEquals(indexedRowsNumeric, indexedRowsString);
        assertTrue(indexedRowsNumeric < num);
        assertEquals(indexedRowsNumeric + indexedRowsNumeric2, num);

        verifyIndexQueryAll(1, table, num, num);
    }

    @Test
    public void verifyIndexWithDecommission() throws Exception
    {
        cluster = initCluster(2);

        // prepare schema ks rf=1 with 2 indexes
        String table = "verify_ndi_during_decommission_test";
        int num = 100;
        prepareDataAndIndexesInTwoSSTables(1, table, null, num, "v1", "v2");

        cluster.forEach(node -> node.flush(KEYSPACE));

        // verify each node contains part of data
        long total = 0;
        for (int i = 1; i <= cluster.size(); i++)
        {
            long indexedRowsNumeric = getIndexedCellCount(i, table, "v1");
            long indexedRowsString = getIndexedCellCount(i, table, "v2");
            assertEquals(indexedRowsNumeric, indexedRowsString);
            assertTrue(indexedRowsNumeric < num);
            total += indexedRowsNumeric;
        }
        assertEquals(num, total);

        // have to change system_distributed and system_traces to RF=1 for decommission to pass in 2-node setup
        cluster.schemaChange(String.format(ALTER_KEYSPACE_TEMPLATE_RF, "system_traces", 1));
        cluster.schemaChange(String.format(ALTER_KEYSPACE_TEMPLATE_RF, "system_distributed", 1));

        // verify data with concurrent decommission
        runWithConcurrentVerification(() -> verifyIndexQueryAll(1, table, num, num), () -> decommission(2));

        cluster.get(2).shutdown();

        verifyIndexQueryAll(1, table, num, num);

        // node1 has all indexed data after decommission
        verifyIndexedCellCount(1, table, "v1", num);
        verifyIndexedCellCount(1, table, "v2", num);
    }
}
