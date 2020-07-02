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

public class IndexRepairTest extends AbstractIndexTest
{
    @Test
    public void shouldReindexDuringRepairWithMV() throws Exception
    {
        // streamed sstables are sent through write path for table with MV
        shouldReindexDuringRepair(true);
    }

    @Test
    public void shouldReindexDuringRepairWithoutMV() throws Exception
    {
        shouldReindexDuringRepair(false);
    }

    private void shouldReindexDuringRepair(boolean withMV) throws Exception
    {
        cluster = initCluster(2);

        // prepare ks rf=1
        String table = "create_index_files_during_repair_test";
        String mv = withMV ? "create_index_files_during_repair_mv" : null;
        int num = 100;
        prepareRf1DataAndIndexesInTwoSSTables(table, mv, num, "v1", "v2");
        verifyIndexQueryAll(2, table, num, num);

        // Alter RF2
        cluster.schemaChange(String.format(ALTER_KEYSPACE_TEMPLATE_RF, KEYSPACE, 2));

        // run repair on node2
        repair(2);

        // Take down the first node and verify that repair-actuated index building has completed:
        cluster.get(1).shutdown();

        // verify index on node2
        verifyIndexQueryOne(2, table, num, num);
    }

    @Test
    public void shouldReadRepairMemoryIndex() throws Exception
    {
        shouldReadRepairIndex(false);
    }

    @Test
    public void shouldReadRepairOnDiskIndex() throws Exception
    {
        shouldReadRepairIndex(true);
    }

    public void shouldReadRepairIndex(boolean flush) throws Exception
    {
        cluster = initCluster(2);

        // prepare schema ks rf=1 with 2 indexes
        cluster.schemaChange(String.format(CREATE_RF2_KEYSPACE_TEMPLATE, KEYSPACE));
        String table = "verify_ndi_during_decommission_test";
        cluster.schemaChange(String.format(CREATE_TABLE_TEMPLATE, KEYSPACE, table));
        cluster.schemaChange(String.format(CREATE_INDEX_TEMPLATE, "", KEYSPACE, table, "V1"));
        cluster.schemaChange(String.format(CREATE_INDEX_TEMPLATE, "", KEYSPACE, table, "V2"));

        // create 100 rows in 1 sstable at 1st node
        int num = 100;
        for (int i = 0; i < num; i++)
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + '.' + table + "(ID1, V1, V2) VALUES ('" + i + "', 0, '0');");

        if (flush)
            cluster.forEach(node -> node.flush(KEYSPACE));

        // verify data is at 1st node only
        verifyIndexQueryLocal(1, table, num, num);
        verifyIndexQueryLocal(2, table, 0, 0);

        // read repair indexed data
        verifyIndexQueryAll(1, table, num, num);

        // verify data is read-repaired at 2nd node
        verifyIndexQueryLocal(2, table, num, num);
    }
}
