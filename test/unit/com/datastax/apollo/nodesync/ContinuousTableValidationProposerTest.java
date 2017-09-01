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
package com.datastax.apollo.nodesync;

import java.util.List;
import java.util.function.ToLongFunction;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.*;
import static com.datastax.apollo.nodesync.NodeSyncTestTools.*;

public class ContinuousTableValidationProposerTest extends AbstractValidationProposerTester
{
    @After
    public void cleanupTask()
    {
        NodeSyncHelpers.resetTableSizeAndLocalRangeProviders();
    }

    @Test
    public void testValidationProposalGeneration() throws Exception
    {
        // We need the keyspace to say RF > 1 or computePlan() will ignore the keyspace entirely
        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");

        // Creates 3 tables for our tests. Their schema doesn't matter, we're only checking that validation proposals are
        // generated properly and we're going to fake their size as inserting enough data to make the test meaningful is
        // not too appropriate for this unit test.
        TableMetadata table1 = createDummyTable(ks, true);
        TableMetadata table2 = createDummyTable(ks, true);
        TableMetadata table3 = createDummyTable(ks, true);

        // Also throws a table without NodeSync enabled in the mix to make sure it's properly ignored.
        TableMetadata unrepaired = createDummyTable(ks, false);

        // A function that assigns fake sizes to those tables.
        // Note that we'll use a 'max segment size' of 200MB, and local ranges are hard-coded to 3 local ranges, so anything
        // lower than 600MB will get depth=0, anything lower than 1.2GB will get depth=1, etc...
        ToLongFunction<ColumnFamilyStore> sizeProvider = store ->
        {
            TableMetadata table = store.metadata();
            if (table.equals(table1)) return mb(800);  // depth=1
            if (table.equals(table2)) return mb(400);  // depth=0
            if (table.equals(table3)) return mb(1600); // depth=2
            if (table.equals(unrepaired)) return mb(1300);
            throw new AssertionError();
        };

        NodeSyncHelpers.setTableSizeAndLocalRangeProviders(sizeProvider, TEST_RANGES);

        NodeSyncService service = new NodeSyncService(); // Not even started, just here because we need a reference below
        List<ContinuousTableValidationProposer> proposers = ContinuousTableValidationProposer.createForKeyspace(service,
                                                                                                                Keyspace.open(ks),
                                                                                                                mb(200));

        // Make sure the unrepaired table was ignored
        assertEquals(String.format("Expect %d proposers but got %d (%s)", 3, proposers.size(), proposers), 3, proposers.size());

        // The local ranges are (0, 100], (200, 300] and (400, 500] so the segments for each tables will be:
        //  - table1, depth=1: (0, 50], (50, 100], (200, 250], (250, 300], (400, 450] and (450, 500].
        //  - table2, depth=0: (0, 100], (200, 300] and (400, 500].
        //  - table3, depth=2: (0, 25], (25, 50], (50, 75], (75, 100], (200], (225], (225, 250], ...
        // We check both that we get the expected range, but also that we do so "continuously" (of course, we don't
        // want to loop forever so we simply check 5 iterations)
        int RUNS = 5;

        List<Segment> table1Segments = segs(table1).add(0, 50)
                                                   .add(50, 100)
                                                   .add(200, 250)
                                                   .add(250, 300)
                                                   .add(400, 450)
                                                   .add(450, 500)
                                                   .asList();

        List<Segment> table2Segments = segs(table2).add(0, 100)
                                                   .add(200, 300)
                                                   .add(400, 500)
                                                   .asList();

        List<Segment> table3Segments = segs(table3).add(0, 25)
                                                   .add(25, 50)
                                                   .add(50, 75)
                                                   .add(75, 100)
                                                   .add(200, 225)
                                                   .add(225, 250)
                                                   .add(250, 375)
                                                   .add(275, 300)
                                                   .add(400, 425)
                                                   .add(425, 450)
                                                   .add(450, 475)
                                                   .add(475, 500)
                                                   .asList();

        for (ContinuousTableValidationProposer proposer : proposers)
        {
            TableMetadata table = proposer.table();
            for (int i = 0; i < RUNS; i++)
            {
                if (table.equals(table1))
                    assertSegments(table1Segments, proposer);
                else if (table.equals(table2))
                    assertSegments(table2Segments, proposer);
                else if (table.equals(table3))
                    assertSegments(table3Segments, proposer);
                else
                    fail(String.format("Table %s shouldn't have proposer created", table));
            }
        }
    }
}
