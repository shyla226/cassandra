/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.Iterator;
import java.util.List;
import java.util.function.ToLongFunction;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.SystemTimeSource;

import static com.datastax.bdp.db.nodesync.NodeSyncTestTools.*;

public class ContinuousValidationProposerTest extends AbstractValidationProposerTester
{
    @BeforeClass
    public static void setup()
    {
        System.setProperty(NodeSyncService.MIN_VALIDATION_INTERVAL_PROP_NAME, "-1");
    }

    @After
    public void cleanupTask()
    {
        NodeSyncHelpers.resetTestParameters();
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

        // A function that assigns fake sizes to those tables.
        // Note that we'll use a 'max segment size' of 200MB, and local ranges are hard-coded to 3 local ranges, so anything
        // lower than 600MB will get depth=0, anything lower than 1.2GB will get depth=1, etc...
        ToLongFunction<ColumnFamilyStore> sizeProvider = store ->
        {
            TableMetadata table = store.metadata();
            if (table.equals(table1)) return mb(800);  // depth=1
            if (table.equals(table2)) return mb(400);  // depth=0
            if (table.equals(table3)) return mb(1600); // depth=2
            throw new AssertionError();
        };

        NodeSyncHelpers.setTestParameters(sizeProvider, TEST_RANGES, mb(200), null);

        NodeSyncService service = new NodeSyncService(); // Not even started, just here because we need a reference below
        NodeSyncState state = new NodeSyncState(service);
        TableState state1 = state.getOrLoad(table1);
        TableState state2 = state.getOrLoad(table2);
        TableState state3 = state.getOrLoad(table3);

        // The local ranges are (0, 100], (200, 300] and (400, 500] so the segments for each tables will be:
        //  - table1, depth=1: (0, 50], (50, 100], (200, 250], (250, 300], (400, 450] and (450, 500].
        //  - table2, depth=0: (0, 100], (200, 300] and (400, 500].
        //  - table3, depth=2: (0, 25], (25, 50], (50, 75], (75, 100], (200], (225], (225, 250], ...

        testContinuousProposer(segs(table1).add(0, 50)
                                           .add(50, 100)
                                           .add(200, 250)
                                           .add(250, 300)
                                           .add(400, 450)
                                           .add(450, 500)
                                           .asList(),
                               state1);

        testContinuousProposer(segs(table2).add(0, 100)
                                           .add(200, 300)
                                           .add(400, 500)
                                           .asList(),
                               state2);

        testContinuousProposer(segs(table3).add(0, 25)
                                           .add(25, 50)
                                           .add(50, 75)
                                           .add(75, 100)
                                           .add(200, 225)
                                           .add(225, 250)
                                           .add(250, 275)
                                           .add(275, 300)
                                           .add(400, 425)
                                           .add(425, 450)
                                           .add(450, 475)
                                           .add(475, 500)
                                           .asList(),
                               state3);
    }

    private void testContinuousProposer(List<Segment> expected, TableState state)
    {
        // We check both that we get the expected range, but also that we do so "continuously" (of course, we don't
        // want to loop forever so we simply check 5 iterations)
        int RUNS = 5;
        Iterator<ValidationProposal> iterator = NodeSyncTestTools.continuousProposerAsIterator(state);
        for (int i = 0; i < RUNS; i++)
            assertSegments(expected, iterator);
    }
}
