/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.List;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.SystemTimeSource;

import static com.datastax.bdp.db.nodesync.NodeSyncTestTools.*;
import static java.util.Arrays.asList;

public class UserValidationProposerTest extends AbstractValidationProposerTester
{
    @After
    public void cleanupTask()
    {
        NodeSyncHelpers.resetTestParameters();
    }

    @Test
    public void testSegmentGeneration() throws Exception
    {
        testSegmentGeneration(0,
                              asList(range(200, 300)),
                              range(200, 250));
        testSegmentGeneration(0,
                              asList(range(200, 300)),
                              range(200, 300));
        testSegmentGeneration(0,
                              asList(range(0, 100), range(200, 300)),
                              range(50, 100), range(200, 300));

        testSegmentGeneration(1,
                              asList(range(200, 250)),
                              range(200, 250));
        testSegmentGeneration(1,
                              asList(range(200, 250), range(250, 300)),
                              range(200, 275));
        testSegmentGeneration(1,
                              asList(range(50, 100), range(200, 250), range(250, 300)),
                              range(50, 100), range(200, 300));

    }

    @SuppressWarnings("unchecked")
    private void testSegmentGeneration(int depth,
                                       List<Range<Token>> expected,
                                       Range... requestedRanges) throws Exception
    {
        // We need the keyspace to say RF > 1 or the proposer creation will complain
        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");

        // Don't set nodesync mostly to test it's ignore by user validations.
        TableMetadata table = createDummyTable(ks, false);

        List<Range<Token>> requested = asList(requestedRanges);

        // We have 3 local ranges and 10MB max seg size, so...
        NodeSyncHelpers.setTestParameters(t -> depth * mb(31), TEST_RANGES, mb(10), null);

        NodeSyncService service = new NodeSyncService(); // Not even started, just here because we need a reference below
        NodeSyncState state = new NodeSyncState(service);
        UserValidationOptions options = new UserValidationOptions("test", table, requested, null);
        UserValidationProposer proposer = UserValidationProposer.create(state, options);

        assertSegments(segs(table).addAll(expected).asList(), proposer);
    }

}