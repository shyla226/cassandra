/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.db.nodesync.TableState.Ref.Status;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.NodeSyncParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TestTimeSource;
import org.apache.cassandra.utils.TimeSource;

import static org.junit.Assert.*;
import static com.datastax.bdp.db.nodesync.NodeSyncTestTools.*;
import static java.util.Arrays.asList;

public class TableStateTest
{
    private static final Logger logger = LoggerFactory.getLogger(TableStateTest.class);
    private static final boolean debugLog = false; // Set to true to have details logs; make debugging (and
                                                   // understanding) the tests much easier.

    @BeforeClass
    public static void init()
    {
        // Sad that we have to do this, but some initialization hell forces us to currently.
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void cleanupTask()
    {
        NodeSyncHelpers.resetTestParameters();
    }

    private static void log(TableState state)
    {
        if (debugLog)
            logger.info(state.toString());
    }

    @Test
    public void testBasicPrioritization() throws Exception
    {
        TimeSource timeSource = new TestTimeSource();
        timeSource.autoAdvance(1, 1, TimeUnit.SECONDS);
        NodeSyncHelpers.setTestParameters(null, null, -1, timeSource);

        // Using a very small deadline so that the small delays between the fake validations below account for a
        // comparatively long time.
        TableMetadata table = metadataBuilder("ks", "table")
                              .addPartitionKeyColumn("k", Int32Type.instance)
                              .params(TableParams.builder().nodeSync(new NodeSyncParams(true, 5, Collections.emptyMap())).build())
                              .build();

        NodeSyncService service = new NodeSyncService(new NodeSyncTestTools.DevNullTableProxy(debugLog));

        // We'll use a depth of 0, so the range we put here will be our segments for that test
        List<Range<Token>> localRanges = asList(range(0, 1), range(1, 2), range(2, 3));
        TableState state = TableState.load(service, table, localRanges, 0);

        Segment s1 = seg(table, 0, 1);
        Segment s2 = seg(table, 1, 2);
        Segment s3 = seg(table, 2, 3);

        log(state);

        TableState.Ref next = state.nextSegmentToValidate().right;
        assertEquals(s1, next.segment());
        assertEquals(Status.UP_TO_DATE, next.checkStatus());
        ValidationLifecycle v1 = ValidationLifecycle.createAndStart(next, false);

        log(state);

        // We pretended to start validation on the first segment, make sure we get the next one now.
        next = state.nextSegmentToValidate().right;
        assertEquals(s2, next.segment());
        assertEquals(Status.UP_TO_DATE, next.checkStatus());
        ValidationLifecycle v2 = ValidationLifecycle.createAndStart(next, false);

        log(state);

        // Complete the first one and make sure we still get the next one first.
        v1.onCompletion(fullInSync(0));

        log(state);

        next = state.nextSegmentToValidate().right;
        assertEquals(s3, next.segment());
        assertEquals(Status.UP_TO_DATE, next.checkStatus());
        ValidationLifecycle v3 = ValidationLifecycle.createAndStart(next, false);

        log(state);

        // Ask for one more: it should be locally locked because while s1 is unlocked, it just got successfully validated
        // while s2 and s3 haven't be validated at all, so even with lock penalty they come first.
        assertTrue(state.nextSegmentToValidate().left.isLocallyLocked());

        // Complete both the validation on the 2nd and 3rd segment (with the one for 3rd segment not successful)...
        v2.onCompletion(fullInSync(0));
        v3.onCompletion(partialRepaired(0));

        log(state);

        // ... and the next segment should be the 3rd one (s1 and s2 last were successful, not s3 so it should be retried first).
        next = state.nextSegmentToValidate().right;
        assertEquals(s3, next.segment());
        assertEquals(Status.UP_TO_DATE, next.checkStatus());
        ValidationLifecycle v4 = ValidationLifecycle.createAndStart(next, false);

        log(state);

        // After that, s1 is now the one having the oldest successful validation
        next = state.nextSegmentToValidate().right;
        assertEquals(s1, next.segment());

        // Now have s3 validation come back before we check the status of the s1 proposal. It should make the status
        // return UPDATED
        v3.onCompletion(partialRepaired(0));
        assertEquals(Status.UPDATED, next.checkStatus());

        log(state);

        // And because s3 was still failing, it should be the one to retry first
        next = state.nextSegmentToValidate().right;
        assertEquals(s3, next.segment());
    }

    @Test
    public void testLocalRangeChangeInvalidates()
    {
        testInvalidations(state -> state.update(asList(range(50, 60), range(60, 70))), true);
    }

    @Test
    public void testNoOpLocalRangeChangeDontInvalidates()
    {
        // Using the same local ranges, not really a change so shouldn't invalidate
        testInvalidations(state -> state.update(asList(range(0, 10), range(10, 20))), false);
    }

    @Test
    public void testDepthDecreaseInvalidates()
    {
        testInvalidations(state -> state.update(0), true);
    }

    @Test
    public void testNoOpDepthChangesDontInvalidates()
    {
        testInvalidations(state -> state.update(1), false);
    }

    @Test
    public void testNoOpDepthIncreaseDontInvalidates()
    {
        testInvalidations(state -> state.update(2), false);
    }

    private void testInvalidations(Consumer<TableState> updateOp, boolean shouldInvalidate)
    {
        TableMetadata table = metadataBuilder("ks", "table")
                              .addPartitionKeyColumn("k", Int32Type.instance)
                              .build();

        NodeSyncService service = new NodeSyncService(new NodeSyncTestTools.DevNullTableProxy(debugLog));

        List<Range<Token>> localRanges = asList(range(0, 10), range(10, 20));
        TableState state = TableState.load(service, table, localRanges, 1);

        TableState.Ref ref = state.nextSegmentToValidate().right;
        updateOp.accept(state);

        assertEquals(shouldInvalidate, ref.isInvalidated());
    }
}