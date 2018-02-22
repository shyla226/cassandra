/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.NodeSyncParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.utils.TestTimeSource;
import org.apache.cassandra.utils.TimeSource;

import static com.datastax.bdp.db.nodesync.NodeSyncTracing.SegmentTracing.NO_TRACING;
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
        TestTimeSource timeSource = new TestTimeSource();
        timeSource.reset(0, 0); // Starting the time at 0 make things easier to follow when debugging
        NodeSyncHelpers.setTestParameters(null, null, -1, timeSource);

        // Using a very small deadline so that the small delays between the fake validations below account for a
        // comparatively long time.
        TableMetadata table = metadataBuilder("ks", "table")
                              .addPartitionKeyColumn("k", Int32Type.instance)
                              .params(TableParams.builder().nodeSync(new NodeSyncParams(true, 10, Collections.emptyMap())).build())
                              .build();

        NodeSyncService service = new NodeSyncService(new NodeSyncTestTools.DevNullTableProxy(debugLog));

        // We'll use a depth of 0, so the range we put here will be our segments for that test
        List<Range<Token>> localRanges = asList(range(0, 1), range(1, 2), range(2, 3));
        TableState state = TableState.load(service, table, localRanges, 0);

        Segment s1 = seg(table, 0, 1);
        Segment s2 = seg(table, 1, 2);
        Segment s3 = seg(table, 2, 3);

        log(state);

        TableState.Ref next = state.nextSegmentToValidate();
        assertEquals(s1, next.segment());
        assertTrue(next.checkStatus().isUpToDate());
        ValidationLifecycle v1 = ValidationLifecycle.createAndStart(next, NO_TRACING);

        log(state);
        timeSource.sleep(1, TimeUnit.SECONDS);

        // We pretended to start validation on the first segment, make sure we get the next one now.
        next = state.nextSegmentToValidate();
        assertEquals(s2, next.segment());
        assertTrue(next.checkStatus().isUpToDate());
        ValidationLifecycle v2 = ValidationLifecycle.createAndStart(next, NO_TRACING);

        log(state);
        timeSource.sleep(1, TimeUnit.SECONDS);

        // Complete the first one and make sure we still get the next one first.
        v1.onCompletion(fullInSync(0), new ValidationMetrics());

        log(state);
        timeSource.sleep(1, TimeUnit.SECONDS);

        next = state.nextSegmentToValidate();
        assertEquals(s3, next.segment());
        assertTrue( next.checkStatus().isUpToDate());
        ValidationLifecycle v3 = ValidationLifecycle.createAndStart(next, NO_TRACING);

        log(state);
        timeSource.sleep(1, TimeUnit.SECONDS);

        // Ask for one more: it should be locally locked because while s1 is unlocked, it just got successfully validated
        // while s2 and s3 haven't be validated at all, so even with lock penalty they come first.
        assertTrue(state.nextSegmentToValidate().segmentStateAtCreation().isLocallyLocked());

        // Complete both the validation on the 2nd and 3rd segment (with the one for 3rd segment not successful)...
        v2.onCompletion(fullInSync(0), new ValidationMetrics());
        v3.onCompletion(partialRepaired(0), new ValidationMetrics());

        log(state);
        timeSource.sleep(1, TimeUnit.SECONDS);

        // ... and the next segment should be the 3rd one (s1 and s2 last were successful, not s3 so it should be retried first).
        next = state.nextSegmentToValidate();
        assertEquals(s3, next.segment());
        assertTrue(next.checkStatus().isUpToDate());
        ValidationLifecycle v4 = ValidationLifecycle.createAndStart(next, NO_TRACING);

        log(state);
        timeSource.sleep(1, TimeUnit.SECONDS);

        // After that, s1 is now the one having the oldest successful validation
        next = state.nextSegmentToValidate();
        assertEquals(s1, next.segment());

        // Now have s3 validation come back failing before we check the status of the s1 proposal. Because it is failing,
        // s3 should be the one with the most priority and checkStatus should indicate that.
        v4.onCompletion(partialRepaired(0), new ValidationMetrics());
        log(state);

        assertFalse(next.checkStatus().isUpToDate());

        log(state);
        timeSource.sleep(1, TimeUnit.SECONDS);

        // And we should get s3 as mentioned above
        next = state.nextSegmentToValidate();
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

        TableState.Ref ref = state.nextSegmentToValidate();
        updateOp.accept(state);

        assertEquals(shouldInvalidate, ref.isInvalidated());
    }

    @Test
    public void testDepthIncrease()
    {
        // Depth increase are not invalidating validations (as tested above), but we should also make sure validation
        // on-flight during such change are still properly taken into account.

        TestTimeSource timeSource = new TestTimeSource();
        timeSource.autoAdvance(1, 1, TimeUnit.SECONDS);
        NodeSyncHelpers.setTestParameters(null, null, -1, timeSource);

        TableMetadata table = metadataBuilder("ks", "table")
                              .addPartitionKeyColumn("k", Int32Type.instance)
                              .build();

        NodeSyncService service = new NodeSyncService(new NodeSyncTestTools.DevNullTableProxy(debugLog));

        TableState state = TableState.load(service, table, asList(range(0, 10), range(20, 30)), 0);

        TableState.Ref ref = state.nextSegmentToValidate();
        // On an initial state, the code will return segments in order initially (even though technically other choice
        // would be as valid). We do rely on this ordering below however so make sure this doesn't change (if it does,
        // the test can be adapted but it's a tad more annoying).
        assertEquals(range(0, 10), ref.segment().range);
        ValidationLifecycle lifecycle = ValidationLifecycle.createAndStart(ref, NO_TRACING);

        // Increase the depth and _then_ finish the validation
        state.update(1);

        lifecycle.onCompletion(fullInSync(0), new ValidationMetrics());

        // We increased the depth by 1, so all segment got split in 2, and the validation was on the first state, so
        // the 2 first segment should now have been validated (and the 2 last shouldn't).
        List<SegmentState> segmentStates = state.dumpSegmentStates();
        assertEquals(4, segmentStates.size());
        for (int i = 0; i < 2; i++)
            assertTrue(segmentStates.get(i).lastValidationTimeMs() > 0);
        for (int i = 2; i < 4; i++)
            assertTrue(segmentStates.get(i).lastValidationTimeMs() < 0);
    }

    @Test
    public void testInvalidationAndStatusToStringRace() throws Exception
    {
        TableMetadata table = metadataBuilder("ks", "table")
                              .addPartitionKeyColumn("k", Int32Type.instance)
                              .build();

        NodeSyncService service = new NodeSyncService(new NodeSyncTestTools.DevNullTableProxy(debugLog));

        List<Range<Token>> localRanges = asList(range(-2, -1), range(-1, 0));
        TableState state = TableState.load(service, table, localRanges, 1);

        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicBoolean stopped = new AtomicBoolean();
        AtomicReference<String> str = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try
            {
                while (!stopped.get())
                {
                    TableState.Ref ref = state.nextSegmentToValidate();
                    ref.lock();
                    String s = ref.checkStatus().toString();
                    str.set(s);
                }
            }
            catch (Exception e)
            {
                failure.set(e);
            }
        });
        t.start();
        for (int i = 0; i < 10000; i++)
            state.update(asList(range(i, i + 1), range(i + 1, i + 2)));

        stopped.set(true);
        t.join();

        if (failure.get() != null)
            throw failure.get();
    }
}