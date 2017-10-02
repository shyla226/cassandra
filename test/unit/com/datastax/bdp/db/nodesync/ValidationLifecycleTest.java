/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.TestTimeSource;
import org.apache.cassandra.utils.TimeSource;

import static com.datastax.bdp.db.nodesync.NodeSyncTestTools.metadataBuilder;
import static org.junit.Assert.*;

import static com.datastax.bdp.db.nodesync.NodeSyncTestTools.*;

public class ValidationLifecycleTest
{
    private static final boolean debugLog = false; // Set to true to have details logs; make debugging (and
                                                   // understanding) the tests easier.

    private static final PageSize pageSize = PageSize.bytesSize(100);

    @BeforeClass
    public static void init()
    {
        // Sad that we have to do this, but some initialization hell forces us to currently.
        DatabaseDescriptor.daemonInitialization();
    }

    @Test(expected = InvalidatedNodeSyncStateException.class)
    public void testInvalidation()
    {
        TableMetadata table = metadataBuilder("ks", "table")
                              .addPartitionKeyColumn("k", Int32Type.instance)
                              .build();

        NodeSyncTestTools.DevNullTableProxy statusProxy = new NodeSyncTestTools.DevNullTableProxy(debugLog);
        NodeSyncService service = new NodeSyncService(statusProxy);
        TableState state = TableState.load(service, table, Collections.singleton(range(0, 10)), 1);

        TableState.Ref ref = state.nextSegmentToValidate();

        ValidationLifecycle lifecycle = ValidationLifecycle.createAndStart(ref,
                                                                           NodeSyncTracing.SegmentTracing.NO_TRACING);
        assertTrue(ref.currentState().isLocallyLocked());
        assertEquals(1, statusProxy.lockCalls);

        // Do a change that invalidate the state
        state.update(Collections.singleton(range(10, 20)));

        // The next onNewPage should now throw
        lifecycle.onNewPage(pageSize);
    }

    @Test
    public void testNormalLifecycle() throws Exception
    {
        TimeSource timeSource = new TestTimeSource();
        timeSource.autoAdvance(1, 1, TimeUnit.SECONDS);
        NodeSyncHelpers.setTestParameters(null, null, -1, timeSource);

        TableMetadata table = metadataBuilder("ks", "table")
                              .addPartitionKeyColumn("k", Int32Type.instance)
                              .build();

        NodeSyncTestTools.DevNullTableProxy statusProxy = new NodeSyncTestTools.DevNullTableProxy(debugLog);
        NodeSyncService service = new NodeSyncService(statusProxy);
        TableState state = TableState.load(service, table, Collections.singleton(range(0, 10)), 1);

        TableState.Ref ref = state.nextSegmentToValidate();

        ValidationLifecycle lifecycle = ValidationLifecycle.createAndStart(ref,
                                                                           NodeSyncTracing.SegmentTracing.NO_TRACING);

        // Starting the lifecycle should have locked it
        assertEquals(1, statusProxy.lockCalls);
        assertTrue(ref.currentState().isLocallyLocked());

        // First call to newPage; shouldn't do anything. Shouldn't refresh the lock in particular
        lifecycle.onNewPage(pageSize);
        assertEquals(1, statusProxy.lockCalls);

        // Now advance the clock enough that the lock should be refreshed, and check that it is
        // Note: we know the lock is refreshed if it's older than 3/4 the lock timeout.
        timeSource.sleep(4 * ValidationLifecycle.LOCK_TIMEOUT_SEC / 5, TimeUnit.SECONDS);

        lifecycle.onNewPage(pageSize);
        assertEquals(2, statusProxy.lockCalls);

        // Complete and check that we 1) have save the validation and 2) unlocked the segment
        long beforeValidation = ref.currentState().lastValidationTimeMs();
        lifecycle.onCompletion(fullInSync(0), new ValidationMetrics());
        assertEquals(2, statusProxy.lockCalls); // Sanity check that it hasn't changed
        assertFalse(ref.currentState().isLocallyLocked());
        assertEquals(1, statusProxy.recordValidationCalls);
        assertTrue(ref.currentState().lastValidationTimeMs() > beforeValidation);
    }

    @Test
    public void testCancellation()
    {
        TableMetadata table = metadataBuilder("ks", "table")
                              .addPartitionKeyColumn("k", Int32Type.instance)
                              .build();

        NodeSyncTestTools.DevNullTableProxy statusProxy = new NodeSyncTestTools.DevNullTableProxy(debugLog);
        NodeSyncService service = new NodeSyncService(statusProxy);
        TableState state = TableState.load(service, table, Collections.singleton(range(0, 10)), 1);

        TableState.Ref ref = state.nextSegmentToValidate();

        ValidationLifecycle lifecycle = ValidationLifecycle.createAndStart(ref,
                                                                           NodeSyncTracing.SegmentTracing.NO_TRACING);
        assertTrue(ref.currentState().isLocallyLocked());
        assertEquals(1, statusProxy.lockCalls);

        lifecycle.cancel("Test");

        assertFalse(ref.currentState().isLocallyLocked());
        assertEquals(1, statusProxy.forceUnlockCalls);
    }
}