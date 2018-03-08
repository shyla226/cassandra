/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.UnknownKeyspaceException;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.TableMetadata;

import static com.datastax.bdp.db.nodesync.NodeSyncTestTools.*;
import static org.junit.Assert.*;

/**
 * Test basic behavior of Validator.
 */
public class ValidatorTest extends CQLTester
{
    private static final boolean debugLog = true; // Set to true to have details logs; make debugging (and
                                                   // understanding) the tests easier.

    private final NodeSyncTestTools.DevNullTableProxy statusProxy = new NodeSyncTestTools.DevNullTableProxy(debugLog);
    private final NodeSyncService service = new NodeSyncService(statusProxy);

    private TableMetadata prepare()
    {
        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '3' }");
        String table = createTable(ks, "CREATE TABLE %s (k int PRIMARY KEY) WITH nodesync = { 'enabled' : 'true' }");
        return tableMetadata(ks, table);
    }

    @Test
    public void testSimpleValidation() throws Exception
    {
        TableMetadata table = prepare();
        TableState state = TableState.load(service, table, Collections.singleton(range(min(), min())), 0);

        TableState.Ref ref = state.nextSegmentToValidate();
        ValidationLifecycle lifecycle = ValidationLifecycle.createAndStart(ref,
                                                                           NodeSyncTracing.SegmentTracing.NO_TRACING);

        Validator validator = new TestValidator(lifecycle, 10, 10);
        ValidationExecutor executor = executor(service, 1000, validator);
        executor.start();

        Uninterruptibles.getUninterruptibly(validator.completionFuture());
        assertEquals(1, statusProxy.lockCalls);
        assertEquals(0, statusProxy.forceUnlockCalls);
        assertEquals(1, statusProxy.recordValidationCalls);
    }

    @Test
    public void testInvalidation() throws Exception
    {
        TableMetadata table = prepare();
        TableState state = TableState.load(service, table, Collections.singleton(range(min(), min())), 0);

        TableState.Ref ref = state.nextSegmentToValidate();
        ValidationLifecycle lifecycle = ValidationLifecycle.createAndStart(ref,
                                                                           NodeSyncTracing.SegmentTracing.NO_TRACING);

        // Create a validator that would basically run indefinitely if not interrupted.
        Validator validator = new TestValidator(lifecycle, Integer.MAX_VALUE, 100);

        ValidationExecutor executor = executor(service, 1000, validator);
        executor.start();

        // Wait just a little bit to make sure the update happens while the validation is running.
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

        // Do a change that invalidate the state
        state.update(Collections.singleton(range(10, 20)));

        try
        {
            Uninterruptibles.getUninterruptibly(validator.completionFuture(), 5, TimeUnit.SECONDS);
            fail("Should have been cancelled");
        }
        catch (CancellationException e)
        {
            // That's what we actually except.
            // further, we should have initially locked the segment and force unlocked it, but not have recorded anything
            assertEquals(1, statusProxy.lockCalls);
            assertEquals(1, statusProxy.forceUnlockCalls);
            assertEquals(0, statusProxy.recordValidationCalls);
        }
    }

    @Test
    public void testLockReleaseOnCreationError()
    {
        TableMetadata table = prepare();
        TableState state = TableState.load(service, table, Collections.singleton(range(min(), min())), 0);

        TableState.Ref ref = state.nextSegmentToValidate();
        ValidationLifecycle lifecycle = ValidationLifecycle.createAndStart(ref,
                                                                           NodeSyncTracing.SegmentTracing.NO_TRACING);

        MigrationManager.announceKeyspaceDrop(table.keyspace, true).blockingAwait();

        try
        {
            Validator.create(lifecycle);
            fail("Validator creation should have failed");
        }
        catch (UnknownKeyspaceException e)
        {
            assertEquals(1, statusProxy.lockCalls);
            assertEquals(1, statusProxy.forceUnlockCalls);
            assertEquals(0, statusProxy.recordValidationCalls);
        }
    }

    @Test
    public void testDirectCancellation()
    {
        testCancellation(v -> v.cancel("cancelled"));
    }

    @Test
    public void testCancellationThroughFuture()
    {
        testCancellation(v -> v.completionFuture().cancel(true));
    }

    private void testCancellation(Consumer<Validator> cancelFct)
    {
        TableMetadata table = prepare();
        TableState state = TableState.load(service, table, Collections.singleton(range(min(), min())), 0);

        TableState.Ref ref = state.nextSegmentToValidate();
        ValidationLifecycle lifecycle = ValidationLifecycle.createAndStart(ref,
                                                                           NodeSyncTracing.SegmentTracing.NO_TRACING);

        // Create a validator that would basically run indefinitely if not interrupted.
        Validator validator = new TestValidator(lifecycle, Integer.MAX_VALUE, 100);

        ValidationExecutor executor = executor(service, 1000, validator);
        executor.start();

        // Wait just a little bit to make sure the validation is running.
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        cancelFct.accept(validator);

        assertTrue(validator.completionFuture().isCancelled());

        // further, we should have initially locked the segment and force unlocked it, but not have recorded anything
        assertEquals(1, statusProxy.lockCalls);
        assertEquals(1, statusProxy.forceUnlockCalls);
        assertEquals(0, statusProxy.recordValidationCalls);
    }
}
