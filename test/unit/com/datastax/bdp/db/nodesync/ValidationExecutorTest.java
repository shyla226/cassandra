/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.NodeSyncConfig;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.collection.History;

import static com.datastax.bdp.db.nodesync.NodeSyncTestTools.*;
import static com.datastax.bdp.db.nodesync.ValidationExecutor.*;
import static org.junit.Assert.*;

public class ValidationExecutorTest extends CQLTester
{
    private static final boolean debugLog = false; // Set to true to have details logs; make debugging (and
                                                   // understanding) the tests easier.

    // Those tests are mostly about testing the controller behavior, and we don't want them to run for too long, so we
    // use a very low interval. That's not realistic, but should be good enough to test basic behavior.
    // Note that with the config below (testConfig()), this means we can "max out" the executor in under 2 seconds
    // (9 steps (3 steps to max out to 3 threads and 6 steps to max out to 8 inflight validations) at 200ms per step).
    private static final long CONTROLLER_INTERVAL_MS = 200;
    private static final long CONTROLLER_WAIT_TIME_SEC = 2;

    private final NodeSyncTestTools.DevNullTableProxy statusProxy = new NodeSyncTestTools.DevNullTableProxy(debugLog);

    private volatile ValidationExecutor executor;

    @After
    public void cleanUp()
    {
        if (executor != null)
            executor.shutdown(true);
        executor = null;
    }

    private static NodeSyncConfig testConfig()
    {
        return testConfig(4, 8);
    }

    private static NodeSyncConfig testConfig(int maxThreads, int maxInflightValidations)
    {
        NodeSyncConfig config = new NodeSyncConfig();
        config.setMax_threads(maxThreads);
        config.setMax_inflight_validations(maxInflightValidations);
        return config;
    }

    private ValidationExecutor prepare(int validations,
                                       int pagesByValidation,
                                       int pageQueryDelayMs,
                                       int rowsPerPage,
                                       int rowSize)
    {
        return prepare(new NodeSyncService(testConfig(), statusProxy), validations, pagesByValidation,
                       pageQueryDelayMs, rowsPerPage, rowSize, -1);
    }

    private ValidationExecutor prepare(NodeSyncService service,
                                       int validations,
                                       int pagesByValidation,
                                       int pageQueryDelayMs,
                                       int rowsPerPage,
                                       int rowSize,
                                       int indexOfValidatorError)
    {
        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '3' }");
        String tbl = createTable(ks, "CREATE TABLE %s (k int PRIMARY KEY) WITH nodesync = { 'enabled' : 'true' }");
        TableMetadata table =  tableMetadata(ks, tbl);
        TableState state = TableState.load(service, table, Collections.singleton(range(min(), min())), 0);

        AtomicInteger suppliedValidations = new AtomicInteger(0);
        Supplier<Validator> supplier = () -> {
            int index = suppliedValidations.getAndIncrement();
            if (index >= validations)
                return null;

            if (index == indexOfValidatorError)
                throw new RuntimeException("Artifical error triggered for testing");

            TableState.Ref ref = state.nextSegmentToValidate();
            ValidationLifecycle lifecycle = ValidationLifecycle.createAndStart(ref,
                                                                               NodeSyncTracing.SegmentTracing.NO_TRACING);

            return new TestValidator(lifecycle, pagesByValidation, pageQueryDelayMs, rowsPerPage, rowSize);
        };
        executor = executor(service, CONTROLLER_INTERVAL_MS, supplier);
        executor.start();
        return executor;
    }

    private void waitForController(ValidationExecutor executor)
    {
        Uninterruptibles.sleepUninterruptibly(CONTROLLER_WAIT_TIME_SEC, TimeUnit.SECONDS);

        History<Action> history = executor.controllerHistory();
        assertTrue(history.size() >= 9);
    }

    // Assert all elements of the provide history match the provided predicate.
    private void assertHistory(ValidationExecutor executor, Predicate<Action> predicate)
    {
        History<Action> history = executor.controllerHistory();
        assertTrue("History is " + history, history.stream().allMatch(predicate));
    }

    private void assertMaxedOutWarn(ValidationExecutor executor, boolean expectWasLogged)
    {
        boolean wasLogged = executor.lastMaxedOutWarn() > 0;
        assertEquals(expectWasLogged, wasLogged);
    }

    @Test
    public void testNoWork()
    {
        ValidationExecutor executor = prepare(0, 0, 0, 0, 0);
        waitForController(executor);

        // We didn't do anything, so we should have considered MINED_OUT.
        assertHistory(executor, a -> a == Action.MINED_OUT);
        assertMaxedOutWarn(executor, false);
    }

    @Test
    public void testRateUnachievable()
    {
        // Continuously submit validations, but have such a per-page delay that even at full capacity the rate (of 1MB/s)
        // is not achievable.
        // In practice, we use a page delay of 0.5 second but with each page being only 10kb, so even with 8 threads,
        // we will do only 8 * 2 pages per second, so 160kb/sec.
        ValidationExecutor executor = prepare(Integer.MAX_VALUE,
                                              10,
                                              500,
                                              10,
                                              1024);
        waitForController(executor);

        // We should have tried to increase all the time, eventually maxing out.
        assertHistory(executor, a -> a.isIncrease() || a == Action.MAXED_OUT);

        // Also make sure we've logged our warning that the rate cannot be achieved.
        assertMaxedOutWarn(executor, true);
    }

    @Test
    public void testUnexpectedErrorRecovery()
    {
        NodeSyncService service = new NodeSyncService(testConfig(1, 2), statusProxy);
        ValidationExecutor executor = prepare(service,
                                              Integer.MAX_VALUE,
                                              Integer.MAX_VALUE,
                                              Integer.MAX_VALUE,
                                              Integer.MAX_VALUE,
                                              Integer.MAX_VALUE,
                                              1);

        Util.spinAssertEquals(2, () -> executor.inFlightValidators().size(), 30);
    }
}
