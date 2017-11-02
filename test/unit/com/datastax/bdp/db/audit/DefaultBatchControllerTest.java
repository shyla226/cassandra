/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.db.audit;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.utils.MutableTimeSource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultBatchControllerTest
{
    private MutableTimeSource ts;
    private final int flushPeriod = 100;
    private final int batchSize = 2;
    private CassandraAuditWriter.DefaultBatchController controller;

    @Before
    public void prepare() throws Exception
    {
        ts = new MutableTimeSource(0);
        controller = new CassandraAuditWriter.DefaultBatchController(flushPeriod, batchSize, ts);
    }

    @Test
    public void testDefaultBatchControllerMarksFlushPeriodExceeded() throws InterruptedException
    {
        assertFalse(controller.flushPeriodExceeded());
        ts.forward(flushPeriod - 1, TimeUnit.MILLISECONDS);
        assertFalse(controller.flushPeriodExceeded());
        ts.forward(flushPeriod / 2, TimeUnit.MILLISECONDS);
        assertTrue(controller.flushPeriodExceeded());
    }

    @Test
    public void testResetDefaultBatchController() throws InterruptedException
    {
        ts.forward(2 * flushPeriod, TimeUnit.MILLISECONDS);
        assertTrue(controller.flushPeriodExceeded());
        controller.reset();
        assertFalse(controller.flushPeriodExceeded());
    }

    @Test
    public void testGetPollPeriodFromDefaultBatchController() throws InterruptedException
    {
        // while we've haven't yet checked if the max flush period is exceeded, the
        // polling time defaults to period value;
        assertEquals(flushPeriod, controller.getNextPollPeriod());
        ts.forward(flushPeriod / 2, TimeUnit.MILLISECONDS);
        assertFalse(controller.flushPeriodExceeded());

        // now we've got a checkpoint, the poll period should be adjusted
        // so that it's between 0 and the remaining time in the period
        long pollPeriod = controller.getNextPollPeriod();
        assertTrue((pollPeriod > 0 && pollPeriod <= (flushPeriod / 2)));

        // finally, when we reset the timer, the poll period should revert
        // to flush period. Sleep for a random time first just to get some
        // variation in the timings
        ts.forward(50, TimeUnit.MILLISECONDS);
        controller.reset();
        assertEquals(controller.getNextPollPeriod(), flushPeriod);
    }
}
