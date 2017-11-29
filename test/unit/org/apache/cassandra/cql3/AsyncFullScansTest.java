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
package org.apache.cassandra.cql3;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cache.ChunkCacheMocks;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


/**
 * Tests randomly causing re-read with NotInCacheException.
 */
public class AsyncFullScansTest extends CQLTester
{
    final int REPS = 50;

    Random rand;

    @Before
    public void setUp()
    {
        rand = new Random();
    }

    @Test
    public void testParallelFullScans() throws Throwable
    {
        testParallelFullScans(false);
    }

    @Test
    public void testParallelFullScansWithCompactions() throws Throwable
    {
        testParallelFullScans(true);
    }

    private void testParallelFullScans(boolean compactWhilstReading) throws Throwable
    {
        DatabaseDescriptor.setSSTablePreempiveOpenIntervalInMB(2);
        interceptCache();

        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        int PARTITIONS = 100;
        int ROWS = 10;
        String junk = RandomStringUtils.random(512); // just some junk to make the sstables larger

        // do not compact whilst flushing, we want to compact when reading or not at all
        disableCompaction();

        for (int j = 0; j < ROWS; j++)
        {
            for (int i = 0; i < PARTITIONS; i++)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, j, junk);

            flush(); // each sstable should have all partitions, with one row each
        }

        final CountDownLatch latch = new CountDownLatch(REPS);
        final AtomicReference<Throwable> error = new AtomicReference<>(null);

        for (int i = 0; i < REPS; i++)
        {
            TPC.ioScheduler().scheduleDirect(() -> {
                 try
                 {
                     for (int rep = 0; rep < REPS; ++rep)
                     {
                         Object[][] rows = getRows(execute("SELECT * FROM %s"));
                         assertEquals(PARTITIONS * ROWS, rows.length);
                     }
                 }
                 catch (Throwable err)
                 {
                     error.compareAndSet(null, err);
                 }
                 finally
                 {
                     latch.countDown();
                 }
             });
        }

        // compact whilst reading if required
        if (compactWhilstReading)
        {
            Thread.sleep(5); // give a chance to the queries to start
            compact();
        }

        latch.await(1, TimeUnit.MINUTES);
        assertNull(error.get());
    }

    public void interceptCache()
    {
        ChunkCacheMocks.interceptCache(rand);
    }

    @After
    public void clearIntercept()
    {
        ChunkCacheMocks.clearIntercept();
    }
}