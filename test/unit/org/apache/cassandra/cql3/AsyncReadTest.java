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

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
public class AsyncReadTest extends CQLTester
{
    final int BASE_COUNT = 1400;
    final int REPS = 50;

    Random rand;

    @Before
    public void setUp()
    {
        rand = new Random(1);
    }

    @Test
    public void testWideIndexingForward() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, d text, PRIMARY KEY (k, c))");
        int COUNT = rand.nextInt(BASE_COUNT / 10) + BASE_COUNT;

        for (int i = 0; i < COUNT; i++)
            execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", 1, i, i, generateString(10 << (i % 12)));
        flush();

        interceptCache();
        for (int rep = 0; rep < REPS; ++rep)
        {
            int i = rand.nextInt(COUNT);
            int j = i + rand.nextInt(BASE_COUNT / 10);
            if (j > COUNT)
                j = COUNT;
            Object[][] rows = getRows(execute("SELECT v FROM %s WHERE k = 1 and c >= ? and c < ?", i, j));
            assertEquals(j - i, rows.length);
        }
    }

    @Test
    public void testWideIndexingReversed() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, d text, PRIMARY KEY (k, c))");
        int COUNT = rand.nextInt(BASE_COUNT / 10) + BASE_COUNT;

        for (int i = 0; i < COUNT; i++)
            execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", 1, i, i, generateString(10 << (i % 12)));
        flush();

        interceptCache();
        for (int rep = 0; rep < REPS; ++rep)
        {
            int i = rand.nextInt(COUNT);
            int j = i + rand.nextInt(BASE_COUNT / 10);
            if (j > COUNT)
                j = COUNT;
            Object[][] rows = getRows(execute("SELECT v FROM %s WHERE k = 1 and c >= ? and c < ? ORDER BY c DESC", i, j));
            assertEquals("Lookup between " + i + " and " + j + " count " + COUNT, j - i, rows.length);
        }
    }

    @Test
    public void testWideIndexForwardIn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, d text, PRIMARY KEY (k, c, v))");
        int COUNT = rand.nextInt(BASE_COUNT / 10) + BASE_COUNT;
        int MULT = 5;

        for (int i = 0; i < COUNT; i++)
        {
            for (int j = 0; j < MULT; ++j)
                execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", i % 3, i, j, generateString(100 << j));
        }

        flush();

        interceptCache();
        for (int rep = 0; rep < REPS; ++rep)
        {
            int sz = rand.nextInt(BASE_COUNT / 50);
            int[] arr = new int[sz];
            for (int i = 0; i < sz; ++i)
            {
                arr[i] = rand.nextInt(COUNT);
            }
            arr = Arrays.stream(arr).distinct().toArray();

            String s = Arrays.stream(arr).mapToObj(Integer::toString).collect(Collectors.joining(","));
            for (int i = 0; i < 3; ++i)
            {
                int ii = i;
                Object[][] rows = getRows(execute("SELECT v FROM %s WHERE k = ? and c IN (" + s + ")", i));
                assertEquals("k = " + i + " IN " + s + " count " + COUNT, MULT * Arrays.stream(arr).filter(x -> x % 3 == ii).count(), rows.length);
            }
        }
    }

    @Test
    public void testWideIndexReversedIn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, d text, PRIMARY KEY (k, c, v))");
        int COUNT = rand.nextInt(BASE_COUNT / 10) + BASE_COUNT;
        int MULT = 5;

        for (int i = 0; i < COUNT; i++)
            for (int j = 0; j < MULT; ++j)
                execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", i % 3, i, j, generateString(100 << j));

        flush();

        interceptCache();
        for (int rep = 0; rep < REPS; ++rep)
        {
            int sz = rand.nextInt(BASE_COUNT / 50);
            int[] arr = new int[sz];
            for (int i = 0; i < sz; ++i)
            {
                arr[i] = rand.nextInt(COUNT);
            }
            arr = Arrays.stream(arr).distinct().toArray();

            String s = Arrays.stream(arr).mapToObj(Integer::toString).collect(Collectors.joining(","));
            for (int i = 0; i < 3; ++i)
            {
                int ii = i;
                Object[][] rows = getRows(execute("SELECT v FROM %s WHERE k = ? and c IN (" + s + ") ORDER BY c DESC", i));
                assertEquals("k = " + i + " IN " + s + " count " + COUNT, MULT * Arrays.stream(arr).filter(x -> x % 3 == ii).count(), rows.length);
            }
        }
    }

    @Test
    public void testForward() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, d text, PRIMARY KEY (k, c))");
        int COUNT = rand.nextInt(BASE_COUNT / 10) + BASE_COUNT;
        int STEP = 32;

        for (int i = 0; i < COUNT; i++)
            execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", i / STEP, i % STEP, i, generateString(10 << (i % 12)));
        flush();

        interceptCache();
        for (int rep = 0; rep < REPS; ++rep)
        {
            int i = rand.nextInt(COUNT);
            Object[][] rows = getRows(execute("SELECT v FROM %s WHERE k = ? and c >= ?", i / STEP, i % STEP));
            int max = STEP;
            if (i / STEP == COUNT / STEP)
                max = COUNT % STEP;
            assertEquals(max - (i % STEP), rows.length);
        }
    }

    @Test
    public void testReversed() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, d text, PRIMARY KEY (k, c))");
        int COUNT = rand.nextInt(BASE_COUNT / 10) + BASE_COUNT;
        int STEP = 32;

        for (int i = 0; i < COUNT; i++)
            execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", i / STEP, i % STEP, i, generateString(10 << (i % 12)));
        flush();

        interceptCache();
        for (int rep = 0; rep < REPS; ++rep)
        {
            int i = rand.nextInt(COUNT);
            Object[][] rows = getRows(execute("SELECT v FROM %s WHERE k = ? and c < ? ORDER BY c DESC", i / STEP, i % STEP));
            assertEquals(i % STEP, rows.length);
        }
    }

    @Test
    public void testRangeQueries() throws Throwable
    {
        interceptCache();

        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        int PARTITIONS = 20;
        int ROWS = 10;
        for (int i = 0; i < PARTITIONS; i++)
            for (int j = 0; j < ROWS; j++)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, j, i * j);

        flush();

        for (int rep = 0; rep < REPS; ++rep)
        {
            Object[][] rows = getRows(execute("SELECT * FROM %s"));
            assertEquals(PARTITIONS * ROWS, rows.length);
        }

        for (int rep = 0; rep < REPS; ++rep)
        {
            int from = rand.nextInt(PARTITIONS - 2);
            int to = 2 + from + rand.nextInt(PARTITIONS - from - 2);

            Object[][] rows = getRows(execute("SELECT k, c, v FROM %s WHERE k <= ? and k >= ? ALLOW FILTERING", to, from));
            assertEquals((to - from + 1) * ROWS, rows.length);

            rows = getRows(execute("SELECT k, c, v FROM %s WHERE k < ? and k >= ? ALLOW FILTERING", to, from));
            assertEquals((to - from) * ROWS, rows.length);

            rows = getRows(execute("SELECT k, c, v FROM %s WHERE k <= ? and k > ? ALLOW FILTERING", to, from));
            assertEquals((to - from) * ROWS, rows.length);

            rows = getRows(execute("SELECT k, c, v FROM %s WHERE k < ? and k > ? ALLOW FILTERING", to, from));
            assertEquals((to - from - 1) * ROWS, rows.length);
        }
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

    String generateString(int length)
    {
        String s = "";
        for (int i = 0; i < length; ++i)
            s += (char) ('a' + (i % 26));
        return s;
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