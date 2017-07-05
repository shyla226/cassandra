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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.io.util.AsynchronousChannelProxy;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;


/**
 * Tests randomly causing re-read with NotInCacheException.
 */
public class AsyncReadTest extends CQLTester
{
    final int BASE_COUNT = 3000;
    final int REPS = 100;

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
            Assert.assertEquals(j - i, rows.length);
        }
    }

    @Test
    public void testWideIndexingReversed() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, d text, PRIMARY KEY (k, c))");
        int COUNT = rand.nextInt(BASE_COUNT / 10) + BASE_COUNT;

        for (int i = 0; i < COUNT; i++)
            execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", 1, i, i, generateString(10 << (i % 12)));

        Assert.assertEquals(243, getRows(execute("SELECT v FROM %s WHERE k = 1 and c >= ? and c < ? ORDER BY c DESC", 395, 638)).length);
        flush();

        // known bad example
        Assert.assertEquals(243, getRows(execute("SELECT v FROM %s WHERE k = 1 and c >= ? and c < ? ORDER BY c DESC", 395, 638)).length);

        interceptCache();
        for (int rep = 0; rep < REPS; ++rep)
        {
            int i = rand.nextInt(COUNT);
            int j = i + rand.nextInt(BASE_COUNT / 10);
            if (j > COUNT)
                j = COUNT;
            Object[][] rows = getRows(execute("SELECT v FROM %s WHERE k = 1 and c >= ? and c < ? ORDER BY c DESC", i, j));
            Assert.assertEquals("Lookup between " + i + " and " + j + " count " + COUNT, j - i, rows.length);
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
                Assert.assertEquals("k = " + i + " IN " + s + " count " + COUNT, MULT * Arrays.stream(arr).filter(x -> x % 3 == ii).count(), rows.length);
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
                Assert.assertEquals("k = " + i + " IN " + s + " count " + COUNT, MULT * Arrays.stream(arr).filter(x -> x % 3 == ii).count(), rows.length);
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
            execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", i/STEP, i%STEP, i, generateString(10 << (i % 12)));
        flush();

        interceptCache();
        for (int rep = 0; rep < REPS; ++rep)
        {
            int i = rand.nextInt(COUNT);
            Object[][] rows = getRows(execute("SELECT v FROM %s WHERE k = ? and c >= ?", i/STEP, i%STEP));
            int max = STEP;
            if (i / STEP == COUNT / STEP)
                max = COUNT % STEP;
            Assert.assertEquals(max - (i % STEP), rows.length);
        }
    }

    @Test
    public void testReversed() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, d text, PRIMARY KEY (k, c))");
        int COUNT = rand.nextInt(BASE_COUNT / 10) + BASE_COUNT;
        int STEP = 32;

        for (int i = 0; i < COUNT; i++)
            execute("INSERT INTO %s (k, c, v, d) VALUES (?, ?, ?, ?)", i/STEP, i%STEP, i, generateString(10 << (i % 12)));
        flush();

        interceptCache();
        for (int rep = 0; rep < REPS; ++rep)
        {
            int i = rand.nextInt(COUNT);
            Object[][] rows = getRows(execute("SELECT v FROM %s WHERE k = ? and c < ? ORDER BY c DESC", i/STEP, i%STEP));
            Assert.assertEquals(i % STEP, rows.length);
        }
    }

    String generateString(int length)
    {
        String s = "";
        for (int i = 0; i < length; ++i)
            s += (char) ('a' + (i % 26));
        return s;
    }

    Random rand = new Random();

    public void interceptCache()
    {
        ChunkCache.instance.intercept(rf -> new TestRebuffererFactory(rf));
    }

    @After
    public void clearIntercept()
    {
        ChunkCache.instance.enable(true);
    }

    class TestRebuffererFactory implements RebuffererFactory
    {
        final RebuffererFactory wrapped;

        TestRebuffererFactory(RebuffererFactory wrapped)
        {
            this.wrapped = wrapped;
        }

        public void close()
        {
            wrapped.close();
        }

        public AsynchronousChannelProxy channel()
        {
            return wrapped.channel();
        }

        public long fileLength()
        {
            return wrapped.fileLength();
        }

        public double getCrcCheckChance()
        {
            return wrapped.getCrcCheckChance();
        }

        public Rebufferer instantiateRebufferer()
        {
            return new TestRebufferer(wrapped.instantiateRebufferer());
        }
    }

    class TestRebufferer implements Rebufferer
    {
        final Rebufferer wrapped;

        TestRebufferer(Rebufferer wrapped)
        {
            this.wrapped = wrapped;
        }

        public void close()
        {
            wrapped.close();
        }

        public AsynchronousChannelProxy channel()
        {
            return wrapped.channel();
        }

        public long fileLength()
        {
            return wrapped.fileLength();
        }

        public double getCrcCheckChance()
        {
            return wrapped.getCrcCheckChance();
        }

        public BufferHolder rebuffer(long position)
        {
            return wrapped.rebuffer(position);
        }

        public void closeReader()
        {
            wrapped.closeReader();
        }

        public BufferHolder rebuffer(long position, ReaderConstraint constraint)
        {
            if (constraint == ReaderConstraint.IN_CACHE_ONLY && rand.nextDouble() > 0.75)
            {
                CompletableFuture<ChunkCache.Buffer> buf = new CompletableFuture<ChunkCache.Buffer>();
                buf.complete(null); // mark ready, so that reload starts immediately
                throw new NotInCacheException(buf);
            }
            return wrapped.rebuffer(position, constraint);
        }
    }
}
