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

package org.apache.cassandra.cql3.continuous.paging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ContinuousPagingOptions;
import org.apache.cassandra.config.ContinuousPagingConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.cql3.continuous.paging.ContinuousPagingTestUtils.*;
import static org.junit.Assert.fail;

/**
 * Main tests for continuous paging, each test exercises a specific feature.
 */
public class ContinuousPagingFeaturesTest extends CQLTester
{

    @BeforeClass
    public static void startup()
    {
        ContinuousPagingTestUtils.startup();
    }

    @Test
    public void testSingleClusteringRow() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(1000)
                                                     .numClusterings(1)
                                                     .partitionSize(1024)
                                                     .schemaSupplier(b -> new FixedSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testContinuousPaging(1, 11000000, ContinuousPagingOptions.PageUnit.BYTES); // should fit all rows
            helper.testContinuousPaging(1, 11000, ContinuousPagingOptions.PageUnit.BYTES); // should fit 10 rows
            helper.testContinuousPaging(1, 110000, ContinuousPagingOptions.PageUnit.BYTES); // should fit 100 rows
            helper.testContinuousPaging(1, 1100, ContinuousPagingOptions.PageUnit.BYTES); // should fit 1 row
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.BYTES); // should fit less than 1 row

            helper.testContinuousPaging(1, 2000, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 1000, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 33, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void testMultipleClusteringRows() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(10)
                                                     .numClusterings(100)
                                                     .partitionSize(10000)
                                                     .schemaSupplier(b -> new FixedSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testContinuousPaging(1, 110000000, ContinuousPagingOptions.PageUnit.BYTES); // should fit all rows
            helper.testContinuousPaging(1, 11000, ContinuousPagingOptions.PageUnit.BYTES); // should fit 30 row
            helper.testContinuousPaging(1, 10, ContinuousPagingOptions.PageUnit.BYTES); // should fit less than 1 row

            helper.testContinuousPaging(1, 10000, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 10, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 1, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void testMultipleClusteringRowsWithCompression() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(10)
                                                     .numClusterings(100)
                                                     .partitionSize(10000)
                                                     .schemaSupplier(b -> new FixedSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize, true))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testContinuousPaging(1, 110000000, ContinuousPagingOptions.PageUnit.BYTES); // should fit all rows
            helper.testContinuousPaging(1, 11000, ContinuousPagingOptions.PageUnit.BYTES); // should fit 30 row
            helper.testContinuousPaging(1, 10, ContinuousPagingOptions.PageUnit.BYTES); // should fit less than 1 row

            helper.testContinuousPaging(1, 10000, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 10, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 1, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void testMultipleClusteringRowsWithVariableSize() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(10)
                                                     .numClusterings(100)
                                                     .partitionSize(10000)
                                                     .schemaSupplier(b -> new VariableSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testContinuousPaging(1, 110000000, ContinuousPagingOptions.PageUnit.BYTES); // should fit all rows
            helper.testContinuousPaging(1, 11000, ContinuousPagingOptions.PageUnit.BYTES); // should fit 30 row
            helper.testContinuousPaging(1, 10, ContinuousPagingOptions.PageUnit.BYTES); // should fit less than 1 row

            helper.testContinuousPaging(1, 10000, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 10, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 1, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    /**
     * Test page sizes that are close to the maximum page size allowed.
     */
    @Test
    public void testLargePages() throws Throwable
    {
        int old = DatabaseDescriptor.setNativeTransportMaxFrameSizeInMb(4); // the max page size is half this value

        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(100000)
                                                     .schemaSupplier(b -> new VariableSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testContinuousPaging(1, 4 * 1024 * 1024, ContinuousPagingOptions.PageUnit.BYTES); // bigger than max max
            helper.testContinuousPaging(1, 2 * 1024 * 1024, ContinuousPagingOptions.PageUnit.BYTES); // max
            helper.testContinuousPaging(1, (2 * 1024 * 1024) - 5000, ContinuousPagingOptions.PageUnit.BYTES); // just smaller than max
        }
        finally
        {
            DatabaseDescriptor.setNativeTransportMaxFrameSizeInMb(old);
        }
    }

    /**
     * Test relatively small pages
     */
    @Test
    public void testSmallPages() throws Throwable
    {
        int old = DatabaseDescriptor.setNativeTransportMaxFrameSizeInMb(4); // the max page size is half this value

        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(1000)
                                                     .schemaSupplier(b -> new VariableSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testContinuousPaging(1, 1000, ContinuousPagingOptions.PageUnit.ROWS); // 10% of the ROWS
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS); // 1% of the ROWS
            helper.testContinuousPaging(1, 10, ContinuousPagingOptions.PageUnit.ROWS); // 0.1% of the ROWS

            helper.testContinuousPaging(1, 1000, ContinuousPagingOptions.PageUnit.BYTES);
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.BYTES);
            helper.testContinuousPaging(1, 10, ContinuousPagingOptions.PageUnit.BYTES);
        }
        finally
        {
            DatabaseDescriptor.setNativeTransportMaxFrameSizeInMb(old);
        }
    }

    /**
     * Test page sizes that are close to the maximum page size allowed, with rows that are bigger than the max page size.
     */
    @Test
    public void testLargePagesEvenLargerRows() throws Throwable
    {
        int old = DatabaseDescriptor.setNativeTransportMaxFrameSizeInMb(3); // the max page size is half this value

        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(1)
                                                     .partitionSize(2 * 1024 * 1024) //max mutation size is 2.5
                                                     .schemaSupplier(b -> new VariableSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize))
                                                     .checkRows(true)
                                                     .checkNumberOfRowsInPage(false) // OK to receive fewer rows in page if page size is bigger than max
                                                     .build())
        {
            helper.testContinuousPaging(1, 3 * 1024 * 1024, ContinuousPagingOptions.PageUnit.BYTES);
            helper.testContinuousPaging(1, 2 * 1024 * 1024, ContinuousPagingOptions.PageUnit.BYTES);
            helper.testContinuousPaging(1, 1 * 1024 * 1024, ContinuousPagingOptions.PageUnit.BYTES);

            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 10, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 1, ContinuousPagingOptions.PageUnit.ROWS);
        }
        finally
        {
            DatabaseDescriptor.setNativeTransportMaxFrameSizeInMb(old);
        }
    }

    /**
     * Make 10% of the rows larger than other rows.
     */
    @Test
    public void testAbnormallyLargeRows() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(10)
                                                     .numClusterings(100)
                                                     .partitionSize(10000)
                                                     .schemaSupplier(b -> new AbonormallyLargeRowsSchema(b.numPartitions, b.numClusterings, b.partitionSize, 0.1))
                                                     .checkRows(true)
                                                     .build())
        {

            helper.testContinuousPaging(1, 10000, ContinuousPagingOptions.PageUnit.BYTES);
            helper.testContinuousPaging(1, 20000, ContinuousPagingOptions.PageUnit.BYTES);
            helper.testContinuousPaging(1, 50000, ContinuousPagingOptions.PageUnit.BYTES);
            helper.testContinuousPaging(1, 100000, ContinuousPagingOptions.PageUnit.BYTES);

            helper.testContinuousPaging(1, 50, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void testSelectSinglePartition() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(1000)
                                                     .schemaSupplier(b -> new SelectInitialPartitionsSchema(b.numPartitions, b.numClusterings, b.partitionSize, 1))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testContinuousPaging(1, 10000, ContinuousPagingOptions.PageUnit.BYTES);
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void testSelectOnlyFirstTenPartitions() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(1000)
                                                     .schemaSupplier(b -> new SelectInitialPartitionsSchema(b.numPartitions, b.numClusterings, b.partitionSize, 10))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testContinuousPaging(1, 10000, ContinuousPagingOptions.PageUnit.BYTES);
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    /**
     * Test interrupting optimized paging after N pages, and then resuming again.
     */
    @Test
    public void testResume() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(1000)
                                                     .schemaSupplier(b -> new FixedSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize))
                                                     .checkRows(true)
                                                     .build())
        {
            // interrupt at page boundaries
            helper.testResumeWithContinuousPaging(500, ContinuousPagingOptions.PageUnit.ROWS, new int [] { 2500});
            helper.testResumeWithContinuousPaging(100, ContinuousPagingOptions.PageUnit.ROWS, new int [] { 1000, 2500, 5000});

            // interrupt within a page
            helper.testResumeWithContinuousPaging(1000, ContinuousPagingOptions.PageUnit.ROWS, new int [] { 100, 500, 2500, 3500, 3750});

            // use a page with bytes page unit
            helper.testResumeWithContinuousPaging(5000, ContinuousPagingOptions.PageUnit.BYTES, new int[] { 100, 500, 5000});
        }
    }

    /**
     * Simulates a slow client with only 1 NIO thread and a long pause on the main thread.
     * Use a small page size compared to the number of rows, so that eventually messages should start to queue up
     * and the NIO thread should become blocked, see StreamingRequestHandlerCallback.onData() in the driver.
     */
    @Test
    public void testSlowClient() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(64)
                                                     .numClientThreads(1)
                                                     .clientPauseMillis(10)
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testContinuousPaging(1, 102400, ContinuousPagingOptions.PageUnit.BYTES); // 1KB * 100 rows
        }
    }

    /** Cancel after 3 pages, we should end up with an incomplete query (paging state != null) */
    @Test
    public void testCancel() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(64)
                                                     .cancelAfter(3)
                                                     .build())
        {
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    /** Cancel just before the last page, this should trigger a failed cancellation server side. */
    @Test
    public void testCancelLate() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(64)
                                                     .cancelAfter(99)
                                                     .build())
        {
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    /** Throttle at 2 pages per second, make sure pages are not received too quickly. */
    @Test
    public void testThrottle() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(64)
                                                     .maxPagesPerSecond(2)
                                                     .checkRows(true)
                                                     .build())
        {
            long durationMillis = helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS);
            assertTrue(String.format("Finished too quickly (%d millis) for this throttling level", durationMillis),
                       durationMillis >= 45000); // 100-10=90 pages at 2 / second -> 45 seconds at least, we exclude
                                                 // the first 10 pages because of the RateLimiter smoothing factor
        }
    }

    /** Test that if we specify a maximum number of pages then we receive at most this number. */
    @Test
    public void testMaxPages() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(64)
                                                     .maxPages(10)
                                                     .build())
        {
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    /** Test that we can receive a single page only. */
    @Test
    public void testSinglePage() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(64)
                                                     .maxPages(1)
                                                     .build())
        {
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    /** Test that if we specify a maximum number of rows then we receive at most this number. */
    @Test
    public void testMaxRows() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(64)
                                                     .maxRows(300)
                                                     .build())
        {
            helper.testContinuousPaging(1, 500, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 100, ContinuousPagingOptions.PageUnit.ROWS);
            helper.testContinuousPaging(1, 43, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    /**
     * Test that we can have max_concurrent_sessions running in parallel.
     * */
    @Test
    public void testConcurrentSessions() throws Throwable
    {
        ContinuousPagingConfig config = DatabaseDescriptor.getContinuousPaging();
        testConcurrentSessions(config.max_concurrent_sessions, null, 0);
    }

    /**
     * Test that we can have multiple sessions, running with different throttling rates.
     * */
    @Test
    public void testConcurrentSessionsWithDifferentThrottling() throws Throwable
    {
        ContinuousPagingConfig config = DatabaseDescriptor.getContinuousPaging();
        int [] maxPagesPerSecond = new int [] {2, 4, 8, 16};
        assertTrue("This test should use at most max_concurrent_sessions",
                   maxPagesPerSecond.length <= config.max_concurrent_sessions);
        testConcurrentSessions(maxPagesPerSecond.length, maxPagesPerSecond, 0);
    }

    /**
     * Test that we cannot have more than max_concurrent_sessions running in parallel.
     * */
    @Test
    public void testOneMoreSessionThanMaxConcurrentSessions() throws Throwable
    {
        ContinuousPagingConfig config = DatabaseDescriptor.getContinuousPaging();
        testTooManyConcurrentSessions(config.max_concurrent_sessions + 1, 1);

    }

    /**
     * Test that we cannot have more than max_concurrent_sessions running in parallel.
     * */
    @Test
    public void testTwoMoreSessionsThanMaxConcurrentSessions() throws Throwable
    {
        ContinuousPagingConfig config = DatabaseDescriptor.getContinuousPaging();
        testTooManyConcurrentSessions(config.max_concurrent_sessions + 2, 2);
    }

    private void testTooManyConcurrentSessions(int numSessions, int numExpectedErrors) throws Throwable
    {
        int [] maxPagesPerSecond = new int [numSessions];
        // we need slow sessions to ensure they don't finish before a new one is started
        Arrays.fill(maxPagesPerSecond, 2);
        testConcurrentSessions(numSessions, maxPagesPerSecond, numExpectedErrors);
    }

    private void testConcurrentSessions(int numThreads, int[] maxPagesPerSecond, int numExpectedErrors) throws Throwable
    {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        CountDownLatch countDownLatch = new CountDownLatch(numThreads);
        List<Throwable> errors = new ArrayList<>();

        TestSchema schema =  new SchemaBuilder(this).numPartitions(100)
                                                    .numClusterings(100)
                                                    .partitionSize(64)
                                                    .build();

        for (int i = 0; i < numThreads; i++)
        {
            final int maxPagesPerSecondIt = maxPagesPerSecond == null ? 0 : maxPagesPerSecond[i];

            executor.submit(() ->
                {
                    try (TestHelper helper = new ContinuousPagingTestUtils.TestBuilder(this, schema)
                                             .maxPagesPerSecond(maxPagesPerSecondIt)
                                             .checkRows(true)
                                             .build())
                    {
                        helper.testContinuousPaging(1, 1000, ContinuousPagingOptions.PageUnit.ROWS);
                    }
                    catch (Throwable t)
                    {
                        errors.add(t);
                    }
                    finally
                    {
                        countDownLatch.countDown();
                    }
                });
        }

        countDownLatch.await(1, TimeUnit.MINUTES);

        if (numExpectedErrors != errors.size())
        {
            for (Throwable e : errors)
                e.printStackTrace();

            fail(String.format("Unexpected number of errors %d, was expecting %d", errors.size(), numExpectedErrors));
        }
    }

    @Test
    public void testGroupBy_1() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).schemaSupplier(b ->
                                                                     new GroupBySchema("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a",
                                                                                       new Object[][]
                                                                                       {
                                                                                         new Object[]{ 1, 2, 6, 4L ,24 },
                                                                                         new Object[]{ 2, 2, 6, 2L, 12 },
                                                                                         new Object[]{ 4, 8, 24, 1L, 24 }
                                                                                       }))
                                                     .checkRows(true)
                                                     .build())
        {
            for (int pageSize = 2; pageSize <= 10; pageSize++)
                helper.testContinuousPaging(1, pageSize, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void testGroupBy_2() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).schemaSupplier(b ->
                                                                     new GroupBySchema("SELECT a, b, c, d FROM %s GROUP BY a, b, c",
                                                                                       new Object[][]
                                                                                       {
                                                                                       new Object[]{ 1, 2, 1, 3 },
                                                                                       new Object[]{ 1, 2, 2, 6 },
                                                                                       new Object[]{ 1, 4, 2, 6 },
                                                                                       new Object[]{ 2, 2, 3, 3 },
                                                                                       new Object[]{ 2, 4, 3, 6 },
                                                                                       new Object[]{ 4, 8, 2, 12 }
                                                                                       }))
                                                     .checkRows(true)
                                                     .build())
        {
            for (int pageSize = 2; pageSize <= 10; pageSize++)
                helper.testContinuousPaging(1, pageSize, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }


    @Test
    public void testGroupBy_3() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).schemaSupplier(b ->
                                                                     new GroupBySchema("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b LIMIT 2",
                                                                                       new Object[][]
                                                                                       {
                                                                                       new Object[]{ 1, 2, 6, 2L, 12  },
                                                                                       new Object[]{ 1, 4, 12, 2L, 24 },
                                                                                       }))
                                                     .checkRows(true)
                                                     .build())
        {
            for (int pageSize = 2; pageSize <= 10; pageSize++)
                helper.testContinuousPaging(1, pageSize, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void testGroupBy_4() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).schemaSupplier(b ->
                                                                     new GroupBySchema("SELECT a, b, c, d FROM %s GROUP BY a, b, c LIMIT 3",
                                                                                       new Object[][]
                                                                                       {
                                                                                       new Object[]{ 1, 2, 1, 3 },
                                                                                       new Object[]{ 1, 2, 2, 6 },
                                                                                       new Object[]{ 1, 4, 2, 6 },
                                                                                       }))
                                                     .checkRows(true)
                                                     .build())
        {
            for (int pageSize = 2; pageSize <= 10; pageSize++)
                helper.testContinuousPaging(1, pageSize, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }


    @Test
    public void testGroupBy_5() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).schemaSupplier(b ->
                                                                     new GroupBySchema("SELECT a, b, c, d FROM %s GROUP BY a, b LIMIT 3",
                                                                                       new Object[][]
                                                                                       {
                                                                                       new Object[]{ 1, 2, 1, 3 },
                                                                                       new Object[]{ 1, 4, 2, 6 },
                                                                                       new Object[]{ 2, 2, 3, 3 },
                                                                                       }))
                                                     .checkRows(true)
                                                     .build())
        {
            for (int pageSize = 2; pageSize <= 10; pageSize++)
                helper.testContinuousPaging(1, pageSize, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }
}
