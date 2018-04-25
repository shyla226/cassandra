/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.cql3.continuous.paging;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ContinuousPagingOptions;
import org.apache.cassandra.cql3.CQLTester;

/**
 * Stress tests for continuous paging.
 */
public class ContinuousPagingStressTest extends CQLTester
{
    @BeforeClass
    public static void startup()
    {
        ContinuousPagingTestUtils.startup();
    }

    @Test
    public void selectEntireTable1KB() throws Throwable
    {
        try(ContinuousPagingTestUtils.TestHelper helper = new ContinuousPagingTestUtils.TestBuilder(this).numPartitions(100)
                                                                                                         .numClusterings(1)
                                                                                                         .partitionSize(1024)
                                                                                                         .checkRows(true)
                                                                                                         .build())
        {

            //warmup
            helper.testLegacyPaging(50, 100);
            helper.testContinuousPaging(50, 104800, ContinuousPagingOptions.PageUnit.BYTES); // row size * 100 rows

            logger.info("Total time reading 1KB table with continuous paging: {} milliseconds",
                        helper.testContinuousPaging(500, 104800, ContinuousPagingOptions.PageUnit.BYTES)); // row size * 100 rows

            logger.info("Total time reading 1KB table page by page: {} milliseconds",
                        helper.testLegacyPaging(500, 100));
        }
    }

    @Test
    public void selectEntireTable10KB() throws Throwable
    {
        try(ContinuousPagingTestUtils.TestHelper helper = new ContinuousPagingTestUtils.TestBuilder(this).numPartitions(250)
                                                                                                         .numClusterings(1)
                                                                                                         .partitionSize(10*1024)
                                                                                                         .schemaSupplier(b -> new ContinuousPagingTestUtils.FixedSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize, false))
                                                                                                         .build())
        {
            //warmup
            helper.testLegacyPaging(50, 100);
            helper.testContinuousPaging(50, 102400, ContinuousPagingOptions.PageUnit.BYTES); // 1KB * 100 rows

            logger.info("Total time reading 10KB table with continuous paging: {} milliseconds",
                        helper.testContinuousPaging(1000, 102400, ContinuousPagingOptions.PageUnit.BYTES)); // 1KB * 100 rows

            logger.info("Total time reading 10KB table page by page: {} milliseconds",
                        helper.testLegacyPaging(500, 100));
        }
    }


    @Test
    public void selectEntireTable64KB() throws Throwable
    {
        try(ContinuousPagingTestUtils.TestHelper helper = new ContinuousPagingTestUtils.TestBuilder(this).numPartitions(200)
                                                                                                         .numClusterings(10)
                                                                                                         .partitionSize(64*1024)
                                                                                                         .build())
        {
            //warmup
            helper.testLegacyPaging(10, 1000);
            helper.testContinuousPaging(10, 1000, ContinuousPagingOptions.PageUnit.ROWS);

            logger.info("Total time reading 65KB table with continuous paging: {} milliseconds",
                        helper.testContinuousPaging(50, 1000, ContinuousPagingOptions.PageUnit.ROWS));

            logger.info("Total time reading 65KB table page by page: {} milliseconds",
                        helper.testLegacyPaging(50, 1000));
        }
    }
}
