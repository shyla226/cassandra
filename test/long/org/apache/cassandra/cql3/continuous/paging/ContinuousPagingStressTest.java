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
        try(ContinuousPagingTestUtils.TestHelper helper = new ContinuousPagingTestUtils.TestBuilder(this).numPartitions(1000)
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
        try(ContinuousPagingTestUtils.TestHelper helper = new ContinuousPagingTestUtils.TestBuilder(this).numPartitions(1000)
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
        try(ContinuousPagingTestUtils.TestHelper helper = new ContinuousPagingTestUtils.TestBuilder(this).numPartitions(500)
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
