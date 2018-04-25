/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.cql3.continuous.paging;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.ServerError;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.BytemanUtil;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
//import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

/**
 * Tests that exercise error conditions in continuous paging by
 * injecting byteman failures.
 */
@RunWith(BMUnitRunner.class)
//@BMUnitConfig(bmunitVerbose=true, debug=true, verbose=true, dumpGeneratedClasses=true)
public class ContinuousPagingErrorsTest extends CQLTester
{
    static
    {
        BytemanUtil.randomizeBytemanPort();
    }

    @BeforeClass
    public static void startup()
    {
        ContinuousPagingTestUtils.startup();
    }

    /**
     * Test that we receive no data if the server fails to write the first page.
     */
    @Test
    @BMRule(name="throw_exception_on_first_page",
    targetClass = "ContinuousPageWriter",
    targetMethod = "sendPage",
    targetLocation = "AT INVOKE java.util.concurrent.ArrayBlockingQueue.add",
    action = "throw new java.lang.IllegalStateException(\"Queue full\");")
    public void serverFailureOnFirstPageTest() throws Throwable
    {
        try(ContinuousPagingTestUtils.TestHelper helper = new ContinuousPagingTestUtils.TestBuilder(this).numPartitions(100)
                                                                                                         .numClusterings(100)
                                                                                                         .partitionSize(2048)
                                                                                                         .failAfter(0)
                                                                                                         .exception(ServerError.class)
                                                                                                         .build())
        {
            helper.testContinuousPaging(1, 500, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    /**
     * Test that we receive at most 1 page if the server fails to write the second page.
     */
    @Test
    @BMRules(rules={@BMRule(name="create_counter_feeder",
                    targetClass = "ContinuousPageWriter",
                    targetMethod = "<init>",
                    action = "createCounter($0)"),
                    @BMRule(name="throw_exception_on_second_page",
                    targetClass = "ContinuousPageWriter",
                    targetMethod = "sendPage",
                    targetLocation = "AT INVOKE java.util.concurrent.ArrayBlockingQueue.add",
                    condition = "incrementCounter($0) >= 2",
                    action = "throw new java.lang.IllegalStateException(\"Queue full\");")})
    public void serverFailureOnSecondPageTest() throws Throwable
    {
        try(ContinuousPagingTestUtils.TestHelper helper = new ContinuousPagingTestUtils.TestBuilder(this).numPartitions(100)
                                                                                                         .numClusterings(100)
                                                                                                         .partitionSize(2048)
                                                                                                         .failAfter(1)
                                                                                                         .exception(ServerError.class)
                                                                                                         .build())
        {
            helper.testContinuousPaging(1, 500, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }

    /**
     * Test what happens if we cannot empty the pages queue.
     */
    @Test
    @BMRules(rules={@BMRule(name="create_count_down_writer",
                    targetClass = "ContinuousPageWriter$Writer",
                    targetMethod = "<init>",
                    action = "createCounter($0)"),
                    @BMRule(name="stop_sending_messages",
                    targetClass = "ContinuousPageWriter$Writer",
                    targetMethod = "processPendingPages",
                    targetLocation = "AT INVOKE java.util.concurrent.ArrayBlockingQueue.poll",
                    condition = "!$0.queue.isEmpty() && incrementCounter($0) >= 3",
                    action = "throw new RuntimeException(\"Failed to take page from queue\");")})
    public void serverFailureEmptyingQueueTest() throws Throwable
    {
        try(ContinuousPagingTestUtils.TestHelper helper = new ContinuousPagingTestUtils.TestBuilder(this).numPartitions(100)
                                                                                                         .numClusterings(100)
                                                                                                         .partitionSize(2048)
                                                                                                         .failAfter(2)
                                                                                                         .exception(OperationTimedOutException.class)
                                                                                                         .build())
        {
            helper.testContinuousPaging(1, 500, ContinuousPagingOptions.PageUnit.ROWS);
        }
    }
}
