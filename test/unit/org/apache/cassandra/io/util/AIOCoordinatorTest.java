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

package org.apache.cassandra.io.util;

import org.junit.Test;

import io.netty.channel.epoll.AIOContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AIOCoordinatorTest
{
    @Test
    public void testSingleIOCore()
    {
        testAIOCoordinator(30, 12, 1);
    }

    @Test
    public void testFixedIOCoresVariableQueueDepth()
    {
        testAIOCoordinator(30, 12, 6);

        testAIOCoordinator(29, 12, 6);

        testAIOCoordinator(28, 12, 6);

        testAIOCoordinator(27, 12, 6);

        testAIOCoordinator(26, 12, 6);

        testAIOCoordinator(25, 12, 6);

        testAIOCoordinator(24, 12, 6);
    }

    @Test
    public void testVariableIOCoresFixedQueueDepth()
    {
        testAIOCoordinator(30, 1, 1);

        testAIOCoordinator(30, 2, 1);

        testAIOCoordinator(30, 3, 1);

        testAIOCoordinator(30, 3, 2);

        testAIOCoordinator(30, 4, 2);

        testAIOCoordinator(30, 4, 3);

        testAIOCoordinator(30, 4, 4);

        testAIOCoordinator(30, 5, 2);

        testAIOCoordinator(30, 5, 3);

        testAIOCoordinator(30, 5, 4);
    }

    // tests usual number of cores (8, 16, 24, 48 minus one) with a queue depth of 30, which is the optimal depth on blade-13 SSDs
    @Test
    public void testLikelyNumberOfIOCores()
    {
        testAIOCoordinator(30, 47, 7);

        testAIOCoordinator(30, 47, 8);

        testAIOCoordinator(128, 47, 32);

        testAIOCoordinator(30, 23, 7);

        testAIOCoordinator(30, 23, 8);

        testAIOCoordinator(30, 15, 7);

        testAIOCoordinator(30, 15, 8);

        testAIOCoordinator(30, 7, 7);
    }

    @Test
    public void testMoreIoCoresThanTheDepth()
    {
        testAIOCoordinator(16, 32, 32);
        testAIOCoordinator(16, 32, 17);
        testAIOCoordinator(16, 32, 16);
    }

    /**
     * Test multiple IO cores.
     *
     * @param globalDepth - the global depth
     * @param numCores - the total number of cores
     * @param numIOCores - the number of IO cores
     */
    private void testAIOCoordinator(int globalDepth, int numCores, int numIOCores)
    {
        AioCoordinator coordinator = new AioCoordinator(numCores, numIOCores, globalDepth);

        int minDepth = globalDepth / numIOCores;
        int totLocalDepths = 0;
        for (int i = 0; i < numCores; i ++)
        {
            AIOContext.Config aio = coordinator.getIOConfig(i);
            if (i >= numIOCores || i >= globalDepth)
            {
                assertNull(aio);
            }
            else
            {
                assertEquals(AioCoordinator.maxPendingDepth, aio.maxPending);
                assertTrue(aio.maxConcurrency == minDepth || aio.maxConcurrency == (minDepth + 1));
                totLocalDepths += aio.maxConcurrency;
            }

            assertEquals(i % numIOCores, coordinator.getIOCore(i));
        }

        assertEquals(globalDepth, totLocalDepths);
    }

    @Test
    public void testAIODisabled()
    {
        final int nCores = 30;
        AioCoordinator coordinator = new AioCoordinator(nCores, 0, 128);

        for (int i = 0; i < nCores; i++)
        {
            assertNull(coordinator.getIOConfig(i));
            assertEquals(0, coordinator.getIOCore(i));
        }
    }
}
