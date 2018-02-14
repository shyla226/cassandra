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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.epoll.AIOContext;

/**
 * The AIO coordinator. This class determines the local queue depth of each AIO reader and
 * keeps track of which TPC threads own an {@link io.netty.channel.epoll.AIOContext}.
 * <p>
 * Given a core id, it will return the core id of the TPC threads that owns an AIO context and
 * to which this core has been assigned.
 */
public class AioCoordinator
{
    private static final Logger logger = LoggerFactory.getLogger(AioCoordinator.class);

    // this is the depth of the queue java side, inside the AIOContext
    final static int maxPendingDepth = 1 << 16;

    private final int numTPCCores;
    private final int numIOCores;
    private final int globalQueueDepth;

    // an array containing the core id performing AIO reads for the TPC core at this index
    private final int[] tpcToAioCores;

    public AioCoordinator(int numTPCCores, int numIOCores, int globalQueueDepth)
    {
        assert numIOCores >= 0 : String.format("Invalid numIOCores: %d", numIOCores);
        assert numIOCores <= numTPCCores : String.format("Expected numIOCores %d to be <= than numTPCCores %d", numIOCores, numTPCCores);

        this.numTPCCores = numTPCCores;
        this.numIOCores = numIOCores;
        this.globalQueueDepth = globalQueueDepth;
        this.tpcToAioCores = new int[numTPCCores];

        if (numIOCores > 0)
            initAioCores();

        logger.debug("AIO coordinator: {}", this);
    }

    private void initAioCores()
    {
        // the first numIOCores are IO cores
        // the remaining TPC cores get distributed in a round robin fashion
        int curr = 0;
        for (int i = 0; i < numTPCCores; i++)
        {
            tpcToAioCores[i] = curr;
            curr++;
            if (curr == numIOCores)
                curr = 0;
        }
    }

    public AIOContext.Config getIOConfig(int coreId)
    {
        assert coreId >= 0 && coreId < tpcToAioCores.length : "Invalid core id: " + coreId;

        if (numIOCores == 0)
            return null;

        int aioCore = tpcToAioCores[coreId];
        int depth = globalQueueDepth % numIOCores == 0
                    ? globalQueueDepth / numIOCores
                    : (int)Math.floor((double)globalQueueDepth / numIOCores) +
                      (aioCore + 1 <= globalQueueDepth % numIOCores ? 1 : 0);

        // if a core is equal to its IO core then it owns an AIO context as long as it has some depth assigned
        AIOContext.Config ret = coreId == aioCore && depth > 0
                                ? new AIOContext.Config(depth, maxPendingDepth)
                                : null;

        if (ret != null)
            logger.debug("Assigning AIO [{}] to core {}", ret, coreId);
        return ret;
    }

    public int getIOCore(int coreId)
    {
        return tpcToAioCores[coreId];
    }

    @Override
    public String toString()
    {
        return String.format("Num cores: %d, Num IO cores: %d, globalQueueDepth: %d, maxPending: %d",
                             numTPCCores, numIOCores, globalQueueDepth, maxPendingDepth);
    }
}
