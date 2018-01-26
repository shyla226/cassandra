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

class SpinningDiskOptimizationStrategy extends DiskOptimizationStrategy
{
    SpinningDiskOptimizationStrategy(int minBufferSize, int maxBufferSize)
    {
        super(minBufferSize, maxBufferSize);
    }

    public String diskType()
    {
        return "Spinning disks (non-SSD)";
    }

    /**
     * For spinning disks always add one page.
     */
    @Override
    public int bufferSize(long recordSize)
    {
        return roundBufferSize(recordSize + minBufferSize);
    }

    @Override
    public int readAheadSizeKb()
    {
        // LZ4 compression is the default for DSE, which means that we normally prefetch 1 buffer of 64kb even
        // if we set a lower value here. If compression is OFF, read-ahead becomes more significant and we should
        // at least prefetch 32 kb to avoid cache misses, since reads on HDDs benefit from larger buffers, we prefetch 64 kb
        return 64;
    }
}
