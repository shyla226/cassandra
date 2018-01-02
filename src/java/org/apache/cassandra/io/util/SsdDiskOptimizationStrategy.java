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

class SsdDiskOptimizationStrategy extends DiskOptimizationStrategy
{
    private final double diskOptimizationPageCrossChance;

    SsdDiskOptimizationStrategy(int minBufferSize, int maxBufferSize, double diskOptimizationPageCrossChance)
    {
        super(minBufferSize, maxBufferSize);
        this.diskOptimizationPageCrossChance = diskOptimizationPageCrossChance;
    }

    public String diskType()
    {
        return "Solid-State drives (SSD)";
    }

    /**
     * For solid state disks only add one page if the chance of crossing to the next page is more
     * than a predifined value.
     *
     * @see org.apache.cassandra.config.Config#disk_optimization_page_cross_chance
     */
    @Override
    public int bufferSize(long recordSize)
    {
        // The crossing probability is calculated assuming a uniform distribution of record
        // start position in a page, so it's the record size modulo the page size divided by
        // the total page size.
        double pageCrossProbability = (recordSize % minBufferSize) / (double)minBufferSize;
        // if the page cross probability is equal or bigger than disk_optimization_page_cross_chance we add one page
        if ((pageCrossProbability - diskOptimizationPageCrossChance) > -1e-16)
            recordSize += minBufferSize;

        return roundBufferSize(recordSize);
    }

    @Override
    public int readAheadSizeKb()
    {
        return 64;
    }
}
