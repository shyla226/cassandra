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

import org.apache.cassandra.config.Config;
import org.apache.cassandra.exceptions.ConfigurationException;

public abstract class DiskOptimizationStrategy
{
    private static final String MIN_BUFFER_SIZE_NAME = "dse.min_buffer_size";
    private static final String MAX_BUFFER_SIZE_NAME = "dse.max_buffer_size";
    private static final int MIN_BUFFER_SIZE = Integer.parseInt(System.getProperty(MIN_BUFFER_SIZE_NAME, "0"));
    private static final int MAX_BUFFER_SIZE = Integer.parseInt(System.getProperty(MAX_BUFFER_SIZE_NAME, "0"));

    /**
     * How many buffers to prefetch for sequential scans such as compactions or partial scans such as range queries.
     */
    public static final int NUM_READ_AHEAD_BUFFERS = Integer.parseInt(System.getProperty("dse.num_read_ahead_buffers", "4"));

    private static final Logger logger = LoggerFactory.getLogger(DiskOptimizationStrategy.class);

    final int minBufferSize;
    final int minBufferSizeMask;
    final int maxBufferSize;

    DiskOptimizationStrategy(int minBufferSize, int maxBufferSize)
    {
        assert Integer.bitCount(minBufferSize) == 1 : String.format("%d must be a power of two", minBufferSize);
        assert Integer.bitCount(maxBufferSize) == 1 : String.format("%d must be a power of two", maxBufferSize);

        this.minBufferSize = minBufferSize;
        this.minBufferSizeMask = minBufferSize - 1;
        this.maxBufferSize = maxBufferSize;

        logger.info("Disk optimization strategy for {} using min buffer size of {} bytes and max buffer size of {} bytes",
                    diskType(), minBufferSize, maxBufferSize);

        logger.info("Read ahead for full or range scans (e.g. range queries, compactions) is {} buffers", NUM_READ_AHEAD_BUFFERS);
    }

    public static DiskOptimizationStrategy create(Config conf)
    {
        DiskOptimizationStrategy ret;

        // The minimum buffer size, must be a power of two. It is also used as a page size,
        // meaning that the spinning disk optimization strategy will always add one more
        // page size to the buffer size and that the ssd strategy will add one more page size
        // if the remainder of the buffer size and page size is at least diskOptimizationPageCrossChance
        // (10% by default) of the page size.
        int minBufferSize = MIN_BUFFER_SIZE > 0 ? MIN_BUFFER_SIZE : 1 << 12; // 4k;

        if (Integer.bitCount(minBufferSize) != 1)
            throw new ConfigurationException(String.format("Min buffer size must be a power of two, instead got %d", minBufferSize));

        // The maximum buffer size, we will never read more than this size.
        // This size is chosen both for historical consistency, as a reasonable upper bound,
        // and because our BufferPool currently has a maximum allocation size of this.
        // Users can override this by setting MAX_BUFFER_SIZE via a system property. If they pick
        // a value bigger than 64k, we issue a warning because the buffer pool will be bypassed
        // in this case.
        int maxBufferSize = MAX_BUFFER_SIZE > 0 ? MAX_BUFFER_SIZE : 1 << 16; // 64k

        if (Integer.bitCount(maxBufferSize) != 1)
            throw new ConfigurationException(String.format("Max buffer size must be a power of two, instead got %d", maxBufferSize));

        if (minBufferSize > maxBufferSize)
            throw new ConfigurationException(String.format("Max buffer size %d must be >= than min buffer size %d", maxBufferSize, minBufferSize));

        if (maxBufferSize > (1 << 16))
            logger.warn("Buffers larger than 64k ({}) are currently not supported by the buffer pool. " +
                        "This will cause longer allocation times for each buffer read from disk, " +
                        "consider lowering -D{} but make sure it is still a power of two and >= -D{}.",
                        maxBufferSize, MAX_BUFFER_SIZE_NAME, MIN_BUFFER_SIZE_NAME);

        switch (conf.disk_optimization_strategy)
        {
            case ssd:
                ret = new SsdDiskOptimizationStrategy(minBufferSize, maxBufferSize, conf.disk_optimization_page_cross_chance);
                break;
            case spinning:
                ret = new SpinningDiskOptimizationStrategy(minBufferSize, maxBufferSize);
                break;
            default:
                throw new ConfigurationException("Unknown disk optimization strategy: " + conf.disk_optimization_strategy);
        }

        return ret;
    }

    public abstract String diskType();

    /**
     * @param recordSize record size
     * @return the buffer size for a given record size.
     */
    public abstract int bufferSize(long recordSize);

    /**
     * Round up to the next multiple of 4k but no more than {@link #maxBufferSize}.
     */
    int roundBufferSize(long size)
    {
        if (size <= minBufferSize)
            return minBufferSize;

        size = (size + minBufferSizeMask) & ~minBufferSizeMask;
        return (int)Math.min(size, maxBufferSize);
    }

    /**
     * Round either up or down to the next power of two, which is required by the
     * {@link org.apache.cassandra.cache.ChunkCache.CachingRebufferer}, but capping to {@link #maxBufferSize}.
     *
     * @param size - the size to round to a power of two, normally this is a buffer size that was previously
     *             returned by a {@link #bufferSize(long)}.
     * @param roundUp - whether to round up or down
     *
     * @return a value rounded to a power of two but never bigger than {@link #maxBufferSize}.
     */
    public int roundForCaching(int size, boolean roundUp)
    {
        if (size <= 2)
            return 2;

        int ret = roundUp
                  ? 1 << (32 - Integer.numberOfLeadingZeros(size - 1))
                  : Integer.highestOneBit(size);

        return Math.min(maxBufferSize, ret);
    }
}
