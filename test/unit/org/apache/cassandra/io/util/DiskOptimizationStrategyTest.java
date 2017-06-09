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

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;

public class DiskOptimizationStrategyTest
{
    @Test
    public void testRoundingBufferSize()
    {
        DiskOptimizationStrategy strategy = new SsdDiskOptimizationStrategy(0.95);
        assertEquals(4096, strategy.roundBufferSize(-1L));
        assertEquals(4096, strategy.roundBufferSize(0));
        assertEquals(4096, strategy.roundBufferSize(1));
        assertEquals(4096, strategy.roundBufferSize(2013));
        assertEquals(4096, strategy.roundBufferSize(4095));
        assertEquals(4096, strategy.roundBufferSize(4096));
        assertEquals(8192, strategy.roundBufferSize(4097));
        assertEquals(8192, strategy.roundBufferSize(8191));
        assertEquals(8192, strategy.roundBufferSize(8192));
        assertEquals(16384, strategy.roundBufferSize(8193));
        assertEquals(65536, strategy.roundBufferSize(65535));
        assertEquals(65536, strategy.roundBufferSize(65536));
        assertEquals(65536, strategy.roundBufferSize(65537));
        assertEquals(65536, strategy.roundBufferSize(10000000000000000L));
    }

    @Test
    public void testBufferSize_ssd()
    {
        DiskOptimizationStrategy strategy = new SsdDiskOptimizationStrategy(0.1);

        assertEquals(4096, strategy.bufferSize(0));
        assertEquals(4096, strategy.bufferSize(10));
        assertEquals(4096, strategy.bufferSize(100));
        assertEquals(4096, strategy.bufferSize(4096));
        assertEquals(8192, strategy.bufferSize(4505));   // just < (4096 + 4096 * 0.1)
        assertEquals(16384, strategy.bufferSize(4506));  // just > (4096 + 4096 * 0.1)

        strategy = new SsdDiskOptimizationStrategy(0.5);
        assertEquals(8192, strategy.bufferSize(4506));  // just > (4096 + 4096 * 0.1)
        assertEquals(8192, strategy.bufferSize(6143));  // < (4096 + 4096 * 0.5)
        assertEquals(16384, strategy.bufferSize(6144));  // = (4096 + 4096 * 0.5)
        assertEquals(16384, strategy.bufferSize(6145));  // > (4096 + 4096 * 0.5)

        strategy = new SsdDiskOptimizationStrategy(1.0); // never add a page
        assertEquals(8192, strategy.bufferSize(8191));
        assertEquals(8192, strategy.bufferSize(8192));

        strategy = new SsdDiskOptimizationStrategy(0.0); // always add a page
        assertEquals(8192, strategy.bufferSize(10));
        assertEquals(8192, strategy.bufferSize(4096));
    }

    @Test
    public void testBufferSize_spinning()
    {
        DiskOptimizationStrategy strategy = new SpinningDiskOptimizationStrategy();

        assertEquals(4096, strategy.bufferSize(0));
        assertEquals(8192, strategy.bufferSize(10));
        assertEquals(8192, strategy.bufferSize(100));
        assertEquals(8192, strategy.bufferSize(4096));
        assertEquals(16384, strategy.bufferSize(4097));
    }

    @Test
    public void testBufferSizeToChunkeSize()
    {
        Config cfg = new Config();
        cfg.file_cache_size_in_mb = 64 * 1024 * 1024;
        DatabaseDescriptor.setConfig(cfg);
        assertEquals(4096, ChunkCache.bufferToChunkSize(-1));
        assertEquals(4096, ChunkCache.bufferToChunkSize(0));
        assertEquals(4096, ChunkCache.bufferToChunkSize(1));
        assertEquals(4096, ChunkCache.bufferToChunkSize(4095));
        assertEquals(4096, ChunkCache.bufferToChunkSize(4096));
        assertEquals(4096, ChunkCache.bufferToChunkSize(4097));
        assertEquals(4096, ChunkCache.bufferToChunkSize(4098));
        assertEquals(8192, ChunkCache.bufferToChunkSize(8193));
        assertEquals(8192, ChunkCache.bufferToChunkSize(12288));
        assertEquals(65536, ChunkCache.bufferToChunkSize(65536));
        assertEquals(65536, ChunkCache.bufferToChunkSize(65537));
        assertEquals(65536, ChunkCache.bufferToChunkSize(131072));

        for (int cs = 4096; cs < 65536; cs <<= 1)
        {
            for (int i = cs; i < cs * 2 - 1; i++)
            {
                assertEquals(cs, ChunkCache.bufferToChunkSize(i));
            }
        }
    }
}
