/**
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
package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class BufferPoolMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("BufferPool");

    /** Total number of misses */
    public final Meter misses;

    /** Total size of buffer pools, in bytes */
    public final Gauge<Long> totalSize;

    /** Total size, in bytes, of direct buffers allocated by the pool but not part of the pool
     *  Direct buffers are allocated outside of the pool either because
     *  they are too large to fit or because the pool has exceeded its maximum limit.
     */
    public final Gauge<Long> overflowSize;

    /** Total size, in bytes, of active buffered being used from the pool currently + overflow */
    public final Gauge<Long> usedSize;

    /**
     * Total size, in bytes, of direct buffers allocated by chunkReaders in order to read
     * data from disk.  These buffers are pooled and limited in size by
     * -Ddse.total_chunk_reader_buffer_limit_mb=32 (default)
     */
    public final Gauge<Long> chunkReaderBufferSize;

    public BufferPoolMetrics()
    {
        misses = Metrics.meter(factory.createMetricName("Misses"));

        totalSize = Metrics.register(factory.createMetricName("Size"), () -> BufferPool.sizeInBytes());
        usedSize = Metrics.register(factory.createMetricName("UsedSize"), () -> BufferPool.usedSizeInBytes());
        overflowSize = Metrics.register(factory.createMetricName("OverflowSize"), () -> BufferPool.sizeInBytesOverLimit());

        chunkReaderBufferSize = Metrics.register(factory.createMetricName("ChunkReaderBufferSize"), () -> ChunkReader.bufferSize.get());
    }
}
