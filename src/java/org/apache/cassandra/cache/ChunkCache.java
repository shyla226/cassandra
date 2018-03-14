/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cache;

import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.io.util.DiskOptimizationStrategy;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.metrics.CacheMissMetrics;
import org.apache.cassandra.utils.PageAware;

public class ChunkCache implements CacheSize
{
    // Approximate ratio of index to data file size for ChunkCache. Leaving too little space for index cache will cause
    // slower lookups, just as leaving too little space for data.
    private static final double INDEX_TO_DATA_RATIO = Double.parseDouble(System.getProperty("dse.cache.index_to_data_ratio", "0.4"));
    private static final int INFLIGHT_DATA_OVERHEAD_IN_MB = Math.max(Integer.getInteger("dse.cache.inflight_data_overhead_in_mb",
                                                                                        (int) (DatabaseDescriptor.getFileCacheSizeInMB() * 0.05)), 32);

    private static final long cacheSize = 1024L * 1024L * Math.max(0, DatabaseDescriptor.getFileCacheSizeInMB() - INFLIGHT_DATA_OVERHEAD_IN_MB);
    private static final boolean roundUp = DatabaseDescriptor.getFileCacheRoundUp();
    private static final DiskOptimizationStrategy diskOptimizationStrategy = DatabaseDescriptor.getDiskOptimizationStrategy();
    public static final ChunkCache instance = cacheSize > 0 ? new ChunkCache() : null;
    private Function<ChunkReader, RebuffererFactory> wrapper = this::wrap;

    public final CacheMissMetrics metrics = new CacheMissMetrics("ChunkCache", this);
    private final ChunkCacheImpl indexPageCache;
    private final ChunkCacheImpl dataPageCache;

    private final int INDEX_BLOCK_SIZE = PageAware.PAGE_SIZE;

    public ChunkCache()
    {
        // TODO: what'd be an optimal ratio?
        long indexCacheSize = (long) (INDEX_TO_DATA_RATIO * cacheSize);
        this.indexPageCache = new ChunkCacheImpl(metrics, indexCacheSize);
        this.dataPageCache = new ChunkCacheImpl(metrics,cacheSize - indexCacheSize);
    }

    public RebuffererFactory wrap(ChunkReader file)
    {
        int size = file.chunkSize();
        if (size == INDEX_BLOCK_SIZE)
            return indexPageCache.wrap(file);
        else
            return dataPageCache.wrap(file);
    }

    public RebuffererFactory maybeWrap(ChunkReader file)
    {
        return wrapper.apply(file);
    }

    @VisibleForTesting
    public void enable(boolean enabled)
    {
        wrapper = enabled ? this::wrap : x -> x;
        indexPageCache.reset();
        dataPageCache.reset();
    }

    @VisibleForTesting
    public void intercept(Function<RebuffererFactory, RebuffererFactory> interceptor)
    {
        final Function<ChunkReader, RebuffererFactory> prevWrapper = wrapper;
        wrapper = rdr -> interceptor.apply(prevWrapper.apply(rdr));
    }

    public static int roundForCaching(int bufferSize)
    {
        return diskOptimizationStrategy.roundForCaching(bufferSize, roundUp);
    }

    @VisibleForTesting
    public void invalidateFile(String fileName)
    {
        indexPageCache.invalidateFile(fileName);
        dataPageCache.invalidateFile(fileName);
    }

    public static void invalidatePosition(FileHandle dfile, long position)
    {
        if (!(dfile.rebuffererFactory() instanceof ChunkCacheImpl.CachingRebufferer))
            return;

        ((ChunkCacheImpl.CachingRebufferer) dfile.rebuffererFactory()).invalidate(position);
    }

    @Override
    public long capacity()
    {
        return indexPageCache.capacity() + dataPageCache.capacity();
    }

    @Override
    public void setCapacity(long capacity)
    {
        throw new AssertionError("Can't set capacity of chunk cache");
    }

    @Override
    public int size()
    {
        return indexPageCache.size() + dataPageCache.size();
    }

    @Override
    public long weightedSize()
    {
        return indexPageCache.weightedSize() + dataPageCache.weightedSize();
    }
}
