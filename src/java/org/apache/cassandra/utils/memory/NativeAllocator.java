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
package org.apache.cassandra.utils.memory;

import java.util.Collection;
import java.util.LinkedList;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.NativeClustering;
import org.apache.cassandra.db.NativeDecoratedKey;
import org.apache.cassandra.db.rows.ArrayBackedRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.NativeCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.UnsafeMemoryAccess;

/**
 * This NativeAllocator uses global slab allocation strategy
 * with slab size that scales exponentially from 8kb to 1Mb to
 * serve allocation of up to 128kb.
 * <p>
 * </p>
 * The slab allocation reduces heap fragmentation from small
 * long-lived objects.
 *
 * Since we only allocate from specific memtable threads, this class does not need to be thread-safe but we assert
 * it's only called from the expected thread.
 */
public class NativeAllocator extends MemtableAllocator
{
    private final static int MAX_REGION_SIZE = 1 * 1024 * 1024;
    private final static int MAX_CLONED_SIZE = 128 * 1024; // bigger than this don't go in the region
    private final static int MIN_REGION_SIZE = 8 * 1024;

    private Region currentRegion = null;
    private Collection<Region> regions = new LinkedList<>();
    private final EnsureOnHeap.CloneToHeap cloneToHeap = new EnsureOnHeap.CloneToHeap();

    private final int coreId;

    protected NativeAllocator(NativePool pool, int coreId)
    {
        super(pool.onHeap.newAllocator(), pool.offHeap.newAllocator());
        assert TPC.isValidCoreId(coreId) || coreId == -1;   // -1 is used for estimateRowOverhead and testing
        this.coreId = coreId;
    }

    private static class CloningRowBuilder extends ArrayBackedRow.Builder
    {
        final NativeAllocator allocator;
        private CloningRowBuilder(NativeAllocator allocator)
        {
            super(true, Integer.MIN_VALUE);
            this.allocator = allocator;
        }

        @Override
        public void newRow(Clustering clustering)
        {
            if (clustering != Clustering.STATIC_CLUSTERING)
                clustering = new NativeClustering(allocator, clustering);
            super.newRow(clustering);
        }

        @Override
        public void addCell(Cell cell)
        {
            super.addCell(new NativeCell(allocator, cell));
        }
    }

    public Row.Builder rowBuilder()
    {
        return new CloningRowBuilder(this);
    }

    public DecoratedKey clone(DecoratedKey key)
    {
        return new NativeDecoratedKey(key.getToken(), this, key.getKey());
    }

    public EnsureOnHeap ensureOnHeap()
    {
        return cloneToHeap;
    }

    public long allocate(int size)
    {
        assert TPC.isOnCore(coreId) || coreId == -1;
        assert size >= 0;
        offHeap().allocated(size);
        // satisfy large allocations directly from JVM since they don't cause fragmentation
        // as badly, and fill up our regions quickly
        if (size > MAX_CLONED_SIZE)
            return allocateOversize(size);

        long peer;
        Region region = currentRegion;
        if (region != null && (peer = region.allocate(size)) > 0)
            return peer;

        peer = trySwapRegion(size).allocate(size);
        assert peer > 0;
        return peer;
    }

    private Region trySwapRegion(int minSize)
    {
        Region current = currentRegion;
        // decide how big we want the new region to be:
        //  * if there is no prior region, we set it to min size
        //  * otherwise we double its size; if it's too small to fit the allocation, we round it up to 4-8x its size
        int size;
        if (current == null) size = MIN_REGION_SIZE;
        else size = current.capacity * 2;
        if (minSize > size)
            size = Integer.highestOneBit(minSize) << 3;
        size = Math.min(MAX_REGION_SIZE, size);

        // first we try and repurpose a previously allocated region
        Region next = new Region(UnsafeMemoryAccess.allocate(size), size);

        // we try to swap in the region we've obtained;
        // if we fail to swap the region, we try to stash it for repurposing later; if we're out of stash room, we free it
        currentRegion = next;
        regions.add(next);
        return next;
    }

    private long allocateOversize(int size)
    {
        // satisfy large allocations directly from JVM since they don't cause fragmentation
        // as badly, and fill up our regions quickly
        Region region = new Region(UnsafeMemoryAccess.allocate(size), size);
        regions.add(region);

        long peer;
        if ((peer = region.allocate(size)) == -1)
            throw new AssertionError();

        return peer;
    }

    public void setDiscarded()
    {
        for (Region region : regions)
            UnsafeMemoryAccess.free(region.peer);

        super.setDiscarded();
    }

    /**
     * A region of memory out of which allocations are sliced.
     *
     * This serves two purposes:
     *  - to provide a step between initialization and allocation, so that racing to CAS a
     *    new region in is harmless
     *  - encapsulates the allocation offset
     */
    private static class Region
    {
        /**
         * Actual underlying data
         */
        private final long peer;

        private final int capacity;

        /**
         * Offset for the next allocation, or the sentinel value -1
         * which implies that the region is still uninitialized.
         */
        private int nextFreeOffset = 0;

        /**
         * Total number of allocations satisfied from this buffer
         */
        private int allocCount = 0;

        /**
         * Create an uninitialized region. Note that memory is not allocated yet, so
         * this is cheap.
         *
         * @param peer peer
         */
        private Region(long peer, int capacity)
        {
            this.peer = peer;
            this.capacity = capacity;
        }

        /**
         * Try to allocate <code>size</code> bytes from the region.
         *
         * @return the successful allocation, or null to indicate not-enough-space
         */
        long allocate(int size)
        {
            int oldOffset = nextFreeOffset;

            if (oldOffset + size > capacity) // capacity == remaining
                return -1;

            nextFreeOffset += size;
            ++allocCount;
            return peer + oldOffset;
        }

        @Override
        public String toString()
        {
            return "Region@" + System.identityHashCode(this) +
                    " allocs=" + allocCount + "waste=" +
                    (capacity - nextFreeOffset);
        }
    }

}
