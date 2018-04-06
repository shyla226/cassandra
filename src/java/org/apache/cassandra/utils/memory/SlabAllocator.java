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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.utils.ByteBufferUtil;
import sun.nio.ch.DirectBuffer;

/**
+ * The SlabAllocator is a bump-the-pointer allocator that allocates
+ * large (1MB) global regions and then doles them out to threads that
+ * request smaller sized (up to 128kb) slices into the array.
 * <p></p>
 * The purpose of this class is to combat heap fragmentation in long lived
 * objects: by ensuring that all allocations with similar lifetimes
 * only to large regions of contiguous memory, we ensure that large blocks
 * get freed up at the same time.
 * <p></p>
 * Otherwise, variable length byte arrays allocated end up
 * interleaved throughout the heap, and the old generation gets progressively
 * more fragmented until a stop-the-world compacting collection occurs.
 *
 * Since we now only write to memtables from one thread, this allocator does not need to be thread-safe but we assert
 * it's only called from the expected thread.
 */
public class SlabAllocator extends MemtableBufferAllocator
{
    private final static int MIN_REGION_SIZE = 8 * 1024;
    private final static int MAX_REGION_SIZE = 1024 * 1024;
    private final static int MAX_CLONED_SIZE = 128 * 1024; // bigger than this don't go in the region

    private Region currentRegion = null;

    // this queue is used to keep references to off-heap allocated regions so that we can free them when we are discarded
    private final Collection<Region> offHeapRegions = new LinkedList<>();
    private final boolean allocateOnHeapOnly;

    private final int coreId;

    SlabAllocator(SubAllocator onHeap, SubAllocator offHeap, boolean allocateOnHeapOnly, int coreId)
    {
        super(onHeap, offHeap);
        assert TPC.isValidCoreId(coreId) || coreId == -1;   // -1 is used for estimateRowOverhead and testing
        this.coreId = coreId;
        this.allocateOnHeapOnly = allocateOnHeapOnly;
    }

    public boolean onHeapOnly()
    {
        return allocateOnHeapOnly;
    }

    public ByteBuffer allocate(int size)
    {
        assert TPC.isOnCore(coreId) || coreId == -1;

        assert size >= 0;
        if (size == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        (allocateOnHeapOnly ? onHeap() : offHeap()).allocated(size);
        // satisfy large allocations directly from JVM since they don't cause fragmentation
        // as badly, and fill up our regions quickly
        if (size > MAX_CLONED_SIZE)
        {
            if (allocateOnHeapOnly)
                return ByteBuffer.allocate(size);

            Region region = new Region(ByteBuffer.allocateDirect(size));
            offHeapRegions.add(region);
            return region.allocate(size);
        }

        if (currentRegion != null)
        {
            ByteBuffer cloned = currentRegion.allocate(size);
            if (cloned != null)
                return cloned;
        }

        int regionSize = getRegionSize(size);
        assert regionSize >= size;

        Region next = new Region(allocateOnHeapOnly ? ByteBuffer.allocate(regionSize) : ByteBuffer.allocateDirect(regionSize));
        if (!allocateOnHeapOnly)
            offHeapRegions.add(next);
        currentRegion = next;

        ByteBuffer cloned = currentRegion.allocate(size);
        assert cloned != null;
        return cloned;
    }

    public int getRegionSize(int minSize)
    {
        Region current = currentRegion;
        // decide how big we want the new region to be:
        //  * if there is no prior region, we set it to min size
        //  * otherwise we double its size; if it's too small to fit the allocation, we round it up to 4-8x its size
        int regionSize;
        if (current == null)
            regionSize = MIN_REGION_SIZE;
        else
            regionSize = current.data.capacity() * 2;

        if (minSize > regionSize)
            regionSize = Integer.highestOneBit(minSize) << 3;

        return Math.min(MAX_REGION_SIZE, regionSize);
    }

    public void setDiscarded()
    {
        for (Region region : offHeapRegions)
            ((DirectBuffer) region.data).cleaner().clean();
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
        private final ByteBuffer data;

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
         * @param buffer bytes
         */
        private Region(ByteBuffer buffer)
        {
            data = buffer;
        }

        /**
         * Try to allocate <code>size</code> bytes from the region.
         *
         * @return the successful allocation, or null to indicate not-enough-space
         */
        public ByteBuffer allocate(int size)
        {
            int oldOffset = nextFreeOffset;

            if (oldOffset + size > data.capacity()) // capacity == remaining
                return null;

            // Try to atomically claim this region
            nextFreeOffset += size;
            ++allocCount;
            return (ByteBuffer) data.duplicate().position(oldOffset).limit(oldOffset + size);
        }

        @Override
        public String toString()
        {
            return "Region@" + System.identityHashCode(this) +
                   " allocs=" + allocCount + "waste=" +
                   (data.capacity() - nextFreeOffset);
        }
    }
}
