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
package org.apache.cassandra.io.compress;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.UnsafeMemoryAccess;
import sun.misc.VM;

public enum BufferType
{
    ON_HEAP
    {
        public ByteBuffer allocate(int size)
        {
            return ByteBuffer.allocate(size);
        }
    },
    OFF_HEAP
    {
        public ByteBuffer allocate(int size)
        {
            return ByteBuffer.allocateDirect(size);
        }
    },
    OFF_HEAP_ALIGNED
    {
        public ByteBuffer allocate(int size)
        {
            return allocateDirectAligned(size);
        }
    };

    public abstract ByteBuffer allocate(int size);

    public static BufferType typeOf(ByteBuffer buffer)
    {
        return buffer.isDirect()
               ? ((UnsafeByteBufferAccess.getAddress(buffer) & -UnsafeMemoryAccess.pageSize()) == 0
                  ? OFF_HEAP_ALIGNED
                  : OFF_HEAP)
               : ON_HEAP;
    }

    private static ByteBuffer allocateDirectAligned(int capacity)
    {
        if (VM.isDirectMemoryPageAligned())
            return ByteBuffer.allocateDirect(capacity);

        int align = UnsafeMemoryAccess.pageSize();
        if (Integer.bitCount(align) != 1)
            throw new IllegalArgumentException("Alignment must be a power of 2");

        ByteBuffer buffer = ByteBuffer.allocateDirect(capacity + align);
        long address = UnsafeByteBufferAccess.getAddress(buffer);
        long offset = address & (align -1); // (address % align)

        if (offset == 0)
        { // already aligned
            buffer.limit(capacity);
        }
        else
        { // shift by offset
            int pos = (int)(align - offset);
            buffer.position(pos);
            buffer.limit(pos + capacity);
        }

        // Note: sliced direct buffers have the root buffer set as it's attachment.
        // see FileUtils.clean()
        return buffer.slice();
    }
}