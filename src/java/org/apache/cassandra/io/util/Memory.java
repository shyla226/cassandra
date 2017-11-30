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

import java.nio.ByteBuffer;

import net.nicoulaj.compilecommand.annotations.Inline;

import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.UnsafeCopy;

/**
 * An off-heap region of memory that must be manually free'd when no longer needed.
 */
public class Memory implements AutoCloseable
{
    private static final ByteBuffer[] NO_BYTE_BUFFERS = new ByteBuffer[0];

    protected long peer;
    // size of the memory region
    protected final long size;

    protected Memory(long bytes)
    {
        if (bytes <= 0)
            throw new AssertionError();
        size = bytes;
        peer = UnsafeMemoryAccess.allocate(size);
        // we permit a 0 peer iff size is zero, since such an allocation makes no sense, and an allocator would be
        // justified in returning a null pointer (and permitted to do so: http://www.cplusplus.com/reference/cstdlib/malloc)
        if (peer == 0)
            throw new OutOfMemoryError();
    }

    // create a memory object that references the exacy same memory location as the one provided.
    // this should ONLY be used by SafeMemory
    protected Memory(Memory copyOf)
    {
        size = copyOf.size;
        peer = copyOf.peer;
    }

    public static Memory allocate(long bytes)
    {
        if (bytes < 0)
            throw new IllegalArgumentException();

        if (Ref.DEBUG_ENABLED)
            return new SafeMemory(bytes);

        return new Memory(bytes);
    }

    public void setByte(long offset, byte b)
    {
        checkBounds(offset, offset + 1);
        UnsafeMemoryAccess.setByte(peer + offset, b);
    }

    public void setMemory(long offset, long bytes, byte b)
    {
        checkBounds(offset, offset + bytes);
        UnsafeMemoryAccess.fill(peer + offset, bytes, b);
    }

    public void setLong(long offset, long l)
    {
        checkBounds(offset, offset + 8);
        UnsafeMemoryAccess.setLong(peer + offset, l);
    }

    public void setInt(long offset, int i)
    {
        checkBounds(offset, offset + 4);
        UnsafeMemoryAccess.setInt(peer + offset, i);
    }

    public void setBytes(long memoryOffset, ByteBuffer buffer)
    {
        if (buffer == null)
            throw new NullPointerException();
        else if (buffer.remaining() == 0)
            return;

        checkBounds(memoryOffset, memoryOffset + buffer.remaining());
        UnsafeCopy.copyBufferToMemory(buffer, buffer.position(), peer + memoryOffset, buffer.remaining());
    }
    /**
     * Transfers count bytes from buffer to Memory
     *
     * @param memoryOffset start offset in the memory
     * @param buffer the data buffer
     * @param bufferOffset start offset of the buffer
     * @param count number of bytes to transfer
     */
    public void setBytes(long memoryOffset, byte[] buffer, int bufferOffset, int count)
    {
        if (buffer == null)
            throw new NullPointerException();
        else if (bufferOffset < 0
                 || count < 0
                 || bufferOffset + count > buffer.length)
            throw new IndexOutOfBoundsException();
        else if (count == 0)
            return;

        checkBounds(memoryOffset, memoryOffset + count);
        UnsafeCopy.copyArrayToMemory(buffer, bufferOffset, peer + memoryOffset, count);
    }

    public byte getByte(long offset)
    {
        checkBounds(offset, offset + 1);
        return UnsafeMemoryAccess.getByte(peer + offset);
    }

    public long getLong(long offset)
    {
        checkBounds(offset, offset + 8);
        return UnsafeMemoryAccess.getLong(peer + offset);
    }

    public int getInt(long offset)
    {
        checkBounds(offset, offset + 4);
        return UnsafeMemoryAccess.getInt(peer + offset);
    }

    /**
     * Transfers count bytes from Memory starting at memoryOffset to buffer starting at bufferOffset
     *
     * @param memoryOffset start offset in the memory
     * @param buffer the data buffer
     * @param bufferOffset start offset of the buffer
     * @param count number of bytes to transfer
     */
    public void getBytes(long memoryOffset, byte[] buffer, int bufferOffset, int count)
    {
        if (buffer == null)
            throw new NullPointerException();
        else if (bufferOffset < 0 || count < 0 || count > buffer.length - bufferOffset)
            throw new IndexOutOfBoundsException();
        else if (count == 0)
            return;

        checkBounds(memoryOffset, memoryOffset + count);
        UnsafeCopy.copyMemoryToArray(peer + memoryOffset, buffer, bufferOffset, count);
    }

    @Inline
    protected void checkBounds(long start, long end)
    {
        assert peer != 0 : "Memory was freed";
        assert start >= 0 && end <= size && start <= end : "Illegal bounds [" + start + ".." + end + "); size: " + size;
    }

    public void put(long trgOffset, Memory memory, long srcOffset, long size)
    {
        checkBounds(trgOffset, trgOffset + size);
        memory.checkBounds(srcOffset, srcOffset + size);
        UnsafeCopy.copyMemoryToMemory(memory.peer + srcOffset, peer + trgOffset, size);
    }

    public Memory copy(long newSize)
    {
        Memory copy = Memory.allocate(newSize);
        copy.put(0, this, 0, Math.min(size(), newSize));
        return copy;
    }

    public void free()
    {
        if (peer != 0) UnsafeMemoryAccess.free(peer);
        else assert size == 0;
        peer = 0;
    }

    public void close()
    {
        free();
    }

    public long size()
    {
        assert peer != 0;
        return size;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Memory))
            return false;
        Memory b = (Memory) o;
        if (peer == b.peer && size == b.size)
            return true;
        return false;
    }

    public ByteBuffer[] asByteBuffers(long offset, long length)
    {
        checkBounds(offset, offset + length);
        if (size() == 0)
            return NO_BYTE_BUFFERS;

        ByteBuffer[] result = new ByteBuffer[(int) (length / Integer.MAX_VALUE) + 1];
        int size = (int) (size() / result.length);
        for (int i = 0 ; i < result.length - 1 ; i++)
        {
            result[i] = UnsafeByteBufferAccess.getByteBuffer(peer + offset, size);
            offset += size;
            length -= size;
        }
        result[result.length - 1] = UnsafeByteBufferAccess.getByteBuffer(peer + offset, (int) length);
        return result;
    }

    public ByteBuffer asByteBuffer(long offset, int length)
    {
        checkBounds(offset, offset + length);
        return UnsafeByteBufferAccess.getByteBuffer(peer + offset, length);
    }

    // MUST provide a buffer created via UnsafeCopy.getHollowDirectByteBuffer()
    public void setByteBuffer(ByteBuffer buffer, long offset, int length)
    {
        checkBounds(offset, offset + length);
        UnsafeByteBufferAccess.setByteBuffer(buffer, peer + offset, length);
    }

    public String toString()
    {
        return toString(peer, size);
    }

    protected static String toString(long peer, long size)
    {
        return String.format("Memory@[%x..%x)", peer, peer + size);
    }
}
