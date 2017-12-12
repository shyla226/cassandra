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

package org.apache.cassandra.utils;

import java.nio.*;

import net.nicoulaj.compilecommand.annotations.Inline;

import static org.apache.cassandra.utils.Architecture.IS_UNALIGNED;
import static org.apache.cassandra.utils.UnsafeAccess.UNSAFE;

public class UnsafeByteBufferAccess
{
    public static final long DIRECT_BYTE_BUFFER_LIMIT_OFFSET;
    public static final long DIRECT_BYTE_BUFFER_POSITION_OFFSET;
    public static final long DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET;
    public static final long BYTE_BUFFER_OFFSET_OFFSET;
    public static final long BYTE_BUFFER_HB_OFFSET;
    public static final long BYTE_BUFFER_NATIVE_ORDER;
    public static final long BYTE_BUFFER_BIG_ENDIAN;

    public static final long DIRECT_BYTE_BUFFER_ADDRESS_OFFSET;
    public static final long DIRECT_BYTE_BUFFER_CAPACITY_OFFSET;
    
    public static final Class<?> DIRECT_BYTE_BUFFER_CLASS;
    public static final long BYTE_ARRAY_BASE_OFFSET;

    static
    {
        try
        {
            DIRECT_BYTE_BUFFER_ADDRESS_OFFSET = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            DIRECT_BYTE_BUFFER_CAPACITY_OFFSET = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("capacity"));
            DIRECT_BYTE_BUFFER_LIMIT_OFFSET = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("limit"));
            DIRECT_BYTE_BUFFER_POSITION_OFFSET = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("position"));

            DIRECT_BYTE_BUFFER_CLASS = ByteBuffer.allocateDirect(0).getClass();
            DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET = UNSAFE.objectFieldOffset(DIRECT_BYTE_BUFFER_CLASS.getDeclaredField("att"));

            BYTE_BUFFER_OFFSET_OFFSET = UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("offset"));
            BYTE_BUFFER_HB_OFFSET = UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("hb"));
            BYTE_BUFFER_NATIVE_ORDER = UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("nativeByteOrder"));
            BYTE_BUFFER_BIG_ENDIAN = UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("bigEndian"));

            BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    @Inline
    public static long getAddress(ByteBuffer buffer)
    {
        return UNSAFE.getLong(buffer, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET);
    }

    @Inline
    public static Object getArray(ByteBuffer buffer)
    {
        return UNSAFE.getObject(buffer, BYTE_BUFFER_HB_OFFSET);
    }

    @Inline
    public static int getOffset(ByteBuffer buffer)
    {
        return UNSAFE.getInt(buffer, BYTE_BUFFER_OFFSET_OFFSET);
    }

    @Inline
    public static boolean nativeByteOrder(ByteBuffer buffer)
    {
        return UNSAFE.getBoolean(buffer, BYTE_BUFFER_NATIVE_ORDER);
    }

    @Inline
    public static boolean bigEndian(ByteBuffer buffer)
    {
        return UNSAFE.getBoolean(buffer, BYTE_BUFFER_BIG_ENDIAN);
    }

    public static Object getAttachment(ByteBuffer instance)
    {
        assert instance.getClass() == DIRECT_BYTE_BUFFER_CLASS;
        return UNSAFE.getObject(instance, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET);
    }

    public static void setAttachment(ByteBuffer instance, Object next)
    {
        assert instance.getClass() == DIRECT_BYTE_BUFFER_CLASS;
        UNSAFE.putObject(instance, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET, next);
    }

    @Inline
    static long bufferOffset(ByteBuffer buffer, Object array)
    {
        long srcOffset;
        if (array != null)
        {
            srcOffset = BYTE_ARRAY_BASE_OFFSET + getOffset(buffer);
        }
        else
        {
            srcOffset = getAddress(buffer);
        }
        return srcOffset;
    }

    /**
     * Why do this? because the JDK only bothers optimising DirectByteBuffer for the unaligned case.
     */
    @Inline
    public static short getShort(ByteBuffer bb)
    {
        Object array = getArray(bb);
        long srcOffset = bb.position() + bufferOffset(bb, array);

        if (IS_UNALIGNED)
        {
            short x = UNSAFE.getShort(array, srcOffset);
            return (nativeByteOrder(bb) ? x : Short.reverseBytes(x));
        }
        else
            return UnsafeMemoryAccess.getShortByByte(array, srcOffset, bigEndian(bb));
    }

    /**
     * Why do this? because the JDK only bothers optimising DirectByteBuffer for the unaligned case.
     */
    @Inline
    public static int getInt(ByteBuffer bb)
    {
        Object array = getArray(bb);
        long srcOffset = bb.position() + bufferOffset(bb, array);

        if (IS_UNALIGNED)
        {
            int x = UNSAFE.getInt(array, srcOffset);
            return (nativeByteOrder(bb)  ? x : Integer.reverseBytes(x));
        }
        else
            return UnsafeMemoryAccess.getIntByByte(array, srcOffset, bigEndian(bb));
    }

    /**
     * Why do this? because the JDK only bothers optimising DirectByteBuffer for the unaligned case.
     */
    @Inline
    public static long getLong(ByteBuffer bb)
    {
        Object array = getArray(bb);
        long srcOffset = bb.position() + bufferOffset(bb, array);

        if (IS_UNALIGNED)
        {
            final long l = UNSAFE.getLong(array, srcOffset);
            return (nativeByteOrder(bb) ? l : Long.reverseBytes(l));
        }
        else
            return UnsafeMemoryAccess.getLongByByte(array, srcOffset, bigEndian(bb));
    }

    /**
     * Why do this? because the JDK only bothers optimising DirectByteBuffer for the unaligned case.
     */
    @Inline
    public static double getDouble(ByteBuffer bb)
    {
        return Double.longBitsToDouble(getLong(bb));

    }

    public static ByteBuffer getByteBuffer(long address, int length)
    {
        return getByteBuffer(address, length, ByteOrder.nativeOrder());
    }

    public static ByteBuffer getByteBuffer(long address, int length, ByteOrder order)
    {
        ByteBuffer instance = getHollowDirectByteBuffer(order);
        setByteBuffer(instance, address, length);
        return instance;
    }

    public static ByteBuffer getHollowDirectByteBuffer()
    {
        return getHollowDirectByteBuffer(ByteOrder.nativeOrder());
    }

    private static ByteBuffer getHollowDirectByteBuffer(ByteOrder order)
    {
        ByteBuffer instance;
        try
        {
            instance = (ByteBuffer) UNSAFE.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
        }
        catch (InstantiationException e)
        {
            throw new AssertionError(e);
        }
        instance.order(order);
        return instance;
    }

    public static void setByteBuffer(ByteBuffer instance, long address, int length)
    {
        UNSAFE.putLong(instance, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, address);
        UNSAFE.putInt(instance, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, length);
        UNSAFE.putInt(instance, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, length);
    }

    public static ByteBuffer duplicateDirectByteBuffer(ByteBuffer source, ByteBuffer hollowBuffer)
    {
        assert source.isDirect();
        UNSAFE.putLong(hollowBuffer, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, UNSAFE.getLong(source, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET));
        UNSAFE.putInt(hollowBuffer, DIRECT_BYTE_BUFFER_POSITION_OFFSET, UNSAFE.getInt(source, DIRECT_BYTE_BUFFER_POSITION_OFFSET));
        UNSAFE.putInt(hollowBuffer, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, UNSAFE.getInt(source, DIRECT_BYTE_BUFFER_LIMIT_OFFSET));
        UNSAFE.putInt(hollowBuffer, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, UNSAFE.getInt(source, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET));
        return hollowBuffer;
    }
}
