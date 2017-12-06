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

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import org.apache.cassandra.utils.FastByteOperations.UnsafeOperations;

import static org.apache.cassandra.utils.UnsafeByteBufferAccess.BYTE_ARRAY_BASE_OFFSET;
import static org.junit.Assert.*;

public class UnsafeUtilsTest
{
    @Test
    public void testTypeGetterSetter()
    {
        final long address = UnsafeMemoryAccess.allocate(128);
        try
        {
            UnsafeMemoryAccess.setByte(address, (byte) 1);
            UnsafeMemoryAccess.setShort(address + 1, (short) 2);
            UnsafeMemoryAccess.setInt(address + 3, 3);
            UnsafeMemoryAccess.setLong(address + 7, 4);
            assertEquals(UnsafeMemoryAccess.getByte(address), (byte) 1);
            assertEquals(UnsafeMemoryAccess.getUnsignedShort(address + 1), 2);
            assertEquals(UnsafeMemoryAccess.getInt(address + 3), 3);
            assertEquals(UnsafeMemoryAccess.getLong(address + 7), 4);
        }
        finally
        {
            UnsafeMemoryAccess.free(address);
        }
    }

    @Test
    public void testBytesGetterSetter()
    {
        final long address = UnsafeMemoryAccess.allocate(128);
        try
        {
            byte[] bytes = new byte[128];
            ThreadLocalRandom.current().nextBytes(bytes);
            bytes[bytes.length-1] = 0;
            UnsafeCopy.copyBufferToMemory(address, ByteBuffer.wrap(bytes));
            byte[] bytesRead = new byte[128];
            UnsafeCopy.copy0(null, address, bytesRead, BYTE_ARRAY_BASE_OFFSET, 128);
            assertArrayEquals(bytes, bytesRead);
            assertEquals(0,
               UnsafeOperations.compare0(null, address, 128,
                                         bytesRead, BYTE_ARRAY_BASE_OFFSET, 128));

            bytesRead[bytesRead.length-1] = 1;
            assertNotEquals(0,
               UnsafeOperations.compare0(null, address, 128,
                                         bytesRead, BYTE_ARRAY_BASE_OFFSET, 128));
        }
        finally
        {
            UnsafeMemoryAccess.free(address);
        }
    }
}
