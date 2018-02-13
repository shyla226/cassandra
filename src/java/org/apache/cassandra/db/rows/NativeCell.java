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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.UnsafeCopy;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeCell extends AbstractCell
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new NativeCell());

    private static final long HAS_CELLPATH = 0;
    private static final long TIMESTAMP = 1;
    private static final long TTL = 9;
    private static final long DELETION = 13;
    private static final long LENGTH = 17;
    private static final long VALUE = 21;

    private final long peer;

    private NativeCell()
    {
        super(null);
        this.peer = 0;
    }

    public NativeCell(NativeAllocator allocator,
                      Cell cell)
    {
        this(allocator,
             cell.column(),
             cell.timestamp(),
             cell.ttl(),
             cell.localDeletionTime(),
             cell.value(),
             cell.path());
    }

    public NativeCell(NativeAllocator allocator,
                      ColumnMetadata column,
                      long timestamp,
                      int ttl,
                      int localDeletionTime,
                      ByteBuffer value,
                      CellPath path)
    {
        super(column);
        long size = simpleSize(value.remaining());

        assert value.order() == ByteOrder.BIG_ENDIAN;
        assert column.isComplex() == (path != null);
        if (path != null)
        {
            assert path.size() == 1;
            size += 4 + path.get(0).remaining();
        }

        if (size > Integer.MAX_VALUE)
            throw new IllegalStateException();

        // cellpath? : timestamp : ttl : localDeletionTime : length : <data> : [cell path length] : [<cell path data>]
        peer = allocator.allocate((int) size);
        UnsafeMemoryAccess.setByte(peer + HAS_CELLPATH, (byte)(path == null ? 0 : 1));
        UnsafeMemoryAccess.setLong(peer + TIMESTAMP, timestamp);
        UnsafeMemoryAccess.setInt(peer + TTL, ttl);
        UnsafeMemoryAccess.setInt(peer + DELETION, localDeletionTime);
        UnsafeMemoryAccess.setInt(peer + LENGTH, value.remaining());
        UnsafeCopy.copyBufferToMemory(peer + VALUE, value);

        if (path != null)
        {
            ByteBuffer pathbuffer = path.get(0);
            assert pathbuffer.order() == ByteOrder.BIG_ENDIAN;

            long offset = peer + VALUE + value.remaining();
            UnsafeMemoryAccess.setInt(offset, pathbuffer.remaining());
            UnsafeCopy.copyBufferToMemory(offset + 4, pathbuffer);
        }
    }

    private static long simpleSize(int length)
    {
        return VALUE + length;
    }

    public long timestamp()
    {
        return UnsafeMemoryAccess.getLong(peer + TIMESTAMP);
    }

    public int ttl()
    {
        return UnsafeMemoryAccess.getInt(peer + TTL);
    }

    public int localDeletionTime()
    {
        return UnsafeMemoryAccess.getInt(peer + DELETION);
    }

    public ByteBuffer value()
    {
        int length = UnsafeMemoryAccess.getInt(peer + LENGTH);
        return UnsafeByteBufferAccess.getByteBuffer(peer + VALUE, length, ByteOrder.BIG_ENDIAN);
    }

    public CellPath path()
    {
        if (UnsafeMemoryAccess.getByte(peer + HAS_CELLPATH) == 0)
            return null;

        long offset = peer + VALUE + UnsafeMemoryAccess.getInt(peer + LENGTH);
        int size = UnsafeMemoryAccess.getInt(offset);
        return CellPath.create(UnsafeByteBufferAccess.getByteBuffer(offset + 4, size, ByteOrder.BIG_ENDIAN));
    }

    public Cell withUpdatedValue(ByteBuffer newValue)
    {
        throw new UnsupportedOperationException();
    }

    public Cell withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, int newLocalDeletionTime)
    {
        return new BufferCell(column, newTimestamp, ttl(), newLocalDeletionTime, value(), path());
    }

    public Cell withUpdatedColumn(ColumnMetadata column)
    {
        return new BufferCell(column, timestamp(), ttl(), localDeletionTime(), value(), path());
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE;
    }

}
