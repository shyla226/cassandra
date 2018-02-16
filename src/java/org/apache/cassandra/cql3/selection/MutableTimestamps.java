/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * {@code Timestamps} implementation that can be modified.
 * <p>A {@code MutableTimestamps} has a fix capacity. If the number of timestamps being added exceed the capacity,
 * an {@code IndexOutOfBoundException} will be thrown. To modify the capacity use the {@code capacity} method.</p>
 */
abstract class MutableTimestamps implements Timestamps
{
    /**
     * The timestamps type.
     */
    public enum TimestampsType
    {
        WRITETIMESTAMPS
        {
            @Override
            long getTimestamp(Cell cell, int nowInSecond)
            {
                return cell.timestamp();
            }

            @Override
            long defaultValue()
            {
                return Long.MIN_VALUE;
            }

            @Override
            ByteBuffer toByteBuffer(long timestamp)
            {
                return timestamp == defaultValue() ? null : ByteBufferUtil.bytes(timestamp);
            }
        },
        TTLS
        {
            @Override
            long getTimestamp(Cell cell, int nowInSecond)
            {
                if (!cell.isExpiring())
                    return defaultValue();

                int remaining = cell.localDeletionTime() - nowInSecond;
                return remaining >= 0 ? remaining : defaultValue();
            }

            @Override
            long defaultValue()
            {
                return -1;
            }

            @Override
            ByteBuffer toByteBuffer(long timestamp)
            {
                return timestamp == defaultValue() ? null : ByteBufferUtil.bytes((int) timestamp);
            }
        };

        /**
         * Extracts the timestamp from the specified cell.
         * @param cell the cell
         * @param nowInSecond the query timestamp insecond
         * @return the timestamp corresponding to this type
         */
        abstract long getTimestamp(Cell cell, int nowInSecond);

        /**
         * Returns the value to use when there is no timestamp.
         * @return the value to use when there is no timestamp
         */
        abstract long defaultValue();

        /**
         * Serializes the specified timestamp.
         * @param timestamp the timestamp to serialize
         * @return the bytes corresponding to the specified timestamp
         */
        abstract ByteBuffer toByteBuffer(long timestamp);
    }

    /**
     * The timestamps type.
     */
    protected final TimestampsType type;

    protected MutableTimestamps(TimestampsType type)
    {
        this.type = type;
    }

    /**
     * Returns the timestamps type.
     * @return the timestamps type
     */
    public TimestampsType type()
    {
        return type;
    }

    /**
     * Changes the list capacity.
     * @param newCapacity the newCapacity.
     */
    public abstract void capacity(int newCapacity);

    /**
     * Appends an empty timestamp at the end of this list.
     */
    public abstract void addNoTimestamp();

    /**
     * Appends the cell timestamps at the end of this list.
     */
    public abstract void addTimestampFrom(Cell cell, int nowInSecond);

    /**
     * Reset the index position.
     */
    public abstract void reset();

    /**
     * Creates a new {@code MutableTimestamps} instance for the specified column type.
     * @param timestampType the timestamps type
     * @param columnType the column type
     * @return a {@code MutableTimestamps} instance for the specified column type
     */
    static MutableTimestamps newTimestamps(TimestampsType timestampType, AbstractType<?> columnType)
    {
        if (!columnType.isMultiCell())
            return new SingleTimestamps(timestampType);

        // For UserType we know that the size will not change so we can initialize the array with the proper capacity.
        if (columnType instanceof UserType)
            return new MultipleTimestamps(timestampType, ((UserType) columnType).size());

        return new MultipleTimestamps(timestampType, 0);
    }

    /**
     * A {@code MutableTimestamps} that can contains multiple timestamps (for unfrozen collections or UDTs).
     */
    private static final class MultipleTimestamps extends MutableTimestamps
    {
        /**
         * The array used to store the timestamps.
         */
        private long[] timestamps;

        /**
         * The data offset.
         */
        private int offset;

        /**
         * The data capacity starting at the offset.
         */
        private int capacity;

        /**
         * The current index
         */
        private int index;

        public MultipleTimestamps(TimestampsType type, int initialCapacity)
        {
            this(type, new long[initialCapacity], 0, initialCapacity);
        }

        public MultipleTimestamps(TimestampsType type, long[] timestamps, int offset, int initialCapacity)
        {
            super(type);
            this.timestamps = timestamps;
            this.offset = offset;
            this.capacity = initialCapacity;
        }

        @Override
        public void addNoTimestamp()
        {
            validateIndex(index);
            timestamps[offset + index++] = type.defaultValue();
        }

        @Override
        public void addTimestampFrom(Cell cell, int nowInSecond)
        {
            validateIndex(index);
            timestamps[offset + index++] = type.getTimestamp(cell, nowInSecond);
        }

        @Override
        public void capacity(int newCapacity)
        {
            if (offset != 0 || timestamps.length != newCapacity)
            {
                timestamps = new long[newCapacity];
                offset = 0;
                capacity = newCapacity;
            }
        }

        @Override
        public void reset()
        {
            index = 0;
        }

        @Override
        public Timestamps get(int index)
        {
            if (index < 0 && index >= capacity)
                return NO_TIMESTAMP;

            return new SingleTimestamps(type, timestamps[offset + index]);
        }

        @Override
        public int size()
        {
            return index;
        }

        @Override
        public Timestamps slice(Range<Integer> range)
        {
            if (range.isEmpty())
                return NO_TIMESTAMP;

            int from = !range.hasLowerBound() ? 0
                                              : range.lowerBoundType() == BoundType.CLOSED ? range.lowerEndpoint()
                                                                                           : range.lowerEndpoint() + 1;
            int to = !range.hasUpperBound() ? size() - 1
                                            : range.upperBoundType() == BoundType.CLOSED ? range.upperEndpoint()
                                            : range.upperEndpoint() - 1;

            int sliceSize = to - from + 1;
            MultipleTimestamps slice = new MultipleTimestamps(type, timestamps, from, sliceSize);
            slice.index = sliceSize;
            return slice;
        }

        @Override
        public ByteBuffer toByteBuffer(ProtocolVersion protocolVersion)
        {
            List<ByteBuffer> buffers = new ArrayList<>(index);
            for (int i = 0, m = size() ; i < m; i++)
            {
                long timestamp = timestamps[offset + i];
                buffers.add(type.toByteBuffer(timestamp));
            }

            if (buffers.isEmpty())
                return null;

            return CollectionSerializer.pack(buffers, capacity, protocolVersion);
        }

        private void validateIndex(int index)
        {
            if (index < 0 && index >= capacity)
                throw new IndexOutOfBoundsException("index: " + index + " capacity: " + capacity);
        }

        @Override
        public String toString()
        {
            return type + ": " + Arrays.toString(Arrays.copyOfRange(timestamps, offset, index));
        }
    }

    private static final class SingleTimestamps extends MutableTimestamps
    {
        private long timestamp;

        public SingleTimestamps(TimestampsType type)
        {
            this(type, type.defaultValue());
        }

        public SingleTimestamps(TimestampsType type, long timestamp)
        {
            super(type);
            this.timestamp = timestamp;
        }

        @Override
        public void addNoTimestamp()
        {
            timestamp = type.defaultValue();
        }

        @Override
        public void addTimestampFrom(Cell cell, int nowInSecond)
        {
            timestamp = type.getTimestamp(cell, nowInSecond);
        }

        @Override
        public void capacity(int newCapacity)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset()
        {
            timestamp = type.defaultValue();
        }

        @Override
        public Timestamps get(int index)
        {
            // If this method is called it means that it is an element selection on a frozen collection/UDT
            // so we can safely return this Timestamps has all the elements also share that Timestamps
            return this;
        }

        @Override
        public Timestamps slice(Range<Integer> range)
        {
            if (range.isEmpty())
                return NO_TIMESTAMP;

            return this;
        }

        @Override
        public int size()
        {
            return 1;
        }

        @Override
        public ByteBuffer toByteBuffer(ProtocolVersion protocolVersion)
        {
            if (timestamp == type.defaultValue())
                return null;

            return type.toByteBuffer(timestamp);
        }

        @Override
        public String toString()
        {
            return type + ": " + Long.toString(timestamp);
        }
    }
}
