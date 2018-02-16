/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;

import com.google.common.collect.Range;

import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Represents a list of timestamps.
 */
interface Timestamps
{
    static Timestamps NO_TIMESTAMP = new Timestamps()
    {
        @Override
        public Timestamps get(int index)
        {
            return this;
        }

        @Override
        public Timestamps slice(Range<Integer> range)
        {
            return this;
        }

        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public ByteBuffer toByteBuffer(ProtocolVersion protocolVersion)
        {
            return null;
        }

        @Override
        public String toString()
        {
            return "no timestamp";
        }
    };

    /**
     * Retrieves the timestamps at the specified position.
     * @param index the timestamps position
     * @return the timestamps at the specified position or a {@code NO_TIMESTAMP}
     */
    Timestamps get(int index);

    /**
     * Returns a view of the portion of the timestamps within the specified
     * range
     * <p>The returned {@code Timestamps} is backed by this {@code Timestamps}.</p>
     *
     * @param range the indexes range
     * @return a view of the specified range within this {@code Timestamps}
     */
    Timestamps slice(Range<Integer> range);

    /**
     * Returns the number of timestamps.
     * @return the number of timestamps
     */
    int size();

    /**
     * Converts the timestamps into their serialized form.
     *
     * @param protocolVersion the protocol version to use for the serialization
     * @return the serialized timestamps.
     */
    ByteBuffer toByteBuffer(ProtocolVersion protocolVersion);
}
