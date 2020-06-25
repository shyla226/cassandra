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
package org.apache.cassandra.index.sai.utils;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteComparable;
import org.apache.cassandra.utils.FastByteOperations;

public class TypeUtil
{
    private static final byte[] IPV4_PREFIX = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1 };

    private TypeUtil() {}

    /**
     * Returns <code>true</code> if given buffer would pass the {@link AbstractType#validate(ByteBuffer)}
     * check. False otherwise.
     */
    public static boolean isValid(ByteBuffer term, AbstractType<?> validator)
    {
        try
        {
            validator.validate(term);
            return true;
        }
        catch (MarshalException e)
        {
            return false;
        }
    }

    /**
     * Returns the smaller of two {@code ByteBuffer} values, based on the result of {@link
     * #compare(ByteBuffer, ByteBuffer, AbstractType)} comparision.
     */
    public static ByteBuffer min(ByteBuffer a, ByteBuffer b, AbstractType<?> type)
    {
        return a == null ?  b : (b == null || compare(b, a, type) > 0) ? a : b;
    }

    /**
     * Returns the greater of two {@code ByteBuffer} values, based on the result of {@link
     * #compare(ByteBuffer, ByteBuffer, AbstractType)} comparision.
     */
    public static ByteBuffer max(ByteBuffer a, ByteBuffer b, AbstractType<?> type)
    {
        return a == null ?  b : (b == null || compare(b, a, type) < 0) ? a : b;
    }

    /**
     * Returns the value length for the given {@link AbstractType}, selecting 16 for types
     * that officially use VARIABLE_LENGTH but are, in fact, of a fixed length.
     */
    public static int fixedSizeOf(AbstractType<?> type)
    {
        if (type.isValueLengthFixed())
            return type.valueLengthIfFixed();
        else if (type instanceof InetAddressType)
            return 16;
        return 16;
    }

    /**
     * Returns <code>true</code> if values of the given {@link AbstractType} should be indexed as strings.
     */
    public static boolean isString(AbstractType<?> type)
    {
        return type instanceof UTF8Type || type instanceof AsciiType;
    }

    /**
     * Fills a byte array with the comparable bytes for a type.
     */
    public static void toComparableBytes(ByteBuffer value, AbstractType<?> type, byte[] bytes)
    {
        if (type instanceof InetAddressType)
        {
            if (value.hasArray())
                System.arraycopy(value.array(), 0, bytes, 0, 16);
            else
                ByteBufferUtil.arrayCopy(value, 0, bytes, 0, 16);
        }
        else
            ByteBufferUtil.toBytes(type.asComparableBytes(value, ByteComparable.Version.OSS41), bytes);
    }

    /**
     * Encode an external term from a memtable index or a compaction. The purpose of this is to
     * allow terms of particular types to be handled differently and not use the default
     * {@link ByteComparable} encoding.
     *
     * @param value the external term to encode
     * @param type the type of the term
     * @return the encoded term
     */
    public static ByteBuffer encode(ByteBuffer value, AbstractType<?> type)
    {
        if (value == null)
            return null;

        if (type instanceof InetAddressType)
        {
            return encodeInetAddress(value);
        }
        return value;
    }

    /**
     * Compare two terms based on their type. This is used in place of {@link AbstractType#compare(ByteBuffer, ByteBuffer)}
     * so that the default comparison can be overridden for specific types.
     *
     * Note: This should be used for all term comparison
     */
    public static int compare(ByteBuffer b1, ByteBuffer b2, AbstractType<?> type)
    {
        if (type instanceof InetAddressType)
        {
            return compareInet(b1, b2);
        }
        return type.compare(b1, b2 );
    }

    /**
     * Compares 2 InetAddress terms by ensuring that both addresses are represented as
     * ipv6 addresses.
     */
    private static int compareInet(ByteBuffer b1, ByteBuffer b2)
    {
        assert isIPv6(b1) && isIPv6(b2);

        return FastByteOperations.compareUnsigned(b1, b2);
    }

    private static boolean isIPv6(ByteBuffer address)
    {
        return address.remaining() == 16;
    }

    private static ByteBuffer encodeInetAddress(ByteBuffer value)
    {
        if (value.remaining() == 4)
        {
            ByteBuffer mapped = ByteBuffer.allocate(16);
            System.arraycopy(IPV4_PREFIX, 0, mapped.array(), 0, IPV4_PREFIX.length);
            ByteBufferUtil.arrayCopy(value, value.position(), mapped, IPV4_PREFIX.length, value.remaining());
            return mapped;
        }
        return value;
    }
}
