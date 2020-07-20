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

import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteComparable;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.Pair;

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
     * Indicates if the type encoding supports rounding of the raw value.
     *
     * This is significant in range searches where we have to make all range
     * queries inclusive when searching the indexes in order to avoid excluding
     * rounded values. Excluded values are removed by post-filtering.
     */
    public static boolean supportsRounding(AbstractType<?> type)
    {
        return type instanceof IntegerType;
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
        else if (type instanceof IntegerType)
            return 20;
        return 16;
    }

    public static AbstractType<?> cellValueType(Pair<ColumnMetadata, IndexTarget.Type> target)
    {
        if (target.left.type.isCollection() && target.left.type.isMultiCell())
        {
            switch (((CollectionType<?>)target.left.type).kind)
            {
                case LIST:
                    return ((CollectionType<?>) target.left.type).valueComparator();
                case SET:
                    return ((CollectionType<?>) target.left.type).nameComparator();
                case MAP:
                    switch (target.right)
                    {
                        case KEYS:
                            return ((CollectionType<?>) target.left.type).nameComparator();
                        case VALUES:
                            return ((CollectionType<?>) target.left.type).valueComparator();
                        case KEYS_AND_VALUES:
                            return CompositeType.getInstance(((CollectionType<?>) target.left.type).nameComparator(),
                                                             ((CollectionType<?>) target.left.type).valueComparator());
                    }
            }
        }
        return target.left.type;
    }

    /**
     * Returns <code>true</code> if values of the given {@link AbstractType} should be indexed as literals.
     */
    public static boolean isLiteral(AbstractType<?> type)
    {
        return type instanceof UTF8Type ||
               type instanceof AsciiType ||
               type instanceof CompositeType ||
               (type.isCollection() && !type.isMultiCell());
    }

    /**
     * Allows overriding the default getString method for {@link CompositeType}. It is
     * a requirement of the {@link ConcurrentRadixTree} that the keys are strings but
     * the getString method of {@link CompositeType} does not return a string that compares
     * in the same order as the underlying {@link ByteBuffer}. To get round this we convert
     * the {@link CompositeType} bytes to a hex string.
     */
    public static String getString(ByteBuffer value, AbstractType<?> type)
    {
        if (type instanceof CompositeType)
            return ByteBufferUtil.bytesToHex(value);
        return type.getString(value);
    }

    /**
     * The inverse of the above method. Overrides the fromString method on {@link CompositeType}
     * in order to convert the hex string to bytes.
     */
    public static ByteBuffer fromString(String value, AbstractType<?> type)
    {
        if (type instanceof CompositeType)
            return ByteBufferUtil.hexToBytes(value);
        return type.fromString(value);
    }

    /**
     * Fills a byte array with the comparable bytes for a type.
     */
    public static void toComparableBytes(ByteBuffer value, AbstractType<?> type, byte[] bytes)
    {
        if (type instanceof InetAddressType)
            ByteBufferUtil.arrayCopy(value, value.hasArray() ? value.arrayOffset() + value.position() : value.position(), bytes, 0, 16);
        else if (type instanceof IntegerType)
            ByteBufferUtil.arrayCopy(value, value.hasArray() ? value.arrayOffset() + value.position() : value.position(), bytes, 0, 20);
        else
            ByteBufferUtil.toBytes(type.asComparableBytes(value, ByteComparable.Version.OSS41), bytes);
    }

    /**
     * Encode an external term from a memtable index or a compaction. The purpose of this is to
     * allow terms of particular types to be handled differently and not use the default
     * {@link ByteComparable} encoding.
     */
    public static ByteBuffer encode(ByteBuffer value, AbstractType<?> type)
    {
        if (value == null)
            return null;

        if (type instanceof InetAddressType)
            return encodeInetAddress(value);
        else if (type instanceof IntegerType)
            return encodeBigInteger(value);
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
            return compareInet(b1, b2);
            // BigInteger values, frozen types and composite types (map entries) use compareUnsigned to maintain
            // a consistent order between the in-memory index and the on-disk index.
        else if ((type instanceof IntegerType) || (type.isCollection() && !type.isMultiCell()) || (type instanceof CompositeType))
            return FastByteOperations.compareUnsigned(b1, b2);

        return type.compare(b1, b2 );
    }

    /**
     * This is used for value comparison in post-filtering - {@link Expression#isSatisfiedBy(ByteBuffer)}.
     *
     * This allows types to decide whether they should be compared based on their encoded value or their
     * raw value. At present only {@link InetAddressType} values are compared by their encoded values to
     * allow for ipv4 -> ipv6 equivalency in searches.
     */
    public static int comparePostFilter(Expression.Value requestedValue, Expression.Value columnValue, AbstractType<?> type)
    {
        if (type instanceof InetAddressType)
            return compareInet(requestedValue.encoded, columnValue.encoded);
            // Override comparisons for frozen collections and composite types (map entries)
        else if ((type.isCollection() && !type.isMultiCell()) || (type instanceof CompositeType))
            return FastByteOperations.compareUnsigned(requestedValue.raw, columnValue.raw);

        return type.compare(requestedValue.raw, columnValue.raw);
    }

    public static Iterator<ByteBuffer> collectionIterator(AbstractType<?> validator,
                                                          ComplexColumnData cellData,
                                                          Pair<ColumnMetadata, IndexTarget.Type> target,
                                                          int nowInSecs)
    {
        if (cellData == null)
            return null;

        Stream<ByteBuffer> stream = StreamSupport.stream(cellData.spliterator(), false).filter(cell -> cell != null && cell.isLive(nowInSecs))
                                                               .map(cell -> cellValue(target, cell));

        if (validator instanceof InetAddressType)
            stream = stream.sorted((c1, c2) -> compareInet(encodeInetAddress(c1), encodeInetAddress(c2)));

        return stream.iterator();
    }

    public static Comparator<ByteBuffer> comparator(AbstractType<?> type)
    {
        // Override the comparator for BigInteger, frozen collections and composite types
        if ((type instanceof IntegerType) || (type.isCollection() && !type.isMultiCell()) || (type instanceof CompositeType))
            return FastByteOperations::compareUnsigned;

        return type;
    }

    private static ByteBuffer cellValue(Pair<ColumnMetadata, IndexTarget.Type> target, Cell<?> cell)
    {
        if (target.left.type.isCollection() && target.left.type.isMultiCell())
        {
            switch (((CollectionType<?>) target.left.type).kind)
            {
                case LIST:
                    return cell.buffer();
                case SET:
                    return cell.path().get(0);
                case MAP:
                    switch (target.right)
                    {
                        case KEYS:
                            return cell.path().get(0);
                        case VALUES:
                            return cell.buffer();
                        case KEYS_AND_VALUES:
                            return CompositeType.build(ByteBufferAccessor.instance, cell.path().get(0), cell.buffer());
                    }
            }
        }
        return cell.buffer();
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

    /**
     * Encode a {@link InetAddress} into a fixed width 16 byte encoded value.
     *
     * The encoded value is byte comparable and prefix compressible.
     *
     * The encoding is done by converting ipv4 addresses to their ipv6 equivalent.
     */
    private static ByteBuffer encodeInetAddress(ByteBuffer value)
    {
        if (value.remaining() == 4)
        {
            int position = value.hasArray() ? value.arrayOffset() + value.position() : value.position();
            ByteBuffer mapped = ByteBuffer.allocate(16);
            System.arraycopy(IPV4_PREFIX, 0, mapped.array(), 0, IPV4_PREFIX.length);
            ByteBufferUtil.arrayCopy(value, position, mapped, IPV4_PREFIX.length, value.remaining());
            return mapped;
        }
        return value;
    }

    /**
     * Encode a {@link BigInteger} into a fixed width 20 byte encoded value.
     *
     * The encoded value is byte comparable and prefix compressible.
     *
     * The format of the encoding is:
     *
     *  The first 4 bytes contain the length of the {@link BigInteger} byte array
     *  with the top bit flipped for positive values.
     *
     *  The remaining 16 bytes contain the 16 most significant bytes of the
     *  {@link BigInteger} byte array.
     *
     *  For {@link BigInteger} values whose underlying byte array is less than
     *  16 bytes, the encoded value is sign extended.
     */
    private static ByteBuffer encodeBigInteger(ByteBuffer value)
    {
        int size = value.remaining();
        int position = value.hasArray() ? value.arrayOffset() + value.position() : value.position();
        byte[] bytes = new byte[20];
        if (size < 16)
        {
            ByteBufferUtil.arrayCopy(value, position, bytes, bytes.length - size, size);
            if ((bytes[bytes.length - size] & 0x80) != 0)
                Arrays.fill(bytes, 4, bytes.length - size, (byte)0xff);
            else
                Arrays.fill(bytes, 4, bytes.length - size, (byte)0x00);
        }
        else
        {
            ByteBufferUtil.arrayCopy(value, position, bytes, 4, 16);
        }
        if ((bytes[4] & 0x80) != 0)
        {
            size = -size;
        }
        bytes[0] = (byte)(size >> 24 & 0xff);
        bytes[1] = (byte)(size >> 16 & 0xff);
        bytes[2] = (byte)(size >> 8 & 0xff);
        bytes[3] = (byte)(size & 0xff);
        bytes[0] ^= 0x80;
        return ByteBuffer.wrap(bytes);
    }
}
