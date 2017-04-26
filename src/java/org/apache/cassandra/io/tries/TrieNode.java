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
package org.apache.cassandra.io.tries;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.utils.PageAware;

/**
 * Trie node types and manipulation mechanisms. The main purpose of this is to allow for handling tries directly as
 * they are on disk without any serialization, and to enable the creation of such files.
 *
 * The serialization methods take as argument a generic {@code SerializationNode} and provide a method {@code typeFor}
 * for choosing a suitable type to represent it, which can then be used to calculate size and write the node. When
 * caller is certain all chilren of the node can fit within the same page, the {@code inpageTypeFor} method can be used
 * instead to achieve better packing.
 *
 * To read a file containing trie nodes, one would use {@code at} to identify the node type and then the various
 * read methods to retrieve the data. They all take a buffer (usually memory-mapped) containing the data, and a position
 * in it that identifies the node.
 *
 * These node types do not specify any treatment of payloads. They are only concerned with providing 4 bits of
 * space for {@code payloadFlags}, and a way of calculating the position after the node. Users of this class by convention
 * use non-zero payloadFlags to indicate a payload exists, write it (possibly in flag-dependent format) at serialization
 * time after the node itself is written, and read it using the {@code payloadPosition} value.
 *
 * To improve efficiency, multiple node types depending on the number of transitions are provided:
 *   -- payload only, which has no outgoing transitions
 *   -- single outgoing transition
 *   -- sparse, which provides a list of transition bytes with corresponding targets
 *   -- dense, where the transitions span a range of values and having the list (and the search in it) can be avoided
 *
 * For each of the transition-carrying types we also have "in-page" versions where transition targets are the 12 lowest
 * bits of the position within the same page. To save one further byte, the single in-page version cannot carry a
 * payload. 
 *
 * This class is effectively an enumeration; abstract class permits instances to extends each other and reuse code.
 */
public abstract class TrieNode
{
    // Consumption (read) methods

    /**
     * Returns the type of node stored at this position. It can then be used to call the methods below.
     */
    public static TrieNode at(ByteBuffer src, int position)
    {
        return values[(src.get(position) >> 4) & 0xF];
    }

    /** Returns the 4 payload flag bits. Node types that cannot carry a payload return 0. */
    public int payloadFlags(ByteBuffer src, int position)
    {
        return src.get(position) & 0x0F;
    }
    /**
     * Return the position just after the node, where the payload is usually stored.
     */
    abstract public int payloadPosition(ByteBuffer src, int position);
    /**
     * Returns search index for the given byte in the node. If exact match is present, this is >= 0, otherwise as in
     * binary search.
     */
    abstract public int search(ByteBuffer src, int position, int transitionByte);       // returns as binarySearch
    /**
     * Returns the byte value for this child index, or Integer.MAX_VALUE if there are no transitions with this index or
     * higher. Argument must be >= 0.
     */
    abstract public int transitionByte(ByteBuffer src, int position, int childIndex);
    /**
     * Returns the upper childIndex limit. Calling transition with values 0 .. transitionRange - 1 is valid.
     */
    abstract public int transitionRange(ByteBuffer src, int position);
    /**
     * Returns position of node to transition to for the given search index. Argument must be positive. May return -1
     * if a transition with that index does not exist (DENSE nodes).
     * Position is the offset of the node within the ByteBuffer. positionLong is its global placement, which is the
     * base for any offset calculations.
     */
    abstract public long transition(ByteBuffer src, int position, long positionLong, int searchIndex);
    /**
     * Returns the highest transition for this node, or -1 if none exist (PAYLOAD_ONLY nodes).
     */
    public long lastTransition(ByteBuffer src, int position, long positionLong)
    {
        return transition(src, position, positionLong, transitionRange(src, position) - 1);
    }
    /**
     * Returns a transition that is higher than the index returned by {@code search}. This may not exist (if the
     * argument was higher than the last transition byte), in which case this returns the given {@code defaultValue}.
     */
    abstract public long greaterTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue);
    /**
     * Returns a transition that is lower than the index returned by {@code search}. Should not be called for
     * searchIndex == 0 / -1, otherwise such transition always exists.
     */
    abstract public long lesserTransition(ByteBuffer src, int position, long positionLong, int searchIndex);

    // Construction (serialization) methods

    /**
     * Returns a node type that is suitable to store the node.
     */
    public static TrieNode typeFor(SerializationNode<?> node, long nodePosition)
    {
        int c = node.childCount();
        if (c == 0)
            return PAYLOAD_ONLY;
        int bppIndex = 0;
        for (int i = 0; i < c; ++i)
        {
            long childPosition = node.serializedPosition(i);
            if (childPosition == -1)
                continue;
            while (!singles[bppIndex].fits(childPosition, nodePosition))
                ++bppIndex;
        }
        if (bppIndex == 0)
            return inpageTypeFor(node);
        if (c == 1)
            return singles[bppIndex];
        TrieNode sparse = sparses[bppIndex];
        TrieNode dense = denses[bppIndex];
        return (sparse.sizeofNode(node) < dense.sizeofNode(node)) ? sparse : dense;
    }

    static final long FORWARD_MAX = PageAware.PAGE_SIZE;

    /**
     * Returns a node type that is suitable to store the node if all transition targets are assumed to be in the
     * same page.
     */
    public static TrieNode inpageTypeFor(SerializationNode<?> node)
    {
        int c = node.childCount();
        if (c == 0)
            return PAYLOAD_ONLY;
        if (c == 1)
            return node.payload() != null ? SINGLE_16 : INPAGE_SINGLE;
        int l = node.transition(0) & 0xFF;
        int r = node.transition(c - 1) & 0xFF;
        if (r - l + 1 <= c * 5 / 3)
            return INPAGE_DENSE;
        return INPAGE_SPARSE;
    }

    /**
     * Returns the size needed to serialize this node.
     */
    abstract public int sizeofNode(SerializationNode<?> node);
    /**
     * Serializes the node. All transition target positions must already have been defined. {@code payloadBits} must
     * be four bits.
     */
    abstract public void serialize(DataOutput out, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException;

    // Implementations

    final int bytesPerPointer;

    TrieNode(int bytesPerPointer)
    {
        this.bytesPerPointer = bytesPerPointer;
    }

    int ordinal = -1;

    static final PayloadOnly PAYLOAD_ONLY = new PayloadOnly();
    static class PayloadOnly extends TrieNode
    {
        // byte flags
        // var payload
        PayloadOnly()
        {
            super(0);
        }

        @Override
        public int payloadPosition(ByteBuffer src, int position)
        {
            return position + 1;
        }

        @Override
        public int search(ByteBuffer src, int position, int transitionByte)
        {
            return -1;
        }

        @Override
        public long transition(ByteBuffer src, int position, long positionLong, int searchIndex)
        {
            return -1;
        }

        @Override
        public long lastTransition(ByteBuffer src, int position, long positionLong)
        {
            return -1;
        }

        @Override
        public long greaterTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue)
        {
            return defaultValue;
        }

        @Override
        public long lesserTransition(ByteBuffer src, int position, long positionLong, int searchIndex)
        {
            return -1;
        }

        @Override
        public int transitionByte(ByteBuffer src, int position, int childIndex)
        {
            return Integer.MAX_VALUE;
        }

        @Override
        public int transitionRange(ByteBuffer src, int position)
        {
            return 0;
        }

        public int sizeofNode(SerializationNode<?> node)
        {
            return 1;
        }

        @Override
        public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            dest.writeByte((ordinal << 4) + (payloadBits & 0x0F));
        }
    };

    static final Single SINGLE_16 = new Single(2);
    static final Single SINGLE_24 = new Single(3);
    static final Single SINGLE_32 = new Single(4);

    static class Single extends TrieNode
    {
        // byte flags
        // byte transition
        // bytesPerPointer bytes transition target
        // var payload

        Single(int bytesPerPointer)
        {
            super(bytesPerPointer);
        }

        @Override
        public int payloadPosition(ByteBuffer src, int position)
        {
            return position + 2 + bytesPerPointer;
        }

        @Override
        public int search(ByteBuffer src, int position, int transitionByte)
        {
            int c = src.get(position + 1) & 0xFF;
            if (transitionByte == c)
                return 0;
            return transitionByte < c ? -1 : -2;
        }

        public long transition(ByteBuffer src, int position, long positionLong, int searchIndex)
        {
            return readBytes(src, position + 2) + positionLong;
        }

        @Override
        public long lastTransition(ByteBuffer src, int position, long positionLong)
        {
            return transition(src, position, positionLong, 0);
        }

        @Override
        public long greaterTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue)
        {
            return (searchIndex == -1) ? transition(src, position, positionLong, 0) : defaultValue;
        }

        @Override
        public long lesserTransition(ByteBuffer src, int position, long positionLong, int searchIndex)
        {
            return transition(src, position, positionLong, 0);
        }

        @Override
        public int transitionByte(ByteBuffer src, int position, int childIndex)
        {
            return childIndex == 0 ? src.get(position + 1) & 0xFF : Integer.MAX_VALUE;
        }

        @Override
        public int transitionRange(ByteBuffer src, int position)
        {
            return 1;
        }

        public int sizeofNode(SerializationNode<?> node)
        {
            return 2 + bytesPerPointer;
        }

        @Override
        public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            int childCount = node.childCount();
            assert childCount == 1;
            dest.writeByte((ordinal << 4) + (payloadBits & 0x0F));

            dest.writeByte(node.transition(0));
            writeBytes(dest, node.serializedPosition(0) - nodePosition);
        }
    };

    static final Single INPAGE_SINGLE = new InPageSingle();
    static class InPageSingle extends Single
    {
        // 4-bit type ordinal
        // 12-bit target offset from page start
        // byte transition
        // no payload!
        InPageSingle()
        {
            super(1);
        }

        @Override
        public int payloadFlags(ByteBuffer src, int position)
        {
            return 0;
        }

        @Override
        public int search(ByteBuffer src, int position, int transitionByte)
        {
            int c = src.get(position + 2) & 0xFF;
            if (transitionByte == c)
                return 0;
            return transitionByte < c ? -1 : -2;
        }

        @Override
        public long transition(ByteBuffer src, int position, long positionLong, int searchIndex)
        {
            return (src.getShort(position) & 0xFFF) | (positionLong & ~0xFFF);
        }

        @Override
        public int transitionByte(ByteBuffer src, int position, int childIndex)
        {
            return childIndex == 0 ? src.get(position + 2) & 0xFF : Integer.MAX_VALUE;
        }

        @Override
        public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            assert payloadBits == 0;
            int childCount = node.childCount();
            assert childCount == 1;
            int offset = (int) node.serializedPosition(0) & 0xFFF;
            dest.writeByte((ordinal << 4) + ((offset >> 8) & 0x0F));
            dest.writeByte((byte) offset);
            dest.writeByte(node.transition(0));
        }
    };

    static final Sparse SPARSE_16 = new Sparse(2);
    static final Sparse SPARSE_24 = new Sparse(3);
    static final Sparse SPARSE_32 = new Sparse(4);
    static final Sparse SPARSE_40 = new Sparse(5);
    static class Sparse extends TrieNode
    {
        // byte flags
        // byte count (<= 255)
        // count bytes transitions
        // count ints transition targets
        // var payload

        Sparse(int bytesPerPointer)
        {
            super(bytesPerPointer);
        }

        public int transitionRange(ByteBuffer src, int position)
        {
            return src.get(position + 1) & 0xFF;
        }

        public int payloadPosition(ByteBuffer src, int position)
        {
            int count = transitionRange(src, position);
            return position + 2 + (bytesPerPointer + 1) * count;
        }

        public int search(ByteBuffer src, int position, int key)
        {
            int l = -1; // known < key
            int r = transitionRange(src, position);   // known > key
            position += 2;

            while (l + 1 < r)
            {
                int m = (l + r + 1) / 2;
                int childTransition = src.get(position + m) & 0xFF;
                int cmp = Integer.compare(key, childTransition);
                if (cmp < 0)
                    r = m;
                else if (cmp > 0)
                    l = m;
                else
                    return m;
            }

            return -r - 1;
        }

        public long transition(ByteBuffer src, int position, long positionLong, int searchIndex)
        {
            return readBytes(src, position + 2 + transitionRange(src, position) + bytesPerPointer * searchIndex) + positionLong;
        }

        public long greaterTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue)
        {
            if (searchIndex < 0)
                searchIndex = -1 - searchIndex;
            else
                ++searchIndex;
            if (searchIndex >= transitionRange(src, position))
                return defaultValue;
            return transition(src, position, positionLong, searchIndex);
        }

        public long lesserTransition(ByteBuffer src, int position, long positionLong, int searchIndex)
        {
            if (searchIndex < 0)
                searchIndex = -2 - searchIndex;
            else
                --searchIndex;
            return transition(src, position, positionLong, searchIndex);
        }

        @Override
        public int transitionByte(ByteBuffer src, int position, int childIndex)
        {
            return childIndex < transitionRange(src, position) ? src.get(position + 2 + childIndex) & 0xFF : Integer.MAX_VALUE;
        }

        public int sizeofNode(SerializationNode<?> node)
        {
            return 2 + node.childCount() * (1 + bytesPerPointer);
        }

        @Override
        public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            int childCount = node.childCount();
            assert childCount > 0;
            assert childCount < 256;
            dest.writeByte((ordinal << 4) + (payloadBits & 0x0F));
            dest.writeByte(childCount);

            for (int i = 0; i < childCount; ++i)
                dest.writeByte(node.transition(i));
            for (int i = 0; i < childCount; ++i)
                writeBytes(dest, node.serializedPosition(i) - nodePosition);
        }
    };
    
    static final InPageSparse INPAGE_SPARSE = new InPageSparse();
    static class InPageSparse extends Sparse
    {
        // byte flags
        // byte count (<= 255)
        // count bytes transitions
        // count 12-bits transition targets
        // var payload
        InPageSparse()
        {
            super(1);
        }

        public int payloadPosition(ByteBuffer src, int position)
        {
            int count = transitionRange(src, position);
            return position + 2 + (5 * count + 1) / 2;
        }

        public long transition(ByteBuffer src, int position, long positionLong, int searchIndex)
        {
            return read12Bits(src, position + 2 + transitionRange(src, position), searchIndex) | (positionLong & ~0xFFF);
        }

        public int sizeofNode(SerializationNode<?> node)
        {
            return 2 + (node.childCount() * 5 + 1) / 2;
        }

        @Override
        public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            int childCount = node.childCount();
            assert childCount < 256;
            dest.writeByte((ordinal << 4) + (payloadBits & 0x0F));
            dest.writeByte(childCount);

            for (int i = 0; i < childCount; ++i)
                dest.writeByte(node.transition(i));
            int i;
            for (i = 0; i + 2 <= childCount; i += 2)
            {
                int p0 = (int) node.serializedPosition(i + 0) & 0xFFF;
                int p1 = (int) node.serializedPosition(i + 1) & 0xFFF;
                dest.writeByte(p0 >> 4);
                dest.writeByte((p0 << 4) | (p1 >> 8));
                dest.writeByte(p1);
            }
            if (i < childCount)
                dest.writeShort((short) (node.serializedPosition(i) & 0xFFF) << 4);
        }
    };

    static final Dense DENSE_16 = new Dense(2);
    static final Dense DENSE_24 = new Dense(3);
    static final Dense DENSE_32 = new Dense(4);
    static final Dense DENSE_40 = new Dense(5);
    static class Dense extends TrieNode
    {
        // byte flags
        // byte start
        // byte length-1
        // length ints transition targets (-1 for not present)
        // var payload

        Dense(int bytesPerPointer)
        {
            super(bytesPerPointer);
        }

        public int transitionRange(ByteBuffer src, int position)
        {
            return 1 + (src.get(position + 2) & 0xFF);
        }

        public int payloadPosition(ByteBuffer src, int position)
        {
            return position + 3 + transitionRange(src, position) * bytesPerPointer;
        }

        public int search(ByteBuffer src, int position, int transitionByte)
        {
            int l = src.get(position + 1) & 0xFF;
            int i = transitionByte - l;
            if (i < 0)
                return -1;
            int len = transitionRange(src, position);
            if (i >= len)
                return -len - 1;
            long t = transition(src, position, 0L, i);
            return t != -1 ? i : -i - 1;
        }

        public long transition(ByteBuffer src, int position, long positionLong, int searchIndex)
        {
            long v = readBytes(src, position + 3 + searchIndex * bytesPerPointer);
            return v != -1 ? v + positionLong : -1;
        }

        public long greaterTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue)
        {
            if (searchIndex < 0)
                searchIndex = -1 - searchIndex;
            else
                ++searchIndex;
            int len = transitionRange(src, position);
            for (; searchIndex < len; ++searchIndex)
            {
                long t = transition(src, position, positionLong, searchIndex);
                if (t != -1)
                    return t;
            }
            return defaultValue;
        }

        public long lesserTransition(ByteBuffer src, int position, long positionLong, int searchIndex)
        {
            if (searchIndex < 0)
                searchIndex = -2 - searchIndex;
            else
                --searchIndex;
            for (; searchIndex >= 0; --searchIndex)
            {
                long t = transition(src, position, positionLong, searchIndex);
                if (t != -1)
                    return t;
            }
            assert false : "transition must always exist at 0 and we should not be called for less of that";
            return -1;
        }

        public int transitionByte(ByteBuffer src, int position, int childIndex)
        {
            if (childIndex >= transitionRange(src, position))
                return Integer.MAX_VALUE;
            int l = src.get(position + 1) & 0xFF;
            return l + childIndex;
        }

        public int sizeofNode(SerializationNode<?> node)
        {
            int l = node.transition(0) & 0xFF;
            int r = node.transition(node.childCount() - 1) & 0xFF;
            return 3 + (r - l + 1) * bytesPerPointer;
        }

        @Override
        public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            int childCount = node.childCount();
            dest.writeByte((ordinal << 4) + (payloadBits & 0x0F));
            int l = node.transition(0) & 0xFF;
            int r = node.transition(childCount - 1) & 0xFF;
            dest.writeByte(l);
            dest.writeByte(r - l);      // r is included, i.e. this is len - 1

            for (int i = 0; i < childCount; ++i)
            {
                int next = node.transition(i) & 0xFF;
                while (l < next)
                {
                    writeBytes(dest, -1);
                    ++l;
                }
                writeTarget(dest, node.serializedPosition(i), nodePosition);
                ++l;
            }
        }

        public void writeTarget(DataOutput dest, long target, long nodePosition) throws IOException
        {
            writeBytes(dest, target - nodePosition);
        }
    };

    static final InPageDense INPAGE_DENSE = new InPageDense();
    static class InPageDense extends Dense
    {
        // byte flags
        // byte start
        // byte length-1
        // length 12-bits transition targets (-1 for not present)
        // var payload

        InPageDense()
        {
            super(1);
        }

        public int payloadPosition(ByteBuffer src, int position)
        {
            return position + 3 + (transitionRange(src, position) * 3 + 1) / 2;
        }

        public long transition(ByteBuffer src, int position, long positionLong, int searchIndex)
        {
            int res = read12Bits(src, position + 3, searchIndex);
            return res == 0xFFF ? -1 : res | (positionLong & ~0xFFF);
        }

        public int sizeofNode(SerializationNode<?> node)
        {
            int l = node.transition(0) & 0xFF;
            int r = node.transition(node.childCount() - 1) & 0xFF;
            return 3 + ((r - l + 1) * 3 + 1) / 2;
        }

        @Override
        public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            int childCount = node.childCount();
            dest.writeByte((ordinal << 4) + (payloadBits & 0x0F));
            int l = node.transition(0) & 0xFF;
            int r = node.transition(childCount - 1) & 0xFF;
            dest.writeByte(l);
            dest.writeByte(r - l);      // r is included, i.e. this is len - 1

            int carry = 0;
            int start = l;
            for (int i = 0; i < childCount; ++i)
            {
                int next = node.transition(i) & 0xFF;
                while (l < next)
                {
                    carry = write12Bits(dest, 0xFFF, l - start, carry);
                    ++l;
                }
                assert (node.serializedPosition(i) & 0xFFF) != 0xFFF;
                carry = write12Bits(dest, (int) node.serializedPosition(i) & 0xFFF, l - start, carry);
                ++l;
            }
            if (((l - start) & 1) == 1)
                dest.writeByte(carry);
        }

    };

    static final LongDense LONG_DENSE = new LongDense();
    static class LongDense extends Dense
    {
        // byte flags
        // byte start
        // byte length-1
        // length long transition targets (-1 for not present)
        // var payload
        LongDense()
        {
            super(8);
        }

        public long transition(ByteBuffer src, int position, long positionLong, int childIndex)
        {
            return src.getLong(position + 3 + childIndex * 8);
        }

        @Override
        public void writeTarget(DataOutput dest, long target, long nodePosition) throws IOException
        {
            dest.writeLong(target);
        }
        
        @Override
        public void writeBytes(DataOutput dest, long ofs) throws IOException
        {
            assert ofs == -1;
            dest.writeLong(ofs);
        }
        
        @Override
        boolean fits(long target, long nodePosition)
        {
            return true;
        }
    };


    static int read12Bits(ByteBuffer src, int base, int searchIndex)
    {
        int word = src.getShort(base + (3 * searchIndex) / 2);
        if ((searchIndex & 1) == 0)
            word = (word >> 4);
        return (word & 0xFFF);
    }

    static int write12Bits(DataOutput dest, int value, int index, int carry) throws IOException
    {
        if ((index & 1) == 0)
        {
            dest.writeByte(value >> 4);
            return value << 4;
        }
        else
        {
            dest.writeByte(carry | (value >> 8));
            dest.writeByte(value);
            return 0;
        }
    }

    long readBytes(ByteBuffer src, int position)
    {
        long ofs = 0;
        for (int i = 0; i < bytesPerPointer; ++i)
            ofs = (ofs << 8) + (src.get(position + i) & 0xFF);
        if (ofs >= FORWARD_MAX)
            ofs -= (1L << 8 * bytesPerPointer);
        return ofs;
    }

    void writeBytes(DataOutput dest, long ofs) throws IOException
    {
        assert ofs < FORWARD_MAX && ofs >= (-1L << bytesPerPointer * 8) + FORWARD_MAX;
        for (int i = bytesPerPointer - 1; i >= 0; --i)
            dest.writeByte((int) (ofs >> (i * 8)));
    }

    boolean fits(long target, long nodePosition)
    {
        if (bytesPerPointer == 1)       // in-page
            return PageAware.pageStart(target) == PageAware.pageStart(nodePosition);
        long ofs = target - nodePosition;
        return ofs < FORWARD_MAX && ofs >= (-1L << bytesPerPointer * 8) + FORWARD_MAX;
    }

    public String toString()
    {
        String res = getClass().getSimpleName();
        if (bytesPerPointer > 1)
            res += (bytesPerPointer * 8);
        return res;
    }

    public static Object nodeTypeString(int ordinal)
    {
        return values[ordinal].toString();
    }

    static final TrieNode[] values = new TrieNode[] { PAYLOAD_ONLY,
                                                      INPAGE_SINGLE, INPAGE_SPARSE, INPAGE_DENSE,
                                                      SINGLE_16, SPARSE_16, DENSE_16,
                                                      SINGLE_24, SPARSE_24, DENSE_24,
                                                      SINGLE_32, SPARSE_32, DENSE_32,
                                                      SPARSE_40, DENSE_40,
                                                      LONG_DENSE};
    // There are only 16 possible types, and we want to encode 5 pointer sizes * 3 types plus PAYLOAD_ONLY and LONG_DENSE.
    // SINGLE_40 does not fit, thus we replace it with something that can do its job using one extra byte.
    static final TrieNode[] singles = new TrieNode[] { INPAGE_SINGLE, SINGLE_16, SINGLE_24, SINGLE_32, DENSE_40, LONG_DENSE };
    static final TrieNode[] sparses = new TrieNode[] { INPAGE_SPARSE, SPARSE_16, SPARSE_24, SPARSE_32, SPARSE_40, LONG_DENSE };
    static final TrieNode[] denses = new TrieNode[] { INPAGE_DENSE, DENSE_16, DENSE_24, DENSE_32, DENSE_40, LONG_DENSE };
    static
    {
        assert values.length <= 16;
        for (int i = 0; i < values.length; ++i)
            values[i].ordinal = i;
    }

    public static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[] { (byte) (PAYLOAD_ONLY.ordinal << 4) } );
}