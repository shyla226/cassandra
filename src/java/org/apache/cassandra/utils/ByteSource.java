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
import java.nio.charset.StandardCharsets;

/**
 * A source of byte, used for byte-order-comparable representations of data.
 */
public interface ByteSource
{
    /** Get the next byte, unsigned. Must be between 0 and 255, or END_OF_STREAM if there are no more bytes. */
    public int next();
    /** Reset the source for another walk. */
    public void reset();

    /** Value returned if at the end of the source. */
    public static final int END_OF_STREAM = -1;

    /**
     * Escape value. Used, among other things, to mark the end of subcomponents (so that shorter compares before anything longer).
     * Actual zeros in input need to be escaped if this is in use (see Reinterpreter).
     */
    public static final int ESCAPE = 0x00;

    // Zeros are encoded as a sequence of ESCAPE, 0 or more of ESCAPED_0_CONT, ESCAPED_0_DONE so zeroed spaces only grow by 1 byte
    public static final int ESCAPED_0_CONT = 0xFE;
    public static final int ESCAPED_0_DONE = 0xFF;

    // Next component marker.
    public static final int NEXT_COMPONENT = 0x40;
    public static final int NEXT_COMPONENT_NULL = 0x3F;
    public static final int NEXT_COMPONENT_NULL_REVERSED = 0x41;
    // These are special endings, for exclusive/inclusive bounds (i.e. smaller than anything with more components, bigger than anything with more components)
    public static final int LT_NEXT_COMPONENT = 0x20;
    public static final int GT_NEXT_COMPONENT = 0x60;

    /**
     * Reinterprets a byte buffer as a byte-comparable source that has 0s escaped and finishes in an escape.
     * (See ByteSource.Reinterpreter/Multi for explanation.) 
     */
    static ByteSource of(ByteBuffer buf)
    {
        return new Reinterpreter(buf);
    }

    /**
     * Reinterprets a byte array as a byte-comparable source that has 0s escaped and finishes in an escape.
     * (See ByteSource.Reinterpreter/Multi for explanation.) 
     */
    static ByteSource of(byte[] buf)
    {
        return new ReinterpreterArray(buf);
    }

    /**
     * Combines a chain of sources, turning their lexicographical comparison into the combination's byte-comparison.
     */
    static ByteSource of(ByteSource... srcs)
    {
        return new Multi(srcs);
    }

    /**
     * Combines a chain of sources, turning their lexicographical comparison into the combination's byte-comparison,
     * with the included terminator character.
     */
    static ByteSource withTerminator(int terminator, ByteSource... srcs)
    {
        return new Multi(srcs, terminator);
    }

    static ByteSource of(String s)
    {
        return new ReinterpreterArray(s.getBytes(StandardCharsets.UTF_8));
    }

    static public ByteSource of(long value)
    {
        return new Number(value ^ (1L<<63), 8);
    }

    static public ByteSource of(int value)
    {
        return new Number(value ^ (1L<<31), 4);
    }

    /**
     * Produce a source for a signed fixed-length number, also translating empty to null.
     * The first byte has its sign bit inverted, and the rest are passed unchanged.
     * Presumes that the length of the buffer is always either 0 or constant for the type.
     */
    static ByteSource optionalSignedFixedLengthNumber(ByteBuffer b)
    {
        return b.hasRemaining() ? signedFixedLengthNumber(b) : null;
    }

    /**
     * Produce a source for a signed fixed-length number.
     * The first byte has its sign bit inverted, and the rest are passed unchanged.
     * Presumes that the length of the buffer is always constant for the type.
     */
    static ByteSource signedFixedLengthNumber(ByteBuffer b)
    {
        return new SignedFixedLengthNumber(b);
    }

    /**
     * Produce a source for a signed fixed-length floating-point number, also translating empty to null.
     * If sign bit is on, returns negated bytes. If not, add the sign bit value.
     * (Sign of IEEE floats is the highest bit, the rest can be compared in magnitude by byte comparison.)
     * Presumes that the length of the buffer is always either 0 or constant for the type.
     */
    static ByteSource optionalSignedFixedLengthFloat(ByteBuffer b)
    {
        return b.hasRemaining() ? signedFixedLengthFloat(b) : null;
    }

    /**
     * Produce a source for a signed fixed-length floating-point number.
     * If sign bit is on, returns negated bytes. If not, add the sign bit value.
     * (Sign of IEEE floats is the highest bit, the rest can be compared in magnitude by byte comparison.)
     * Presumes that the length of the buffer is always constant for the type.
     */
    static ByteSource signedFixedLengthFloat(ByteBuffer b)
    {
        return new SignedFixedLengthFloat(b);
    }

    static public ByteSource empty()
    {
        return EMPTY;
    }

    /**
     * Returns a separator for two byte sources, i.e. something that is definitely > prevMax, and <= currMin, assuming
     * prevMax < currMin.
     * This returns the shortest prefix of currMin that is greater than prevMax.
     */
    public static ByteSource separatorPrefix(ByteSource prevMax, ByteSource currMin)
    {
        return new Separator(prevMax, currMin, true);
    }

    /**
     * Returns a separator for two byte sources, i.e. something that is definitely > prevMax, and <= currMin, assuming
     * prevMax < currMin.
     * This is a source of length 1 longer than the common prefix of the two sources, with last byte one higher than the
     * prevMax source.
     */
    public static ByteSource separatorGt(ByteSource prevMax, ByteSource currMin)
    {
        return new Separator(prevMax, currMin, false);
    }

    public static int compare(ByteSource bs1, ByteSource bs2)
    {
        if (bs1 == null || bs2 == null)
            return Boolean.compare(bs1 != null, bs2 != null);

        bs1.reset();
        bs2.reset();
        while (true)
        {
            int b1 = bs1.next();
            int b2 = bs2.next();
            int cmp = Integer.compare(b1, b2);
            if (cmp != 0)
                return cmp;
            if (b1 == ByteSource.END_OF_STREAM)
                return 0;
        }
    }

    public static ByteSource oneByte(int i)
    {
        assert i >= 0 && i <= 0xFF;
        return new WithToString()
        {
            boolean given = false;
            public int next()
            {
                if (given)
                    return END_OF_STREAM;
                given = true;
                return i;
            }

            public void reset()
            {
                given = false;
            }
        };
    }

    public static ByteSource cut(ByteSource src, int cutoff)
    {
        return new WithToString()
        {
            int pos = 0;

            @Override
            public int next()
            {
                return pos++ < cutoff ? src.next() : END_OF_STREAM;
            }

            @Override
            public void reset()
            {
                src.reset();
                pos = 0;
            }
        };
    }

    public static int diffPoint(ByteSource s1, ByteSource s2)
    {
        s1.reset();
        s2.reset();
        int pos = 1;
        int b;
        while ((b = s1.next()) == s2.next() && b != END_OF_STREAM)
            ++pos;
        return pos;
    }

    static ByteSource MAX = new ByteSource()
    {
        public int next()
        {
            return 0xFF;
        }

        public void reset()
        {
        }

        public String toString()
        {
            return "MAX";
        }
    };

    public static ByteSource max()
    {
        return MAX;
    }

    abstract class WithToString implements ByteSource
    {
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            reset();
            for (int b = next(); b != END_OF_STREAM; b = next())
                builder.append(Integer.toHexString((b >> 4) & 0xF)).append(Integer.toHexString(b & 0xF));
            return builder.toString();
        }
    }

    static ByteSource EMPTY = new WithToString()
    {
        public int next()
        {
            return END_OF_STREAM;
        }

        public void reset()
        {
        }
    };

    static final int NONE = -2;

    /**
     * Variable-length encoding. Escapes 0s as ESCAPE + zero or more ESCAPED_0_CONT + ESCAPED_0_DONE.
     * Finishes with an escape value (to which Multi will add non-zero component separator)
     * E.g. A00B translates to 4100FEFF4200
     *      A0B0               4100FF4200FE00
     *
     * If in a single byte source, the bytes could be simply passed unchanged, but this would not allow us to
     * combine components. This translation preserves order, and since the encoding for 0 is higher than the separator
     * also makes sure shorter components are treated as smaller.
     */
    static class Reinterpreter extends WithToString
    {
        ByteBuffer buf;
        int bufpos;
        boolean escaped;

        Reinterpreter(ByteBuffer buf)
        {
            this.buf = buf;
            reset();
        }

        public int next()
        {
            if (bufpos >= buf.limit())
            {
                if (escaped)
                {
                    escaped = false;
                    return ESCAPED_0_CONT;
                }
                return bufpos++ > buf.limit() ? END_OF_STREAM : ESCAPE;
            }

            int b = buf.get(bufpos++) & 0xFF;
            if (!escaped)
            {
                if (b == ESCAPE)
                    escaped = true;
                return b;
            }
            else
            {
                if (b == ESCAPE)
                    return ESCAPED_0_CONT;
                --bufpos;
                escaped = false;
                return ESCAPED_0_DONE;
            }
        }

        public void reset()
        {
            bufpos = buf.position();
            escaped = false;
        }
    }

    /**
     * Variable-length encoding. Escapes 0s as ESCAPE + zero or more ESCAPED_0_CONT + ESCAPED_0_DONE.
     * Finishes with an escape value (to which Multi will add non-zero component separator)
     * E.g. A00B translates to 4100FEFF4200
     *      A0B0               4100FF4200FF00
     *
     * If in a single byte source, the bytes could be simply passed unchanged, but this would not allow us to
     * combine components. This translation preserves order, and since the encoding for 0 is higher than the separator
     * also makes sure shorter components are treated as smaller.
     */
    static class ReinterpreterArray extends WithToString
    {
        byte[] buf;
        int bufpos;
        boolean escaped;

        ReinterpreterArray(byte[] buf)
        {
            this.buf = buf;
            reset();
        }

        public int next()
        {
            if (bufpos >= buf.length)
            {
                if (escaped)
                {
                    escaped = false;
                    return ESCAPED_0_CONT;
                }
                return bufpos++ > buf.length ? END_OF_STREAM : ESCAPE;
            }

            int b = buf[bufpos++] & 0xFF;
            if (!escaped)
            {
                if (b == ESCAPE)
                    escaped = true;
                return b;
            }
            else
            {
                if (b == ESCAPE)
                    return ESCAPED_0_CONT;
                --bufpos;
                escaped = false;
                return ESCAPED_0_DONE;
            }
        }

        public void reset()
        {
            bufpos = 0;
            escaped = false;
        }
    }

    /**
     * Fixed length signed number encoding. Inverts first bit (so that neg < pos), then just posts all bytes from the
     * buffer. Assumes buffer is of correct length.
     */
    static class SignedFixedLengthNumber extends WithToString
    {
        ByteBuffer buf;
        int bufpos;

        public SignedFixedLengthNumber(ByteBuffer buf)
        {
            this.buf = buf;
            bufpos = buf.position();
        }

        public int next()
        {
            if (bufpos >= buf.limit())
                return END_OF_STREAM;
            int v = buf.get(bufpos) & 0xFF;
            if (bufpos == buf.position())
                v ^= 0x80;
            ++bufpos;
            return v;
        }

        public void reset()
        {
            bufpos = buf.position();
        }
    }

    static class Number extends WithToString
    {
        final int length;
        final long value;
        int pos;

        public Number(long value, int length)
        {
            this.length = length;
            this.value = value;
            reset();
        }

        public void reset()
        {
            pos = length;
        }

        public int next()
        {
            if (pos == 0)
                return END_OF_STREAM;
            return (int) ((value >> (--pos * 8)) & 0xFF);
        }
    }

    /**
     * Fixed length signed floating point number encoding. First bit is sign. If positive, add sign bit value to make
     * greater than all negatives. If not, invert all content to make negatives with bigger magnitude smaller.
     */
    static class SignedFixedLengthFloat extends WithToString
    {
        ByteBuffer buf;
        int bufpos;
        boolean invert;

        public SignedFixedLengthFloat(ByteBuffer buf)
        {
            this.buf = buf;
            bufpos = buf.position();
        }

        public int next()
        {
            if (bufpos >= buf.limit())
                return END_OF_STREAM;
            int v = buf.get(bufpos) & 0xFF;
            if (bufpos == buf.position())
            {
                invert = v >= 0x80;
                v |= 0x80;
            }
            if (invert)
                v = v ^ 0xFF;
            ++bufpos;
            return v;
        }

        public void reset()
        {
            bufpos = buf.position();
        }
    }

    /**
     * Combination of multiple byte sources. Adds NEXT_COMPONENT before sources, or NEXT_COMPONENT_NULL if next is null.
     */
    static class Multi extends WithToString
    {
        final ByteSource[] srcs;
        int srcnum = -1;
        int terminator;

        Multi(ByteSource[] srcs)
        {
            this(srcs, END_OF_STREAM);
        }

        Multi(ByteSource[] srcs, int terminator)
        {
            this.srcs = srcs;
            this.terminator = terminator;
        }

        public int next()
        {
            if (srcnum == srcs.length)
                return END_OF_STREAM;

            int b = END_OF_STREAM;
            if (srcnum >= 0 && srcs[srcnum] != null)
                b = srcs[srcnum].next();
            if (b > END_OF_STREAM)
                return b;

            ++srcnum;
            if (srcnum == srcs.length)
                return terminator;
            if (srcs[srcnum] == null)
                return NEXT_COMPONENT_NULL;
            srcs[srcnum].reset();
            return NEXT_COMPONENT;
        }

        public void reset()
        {
            srcnum = -1;
        }
    }

    /**
     * Construct the shortest prefix of prevMax (for useCurr = false) or currMin (for useCurr = true) that separates
     * the two byte sources.
     */
    static class Separator extends ByteSource.WithToString
    {
        final ByteSource prev;
        final ByteSource curr;
        boolean done = false;
        final boolean useCurr;

        Separator(ByteSource prevMax, ByteSource currMin, boolean useCurr)
        {
            this.prev = prevMax;
            this.curr = currMin;
            this.useCurr = useCurr;
        }

        public int next()
        {
            if (done)
                return END_OF_STREAM;
            int p = prev.next();
            int c = curr.next();
            assert p <= c : prev + " not less than " + curr;
            if (p == c)
                return c;
            done = true;
            return useCurr ? c : p + 1;
        }

        public void reset()
        {
            done = false;
            prev.reset();
            curr.reset();
        }
    }

    static ByteSource optionalFixedLength(ByteBuffer b)
    {
        return b.hasRemaining() ? fixedLength(b) : null;
    }

    public static ByteSource fixedLength(ByteBuffer b)
    {
        return new WithToString()
        {
            int pos = b.position() - 1;

            @Override
            public int next()
            {
                return ++pos < b.limit() ? b.get(pos) & 0xFF : -1;
            }

            @Override
            public void reset()
            {
                pos = b.position() - 1;
            }
        };
    }

    public static ByteSource fourBit(ByteSource s)
    {
        return new WithToString()
        {
            int pos = 0;
            int v = 0;

            @Override
            public int next()
            {
                if ((pos++ & 1) == 0)
                {
                    v = s.next();
                    if (v == END_OF_STREAM)
                        return END_OF_STREAM;
                    return (v >> 4) & 0xF;
                }
                else
                    return v & 0xF;
            }

            @Override
            public void reset()
            {
                s.reset();
                pos = 0;
            }
        };
    }

    /**
     * Splits each byte into portions of bitCount bits.
     * @param s source
     * @param bitCount number of bits to issue at a time, 1-4 make sense
     */
    public static ByteSource splitBytes(ByteSource s, int bitCount)
    {
        return new WithToString()
        {
            int pos = 8;
            int v = 0;
            int mask = (1 << bitCount) - 1;

            @Override
            public int next()
            {
                if ((pos += bitCount) >= 8)
                {
                    pos = 0;
                    v = s.next();
                    if (v == END_OF_STREAM)
                        return END_OF_STREAM;
                }
                v <<= bitCount;
                return (v >> 8) & mask;
            }

            @Override
            public void reset()
            {
                s.reset();
                pos = 8;
            }
        };
    }
}
