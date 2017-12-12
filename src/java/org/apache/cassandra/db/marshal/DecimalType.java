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
package org.apache.cassandra.db.marshal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.DecimalSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;

public class DecimalType extends NumberType<BigDecimal>
{
    public static final DecimalType instance = new DecimalType();

    private static final ArgumentDeserializer ARGUMENT_DESERIALIZER = new DefaultArgumentDerserializer(instance);

    DecimalType() {super(ComparisonType.CUSTOM);} // singleton

    public boolean isEmptyValueMeaningless()
    {
        return true;
    }

    @Override
    public boolean isFloatingPoint()
    {
        return true;
    }

    public int compareCustom(ByteBuffer o1, ByteBuffer o2)
    {
        if (!o1.hasRemaining() || !o2.hasRemaining())
            return o1.hasRemaining() ? 1 : o2.hasRemaining() ? -1 : 0;

        return compose(o1).compareTo(compose(o2));
    }

    /**
     * Constructs a byte-comparable representation. 
     * This is rather difficult and involves reconstructing the decimal.
     *
     * To compare, we need a normalized value, i.e. one with a sign, exponent and (0,1) mantissa. To avoid
     * loss of precision, both exponent and mantissa need to be base-100.  We can't get this directly off the serialized
     * bytes, as they have base-10 scale and base-256 unscaled part.
     *
     * We store:
     *     - sign bit inverted * 0x80 + 0x40 + signed exponent length, where exponent is negated if value is negative
     *     - zero or more exponent bytes (as given by length)
     *     - 0x80 + first pair of decimal digits, negative is value is negative, rounded to -inf
     *     - zero or more 0x80 + pair of decimal digits, always positive
     *     - trailing 0x00
     * Zero is special-cased as 0x80.
     *
     * Because the trailing 00 cannot be produced from a pair of decimal digits (positive or not), no value can be
     * a prefix of another.
     *
     * Encoding examples:
     *    1.1    as       c1 = 0x80 (positive number) + 0x40 + (positive exponent) 0x01 (exp length 1)
     *                    01 = exponent 1 (100^1)
     *                    81 = 0x80 + 01 (0.01)
     *                    8a = 0x80 + 10 (....10)   0.0110e2
     *                    00
     *    -1     as       3f = 0x00 (negative number) + 0x40 - (negative exponent) 0x01 (exp length 1)
     *                    ff = exponent -1. negative number, thus 100^1
     *                    7f = 0x80 - 01 (-0.01)    -0.01e2
     *                    00
     *    -99.9  as       3f = 0x00 (negative number) + 0x40 - (negative exponent) 0x01 (exp length 1)
     *                    ff = exponent -1. negative number, thus 100^1
     *                    1c = 0x80 - 100 (-1.00)
     *                    8a = 0x80 + 10  (+....10) -0.999e2
     *                    00 
     * 
     */
    public ByteSource asByteComparableSource(ByteBuffer buf)
    {
        BigDecimal value = compose(buf);
        if (value.equals(BigDecimal.ZERO))
            return ByteSource.oneByte(0x80);
        long scale = (((long) value.scale()) - value.precision()) & ~1;
        boolean negative = value.signum() < 0;
        final int negmul = negative ? -1 : 1;
        final long exponent = (-scale * negmul) / 2;
        if (scale > Integer.MAX_VALUE || scale < Integer.MIN_VALUE)
        {
            // We are practically out of range here, but let's handle that anyway
            int mv = Long.signum(scale) * Integer.MAX_VALUE;
            value = value.scaleByPowerOfTen(mv);
            scale -= mv;
        }
        final BigDecimal mantissa = value.scaleByPowerOfTen(Ints.checkedCast(scale)).stripTrailingZeros();
        assert mantissa.abs().compareTo(BigDecimal.ONE) < 0;

        return new ByteSource.WithToString()
        {
            int posInExp = 0;
            BigDecimal current = mantissa;

            @Override
            public void reset()
            {
                posInExp = 0;
                current = mantissa;
            }

            @Override
            public int next()
            {
                if (posInExp < 5)
                {
                    if (posInExp == 0)
                    {
                        int absexp = (int) (exponent < 0 ? -exponent : exponent);
                        while (posInExp < 5 && absexp >> (32 - ++posInExp * 8) == 0) {}
                        int explen = 0x40 + (exponent < 0 ? -1 : 1) * (5 - posInExp);
                        return explen + (negative ? 0x00 : 0x80);
                    }
                    else
                        return (int) ((exponent >> (32 - posInExp++ * 8))) & 0xFF;
                }
                if (current == null)
                    return END_OF_STREAM;
                if (current.equals(BigDecimal.ZERO))
                {
                    current = null;
                    return 0x00;
                }
                else
                {
                    BigDecimal v = current.scaleByPowerOfTen(2);
                    BigDecimal floor = v.setScale(0, BigDecimal.ROUND_FLOOR);
                    current = v.subtract(floor);
                    return floor.byteValueExact() + 0x80;
                }
            }
        };
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty()) return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        BigDecimal decimal;

        try
        {
            decimal = new BigDecimal(source);
        }
        catch (Exception e)
        {
            throw new MarshalException(String.format("unable to make BigDecimal from '%s'", source), e);
        }

        return decompose(decimal);
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            return new Constants.Value(getSerializer().serialize(new BigDecimal(parsed.toString())));
        }
        catch (NumberFormatException exc)
        {
            throw new MarshalException(String.format("Value '%s' is not a valid representation of a decimal value", parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return getSerializer().deserialize(buffer).toString();
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.DECIMAL;
    }

    public TypeSerializer<BigDecimal> getSerializer()
    {
        return DecimalSerializer.instance;
    }

    /**
     * Converts the specified number into a <code>BigDecimal</code>.
     *
     * @param value the value to convert
     * @return the converted value
     */
    protected BigDecimal toBigDecimal(Number number)
    {
        if (number instanceof BigDecimal)
            return (BigDecimal) number;

        if (number instanceof BigInteger)
            return new BigDecimal((BigInteger) number);

        double d = number.doubleValue();

        if (Double.isNaN(d))
            throw new NumberFormatException("A NaN cannot be converted into a decimal");

        if (Double.isInfinite(d))
            throw new NumberFormatException("An infinite number cannot be converted into a decimal");

        return BigDecimal.valueOf(d);
    }

    @Override
    public ByteBuffer add(Number left, Number right)
    {
        return decompose(toBigDecimal(left).add(toBigDecimal(right), MathContext.DECIMAL128));
    }

    @Override
    public ByteBuffer substract(Number left, Number right)
    {
        return decompose(toBigDecimal(left).subtract(toBigDecimal(right), MathContext.DECIMAL128));
    }

    @Override
    public ByteBuffer multiply(Number left, Number right)
    {
        return decompose(toBigDecimal(left).multiply(toBigDecimal(right), MathContext.DECIMAL128));
    }

    @Override
    public ByteBuffer divide(Number left, Number right)
    {
        return decompose(toBigDecimal(left).divide(toBigDecimal(right), MathContext.DECIMAL128));
    }

    @Override
    public ByteBuffer mod(Number left, Number right)
    {
        return decompose(toBigDecimal(left).remainder(toBigDecimal(right), MathContext.DECIMAL128));
    }

    @Override
    public ByteBuffer negate(Number input)
    {
        return decompose(toBigDecimal(input).negate());
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return ARGUMENT_DESERIALIZER;
    }
}
