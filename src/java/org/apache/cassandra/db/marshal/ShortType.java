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

import java.nio.ByteBuffer;

import org.apache.commons.lang3.mutable.MutableShort;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.*;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.*;

public class ShortType extends NumberType<Short>
{
    public static final ShortType instance = new ShortType();

    ShortType()
    {
        // VAR_LENGTH due to compatibility reasons, should be 2
        super(ComparisonType.PRIMITIVE_COMPARE, VARIABLE_LENGTH, PrimitiveType.SHORT);
    } // singleton

    public static int compareType(ByteBuffer o1, ByteBuffer o2)
    {
        return Short.compare(UnsafeByteBufferAccess.getShort(o1), UnsafeByteBufferAccess.getShort(o2));
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        short s;

        try
        {
            s = Short.parseShort(source);
        }
        catch (Exception e)
        {
            throw new MarshalException(String.format("Unable to make short from '%s'", source), e);
        }

        return decompose(s);
    }

    public ByteSource asByteComparableSource(ByteBuffer buf)
    {
        return ByteSource.signedFixedLengthNumber(buf);
    }

    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String || parsed instanceof Number)
            return new Constants.Value(fromString(String.valueOf(parsed)));

        throw new MarshalException(String.format(
                "Expected a short value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return getSerializer().deserialize(buffer).toString();
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.SMALLINT;
    }

    public TypeSerializer<Short> getSerializer()
    {
        return ShortSerializer.instance;
    }

    @Override
    public ByteBuffer add(Number left, Number right)
    {
        return ByteBufferUtil.bytes((short) (left.shortValue() + right.shortValue()));
    }

    @Override
    public ByteBuffer substract(Number left, Number right)
    {
        return ByteBufferUtil.bytes((short) (left.shortValue() - right.shortValue()));
    }

    @Override
    public ByteBuffer multiply(Number left, Number right)
    {
        return ByteBufferUtil.bytes((short) (left.shortValue() * right.shortValue()));
    }

    @Override
    public ByteBuffer divide(Number left, Number right)
    {
        return ByteBufferUtil.bytes((short) (left.shortValue() / right.shortValue()));
    }

    public ByteBuffer mod(Number left, Number right)
    {
        return ByteBufferUtil.bytes((short) (left.shortValue() % right.shortValue()));
    }

    public ByteBuffer negate(Number input)
    {
        return ByteBufferUtil.bytes((short) -input.shortValue());
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return new NumberArgumentDeserializer<MutableShort>(new MutableShort())
        {
            @Override
            protected void setMutableValue(MutableShort mutable, ByteBuffer buffer)
            {
                mutable.setValue(ByteBufferUtil.toShort(buffer));
            }
        };
    }
}
