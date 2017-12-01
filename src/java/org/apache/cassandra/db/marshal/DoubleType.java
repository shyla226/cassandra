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

import org.apache.commons.lang3.mutable.MutableDouble;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.*;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.*;

public class DoubleType extends NumberType<Double>
{
    public static final DoubleType instance = new DoubleType();

    DoubleType() {super(ComparisonType.FIXED_SIZE_VALUE, 8, FixedSizeType.DOUBLE);} // singleton

    public boolean isEmptyValueMeaningless()
    {
        return true;
    }

    @Override
    public boolean isFloatingPoint()
    {
        return true;
    }

    public static int compareType(ByteBuffer o1, ByteBuffer o2)
    {
        return Double.compare(UnsafeByteBufferAccess.getDouble(o1), UnsafeByteBufferAccess.getDouble(o2));
    }

    public ByteSource asByteComparableSource(ByteBuffer buf)
    {
        return ByteSource.optionalSignedFixedLengthFloat(buf);
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
      // Return an empty ByteBuffer for an empty string.
      if (source.isEmpty())
          return ByteBufferUtil.EMPTY_BYTE_BUFFER;

      try
      {
          return decompose(Double.valueOf(source));
      }
      catch (NumberFormatException e1)
      {
          throw new MarshalException(String.format("Unable to make double from '%s'", source), e1);
      }
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            if (parsed instanceof String)
                return new Constants.Value(fromString((String) parsed));
            else
                return new Constants.Value(getSerializer().serialize(((Number) parsed).doubleValue()));
        }
        catch (ClassCastException exc)
        {
            throw new MarshalException(String.format(
                    "Expected a double value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return getSerializer().deserialize(buffer).toString();
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.DOUBLE;
    }

    public TypeSerializer<Double> getSerializer()
    {
        return DoubleSerializer.instance;
    }

    @Override
    public ByteBuffer add(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.doubleValue() + right.doubleValue());
    }

    @Override
    public ByteBuffer substract(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.doubleValue() - right.doubleValue());
    }

    @Override
    public ByteBuffer multiply(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.doubleValue() * right.doubleValue());
    }

    @Override
    public ByteBuffer divide(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.doubleValue() / right.doubleValue());
    }

    @Override
    public ByteBuffer mod(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.doubleValue() % right.doubleValue());
    }

    @Override
    public ByteBuffer negate(Number input)
    {
        return ByteBufferUtil.bytes(-input.doubleValue());
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return new NumberArgumentDeserializer<MutableDouble>(new MutableDouble())
        {
            @Override
            protected void setMutableValue(MutableDouble mutable, ByteBuffer buffer)
            {
                mutable.setValue(ByteBufferUtil.toDouble(buffer));
            }
        };
    }
}
