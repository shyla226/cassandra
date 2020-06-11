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
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.serializers.MarshalException;

public class TypeUtil
{
    private TypeUtil() {}

    /**
     * Returns <code>true</code> if given buffer would pass the {@link AbstractType#validate(java.nio.ByteBuffer)}
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
     * AbstractType#compare(java.nio.ByteBuffer, java.nio.ByteBuffer)} comparision.
     */
    public static ByteBuffer min(ByteBuffer a, ByteBuffer b, AbstractType<?> type)
    {
        return a == null ?
               b : (b == null || type.compare(b, a) > 0) ? a : b;
    }

    /**
     * Returns the greater of two {@code ByteBuffer} values, based on the result of {@link
     * AbstractType#compare(java.nio.ByteBuffer, java.nio.ByteBuffer)} comparision.
     */
    public static ByteBuffer max(ByteBuffer a, ByteBuffer b, AbstractType<?> type)
    {
        return a == null ?
               b : (b == null || type.compare(b, a) < 0) ? a : b;
    }

    /**
     * Returns the value length for the given {@link AbstractType}, selecting 16 for types 
     * that officially use VARIABLE_LENGTH but are, in fact, of a fixed length.
     */
    public static int fixedSizeOf(AbstractType<?> type)
    {
        return type.isValueLengthFixed() ? type.valueLengthIfFixed() : 16;
    }

    /**
     * Returns <code>true</code> if values of the given {@link AbstractType} should be indexed as strings.
     */
    public static boolean isString(AbstractType<?> type)
    {
        return type instanceof UTF8Type || type instanceof AsciiType;
    }
}
