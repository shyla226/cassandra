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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * This is a guard against breaking the unversioned and under documented serialization properties of the AbstractType
 * properties. This is documenting current behaviour as standard until we have time to invest in sorting out the mess.
 */
public class TypeValueLengthTest
{
    static final int VARIABLE_LENGTH = -1;

    @Test
    public void testVariableLength()
    {
        assertEquals(VARIABLE_LENGTH, ByteType.instance.valueLengthIfFixed());// Gaaaaah!!!! I know
        assertEquals(VARIABLE_LENGTH, ShortType.instance.valueLengthIfFixed());// Gaaaaah!!!! I know
        assertEquals(VARIABLE_LENGTH, IntegerType.instance.valueLengthIfFixed());
        assertEquals(VARIABLE_LENGTH, CompositeType.getInstance(Int32Type.instance).valueLengthIfFixed());
        assertEquals(VARIABLE_LENGTH, DecimalType.instance.valueLengthIfFixed());
        assertEquals(VARIABLE_LENGTH, AsciiType.instance.valueLengthIfFixed());
        assertEquals(VARIABLE_LENGTH, UTF8Type.instance.valueLengthIfFixed());
        assertEquals(VARIABLE_LENGTH, ListType.getInstance(Int32Type.instance, true).valueLengthIfFixed());
        assertEquals(VARIABLE_LENGTH, MapType.getInstance(Int32Type.instance,Int32Type.instance, true).valueLengthIfFixed());
        assertEquals(VARIABLE_LENGTH, ListType.getInstance(Int32Type.instance, false).valueLengthIfFixed());
        assertEquals(VARIABLE_LENGTH, MapType.getInstance(Int32Type.instance,Int32Type.instance, false).valueLengthIfFixed());

        assertEquals(VARIABLE_LENGTH, ReversedType.getInstance(IntegerType.instance).valueLengthIfFixed());
    }

    @Test
    public void testFixedLength()
    {
        assertEquals(1, BooleanType.instance.valueLengthIfFixed());
        assertEquals(8, DateType.instance.valueLengthIfFixed());
        assertEquals(8, DoubleType.instance.valueLengthIfFixed());
        assertEquals(0, EmptyType.instance.valueLengthIfFixed());
        assertEquals(4, FloatType.instance.valueLengthIfFixed());
        assertEquals(4, Int32Type.instance.valueLengthIfFixed());
        assertEquals(16, LexicalUUIDType.instance.valueLengthIfFixed());
        assertEquals(8, LongType.instance.valueLengthIfFixed());
        assertEquals(8, TimestampType.instance.valueLengthIfFixed());
        assertEquals(16, TimeUUIDType.instance.valueLengthIfFixed());
        assertEquals(16, UUIDType.instance.valueLengthIfFixed());
        assertEquals(16, ReversedType.getInstance(UUIDType.instance).valueLengthIfFixed());
    }
}
