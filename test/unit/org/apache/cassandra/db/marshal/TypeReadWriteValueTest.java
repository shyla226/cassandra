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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * This is a guard against breaking the unversioned and under documented serialization properties of the AbstractType
 * properties. This is documenting current behaviour as standard until we have time to invest in sorting out the mess.
 */
public class TypeReadWriteValueTest
{
    @Test
    public void testByte() throws IOException
    {
        testVariableLengthType(ByteType.instance, (byte) 1, 1);
    }

    @Test
    public void testShort() throws IOException
    {
        testVariableLengthType(ShortType.instance, (short) 1, 2);
    }

    @Test
    public void testInteger2() throws IOException
    {
        testVariableLengthType(IntegerType.instance, new BigInteger(new byte[]{127,1}), 2);
    }

    @Test
    public void testInteger8() throws IOException
    {
        testVariableLengthType(IntegerType.instance, new BigInteger(new byte[]{127,0,0,0,0,0,0,1}), 8);
    }

    @Test
    public void testInteger16() throws IOException
    {
        testVariableLengthType(IntegerType.instance, new BigInteger(new byte[]{127,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1}), 16);
    }

    @Test
    public void testCompositeType() throws IOException
    {
        CompositeType type = CompositeType.getInstance(Int32Type.instance);
        testVariableLengthType(type, ByteBuffer.allocate(4).putInt(1).clear(), 4);
    }

    @Test
    public void testDecimalType() throws IOException
    {
        testVariableLengthType(DecimalType.instance, new BigDecimal("1234"), 6);
    }

    @Test
    public void testAsciiType() throws IOException
    {
        testVariableLengthType(AsciiType.instance, "bugger", 6);
    }

    @Test
    public void testWTF8Type() throws IOException
    {
        testVariableLengthType(UTF8Type.instance, "bugger", 6);
    }

    @Test
    public void testListType() throws IOException
    {
        ListType<Integer> type = ListType.getInstance(Int32Type.instance, false);
        testVariableLengthType(type, Arrays.asList(1, 2, 3, 4), 36);
    }

    @Test
    public void testMapType() throws IOException
    {
        MapType<Integer, Integer> type = MapType.getInstance(Int32Type.instance, Int32Type.instance, false);
        Map<Integer,Integer> map = new HashMap<>();
        map.put(1,2);
        map.put(3,4);
        testVariableLengthType(type, map, 36);
    }

    @Test(expected = AssertionError.class)
    public void testEmpty() throws IOException
    {
        testFixedLengthType(EmptyType.instance, null);
    }

    @Test
    public void testBoolean() throws IOException
    {
        testFixedLengthType(BooleanType.instance, true);
    }

    @Test
    public void testInt32() throws IOException
    {
        testFixedLengthType(Int32Type.instance, 1);
    }

    @Test
    public void testFloat() throws IOException
    {
        testFixedLengthType(FloatType.instance, (float) 1);
    }

    @Test
    public void testLong() throws IOException
    {
        testFixedLengthType(LongType.instance, (long) 1);
    }

    @Test
    public void testTimestamp() throws IOException
    {
        testFixedLengthType(TimestampType.instance, new Date());
    }

    @Test
    public void testDate() throws IOException
    {
        testFixedLengthType(DateType.instance, new Date());
    }

    @Test
    public void testDouble() throws IOException
    {
        testFixedLengthType(DoubleType.instance, (double) 1);
    }

    @Test
    public void testUUID() throws IOException
    {
        testFixedLengthType(UUIDType.instance, new UUID(1234L,1234L));
    }

    @Test
    public void testLexUUID() throws IOException
    {
        testFixedLengthType(LexicalUUIDType.instance, new UUID(1234L,1234L));
    }

    @Test
    public void testTimeUUID() throws IOException
    {
        testFixedLengthType(TimeUUIDType.instance, new UUID(1234L,1234L));
    }

    private void testFixedLengthType(AbstractType type, Object value) throws IOException
    {
        DataOutputPlus out = mock(DataOutputPlus.class);

        final ByteBuffer valueWrite = type.decompose(value);

        type.writeValue(valueWrite, out);
        verify(out).write(valueWrite);
        Mockito.verifyNoMoreInteractions(out);

        DataInputPlus in = mock(DataInputPlus.class);
        doAnswer(copyAnswer(valueWrite)).when(in).readFully(any(byte[].class));

        ByteBuffer valueRead = type.readValue(in);

        verify(in).readFully(any(byte[].class));
        assertEquals(valueWrite.clear(), valueRead.clear());
    }

    private Answer copyAnswer(ByteBuffer valueWrite)
    {
        return invocationOnMock ->
                 {
                     byte[] bytes = (byte[])invocationOnMock.getArguments()[0];
                     valueWrite.position(0);
                     valueWrite.get(bytes);
                     return null;
                 };
    }

    protected void testVariableLengthType(AbstractType type, Object value, long length) throws IOException
    {

        DataOutputPlus out = mock(DataOutputPlus.class);
        ByteBuffer valueWrite = type.decompose(value);

        type.writeValue(valueWrite, out);

        InOrder inOrder = inOrder(out);
        inOrder.verify(out).writeUnsignedVInt(Mockito.eq(length));
        inOrder.verify(out).write(valueWrite);
        Mockito.verifyNoMoreInteractions(out);


        DataInputPlus in = mock(DataInputPlus.class);
        when(in.readUnsignedVInt()).thenReturn(length);
        doAnswer(copyAnswer(valueWrite)).when(in).readFully(any(byte[].class));

        ByteBuffer valueRead = type.readValue(in);

        verify(in).readUnsignedVInt();
        verify(in).readFully(any(byte[].class));
        assertEquals(valueWrite.clear(), valueRead.clear());
        Mockito.verifyNoMoreInteractions(in);
    }
}
