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
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.mockito.InOrder;
import org.mockito.Mockito;

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
    public void testDouble() throws IOException
    {
        testFixedLengthType(DoubleType.instance, (double) 1);
    }

    protected void testFixedLengthType(AbstractType type, Object value) throws IOException
    {
        DataOutputPlus out = mock(DataOutputPlus.class);
        final ByteBuffer valueWrite = type.decompose(value);
        type.writeValue(valueWrite, out);
        verify(out).write(valueWrite);
        Mockito.verifyNoMoreInteractions(out);
        DataInputPlus in = mock(DataInputPlus.class);
        doAnswer(invocationOnMock ->
                 {
                     byte[] bytes = (byte[])invocationOnMock.getArguments()[0];
                     valueWrite.position(0);
                     valueWrite.get(bytes);
                     return null;
                 }).when(in).readFully(any(byte[].class));
        ByteBuffer valueRead = type.readValue(in);
        verify(in).readFully(any(byte[].class));
        assertEquals(valueWrite.clear(), valueRead.clear());
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
        doAnswer(invocationOnMock ->
                 {
                     byte[] bytes = (byte[]) invocationOnMock.getArguments()[0];
                     valueWrite.position(0);
                     valueWrite.get(bytes);
                     return null;
                 }).when(in).readFully(any(byte[].class));
        ByteBuffer valueRead = type.readValue(in);
        verify(in).readUnsignedVInt();
        verify(in).readFully(any(byte[].class));
        assertEquals(valueWrite.clear(), valueRead.clear());
        Mockito.verifyNoMoreInteractions(in);
    }
}
