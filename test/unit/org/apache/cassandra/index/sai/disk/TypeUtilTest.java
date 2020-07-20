/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sai.disk;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.utils.ByteComparable;

public class TypeUtilTest extends NdiRandomizedTest
{
    @Test
    public void shouldCompareByteBuffers()
    {
        final ByteBuffer a = Int32Type.instance.decompose(1);
        final ByteBuffer b = Int32Type.instance.decompose(2);

        assertEquals(a, TypeUtil.min(a, b, Int32Type.instance));
        assertEquals(a, TypeUtil.min(b, a, Int32Type.instance));
        assertEquals(a, TypeUtil.min(a, a, Int32Type.instance));
        assertEquals(b, TypeUtil.min(b, b, Int32Type.instance));
        assertEquals(b, TypeUtil.min(null, b, Int32Type.instance));
        assertEquals(a, TypeUtil.min(a, null, Int32Type.instance));

        assertEquals(b, TypeUtil.max(b, a, Int32Type.instance));
        assertEquals(b, TypeUtil.max(a, b, Int32Type.instance));
        assertEquals(a, TypeUtil.max(a, a, Int32Type.instance));
        assertEquals(b, TypeUtil.max(b, b, Int32Type.instance));
        assertEquals(b, TypeUtil.max(null, b, Int32Type.instance));
        assertEquals(a, TypeUtil.max(a, null, Int32Type.instance));
    }

    @Test
    public void testBigIntegerEncoding()
    {
        Random rng = new Random(-9078270684023566599L);

        BigInteger[] data = new BigInteger[10000];
        for (int i = 0; i < data.length; i++)
        {
            BigInteger randomNumber = new BigInteger(rng.nextInt(1000), rng);
            if (rng.nextBoolean())
                randomNumber = randomNumber.negate();

            data[i] = randomNumber;
        }

        Arrays.sort(data, BigInteger::compareTo);

        for (int i = 1; i < data.length; i++)
        {
            BigInteger i0 = data[i - 1];
            BigInteger i1 = data[i];
            assertTrue("#" + i, i0.compareTo(i1) <= 0);

            ByteBuffer b0 = TypeUtil.encode(ByteBuffer.wrap(i0.toByteArray()), IntegerType.instance);
            ByteBuffer b1 = TypeUtil.encode(ByteBuffer.wrap(i1.toByteArray()), IntegerType.instance);
            assertTrue("#" + i, TypeUtil.compare(b0, b1, IntegerType.instance) <= 0);
        }
    }

    @Test
    public void testMapEntryEncoding()
    {
        Random rng = new Random(-9078270684023566599L);
        CompositeType type = CompositeType.getInstance(UTF8Type.instance, Int32Type.instance);

        // simulate: index memtable insertion
        String[] data = new String[10000];
        byte[] temp = new byte[100];
        for (int i = 0; i < data.length; i++)
        {
            rng.nextBytes(temp);
            String v1 = new String(temp);
            int v2 = rng.nextInt();

            data[i] = TypeUtil.getString(type.decompose(v1, v2), type);
        }

        Arrays.sort(data, String::compareTo);

        for (int i = 1; i < data.length; i++)
        {
            // simulate: index memtable flush
            ByteBuffer b0 = TypeUtil.fromString(data[i - 1], type);
            ByteBuffer b1 = TypeUtil.fromString(data[i], type);
            assertTrue("#" + i, TypeUtil.compare(b0, b1, type) <= 0);

            // simulate: saving into on-disk trie
            ByteComparable t0 = ByteComparable.fixedLength(b0);
            ByteComparable t1 = ByteComparable.fixedLength(b1);
            assertTrue("#" + i, ByteComparable.compare(t0, t1, ByteComparable.Version.OSS41) <= 0);
        }
    }
}
