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

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.db.marshal.Int32Type;

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
}
