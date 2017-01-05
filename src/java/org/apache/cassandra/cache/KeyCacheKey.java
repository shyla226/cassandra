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
package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;

public class KeyCacheKey extends CacheKey
{
    protected Descriptor desc;

    private static final long EMPTY_SIZE = ObjectSizes.measure(new KeyCacheKey(null, null, ByteBufferUtil.EMPTY_BYTE_BUFFER));

    // keeping an array instead of a ByteBuffer lowers the overhead of the key cache working set,
    // without extra copies on lookup since client-provided key ByteBuffers will be array-backed already
    protected byte[] key;
    protected boolean copyKey;
    protected int keyOffset;
    protected int keyLength;

    public KeyCacheKey(Pair<String, String> ksAndCFName, Descriptor desc, ByteBuffer key)
    {
        this(ksAndCFName, desc, key, true);
    }

    public KeyCacheKey(Pair<String, String> ksAndCFName, Descriptor desc, ByteBuffer key, boolean copyKey)
    {

        super(ksAndCFName);
        this.desc = desc;
        this.copyKey = copyKey || key.isDirect();
        if (this.copyKey)
        {
            this.key = ByteBufferUtil.getArray(key);
            this.keyOffset = 0;
            this.keyLength = this.key.length;
        }
        else
        {
            this.key = key.array();
            this.keyOffset = key.arrayOffset() + key.position();
            this.keyLength = keyOffset + key.remaining();
        }

        assert this.key != null;
    }

    public byte[] key()
    {
        assert copyKey;
        return key;
    }

    public Descriptor desc()
    {
        return desc;
    }

    public String toString()
    {
        return String.format("KeyCacheKey(%s, %s)", desc, ByteBufferUtil.bytesToHex(ByteBuffer.wrap(key, keyOffset, keyLength)));
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + (copyKey ? ObjectSizes.sizeOfArray(key) : 0);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || !o.getClass().isAssignableFrom(getClass())) return false;

        KeyCacheKey that = (KeyCacheKey) o;

        return ksAndCFName.equals(that.ksAndCFName) && desc.equals(that.desc) && FastByteOperations.compareUnsigned(key, keyOffset, keyLength, that.key, that.keyOffset, that.keyLength) == 0;
    }

    @Override
    public int hashCode()
    {
        int result = ksAndCFName.hashCode();
        result = 31 * result + desc.hashCode();
        result = 31 * result + keyHashCode();
        return result;
    }

    private int keyHashCode()
    {
        if (key == null)
            return 0;

        int result = 1;
        for (int i = keyOffset, length = keyLength; i < length; i++)
            result = 31 * result + key[i];

        return result;
    }

}
