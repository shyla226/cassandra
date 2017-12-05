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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.KeyBound;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.IFilter.FilterKey;

/**
 * Represents a decorated key, handy for certain operations
 * where just working with strings gets slow.
 *
 * We do a lot of sorting of DecoratedKeys, so for speed, we assume that tokens correspond one-to-one with keys.
 * This is not quite correct in the case of RandomPartitioner (which uses MD5 to hash keys to tokens);
 * if this matters, you can subclass RP to use a stronger hash, or use a non-lossy tokenization scheme (as in the
 * OrderPreservingPartitioner classes).
 */
public abstract class DecoratedKey extends PartitionPosition implements FilterKey
{
    public static final Comparator<DecoratedKey> comparator = new Comparator<DecoratedKey>()
    {
        public int compare(DecoratedKey o1, DecoratedKey o2)
        {
            return o1.compareTo(o2);
        }
    };
    private int hashCode = -1;

    public DecoratedKey(Token token)
    {
        super(token, PartitionPosition.Kind.ROW_KEY);
        assert token != null;
    }

    @Override
    public int hashCode()
    {
        int currHashCode = this.hashCode;
        if (currHashCode == -1)
        {
            // hash of key is enough
            currHashCode = getKey().hashCode();
            this.hashCode = currHashCode;
        }
        return currHashCode;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null || !(obj instanceof DecoratedKey))
            return false;

        DecoratedKey other = (DecoratedKey)obj;
        return ByteBufferUtil.compareUnsigned(getKey(), other.getKey()) == 0; // we compare faster than BB.equals for array backed BB
    }

    public int compareTo(PartitionPosition pos)
    {
        if (this == pos)
            return 0;

        int cmp = token.compareTo(pos.token);
        if (cmp != 0)
            return cmp;

        // delegate to Token.KeyBound if needed
        if (pos.kind != Kind.ROW_KEY)
            return -pos.compareTo(this);

        return ByteBufferUtil.compareUnsigned(getKey(), ((DecoratedKey) pos).getKey());
    }

    public static int compareTo(IPartitioner partitioner, ByteBuffer key, PartitionPosition position)
    {
        // delegate to Token.KeyBound if needed
        if (!(position instanceof DecoratedKey))
            return -position.compareTo(partitioner.decorateKey(key));

        DecoratedKey otherKey = (DecoratedKey) position;
        int cmp = partitioner.getToken(key).compareTo(otherKey.getToken());
        return cmp == 0 ? ByteBufferUtil.compareUnsigned(key, otherKey.getKey()) : cmp;
    }

    public ByteSource asByteComparableSource()
    {
        return ByteSource.of(token.asByteComparableSource(), ByteSource.of(getKey()));
    }

    public IPartitioner getPartitioner()
    {
        return token.getPartitioner();
    }

    public KeyBound minValue()
    {
        return getPartitioner().getMinimumToken().minKeyBound();
    }

    public boolean isMinimum()
    {
        // A DecoratedKey can never be the minimum position on the ring
        return false;
    }

    @Override
    public String toString()
    {
        String keystring = getKey() == null ? "null" : ByteBufferUtil.bytesToHex(getKey());
        return "DecoratedKey(" + getToken() + ", " + keystring + ")";
    }

    public abstract ByteBuffer getKey();

    public void filterHash(long[] dest)
    {
        ByteBuffer key = getKey();
        MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), 0, dest);
    }
}
