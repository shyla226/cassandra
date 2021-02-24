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
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * The primary key of a row, composed by the partition key and the clustering key.
 */
public class PrimaryKey implements Comparable<PrimaryKey>
{
    public static final PrimaryKey MINIMUM = new PrimaryKey(Kind.MARKER, null, null, null, Long.MIN_VALUE);
    public static final PrimaryKey MAXIMUM = new PrimaryKey(Kind.MARKER, null, null, null, Long.MAX_VALUE);

    private static final ClusteringComparator EMPTY_COMPARATOR = new ClusteringComparator();

    public enum Kind
    {
        MARKER,
        TOKEN,
        MAPPED,
        UNMAPPED,

    }

    public final Kind kind;
    public final DecoratedKey partitionKey;
    private final Clustering clustering;
    private final ClusteringComparator clusteringComparator;
    private final long sstableRowId;

    public static class PrimaryKeyFactory
    {
        private final IPartitioner partitioner;
        private final ClusteringComparator comparator;

        public PrimaryKeyFactory(TableMetadata tableMetadata)
        {
            this.partitioner = tableMetadata.partitioner;
            this.comparator = tableMetadata.comparator;
        }

        public PrimaryKeyFactory(IPartitioner partitioner, ClusteringComparator comparator)
        {
            this.partitioner = partitioner;
            this.comparator = comparator;
        }

        public PrimaryKey createKey(DecoratedKey partitionKey, Clustering clustering, long sstableRowId)
        {
            return new PrimaryKey(partitionKey, clustering, comparator, sstableRowId);
        }

        public PrimaryKey createKey(DecoratedKey partitionKey, Clustering clustering)
        {
            return new PrimaryKey(partitionKey, clustering, comparator);
        }

        public PrimaryKey createKey(ByteComparable comparable, long sstableRowId)
        {
            ByteSource.Peekable peekable = comparable.asPeekableBytes(ByteComparable.Version.OSS41);
            Token token = partitioner.getTokenFactory().fromComparableBytes(ByteSourceInverse.nextComponentSource(peekable), ByteComparable.Version.OSS41);
            byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable));
            DecoratedKey key =  new BufferDecoratedKey(token, ByteBuffer.wrap(keyBytes));

            if (comparator.size() == 0)
                return new PrimaryKey(key, Clustering.EMPTY, comparator);

            ByteBuffer[] values = new ByteBuffer[comparator.size()];

            byte[] clusteringKeyBytes;
            int index = 0;

            while ((clusteringKeyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable))) != null)
            {
                values[index++] = ByteBuffer.wrap(clusteringKeyBytes);
                if (peekable.peek() == ByteSource.TERMINATOR)
                    break;
            }

            Clustering clustering = Clustering.make(values);

            return new PrimaryKey(key, clustering, comparator, sstableRowId);
        }

        public PrimaryKey createKey(ByteComparable comparable)
        {
            ByteSource.Peekable peekable = comparable.asPeekableBytes(ByteComparable.Version.OSS41);
            Token token = partitioner.getTokenFactory().fromComparableBytes(ByteSourceInverse.nextComponentSource(peekable), ByteComparable.Version.OSS41);
            byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable));
            DecoratedKey key =  new BufferDecoratedKey(token, ByteBuffer.wrap(keyBytes));

            if (comparator.size() == 0)
                return new PrimaryKey(key, Clustering.EMPTY, comparator);

            ByteBuffer[] values = new ByteBuffer[comparator.size()];

            byte[] clusteringKeyBytes;
            int index = 0;

            while ((clusteringKeyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable))) != null)
            {
                values[index++] = ByteBuffer.wrap(clusteringKeyBytes);
                if (peekable.peek() == ByteSource.TERMINATOR)
                    break;
            }

            Clustering clustering = Clustering.make(values);

            return new PrimaryKey(key, clustering, comparator);
        }

        public PrimaryKey createKey(byte[] bytes)
        {
            ByteSource.Peekable peekable = ByteSource.peekable(ByteSource.of(bytes, ByteComparable.Version.OSS41));
            Token token = partitioner.getTokenFactory().fromComparableBytes(ByteSourceInverse.nextComponentSource(peekable), ByteComparable.Version.OSS41);
            byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable));
            DecoratedKey key =  new BufferDecoratedKey(token, ByteBuffer.wrap(keyBytes));

            if (comparator.size() == 0)
                return new PrimaryKey(key, Clustering.EMPTY, comparator);

            ByteBuffer[] values = new ByteBuffer[comparator.size()];

            byte[] clusteringKeyBytes;
            int index = 0;

            while ((clusteringKeyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable))) != null)
            {
                values[index++] = ByteBuffer.wrap(clusteringKeyBytes);
                if (peekable.peek() == ByteSource.TERMINATOR)
                    break;
            }

            Clustering clustering = Clustering.make(values);

            return new PrimaryKey(key, clustering, comparator);
        }

        public PrimaryKey createKey(DecoratedKey key)
        {
            return new PrimaryKey(key, Clustering.EMPTY, EMPTY_COMPARATOR);
        }

        public PrimaryKey createKey(Token token)
        {
            return new PrimaryKey(token);
        }
    }

    public static PrimaryKeyFactory factory(TableMetadata tableMetadata)
    {
        return new PrimaryKeyFactory(tableMetadata);
    }

    public static PrimaryKeyFactory factory(IPartitioner partitioner, ClusteringComparator comparator)
    {
        return new PrimaryKeyFactory(partitioner, comparator);
    }

    public static PrimaryKeyFactory factory()
    {
        return new PrimaryKeyFactory(Murmur3Partitioner.instance, EMPTY_COMPARATOR);
    }


    private static PrimaryKey of(DecoratedKey partitionKey)
    {
        return PrimaryKey.of(partitionKey, Clustering.EMPTY, new ClusteringComparator());
    }
    /**
     * Returns a new primary key composed by the specified partition and clustering keys.
     *
     * @param partitionKey a partition key
     * @param clustering a clustering key
     * @param clusteringComparator the clustering comparator for the table
     * @return a new primary key composed by {@code partitionKey} and {@code ClusteringKey}
     */
    private static PrimaryKey of(DecoratedKey partitionKey, Clustering clustering, ClusteringComparator clusteringComparator)
    {
        return new PrimaryKey(Kind.UNMAPPED, partitionKey, clustering, clusteringComparator, -1);
    }

    private static PrimaryKey of(DecoratedKey partitionKey, Clustering clustering, ClusteringComparator clusteringComparator, long sstableRowId)
    {
        return new PrimaryKey(Kind.MAPPED, partitionKey, clustering, clusteringComparator, sstableRowId);
    }

    private static PrimaryKey of(ByteComparable comparable, IPartitioner partitioner, ClusteringComparator clusteringComparator, long sstableRowId)
    {
        ByteSource.Peekable peekable = comparable.asPeekableBytes(ByteComparable.Version.OSS41);
        Token token = partitioner.getTokenFactory().fromComparableBytes(ByteSourceInverse.nextComponentSource(peekable), ByteComparable.Version.OSS41);
        byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable));
        DecoratedKey key =  new BufferDecoratedKey(token, ByteBuffer.wrap(keyBytes));

        if (clusteringComparator.size() == 0)
            return PrimaryKey.of(key, Clustering.EMPTY, clusteringComparator, sstableRowId);

        ByteBuffer[] values = new ByteBuffer[clusteringComparator.size()];

        byte[] clusteringKeyBytes;
        int index = 0;

        while ((clusteringKeyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable))) != null)
        {
            values[index++] = ByteBuffer.wrap(clusteringKeyBytes);
            if (peekable.peek() == ByteSource.TERMINATOR)
                break;
        }

        Clustering clustering = Clustering.make(values);

        return PrimaryKey.of(key, clustering, clusteringComparator, sstableRowId);
    }

    private static PrimaryKey of(Token token)
    {
        return new PrimaryKey(Kind.TOKEN, new BufferDecoratedKey(token, ByteBufferUtil.EMPTY_BYTE_BUFFER), Clustering.EMPTY, EMPTY_COMPARATOR, -1);
    }

    private PrimaryKey(DecoratedKey partitionKey, Clustering clustering, ClusteringComparator comparator)
    {
        this(Kind.UNMAPPED, partitionKey, clustering, comparator, -1);
    }

    private PrimaryKey(DecoratedKey partitionKey, Clustering clustering, ClusteringComparator comparator, long sstableRowId)
    {
        this(Kind.MAPPED, partitionKey, clustering, comparator, sstableRowId);
    }

    private PrimaryKey(Token token)
    {
        this(Kind.TOKEN, new BufferDecoratedKey(token, ByteBufferUtil.EMPTY_BYTE_BUFFER), Clustering.EMPTY, EMPTY_COMPARATOR, -1);
    }

    private PrimaryKey(Kind kind, DecoratedKey partitionKey, Clustering clustering, ClusteringComparator clusteringComparator, long sstableRowId)
    {
        this.kind = kind;
        this.partitionKey = partitionKey;
        this.clustering = clustering;
        this.clusteringComparator = clusteringComparator;
        this.sstableRowId = sstableRowId;
    }

    public int size()
    {
        assert kind != Kind.MARKER;

        return partitionKey.getKey().remaining() + clustering.dataSize();
    }

    @Override
    public String toString()
    {
        return String.format("PrimaryKey: { partition : %s, clustering: %s} "+getClass().getSimpleName(),
                             ByteBufferUtil.bytesToHex(partitionKey.getKey()),
                             String.join(",", Arrays.stream(clustering.getBufferArray())
                                                    .map(ByteBufferUtil::bytesToHex)
                                                    .collect(Collectors.toList())));
    }

    public byte[] asBytes()
    {
        ByteSource source = asComparableBytes(ByteComparable.Version.OSS41);

    }

    public ByteSource asComparableBytes(ByteComparable.Version version)
    {
        assert kind != Kind.MARKER;

        ByteSource[] sources = new ByteSource[clustering.size() + 2];
        sources[0] = partitionKey.getToken().asComparableBytes(version);
        sources[1] = ByteSource.of(partitionKey.getKey(), version);
        for (int index = 0; index < clustering.size(); index++)
        {
            sources[index + 2] = ByteSource.of(clustering.bufferAt(index), version);
        }
        return ByteSource.withTerminator(version == ByteComparable.Version.LEGACY
                                         ? ByteSource.END_OF_STREAM
                                         : ByteSource.TERMINATOR,
                                         sources);
    }

    public Clustering clustering()
    {
        assert clustering != null && kind != Kind.TOKEN && kind != Kind.MARKER;
        return clustering;
    }

    public ClusteringComparator clusteringComparator()
    {
        assert clusteringComparator != null && kind != Kind.TOKEN && kind != Kind.MARKER;
        return clusteringComparator;
    }

    public long sstableRowId()
    {
        assert sstableRowId >= 0 && kind == Kind.MAPPED && kind != Kind.MARKER;
        return sstableRowId;
    }

    @Override
    public int compareTo(PrimaryKey o)
    {
        if (((this == MINIMUM) && (o == MINIMUM)) || ((this == MAXIMUM) && (o == MAXIMUM)))
            return 0;
        if ((this == MINIMUM) || (o == MAXIMUM))
            return -1;
        if ((this == MAXIMUM) || (o == MINIMUM))
            return 1;
        int cmp = partitionKey.compareTo(o.partitionKey);
        System.out.println("clustering(size = " + clustering.size() + ", kind = " + kind + ", o.clustering(size = " + o.clustering.size() + ", kind = " + o.kind + ")");
        return cmp != 0 ? cmp : clusteringComparator().compare(clustering, o.clustering);
    }
}
