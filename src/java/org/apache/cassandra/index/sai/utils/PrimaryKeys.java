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
package org.apache.cassandra.index.sai.utils;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableSortedSet;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;

/**
 * A sorted set of {@link PrimaryKey}s.
 *
 * The primary keys are sorted first by token, then by partition key value, and then by clustering.
 */
public interface PrimaryKeys extends Iterable<PrimaryKey>
{
    /**
     * Returns a new empty {@link PrimaryKey} sorted set using the specified clustering comparator.
     *
     * @param clusteringComparator a clustering comparator
     * @return a new empty primary key set
     */
    static PrimaryKeys create(ClusteringComparator clusteringComparator)
    {
        return clusteringComparator.size() == 0 ? new Skinny() : new Wide(clusteringComparator);
    }

    /**
     * Adds the specified {@link PrimaryKey}.
     *
     * @param key a primary key
     */
    default void add(PrimaryKey key)
    {
        add(key.partitionKey(), key.clustering());
    }

    /**
     * Adds the primary key defined by the specified partition key and clustering.
     *
     * @param key a partition key
     * @param clustering a clustering key
     */
    void add(DecoratedKey key, Clustering clustering);

    /**
     * Returns all the partition keys.
     *
     * @return all the partition keys
     */
    SortedSet<DecoratedKey> partitionKeys();

    /**
     * Returns the clustering keys for the specified partition key.
     *
     * @param key a partition key
     * @return the clustering keys for {@code key}, or {@code null} if the key is not present
     */
    @Nullable
    SortedSet<Clustering> get(DecoratedKey key);

    /**
     * Returns the number of primary keys.
     *
     * @return the number of primary keys
     */
    int size();

    /**
     * Returns if this primary key set is empty.
     *
     * @return {@code true} if this is empty, {@code false} otherwise
     */
    boolean isEmpty();

    Stream<PrimaryKey> stream();

    @Override
    @SuppressWarnings("NullableProblems")
    default Iterator<PrimaryKey> iterator()
    {
        return stream().iterator();
    }

    /**
     * A {@link PrimaryKeys} implementation for tables without a defined clustering key,
     * in which case the clustering key is always {@link Clustering#EMPTY}.
     */
    @ThreadSafe
    class Skinny implements PrimaryKeys
    {
        private static final ImmutableSortedSet<Clustering> clusterings =
                ImmutableSortedSet.orderedBy((Clustering x, Clustering y) -> new ClusteringComparator().compare(x, y))
                                  .add(Clustering.EMPTY).build();

        public final ConcurrentSkipListSet<DecoratedKey> keys;

        private Skinny()
        {
            this.keys = new ConcurrentSkipListSet<>();
        }

        @Override
        public void add(DecoratedKey key, Clustering clustering)
        {
            assert clustering == Clustering.EMPTY;
            keys.add(key);
        }

        @Override
        public SortedSet<DecoratedKey> partitionKeys()
        {
            return keys;
        }

        @Override
        public ImmutableSortedSet<Clustering> get(DecoratedKey key)
        {
            return keys.contains(key) ? clusterings : null;
        }

        @Override
        public int size()
        {
            return keys.size();
        }

        @Override
        public boolean isEmpty()
        {
            return keys.isEmpty();
        }

        @Override
        public Stream<PrimaryKey> stream()
        {
            return keys.stream().map(PrimaryKey.Skinny::new);
        }
    }

    /**
     * A {@link PrimaryKeys} implementation for tables with a defined clustering key.
     */
    @ThreadSafe
    class Wide implements PrimaryKeys
    {
        private final ClusteringComparator clusteringComparator;
        private final ConcurrentSkipListMap<DecoratedKey, ConcurrentSkipListSet<Clustering>> keys;

        private Wide(ClusteringComparator clusteringComparator)
        {
            this.clusteringComparator = clusteringComparator;
            this.keys = new ConcurrentSkipListMap<>();
        }

        @Override
        public void add(DecoratedKey key, Clustering clustering)
        {
            ConcurrentSkipListSet<Clustering> keys = this.keys.get(key);

            if (keys == null)
            {
                ConcurrentSkipListSet<Clustering> newKeys = new ConcurrentSkipListSet<>(clusteringComparator);
                keys = this.keys.putIfAbsent(key, newKeys);
                if (keys == null)
                {
                    keys = newKeys;
                }
            }

            keys.add(clustering);
        }

        @Override
        public SortedSet<DecoratedKey> partitionKeys()
        {
            return keys.keySet();
        }

        @Override
        public SortedSet<Clustering> get(DecoratedKey key)
        {
            return keys.get(key);
        }

        @Override
        public int size()
        {
            return keys.values().stream().mapToInt(Set::size).sum();
        }

        @Override
        public boolean isEmpty()
        {
            return keys.isEmpty();
        }

        @Override
        public Stream<PrimaryKey> stream()
        {
            return keys.entrySet().stream().flatMap(e -> e.getValue().stream().map(c -> PrimaryKey.of(e.getKey(), c)));
        }
    }
}
