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
package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.AbstractIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.utils.Pair;

public class SkipListMemoryIndex extends MemoryIndex
{
    public static final int CSLM_OVERHEAD = 128; // average overhead of CSLM

    private static final int MINIMUM_QUEUE_SIZE = 128;

    private static final FastThreadLocal<Integer> lastQueueSize = new FastThreadLocal<Integer>()
    {
        protected Integer initialValue()
        {
            return MINIMUM_QUEUE_SIZE;
        }
    };

    private final ConcurrentSkipListMap<ByteBuffer, PrimaryKeys> index;

    public SkipListMemoryIndex(ColumnContext columnContext)
    {
        super(columnContext);
        index = new ConcurrentSkipListMap<>(columnContext.getValidator());
    }

    @Override
    public long add(DecoratedKey key, Clustering clustering, ByteBuffer value)
    {
        if (value != null) setMinMaxTerm(value);

        long overhead = CSLM_OVERHEAD; // DKs are shared
        PrimaryKeys keys = index.get(value);

        if (keys == null)
        {
            PrimaryKeys newKeys = PrimaryKeys.create(columnContext.clusteringComparator());
            keys = index.putIfAbsent(value, newKeys);
            if (keys == null)
            {
                overhead += CSLM_OVERHEAD + value.remaining();
                keys = newKeys;
            }
        }

        keys.add(key, clustering);

        return overhead;
    }

    @Override
    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        ByteBuffer min = expression.lower == null ? null : expression.lower.value;
        ByteBuffer max = expression.upper == null ? null : expression.upper.value;

        SortedMap<ByteBuffer, PrimaryKeys> search;

        if (min == null && max == null)
        {
            throw new IllegalArgumentException();
        }
        if (min != null && max != null)
        {
            search = index.subMap(min, expression.lower.inclusive, max, expression.upper.inclusive);
        }
        else if (min == null)
        {
            search = index.headMap(max, expression.upper.inclusive);
        }
        else
        {
            search = index.tailMap(min, expression.lower.inclusive);
        }

        // With an equality query, there should be at most one value in our results, so avoid
        // the whole priority queue apparatus:
        if (expression.getOp() == Expression.Op.EQ)
        {
            int valueCount = search.size();

            if (valueCount == 0)
            {
                return RangeIterator.empty();
            }
            else if (valueCount == 1)
            {
                ByteBuffer firstKey = search.firstKey();
                PrimaryKeys keys = search.get(firstKey);

                return new KeyRangeIterator(keys.partitionKeys());
            }

            throw new IllegalStateException("There should never be more than one value for an equality query. " + expression);
        }

        long minimumTokenValue = Long.MAX_VALUE;
        long maximumTokenValue = Long.MIN_VALUE;

        // We use the size of the priority queue for the last query on this thread as a simple
        // heuristic for initial size here. Otherwise, we would pay a significant penalty for
        // continuously growing the array backing the queue itself.
        PriorityQueue<DecoratedKey> mergedKeys = new PriorityQueue<>(lastQueueSize.get(), DecoratedKey.comparator);

        for (PrimaryKeys keys : search.values())
        {
            if (keys.isEmpty())
                continue;

            SortedSet<DecoratedKey> partitionKeys = keys.partitionKeys();

            // shortcut to avoid generating iterator
            if (partitionKeys.size() == 1)
            {
                DecoratedKey first = partitionKeys.first();
                if (keyRange.contains(first))
                {
                    mergedKeys.add(first);

                    long currentTokenValue = (long) first.getToken().getTokenValue();
                    minimumTokenValue = Math.min(minimumTokenValue, currentTokenValue);
                    maximumTokenValue = Math.max(maximumTokenValue, currentTokenValue);
                }

                continue;
            }

            // skip entire partition keys if they don't overlap
            if (!keyRange.right.isMinimum() && partitionKeys.first().compareTo(keyRange.right) > 0
                || partitionKeys.last().compareTo(keyRange.left) < 0)
                continue;

            for (DecoratedKey key : partitionKeys)
            {
                if (keyRange.contains(key))
                {
                    mergedKeys.add(key);

                    long currentTokenValue = (long) key.getToken().getTokenValue();
                    minimumTokenValue = Math.min(minimumTokenValue, currentTokenValue);
                    maximumTokenValue = Math.max(maximumTokenValue, currentTokenValue);
                }
            }
        }

        if (mergedKeys.isEmpty())
        {
            return RangeIterator.empty();
        }

        lastQueueSize.set(Math.max(MINIMUM_QUEUE_SIZE, mergedKeys.size()));
        return new KeyRangeIterator(minimumTokenValue, maximumTokenValue, mergedKeys);
    }

    @Override
    public Iterator<Pair<ByteBuffer, PrimaryKeys>> iterator()
    {
        return new AbstractIterator<Pair<ByteBuffer, PrimaryKeys>>()
        {
            final Iterator<Map.Entry<ByteBuffer, PrimaryKeys>> entries = index.entrySet().iterator();

            @Override
            protected Pair<ByteBuffer, PrimaryKeys> computeNext()
            {
                while (entries.hasNext())
                {
                    Map.Entry<ByteBuffer, PrimaryKeys> entry = entries.next();
                    if (entry.getValue() != null && !entry.getValue().isEmpty())
                        return Pair.create(entry.getKey(), entry.getValue());
                }
                return endOfData();
            }
        };
    }
}
