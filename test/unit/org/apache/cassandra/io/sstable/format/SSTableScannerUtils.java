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

package org.apache.cassandra.io.sstable.format;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.dht.AbstractBounds.isEmpty;

/**
 * Utilities shared by {@link SSTableScannerTest} and {@link AsyncSSTableScannerTest}.
 */
class SSTableScannerUtils
{
    static String toKey(int key)
    {
        return String.format("%03d", key);
    }

    // we produce all DataRange variations that produce an inclusive start and exclusive end range
    static Iterable<DataRange> dataRanges(TableMetadata metadata, int start, int end)
    {
        if (end < start)
            return dataRanges(metadata, start, end, false, true);
        return Iterables.concat(dataRanges(metadata, start, end, false, false),
                                dataRanges(metadata, start, end, false, true),
                                dataRanges(metadata, start, end, true, false),
                                dataRanges(metadata, start, end, true, true)
        );
    }

    static Iterable<DataRange> dataRanges(TableMetadata metadata, int start, int end, boolean inclusiveStart, boolean inclusiveEnd)
    {
        List<DataRange> ranges = new ArrayList<>();
        if (start == end + 1)
        {
            assert !inclusiveStart && inclusiveEnd;
            ranges.add(dataRange(metadata, min(start), false, max(end), true));
            ranges.add(dataRange(metadata, min(start), false, min(end + 1), true));
            ranges.add(dataRange(metadata, max(start - 1), false, max(end), true));
            ranges.add(dataRange(metadata, dk(start - 1), false, dk(start - 1), true));
        }
        else
        {
            for (PartitionPosition s : starts(start, inclusiveStart))
            {
                for (PartitionPosition e : ends(end, inclusiveEnd))
                {
                    if (end < start && e.compareTo(s) > 0)
                        continue;
                    if (!isEmpty(new AbstractBounds.Boundary<>(s, inclusiveStart), new AbstractBounds.Boundary<>(e, inclusiveEnd)))
                        continue;
                    ranges.add(dataRange(metadata, s, inclusiveStart, e, inclusiveEnd));
                }
            }
        }
        return ranges;
    }

    static Iterable<PartitionPosition> starts(int key, boolean inclusive)
    {
        return Arrays.asList(min(key), max(key - 1), dk(inclusive ? key : key - 1));
    }

    static Iterable<PartitionPosition> ends(int key, boolean inclusive)
    {
        return Arrays.asList(max(key), min(key + 1), dk(inclusive ? key : key + 1));
    }

    static DecoratedKey dk(int key)
    {
        return Util.dk(toKey(key));
    }

    static Token token(int key)
    {
        return key == Integer.MIN_VALUE ? Util.testPartitioner().getMinimumToken() : Util.token(toKey(key));
    }

    static PartitionPosition min(int key)
    {
        return token(key).minKeyBound();
    }

    static PartitionPosition max(int key)
    {
        return token(key).maxKeyBound();
    }

    static DataRange dataRange(TableMetadata metadata, PartitionPosition start, boolean startInclusive, PartitionPosition end, boolean endInclusive)
    {
        Slices.Builder sb = new Slices.Builder(metadata.comparator);
        ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(sb.build(), false);

        return new DataRange(AbstractBounds.bounds(start, startInclusive, end, endInclusive), filter);
    }

    static Range<Token> rangeFor(int start, int end)
    {
        return new Range<Token>(token(start),
                                token(end));
    }

    static Collection<Range<Token>> makeRanges(int ... keys)
    {
        Collection<Range<Token>> ranges = new ArrayList<Range<Token>>(keys.length / 2);
        for (int i = 0; i < keys.length; i += 2)
            ranges.add(rangeFor(keys[i], keys[i + 1]));
        return ranges;
    }

    static void insertRowWithKey(TableMetadata metadata, int key)
    {
        long timestamp = System.currentTimeMillis();

        new RowUpdateBuilder(metadata, timestamp, toKey(key))
        .clustering("col")
        .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
        .build()
        .applyUnsafe();
    }

}
