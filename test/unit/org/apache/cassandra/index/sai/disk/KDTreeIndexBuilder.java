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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.junit.Assert;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SSTableContext.KeyFetcher;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.NumericIndexWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListeners;
import org.apache.cassandra.index.sai.utils.AbstractIterator;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.LongArrays;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.index.sai.disk.IndexWriterConfig.defaultConfig;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class KDTreeIndexBuilder
{
    private final IndexComponents indexComponents;
    private final AbstractType<?> type;
    private final AbstractIterator<Pair<ByteBuffer, IntArrayList>> terms;
    private final int size;
    private final int minSegmentRowId;
    private final int maxSegmentRowId;
    private final LongArray segmentRowIdToToken = LongArrays.identity().build();
    private final LongArray segmentRowIdToOffset = LongArrays.identity().build();
    private final KeyFetcher keyFetcher = new KeyFetcher() {
        @Override
        public DecoratedKey apply(RandomAccessReader reader, long keyOffset)
        {
            return dk(String.format("pkvalue_%07d", keyOffset));
        }

        @Override
        public RandomAccessReader createReader()
        {
            return null;
        }
    };

    public KDTreeIndexBuilder(IndexComponents indexComponents,
                              AbstractType<?> type,
                              AbstractIterator<Pair<ByteBuffer, IntArrayList>> terms,
                              int size,
                              int minSegmentRowId,
                              int maxSegmentRowId)
    {
        this.indexComponents = indexComponents;
        this.type = type;
        this.terms = terms;
        this.size = size;
        this.minSegmentRowId = minSegmentRowId;
        this.maxSegmentRowId = maxSegmentRowId;
    }

    KDTreeIndexSearcher flushAndOpen() throws IOException
    {
        final TermsIterator termEnum = new MemtableTermsIterator(null, null, terms);
        final ImmutableOneDimPointValues pointValues = ImmutableOneDimPointValues.fromTermEnum(termEnum, type);

        final SegmentMetadata metadata;
        try (NumericIndexWriter writer = new NumericIndexWriter(indexComponents, TypeUtil.fixedSizeOf(type), maxSegmentRowId, size, defaultConfig("test")))
        {
            final SegmentMetadata.ComponentMetadataMap indexMetas = writer.writeAll(pointValues);
            metadata = new SegmentMetadata(0,
                                          size,
                                          minSegmentRowId,
                                          maxSegmentRowId,
                                          // min/max is unused for now
                                          Murmur3Partitioner.instance.decorateKey(UTF8Type.instance.fromString("a")),
                                          Murmur3Partitioner.instance.decorateKey(UTF8Type.instance.fromString("b")),
                                          UTF8Type.instance.fromString("c"),
                                          UTF8Type.instance.fromString("d"),
                                          indexMetas);
        }

        try (SSTableIndex.PerIndexFiles indexFiles = new SSTableIndex.PerIndexFiles(indexComponents, false))
        {
            Segment segment = new Segment(() -> segmentRowIdToToken, () -> segmentRowIdToOffset, keyFetcher, indexFiles, metadata);
            KDTreeIndexSearcher searcher = IndexSearcher.open(segment, QueryEventListeners.NO_OP_BKD_LISTENER);
            assertThat(searcher, is(instanceOf(KDTreeIndexSearcher.class)));
            return searcher;
        }
    }

    /**
     * Returns a k-d tree index where:
     * 1. term values have 32b
     * 2. term value is equal to {@code startTermInclusive} + row id;
     * 3. tokens and offsets are equal to row id;
     */
    public static IndexSearcher buildInt32Searcher(IndexComponents indexComponents, int startTermInclusive, int endTermExclusive)
            throws IOException
    {
        final int size = endTermExclusive - startTermInclusive;
        Assert.assertTrue(size > 0);
        KDTreeIndexBuilder indexBuilder = new KDTreeIndexBuilder(indexComponents,
                Int32Type.instance,
                singleOrd(int32Range(startTermInclusive, endTermExclusive), startTermInclusive, size),
                size,
                startTermInclusive,
                endTermExclusive);
        return indexBuilder.flushAndOpen();
    }

    /**
     * Returns a k-d tree index where:
     * 1. term values have 64b
     * 2. term value is equal to {@code startTermInclusive} + row id;
     * 3. tokens and offsets are equal to row id;
     */
    public static IndexSearcher buildLongSearcher(IndexComponents indexComponents, long startTermInclusive, long endTermExclusive)
            throws IOException
    {
        final long size = endTermExclusive - startTermInclusive;
        Assert.assertTrue(size > 0);
        KDTreeIndexBuilder indexBuilder = new KDTreeIndexBuilder(indexComponents,
                LongType.instance,
                singleOrd(longRange(startTermInclusive, endTermExclusive), Math.toIntExact(startTermInclusive), Math.toIntExact(size)),
                Math.toIntExact(size),
                Math.toIntExact(startTermInclusive),
                Math.toIntExact(endTermExclusive));
        return indexBuilder.flushAndOpen();
    }

    /**
     * Returns a k-d tree index where:
     * 1. term values have 16b
     * 2. term value is equal to {@code startTermInclusive} + row id;
     * 3. tokens and offsets are equal to row id;
     */
    public static IndexSearcher buildShortSearcher(IndexComponents indexComponents, short startTermInclusive, short endTermExclusive)
            throws IOException
    {
        final int size = endTermExclusive - startTermInclusive;
        Assert.assertTrue(size > 0);
        KDTreeIndexBuilder indexBuilder = new KDTreeIndexBuilder(indexComponents,
                ShortType.instance,
                singleOrd(shortRange(startTermInclusive, endTermExclusive), startTermInclusive, size),
                size,
                startTermInclusive,
                endTermExclusive);
        return indexBuilder.flushAndOpen();
    }

    /**
     * Returns inverted index where each posting list contains exactly one element equal to the terms ordinal number +
     * given offset.
     */
    public static AbstractIterator<Pair<ByteBuffer, IntArrayList>> singleOrd(Iterator<ByteBuffer> terms, int segmentRowIdOffset, int size)
    {
        return new AbstractIterator<Pair<ByteBuffer, IntArrayList>>()
        {
            private long currentTerm = 0;
            private int currentSegmentRowId = segmentRowIdOffset;

            @Override
            protected Pair<ByteBuffer, IntArrayList> computeNext()
            {
                if (currentTerm++ >= size)
                {
                    return endOfData();
                }

                IntArrayList postings = new IntArrayList();
                postings.add(currentSegmentRowId++);
                assertTrue(terms.hasNext());
                return Pair.create(terms.next(), postings);
            }
        };
    }

    /**
     * Returns sequential ordered encoded ints from {@code startInclusive} (inclusive) to {@code endExclusive}
     * (exclusive) by an incremental step of {@code 1}.
     */
    public static Iterator<ByteBuffer> int32Range(int startInclusive, int endExclusive)
    {
        return IntStream.range(startInclusive, endExclusive)
                .mapToObj(Int32Type.instance::decompose)
                .collect(Collectors.toList())
                .iterator();
    }

    /**
     * Returns sequential ordered encoded longs from {@code startInclusive} (inclusive) to {@code endExclusive}
     * (exclusive) by an incremental step of {@code 1}.
     */
    public static Iterator<ByteBuffer> longRange(long startInclusive, long endExclusive)
    {
        return LongStream.range(startInclusive, endExclusive)
                .mapToObj(LongType.instance::decompose)
                .collect(Collectors.toList())
                .iterator();
    }

    /**
     * Returns sequential ordered encoded shorts from {@code startInclusive} (inclusive) to {@code endExclusive}
     * (exclusive) by an incremental step of {@code 1}.
     */
    public static Iterator<ByteBuffer> shortRange(short startInclusive, short endExclusive)
    {
        return IntStream.range(startInclusive, endExclusive)
                .mapToObj(i -> ShortType.instance.decompose((short) i))
                .collect(Collectors.toList())
                .iterator();
    }
}
