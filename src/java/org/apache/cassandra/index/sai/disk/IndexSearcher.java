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

import java.io.Closeable;
import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.SSTableContext.KeyFetcher;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;

/**
 * Abstract reader for individual segments of an on-disk index.
 *
 * Accepts shared resources (token/offset file readers), and uses them to perform lookups against on-disk data
 * structures.
 */
public abstract class IndexSearcher implements Closeable
{
    private final LongArray.Factory rowIdToTokenFactory;
    private final LongArray.Factory rowIdToOffsetFactory;
    private final KeyFetcher keyFetcher;
    final SSTableIndex.PerIndexFiles indexFiles;

    final SegmentMetadata metadata;

    final IndexComponents indexComponents;

    IndexSearcher(Segment segment)
    {
        this.indexComponents = segment.indexFiles.components();
        this.rowIdToTokenFactory = segment.segmentRowIdToTokenFactory;
        this.rowIdToOffsetFactory = segment.segmentRowIdToOffsetFactory;
        this.keyFetcher = segment.keyFetcher;
        this.indexFiles = segment.indexFiles;
        this.metadata = segment.metadata;
    }

    public static IndexSearcher open(boolean isString, Segment segment, ColumnQueryMetrics listener) throws IOException
    {
        return isString ? open(segment, (QueryEventListener.TrieIndexEventListener) listener)
                        : open(segment, (QueryEventListener.BKDIndexEventListener) listener);
    }

    public static InvertedIndexSearcher open(Segment segment, QueryEventListener.TrieIndexEventListener listener) throws IOException
    {
        return new InvertedIndexSearcher(segment, listener);
    }

    public static KDTreeIndexSearcher open(Segment segment, QueryEventListener.BKDIndexEventListener listener) throws IOException
    {
        return new KDTreeIndexSearcher(segment, listener);
    }

    /**
     * @return number of per-index open files attached to a sstable
     */
    public static int openPerIndexFiles(AbstractType<?> columnType)
    {
        return TypeUtil.isLiteral(columnType) ? InvertedIndexSearcher.openPerIndexFiles() : KDTreeIndexSearcher.openPerIndexFiles();
    }

    /**
     * Search on-disk index synchronously.
     *
     * @param expression to filter on disk index
     * @param context to track per sstable cache and per query metrics
     *
     * @return {@link RangeIterator} that matches given expression
     */
    public abstract RangeIterator search(Expression expression, SSTableQueryContext context);

    RangeIterator toIterator(PostingList postingList, SSTableQueryContext context)
    {
        if (postingList == null)
            return RangeIterator.empty();

        final RangeIteratorStatistics rangeIteratorStatistics = new RangeIteratorStatistics();
        if (rangeIteratorStatistics.noOverlap)
            return RangeIterator.empty();

        RangeIterator iterator = new PostingListRangeIterator(postingList,
                                                              rangeIteratorStatistics,
                                                              rowIdToTokenFactory,
                                                              rowIdToOffsetFactory,
                                                              keyFetcher,
                                                              context,
                                                              indexComponents);

        // "hasNext()" will attempt to compute next token and update its proper current value instead of using min which
        // it's just theoretical lower bound included in RangeIteratorStatistics..
        // Because RangeIterator expects proper "getCurrent" value used for sorting in PriorityQueue
        if (!iterator.hasNext())
            return RangeIterator.empty();

        return iterator;
    }

    public class RangeIteratorStatistics
    {
        long minToken;
        long maxToken;
        long maxPartitionOffset;
        boolean noOverlap;

        RangeIteratorStatistics()
        {
            // Note: min token will be used to init RangeIterator#current which is used to ranges in RangeIterator#Build#ranges.
            minToken = toLongToken(metadata.minKey);
            // use segment's metadata for the range iterator, may not be accurate, but should not matter to performance.
            maxToken = metadata.maxKey.isMinimum()
                    ? toLongToken(DatabaseDescriptor.getPartitioner().getMaximumToken())
                    : toLongToken(metadata.maxKey);

            maxPartitionOffset = Long.MAX_VALUE;
        }
    }

    private static long toLongToken(DecoratedKey key)
    {
        return toLongToken(key.getToken());
    }

    private static long toLongToken(Token token)
    {
        return (long) token.getTokenValue();
    }
}
