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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.SSTableContext.KeyFetcher;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.Token;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.Throwables;

/**
 * A range iterator based on {@link PostingList}.
 *
 * <ol>
 *   <li> fetch next segment row id from posting list or skip to specific segment row id if {@link #skipTo(Long)} is called </li>
 *   <li> produce a {@link OnDiskKeyProducer.OnDiskToken} from {@link OnDiskKeyProducer#produceToken(long, int)} which is used
 *       to avoid fetching duplicated keys due to partition-level indexing on wide partition schema.
 *       <br/>
 *       Note: in order to reduce disk access in multi-index query, partition keys will only be fetched for intersected tokens
 *       in {@link StorageAttachedIndexSearcher}.
 *  </li>
 * </ol>
 *
 */

@NotThreadSafe
public class PostingListRangeIterator extends RangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Stopwatch timeToExhaust = Stopwatch.createStarted();
    private final SSTableQueryContext context;
    private final IndexComponents components;

    private final PostingList postingList;
    private LongArray segmentRowIdToToken = null;
    private LongArray segmentRowIdToOffset = null;
    private RandomAccessReader keyReader = null;

    private boolean needsSkipping = false;
    private long skipToToken = Long.MIN_VALUE;

    private final OnDiskKeyProducer producer;

    /**
     * Create a direct PostingListRangeIterator where the underlying PostingList is materialised
     * immediately so the posting list size can be used.
     */
    public PostingListRangeIterator(PostingList postingList,
                                    IndexSearcher.RangeIteratorStatistics rangeIteratorStatistics,
                                    LongArray.Factory segmentRowIdToTokenFactory,
                                    LongArray.Factory segmentRowIdToOffsetFactory,
                                    KeyFetcher keyFetcher,
                                    SSTableQueryContext context,
                                    IndexComponents components)
    {
        super(rangeIteratorStatistics.minToken, rangeIteratorStatistics.maxToken, postingList.size());
        this.postingList = postingList;
        long maxPartitionOffset = rangeIteratorStatistics.maxPartitionOffset;
        this.context = context;
        this.components = components;

        try
        {
            // startingIndex of 0 means `findTokenRowId` should search all tokens in the segment.
            this.segmentRowIdToToken = segmentRowIdToTokenFactory.openTokenReader(0, context);
            this.segmentRowIdToOffset = segmentRowIdToOffsetFactory.open();
            this.keyReader = keyFetcher.createReader();
            this.producer = new OnDiskKeyProducer(keyFetcher, keyReader, segmentRowIdToOffset, maxPartitionOffset);
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(segmentRowIdToToken, segmentRowIdToOffset, keyReader);
            throw t;
        }
    }

    @Override
    protected void performSkipTo(Long nextToken)
    {
        if (skipToToken >= nextToken)
            return;

        skipToToken = nextToken;
        needsSkipping = true;
    }

    @Override
    protected Token computeNext()
    {
        try
        {
            context.queryContext.checkpoint();

            // just end the iterator if we don't have a postingList or current segment is skipped
            if (postingList == null || exhausted())
                return endOfData();

            int segmentRowId = getNextSegmentRowId();
            if (segmentRowId == PostingList.END_OF_STREAM)
                return endOfData();

            return getNextToken(segmentRowId);
        }
        catch (Throwable t)
        {
            if (!(t instanceof AbortedOperationException))
                logger.error(components.logMessage("Unable to provide next token!"), t);

            throw Throwables.cleaned(t);
        }
    }

    @Override
    public void close() throws IOException
    {
        if (logger.isTraceEnabled())
        {
            final long exhaustedInMills = timeToExhaust.stop().elapsed(TimeUnit.MILLISECONDS);
            logger.trace(components.logMessage("PostinListRangeIterator exhausted after {} ms"), exhaustedInMills);
        }

        if (postingList != null)
            postingList.close();
        FileUtils.closeQuietly(segmentRowIdToToken, segmentRowIdToOffset, keyReader);
    }

    private boolean exhausted()
    {
        return needsSkipping && skipToToken > getMaximum();
    }

    /**
     * reads the next row ID from the underlying posting list, potentially skipping to get there.
     */
    private int getNextSegmentRowId() throws IOException
    {
        if (needsSkipping)
        {
            int targetRowID = Math.toIntExact(segmentRowIdToToken.findTokenRowID(skipToToken));
            // skipToToken is larger than max token in token file
            if (targetRowID < 0)
            {
                return PostingList.END_OF_STREAM;
            }

            int segmentRowId = postingList.advance(targetRowID);

            needsSkipping = false;
            return segmentRowId;
        }
        else
        {
            return postingList.nextPosting();
        }
    }

    /**
     * takes a segment row ID and produces a {@link Token} for its partition key.
     */
    public Token getNextToken(int segmentRowId)
    {
        assert segmentRowId != PostingList.END_OF_STREAM;

        long tokenValue = segmentRowIdToToken.get(segmentRowId);

        // Used to remove duplicated key offset
        return producer.produceToken(tokenValue, segmentRowId);
    }
}
