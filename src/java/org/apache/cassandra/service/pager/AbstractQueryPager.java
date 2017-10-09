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
package org.apache.cassandra.service.pager;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.Flow;

abstract class AbstractQueryPager<T extends ReadCommand> implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractQueryPager.class);

    protected final T command;
    protected final ProtocolVersion protocolVersion;
    private final boolean enforceStrictLiveness;

    /** The internal pager is created when the fetch command is issued since its properties will depend on
        the page limits and whether we need to retrieve a single page or multiple pages, however we need
        to access its state at a higher level so we store it here. It is also removed when the iterator is closed.
        So this field may be null.
     */
    private Pager internalPager;

    /** The total number of rows remaining to be fetched: this is initialized with the limit count, and capped by it */
    private int remaining;

    /** This is set to true when we want the last returned row to be also part of the query used when fetching a page */
    protected boolean inclusive;

    /** This is the last key we've been reading from (or can still be reading within). This is the key for
       which remainingInPartition makes sense: if we're starting another key, we should reset remainingInPartition
      (and this is done in PagerIterator). This can be null (when we start). */
    public DecoratedKey lastKey;

    /** The total number of rows remaining to be fetched in the current partition: this is initialized with the limit
     * partition count, and capped by it  */
    private int remainingInPartition;

    /** This is set to true once the iterator is closed, and only then, if we have run out of data */
    private boolean exhausted;

    /** This is the last counter from the latest page iteration. */
    DataLimits.Counter lastCounter;

    protected AbstractQueryPager(T command, ProtocolVersion protocolVersion)
    {
        this.command = command;
        this.protocolVersion = protocolVersion;
        this.remaining = command.limits().count();
        this.remainingInPartition = command.limits().perPartitionCount();
        this.enforceStrictLiveness = command.metadata().enforceStrictLiveness();

        if (logger.isTraceEnabled())
            logger.trace("{} - created with {}/{}/{}", hashCode(), command.limits(), remaining, remainingInPartition);
    }

    public Flow<FlowablePartition> fetchPage(PageSize pageSize, ReadContext ctx)
    {
        return innerFetch(pageSize, (pageCommand) -> pageCommand.execute(ctx));
    }

    public Flow<FlowablePartition> fetchPageInternal(PageSize pageSize)
    {
        return innerFetch(pageSize, ReadQuery::executeInternal);
    }

    public Flow<FlowableUnfilteredPartition> fetchPageUnfiltered(PageSize pageSize)
    {
        assert internalPager == null : "only one iteration at a time is supported";

        if (isExhausted())
            return Flow.empty();

        final ReadCommand pageCommand = nextPageReadCommand(nextPageLimits(), pageSize);
        internalPager = new UnfilteredPager(pageCommand.limits().duplicate(), command.nowInSec());
        return ((UnfilteredPager)internalPager).apply(pageCommand.executeLocally());
    }

    private Flow<FlowablePartition> innerFetch(PageSize pageSize, Function<ReadCommand, Flow<FlowablePartition>> dataSupplier)
    {
        assert internalPager == null : "only one iteration at a time is supported";

        if (isExhausted())
            return Flow.empty();

        final ReadCommand pageCommand = nextPageReadCommand(nextPageLimits(), pageSize);
        internalPager = new RowPager(pageCommand.limits().duplicate(), command.nowInSec());
        return ((RowPager) internalPager).apply(dataSupplier.apply(pageCommand));
    }

    private DataLimits nextPageLimits()
    {
        return limits().withCount(Math.min(limits().count(), remaining));
    }

    private class UnfilteredPager extends Pager<Unfiltered>
    {
        private UnfilteredPager(DataLimits pageLimits, int nowInSec)
        {
            super(pageLimits, nowInSec);
        }

        Flow<FlowableUnfilteredPartition> apply(Flow<FlowableUnfilteredPartition> source)
        {
            return source.flatMap(partition ->{
                // If this is the first partition of this page, this could be the continuation of a partition we've started
                // on the previous page. In which case, we could have the problem that the partition has no more "regular"
                // rows (but the page size is such we didn't knew before) but it does have a static row. We should then skip
                // the partition as returning it would means to the upper layer that the partition has "only" static columns,
                // which is not the case (and we know the static results have been sent on the previous page).
                if (internalPager.isFirstPartition)
                {
                    internalPager.isFirstPartition = false;
                    if (isPreviouslyReturnedPartition(partition.header.partitionKey))
                        return partition.content.skipMapEmpty(c -> applyToPartition(partition.withContent(c)));
                }

                return Flow.just(applyToPartition(partition));

            }).doOnClose(this::onClose);
        }

        private FlowableUnfilteredPartition applyToPartition(FlowableUnfilteredPartition partition)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - applyToPartition {}",
                             AbstractQueryPager.this.hashCode(),
                             ByteBufferUtil.bytesToHex(partition.header.partitionKey.getKey()));

            currentKey = partition.header.partitionKey;
            applyToStatic(partition.staticRow);

            return DataLimits.truncateUnfiltered(counter,
                                                 partition.mapContent(unfiltered -> unfiltered instanceof Row
                                                                                 ? applyToRow((Row)unfiltered)
                                                                                 : unfiltered));

        }
    }

    private class RowPager extends Pager<Row>
    {

        private RowPager(DataLimits pageLimits, int nowInSec)
        {
            super(pageLimits, nowInSec);
        }

        Flow<FlowablePartition> apply(Flow<FlowablePartition> source)
        {
//            return source.map(partition -> {
//                if (internalPager.isFirstPartition)
//                    internalPager.isFirstPartition = false;
//
//                return applyToPartition(partition);
//            }).doOnClose(this::onClose);

            return source.flatMap(partition ->{
                // If this is the first partition of this page, this could be the continuation of a partition we've started
                // on the previous page. In which case, we could have the problem that the partition has no more "regular"
                // rows (but the page size is such we didn't knew before) but it does have a static row. We should then skip
                // the partition as returning it would means to the upper layer that the partition has "only" static columns,
                // which is not the case (and we know the static results have been sent on the previous page).
                if (internalPager.isFirstPartition)
                {
                    internalPager.isFirstPartition = false;
                    if (isPreviouslyReturnedPartition(partition.header.partitionKey))
                        return partition.content.skipMapEmpty(c -> applyToPartition(partition.withContent(c)));
                }

                return Flow.just(applyToPartition(partition));

            }).doOnClose(this::onClose);
        }

        private FlowablePartition applyToPartition(FlowablePartition partition)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - applyToPartition {}",
                             AbstractQueryPager.this.hashCode(),
                             ByteBufferUtil.bytesToHex(partition.header.partitionKey.getKey()));

            currentKey = partition.header.partitionKey;
            applyToStatic(partition.staticRow);

            return DataLimits.truncateFiltered(counter, partition.mapContent(this::applyToRow));

        }
    }
    /**
     * A transformation to keep track of the lastRow that was iterated and to determine
     * when a page is available. If fetching only a single page, it also stops the iteration after 1 page.
     */
    private class Pager<T extends Unfiltered>
    {
        protected final DataLimits pageLimits;
        protected final DataLimits.Counter counter;
        protected DecoratedKey currentKey;
        protected Row lastRow;
        protected boolean isFirstPartition = true;

        protected Pager(DataLimits pageLimits, int nowInSec)
        {
            this.counter = pageLimits.newCounter(nowInSec, true, command.selectsFullPartition(), enforceStrictLiveness);
            this.pageLimits = pageLimits;
        }

        protected void onClose()
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - onClose called with {}/{}", AbstractQueryPager.this.hashCode(), lastKey, lastRow);

            // In some case like GROUP BY a counter need to know when the processing is completed.
            counter.endOfIteration();

            recordLast(lastKey, lastRow);

            remaining = getRemaining();
            remainingInPartition = getRemainingInPartition();

            // if the counter did not count up to the page limits, then the iteration must have reached the end
            exhausted = pageLimits.isExhausted(counter);

            if (logger.isTraceEnabled())
                logger.trace("{} - exhausted {}, counter: {}, remaining: {}/{}",
                            AbstractQueryPager.this.hashCode(), exhausted, counter, remaining, remainingInPartition);

            // remove the internal page so that we know that the iteration is finished
            internalPager = null;

            lastCounter = counter;
        }

        private int getRemaining()
        {
            return remaining - counter.counted();
        }

        // If the clustering of the last row returned is a static one, it means that the partition was only
        // containing data within the static columns. If the clustering of the last row returned is empty
        // it means that there is only one row per partition. Therefore, in both cases there are no data remaining
        // within the partition.
        private int getRemainingInPartition()
        {
            if (lastRow != null && lastRow.clustering().size() == 0)
            {
                return 0;
            }
            else
            {
                return remainingInPartition - counter.countedInCurrentPartition();
            }
        }

        protected Row applyToStatic(Row row)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - applyToStaticRow {}", AbstractQueryPager.this.hashCode(), !row.isEmpty());

            if (!row.isEmpty())
            {
                remainingInPartition = pageLimits.perPartitionCount();
                lastKey = currentKey;
                lastRow = row;
            }
            return row;
        }

        protected Row applyToRow(Row row)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - applyToRow {}",
                             AbstractQueryPager.this.hashCode(),
                             row.clustering() == null ? "null" : row.clustering().toBinaryString());

            if (!currentKey.equals(lastKey))
            {
                remainingInPartition = pageLimits.perPartitionCount();
                lastKey = currentKey;
            }

            lastRow = row;
            return row;
        }
    }

    void restoreState(DecoratedKey lastKey, int remaining, int remainingInPartition, boolean inclusive)
    {
        if (logger.isTraceEnabled())
            logger.trace("{} - restoring state: {}/{}/{}/{}", hashCode(), lastKey, remaining, remainingInPartition, inclusive);

        this.lastKey = lastKey;
        this.remaining = remaining;
        this.remainingInPartition = remainingInPartition;
        this.inclusive = inclusive;
    }

    public int counted()
    {
        return internalPager != null ? internalPager.counter.counted() : 0;
    }

    public boolean isExhausted()
    {
        return exhausted || limitsExceeded();
    }

    private boolean limitsExceeded()
    {
        return remaining == 0 || ((this instanceof SinglePartitionPager) && remainingInPartition == 0);
    }

    @Override
    public PagingState state(boolean inclusive)
    {
        return internalPager != null
               ? makePagingState(lastKey, internalPager.lastRow, inclusive)
               : makePagingState(inclusive);
    }

    public int maxRemaining()
    {
        return internalPager == null ? remaining : internalPager.getRemaining();
    }

    int remainingInPartition()
    {
        return internalPager == null ? remainingInPartition : internalPager.getRemainingInPartition();
    }

    DataLimits limits()
    {
        return command.limits();
    }

    protected abstract PagingState makePagingState(DecoratedKey lastKey, Row lastRow, boolean inclusive);
    protected abstract PagingState makePagingState(boolean inclusive);

    protected abstract ReadCommand nextPageReadCommand(DataLimits limits, PageSize pageSize);
    protected abstract void recordLast(DecoratedKey key, Row row);
    protected abstract boolean isPreviouslyReturnedPartition(DecoratedKey key);
}
