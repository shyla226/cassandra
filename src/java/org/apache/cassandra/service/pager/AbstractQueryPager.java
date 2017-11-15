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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

abstract class AbstractQueryPager<T extends ReadCommand> implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractQueryPager.class);

    protected final T command;
    protected final DataLimits limits;
    protected final ProtocolVersion protocolVersion;
    private final boolean enforceStrictLiveness;

    /** The internal pager is created when the fetch command is issued since its properties will depend on
        the page limits and whether we need to retrieve a single page or multiple pages, however we need
        to access its state at a higher level so we store it here. It is also removed when the iterator is closed.
        So this field may be null.
     */
    private Pager internalPager;

    /** The total number of rows remaining to be fetched*/
    private int remaining;

    /** This is set to true when we want the last row returned by the previous pager to be also part of the query
     * used when fetching the next page. This is the case when the previous pager returned the row but the consumer
     * never had a chance to use it. Currently this is only true when
     * {@link org.apache.cassandra.cql3.continuous.paging.ContinuousPagingService.PageBuilder} needs to send
     * a page to the client. It basically means that the client never received the last row written in the paging state.
     * This row can also be a static row of an empty partition or a static compact table.
     */
    protected boolean inclusive;

    /** This is the last key we've been reading from (or can still be reading within). This is the key for
       which remainingInPartition makes sense: if we're starting another key, we should reset remainingInPartition
      (and this is done in PagerIterator). This can be null (when we start). */
    private DecoratedKey lastKey;

    /** The total number of rows remaining to be fetched in the current partition */
    private int remainingInPartition;

    /** This is set to true once the iterator is closed, and only then, if we have run out of data */
    private boolean exhausted;

    protected AbstractQueryPager(T command, ProtocolVersion protocolVersion)
    {
        this.command = command;
        this.protocolVersion = protocolVersion;
        this.limits = command.limits();
        this.enforceStrictLiveness = command.metadata().enforceStrictLiveness();

        this.remaining = limits.count();
        this.remainingInPartition = limits.perPartitionCount();

        if (logger.isTraceEnabled())
            logger.trace("{} - created with {}/{}/{}", hashCode(), command.limits(), remaining, remainingInPartition);
    }

    public ReadExecutionController executionController()
    {
        return command.executionController();
    }

    @SuppressWarnings("resource")
    public PartitionIterator fetchPage(int pageSize, ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime, boolean forContinuousPaging)
    {
        return innerFetch(pageSize, (pageCommand) -> pageCommand.execute(consistency, clientState, queryStartNanoTime, forContinuousPaging));
    }

    @SuppressWarnings("resource")
    public PartitionIterator fetchPageInternal(int pageSize, ReadExecutionController executionController)
    {
        return innerFetch(pageSize, (pageCommand) -> pageCommand.executeInternal(executionController));
    }

    @SuppressWarnings("resource")
    public UnfilteredPartitionIterator fetchPageUnfiltered(int pageSize, ReadExecutionController executionController, CFMetaData cfm)
    {
        assert internalPager == null : "only one iteration at a time is supported";

        if (isExhausted())
            return EmptyIterators.unfilteredPartition(cfm, false);

        final int toFetch = Math.min(pageSize, remaining);
        final ReadCommand pageCommand = nextPageReadCommand(toFetch);
        internalPager = new UnfilteredPager(limits.forPaging(toFetch), pageCommand, command.nowInSec());
        return Transformation.apply(pageCommand.executeLocally(executionController), internalPager);
    }

    private PartitionIterator innerFetch(int pageSize, Function<ReadCommand, PartitionIterator> itSupplier)
    {
        assert internalPager == null : "only one iteration at a time is supported";

        if (isExhausted())
            return EmptyIterators.partition();

        final int toFetch = Math.min(pageSize, remaining);
        final ReadCommand pageCommand = nextPageReadCommand(toFetch);
        internalPager = new RowPager(limits.forPaging(toFetch), pageCommand, command.nowInSec());
        return Transformation.apply(itSupplier.apply(pageCommand), internalPager);
    }

    private class UnfilteredPager extends Pager<Unfiltered>
    {
        private UnfilteredPager(DataLimits pageLimits, ReadCommand pageCommand, int nowInSec)
        {
            super(pageLimits, pageCommand, nowInSec);
        }

        protected BaseRowIterator<Unfiltered> apply(BaseRowIterator<Unfiltered> partition)
        {
            return Transformation.apply(counter.applyTo((UnfilteredRowIterator) partition), this);
        }
    }

    private class RowPager extends Pager<Row>
    {

        private RowPager(DataLimits pageLimits, ReadCommand pageCommand, int nowInSec)
        {
            super(pageLimits, pageCommand, nowInSec);
        }

        protected BaseRowIterator<Row> apply(BaseRowIterator<Row> partition)
        {
            return Transformation.apply(counter.applyTo((RowIterator) partition), this);
        }
    }

    /**
     * A transformation to keep track of the lastRow that was iterated and to determine
     * when a page is available. If fetching only a single page, it also stops the iteration after 1 page.
     */
    private abstract class Pager<T extends Unfiltered> extends Transformation<BaseRowIterator<T>>
    {
        private final DataLimits pageLimits;
        protected final DataLimits.Counter counter;
        private final ReadCommand pageCommand;
        private DecoratedKey currentKey;
        private Row lastRow;
        private boolean isFirstPartition = true;

        private Pager(DataLimits pageLimits, ReadCommand pageCommand, int nowInSec)
        {
            this.counter = pageLimits.newCounter(nowInSec, true, command.selectsFullPartition(), enforceStrictLiveness);
            this.pageLimits = pageLimits;
            this.pageCommand = pageCommand;
        }

        @Override
        public BaseRowIterator<T> applyToPartition(BaseRowIterator<T> partition)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - applyToPartition {}",
                             AbstractQueryPager.this.hashCode(),
                             ByteBufferUtil.bytesToHex(partition.partitionKey().getKey()));

            currentKey = partition.partitionKey();

            // If this is the first partition of this page, this could be the continuation of a partition we've started
            // on the previous page. In which case, we could have the problem that the partition has no more "regular"
            // rows (but the page size is such we didn't knew before) but it does have a static row. We should then skip
            // the partition as returning it would means to the upper layer that the partition has "only" static columns,
            // which is not the case (and we know the static results have been sent on the previous page). If inclusive
            // is true however, then we know at least one row was not sent in the previous page, it could well be the
            // static row of an empty partition that was not sent.
            if (isFirstPartition)
            {
                isFirstPartition = false;
                if (isPreviouslyReturnedPartition(currentKey) && !partition.hasNext() && !inclusive)
                {
                    partition.close();
                    return null;
                }
            }

            return apply(partition);
        }

        protected abstract BaseRowIterator<T> apply(BaseRowIterator<T> partition);

        @Override
        public void onClose()
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - onClose called with {}/{}", AbstractQueryPager.this.hashCode(), lastKey, lastRow);

            // In some case like GROUP BY a counter need to know when the processing is completed.
            counter.onClose();

            recordLast(lastKey, lastRow);

            remaining = getRemaining();
            remainingInPartition = getRemainingInPartition();

            // if the page command was not aborted and if the counter did not count
            // up to the page limits, then the iteration must have reached the end
            exhausted = !pageCommand.aborted() && pageLimits.isExhausted(counter);

            if (logger.isTraceEnabled())
                logger.trace("{} - exhausted {}, counter: {}, remaining: {}/{}",
                             AbstractQueryPager.this.hashCode(), exhausted, counter, remaining, remainingInPartition);

            // remove the internal page so that we know that the iteration is finished
            internalPager = null;
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
            if (lastRow != null && (lastRow.clustering() == Clustering.STATIC_CLUSTERING
                                    || lastRow.clustering() == Clustering.EMPTY))
            {
                return 0;
            }
            else
            {
                return remainingInPartition - counter.countedInCurrentPartition();
            }
        }

        public Row applyToStatic(Row row)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - applyToStaticRow {}", AbstractQueryPager.this.hashCode(), !row.isEmpty());

            if (!row.isEmpty())
            {
                remainingInPartition = limits.perPartitionCount();
                lastKey = currentKey;
                lastRow = row;
            }
            return row;
        }

        @Override
        public Row applyToRow(Row row)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - applyToRow {}",
                             AbstractQueryPager.this.hashCode(),
                             row.clustering() == null ? "null" : row.clustering().toBinaryString());

            if (!currentKey.equals(lastKey))
            {
                remainingInPartition = limits.perPartitionCount();
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

    protected abstract PagingState makePagingState(DecoratedKey lastKey, Row lastRow, boolean inclusive);
    protected abstract PagingState makePagingState(boolean inclusive);

    protected abstract ReadCommand nextPageReadCommand(int pageSize);
    protected abstract void recordLast(DecoratedKey key, Row row);
    protected abstract boolean isPreviouslyReturnedPartition(DecoratedKey key);
}
