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

import java.nio.ByteBuffer;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.aggregation.GroupingState;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.Flow;

/**
 * {@code QueryPager} that takes care of fetching the pages for aggregation queries.
 * <p>
 * For aggregation/group by queries, the user page size is in number of groups. But each group could be composed of very
 * many rows so to avoid running into OOMs, this pager will page internal queries into sub-pages. So each call to
 * {@link QueryPager#fetchPage(PageSize, ReadContext)} may (transparently) yield multiple internal queries (sub-pages).
 * <p>
 * Please note due to the fact the page size is used to compute the number of groups to page through, if the page size 
 * unit is in bytes, a default number of groups is used.
 */
public final class AggregationQueryPager implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(AggregationQueryPager.class);

    /**
     * For aggregated queries the user may specify a small page size. In this case we
     * want to use a minimum value for inner paging across replicas to avoid too many
     * network round trips. For example, a client wants a count or a sum with a page size
     * of 100 rows (which is the default used by cqlsh) but the aggregation is over a large range,
     * in this case we are better off fetching larger pages from replicas.
     *
     * Here we set a size of 2 MB, we then converted to an estimated number of rows.
     */
    private static final PageSize MIN_SUB_PAGE_SIZE = PageSize.bytesSize(2 * 1024 * 1024);

    private static final int DEFAULT_GROUPS = 100;

    private final DataLimits limits;

    // The sub-pager, used to retrieve the next sub-page.
    private QueryPager subPager;

    // the timeout in nanoseconds, if more time has elapsed, a ReadTimeoutException will be raised
    private final long timeout;

    // the smallest number of rows to fetch for sub-pages
    private final int minSubPageSizeRows;

    public AggregationQueryPager(QueryPager subPager, DataLimits limits, long timeoutMillis, int avgRowSize)
    {
        this.subPager = subPager;
        this.limits = limits;
        this.timeout = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        this.minSubPageSizeRows = MIN_SUB_PAGE_SIZE.inEstimatedRows(avgRowSize);
    }

    @Override
    public Flow<FlowablePartition> fetchPage(PageSize pageSize, ReadContext ctx)
    {
        if (limits.isGroupByLimit())
            return new GroupByPartitions(pageSize, ctx).partitions();

        return new AggregatedPartitions(pageSize, ctx).partitions();
    }

    @Override
    public Flow<FlowablePartition> fetchPageInternal(PageSize pageSize)
    {        
        if (limits.isGroupByLimit())
            return new GroupByPartitions(pageSize, null).partitions();

        return new AggregatedPartitions(pageSize, null).partitions();
    }

    @Override
    public boolean isExhausted()
    {
        return subPager.isExhausted();
    }

    @Override
    public int maxRemaining()
    {
        return subPager.maxRemaining();
    }

    @Override
    public PagingState state(boolean inclusive)
    {
        return subPager.state(inclusive);
    }

    @Override
    public QueryPager withUpdatedLimit(DataLimits newLimits)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Group by partitions.
     * <p>
     * This class takes care of concatenating sub-pages until done. It also makes sure that partitions
     * with the same key across sub-pages are grouped correctly. Before each sub-page is retrieved we need
     * to update the sub-pager, for which we need to keep track of some state such as the last partition key
     * and clustering.
     */
    class GroupByPartitions
    {
        /**
         * The original page size: not currently used as everything is computed in number of rows.
         */
        private final PageSize pageSize;
        
        /**
         * The top-level page size in number of groups.
         */
        private final int topPages;

        // For distributed and local queries, null for internal queries
        @Nullable
        private final ReadContext ctx;

        /**
         * The key of the last partition processed.
         */
        private ByteBuffer lastPartitionKey;

        /**
         * The clustering of the last row processed
         */
        private Clustering lastClustering;

        /**
         * The initial amount of row remaining
         */
        private int initialMaxRemaining;

        GroupByPartitions(PageSize pageSize, ReadContext ctx)
        {
            this.pageSize = pageSize;
            this.topPages = handlePageSize(pageSize);
            this.ctx = ctx;
            if (logger.isTraceEnabled())
                logger.trace("{} - created with page size={}, ctx={}", hashCode(), topPages, ctx);
        }

        /**
         * Return the partitions by concatenating sub-pages as long as we are not done, see {@link #moreContents()}.
         * <p>
         * Because the same partition key may span two partitions across two different sub-pages, it is necessary to group the
         * partitions by partition key, but at the same time we should publish partitions immediately to allow the sub-pager
         * to make progress since {@link #moreContents()} relies on the sub-pager counter to work out if it needs more pages.
         * <p>
         * In addition to concatenating and grouping same key partitions, we also need to track the last partition key
         * and the last clustering, see {@link #applyToPartition(FlowablePartition)} and {@link #applyToRow(Row)}.
         *
         * @return the partitions for this query.
         */
        Flow<FlowablePartition> partitions()
        {
            initialMaxRemaining = subPager.maxRemaining();
            Flow<FlowablePartition> ret = fetchSubPage(topPages);

            // the existing iterator based approach would merge partitions with the same key across two different pages
            // however it looks like we don't need to do this, because we create a new pager with a grouping state
            // that can handle splitting partitions across pages, and so a simple concat of pages is sufficient
            return ret.concatWith(this::moreContents).map(this::applyToPartition);
        }

        private FlowablePartition applyToPartition(FlowablePartition partition)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} applyToPartition {}", hashCode(), ByteBufferUtil.bytesToHex(partition.header().partitionKey.getKey()));

            checkTimeout();

            lastPartitionKey = partition.partitionKey().getKey();
            lastClustering = null;
            return partition.mapContent(this::applyToRow);
        }

        private Row applyToRow(Row row)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - applyToRow {}", hashCode(), row.clustering() == null ? "null" : row.clustering().toBinaryString());

            checkTimeout();

            lastClustering = row.clustering();
            return row;
        }

        private int handlePageSize(PageSize pageSize)
        {
            // If the page size unit is in bytes, resort to a default number:
            int size = pageSize.isInBytes() ? DEFAULT_GROUPS : pageSize.rawSize();
            
            // If the paging is off, the pageSize will be <= 0. So we need to replace
            // it by DataLimits.NO_LIMIT
            return size <= 0 ? DataLimits.NO_ROWS_LIMIT : size;
        }

        private Flow<FlowablePartition> moreContents()
        {
            int counted = initialMaxRemaining - subPager.maxRemaining();

            if (logger.isTraceEnabled())
                logger.trace("{} - moreContents() called with last: {}/{}, counted: {}",
                             hashCode(),
                             lastPartitionKey == null ? "null" : ByteBufferUtil.bytesToHex(lastPartitionKey),
                             lastClustering == null ? "null" : lastClustering.toBinaryString(),
                             counted);


            if (isDone(topPages, counted) || subPager.isExhausted())
            {
                if (logger.isTraceEnabled())
                    logger.trace("{} - moreContents() returns null: {}, {}, [{}] exhausted? {}",
                                 hashCode(), counted, topPages, subPager.hashCode(), subPager.isExhausted());

                return null;
            }

            subPager = updatePagerLimit(subPager, limits, lastPartitionKey, lastClustering);
            return fetchSubPage(computeSubPageSize(topPages, counted));
        }

        protected boolean isDone(int pageSize, int counted)
        {
            return counted == pageSize;
        }

        protected void checkTimeout()
        {
            // internal queries are not guarded by a timeout because cont. paging queries can be aborted
            // and system queries should not be aborted
            if (ctx == null)
                return;

            long elapsed = System.nanoTime() - ctx.queryStartNanos;
            if (elapsed > timeout)
                throw new ReadTimeoutException(ctx.consistencyLevel);
        }

        /**
         * Updates the pager with the new limits if needed.
         *
         * @param pager the pager previously used
         * @param limits the DataLimits
         * @param lastPartitionKey the partition key of the last row returned
         * @param lastClustering the clustering of the last row returned
         * @return the pager to use to query the next page of data
         */
        protected QueryPager updatePagerLimit(QueryPager pager,
                                              DataLimits limits,
                                              ByteBuffer lastPartitionKey,
                                              Clustering lastClustering)
        {
            GroupingState state = new GroupingState(lastPartitionKey, lastClustering);
            DataLimits newLimits = limits.forGroupByInternalPaging(state);
            return pager.withUpdatedLimit(newLimits);
        }

        /**
         * Computes the size of the next sub-page to retrieve.
         *
         * @param pageSize the top-level page size
         * @param counted the number of result returned so far by the previous sub-pages
         * @return the size of the next sub-page to retrieve
         */
        protected int computeSubPageSize(int pageSize, int counted)
        {
            return pageSize - counted;
        }

        /**
         * Fetches the next sub-page.
         *
         * @param subPageSize the sub-page size in number of groups
         * @return the next sub-page
         */
        private Flow<FlowablePartition> fetchSubPage(int subPageSize)
        {
            if (logger.isTraceEnabled())
                logger.trace("Fetching sub-page with consistency {}", ctx == null ? "<internal>" : ctx.consistencyLevel);

            return ctx == null
                 ? subPager.fetchPageInternal(PageSize.rowsSize(subPageSize))
                 : subPager.fetchPage(PageSize.rowsSize(subPageSize), ctx.withStartTime(System.nanoTime()));
        }
    }

    /**
     * Partitions for queries without Group By but with aggregates.
     *
     * <p>For maintaining backward compatibility we are forced to use the {@link org.apache.cassandra.db.filter.DataLimits.CQLLimits} instead of the
     * {@link org.apache.cassandra.db.filter.DataLimits.CQLGroupByLimits}. Due to that pages need to be fetched in a different way.</p>
     */
    private final class AggregatedPartitions extends GroupByPartitions
    {
        AggregatedPartitions(PageSize pageSize, ReadContext ctx)
        {
            super(pageSize, ctx);
        }

        @Override
        protected QueryPager updatePagerLimit(QueryPager pager,
                                              DataLimits limits,
                                              ByteBuffer lastPartitionKey,
                                              Clustering lastClustering)
        {
            return pager;
        }

        @Override
        protected boolean isDone(int pageSize, int counted)
        {
            return false;
        }

        @Override
        protected int computeSubPageSize(int pageSize, int counted)
        {
            return Math.max(minSubPageSizeRows, pageSize);
        }
    }
}
