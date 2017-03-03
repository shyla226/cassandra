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
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.reactivex.Single;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.aggregation.GroupingState;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * {@code QueryPager} that takes care of fetching the pages for aggregation queries.
 * <p>
 * For aggregation/group by queries, the user page size is in number of groups. But each group could be composed of very
 * many rows so to avoid running into OOMs, this pager will page internal queries into sub-pages. So each call to
 * {@link QueryPager#fetchPage(int, ConsistencyLevel, ClientState, long, boolean)}  may (transparently) yield multiple
 * internal queries (sub-pages).
 */
public final class AggregationQueryPager implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(AggregationQueryPager.class);

    private final DataLimits limits;

    // The sub-pager, used to retrieve the next sub-page.
    private QueryPager subPager;

    public AggregationQueryPager(QueryPager subPager, DataLimits limits)
    {
        this.subPager = subPager;
        this.limits = limits;
    }

    @Override
    public Single<PartitionIterator> fetchPage(int pageSize,
                                       ConsistencyLevel consistency,
                                       ClientState clientState,
                                       long queryStartNanoTime,
                                       boolean forContinuousPaging)
    {
        if (limits.isGroupByLimit())
            return Single.just(new GroupByPartitionIterator(pageSize, consistency, clientState, queryStartNanoTime, forContinuousPaging));

        return Single.just(new AggregationPartitionIterator(pageSize, consistency, clientState, queryStartNanoTime, forContinuousPaging));
    }

    @Override
    public Single<PartitionIterator> fetchPageInternal(int pageSize)
    {
        if (limits.isGroupByLimit())
            return Single.just(new GroupByPartitionIterator(pageSize, null, System.nanoTime()));

        return Single.just(new AggregationPartitionIterator(pageSize, null, System.nanoTime()));
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
     * <code>PartitionIterator</code> that automatically fetch a new sub-page of data if needed when the current iterator is
     * exhausted.
     */
    class GroupByPartitionIterator implements PartitionIterator
    {
        /**
         * The top-level page size in number of groups.
         */
        private final int pageSize;

        // For distributed and local queries, null for internal queries
        private final ConsistencyLevel consistency;

        // For distributed queries, null for local and distributed queries
        private final ClientState clientState;

        /**
         * The <code>PartitionIterator</code> over the last page retrieved.
         */
        private PartitionIterator partitionIterator;

        /**
         * The next <code>RowIterator</code> to be returned.
         */
        private RowIterator next;

        /**
         * Specify if all the data have been returned.
         */
        private boolean endOfData;

        /**
         * Keeps track if the partitionIterator has been closed or not.
         */
        private boolean closed;

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

        private final boolean forContinuousPaging;

        private final long queryStartNanoTime;

        GroupByPartitionIterator(int pageSize,
                                 ConsistencyLevel consistency,
                                 long queryStartNanoTime)
       {
           this(pageSize, consistency, null, queryStartNanoTime, false);
       }

        private GroupByPartitionIterator(int pageSize,
                                         ConsistencyLevel consistency,
                                         ClientState clientState,
                                         long queryStartNanoTime,
                                         boolean forContinuousPaging)
        {
            this.pageSize = handlePagingOff(pageSize);
            this.consistency = consistency;
            this.clientState = clientState;
            this.queryStartNanoTime = queryStartNanoTime;
            this.forContinuousPaging = forContinuousPaging;
        }

        private int handlePagingOff(int pageSize)
        {
            // If the paging is off, the pageSize will be <= 0. So we need to replace
            // it by DataLimits.NO_LIMIT
            return pageSize <= 0 ? DataLimits.NO_LIMIT : pageSize;
        }

        public final void close()
        {
            if (!closed)
            {
                closed = true;
                if (partitionIterator != null)
                    partitionIterator.close();
            }
        }

        public final boolean hasNext()
        {
            if (endOfData)
                return false;

            if (next != null)
                return true;

            fetchNextRowIterator();

            return next != null;
        }

        /**
         * Loads the next <code>RowIterator</code> to be returned.
         */
        private void fetchNextRowIterator()
        {
            if (logger.isTraceEnabled())
                logger.trace("fetchNextRowIterator(), p.it. {}, last: {}/{}",
                             partitionIterator,
                             lastPartitionKey == null ? "null" : ByteBufferUtil.bytesToHex(lastPartitionKey),
                             lastClustering == null ? "null" : lastClustering.toBinaryString());
            if (partitionIterator == null)
            {
                initialMaxRemaining = subPager.maxRemaining();
                partitionIterator = fetchSubPage(pageSize);
            }

            while (!partitionIterator.hasNext())
            {
                partitionIterator.close();

                int counted = initialMaxRemaining - subPager.maxRemaining();
                if (isDone(pageSize, counted) || subPager.isExhausted())
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Returning because done: {}, {}, {}, {}, {}",
                                     counted, pageSize, subPager.isExhausted(),
                                     lastPartitionKey == null ? "null" : ByteBufferUtil.bytesToHex(lastPartitionKey),
                                     lastClustering == null ? "null" : lastClustering.toBinaryString());

                    endOfData = true;
                    closed = true;
                    return;
                }

                subPager = updatePagerLimit(subPager, limits, lastPartitionKey, lastClustering);
                partitionIterator = fetchSubPage(computeSubPageSize(pageSize, counted));
            }

            next = partitionIterator.next();
        }

        protected boolean isDone(int pageSize, int counted)
        {
            return counted == pageSize;
        }

        /**
         * Updates the pager with the new limits if needed.
         *
         * @param pager the pager previoulsy used
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
         * Fetchs the next sub-page.
         *
         * @param subPageSize the sub-page size in number of groups
         * @return the next sub-page
         */
        private PartitionIterator fetchSubPage(int subPageSize)
        {
            if (logger.isTraceEnabled())
                logger.trace("Fetching sub-page with consitency {}", consistency);

            return consistency == null
                 ? subPager.fetchPageInternal(subPageSize).blockingGet()
                 : subPager.fetchPage(subPageSize, consistency, clientState, queryStartNanoTime, forContinuousPaging).blockingGet();
        }

        public final RowIterator next()
        {
            if (!hasNext())
                throw new NoSuchElementException();

            RowIterator iterator = new GroupByRowIterator(next);
            lastPartitionKey = iterator.partitionKey().getKey();
            next = null;
            return iterator;
        }

        private class GroupByRowIterator implements RowIterator
        {
            /**
             * The decorated <code>RowIterator</code>.
             */
            private RowIterator rowIterator;

            /**
             * Keeps track if the decorated iterator has been closed or not.
             */
            private boolean closed;

            GroupByRowIterator(RowIterator delegate)
            {
                this.rowIterator = delegate;
            }

            public TableMetadata metadata()
            {
                return rowIterator.metadata();
            }

            public boolean isReverseOrder()
            {
                return rowIterator.isReverseOrder();
            }

            public RegularAndStaticColumns columns()
            {
                return rowIterator.columns();
            }

            public DecoratedKey partitionKey()
            {
                return rowIterator.partitionKey();
            }

            public Row staticRow()
            {
                Row row = rowIterator.staticRow();
                lastClustering = null;
                return row;
            }

            public boolean isEmpty()
            {
                return this.rowIterator.isEmpty() && !hasNext();
            }

            public void close()
            {
                if (!closed)
                    rowIterator.close();
            }

            public boolean hasNext()
            {
                if (rowIterator.hasNext())
                    return true;

                DecoratedKey partitionKey = rowIterator.partitionKey();

                rowIterator.close();

                // Fetch the next RowIterator
                GroupByPartitionIterator.this.hasNext();

                // if the previous page was ending within the partition the
                // next RowIterator is the continuation of this one
                if (next != null && partitionKey.equals(next.partitionKey()))
                {
                    rowIterator = next;
                    next = null;
                    return rowIterator.hasNext();
                }

                closed = true;
                return false;
            }

            public Row next()
            {
                Row row = this.rowIterator.next();
                lastClustering = row.clustering();
                return row;
            }
        }
    }

    /**
     * <code>PartitionIterator</code> for queries without Group By but with aggregates.
     * <p>For maintaining backward compatibility we are forced to use the {@link org.apache.cassandra.db.filter.DataLimits.CQLLimits} instead of the
     * {@link org.apache.cassandra.db.filter.DataLimits.CQLGroupByLimits}. Due to that pages need to be fetched in a different way.</p>
     */
    private final class AggregationPartitionIterator extends GroupByPartitionIterator
    {
        AggregationPartitionIterator(int pageSize,
                                     ConsistencyLevel consistency,
                                     ClientState clientState,
                                     long queryStartNanoTime,
                                     boolean forContinuousPaging)
        {
            super(pageSize, consistency, clientState, queryStartNanoTime, forContinuousPaging);
        }

        AggregationPartitionIterator(int pageSize,
                                     ConsistencyLevel consistency,
                                     long queryStartNanoTime)
        {
            super(pageSize, consistency, queryStartNanoTime);
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
            return pageSize;
        }
    }
}
