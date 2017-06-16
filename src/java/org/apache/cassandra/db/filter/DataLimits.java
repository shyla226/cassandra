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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.aggregation.GroupingState;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.CsFlow;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * Object in charge of tracking if we have fetch enough data for a given query.
 *
 * This is more complicated than a single count because we support PER PARTITION
 * limits, but also due to GROUP BY and paging.
 */
public abstract class DataLimits
{
    private static final Logger logger = LoggerFactory.getLogger(DataLimits.class);

    public static final Versioned<ReadVersion, Serializer> serializers = ReadVersion.versioned(Serializer::new);

    public static final int NO_LIMIT = Integer.MAX_VALUE;

    public static final DataLimits NONE = new CQLLimits(NO_LIMIT)
    {
        @Override
        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec, boolean countPartitionsWithOnlyStaticData)
        {
            return false;
        }
    };

    // We currently deal with distinct queries by querying full partitions but limiting the result at 1 row per
    // partition (see SelectStatement.makeFilter). So an "unbounded" distinct is still actually doing some filtering.
    public static final DataLimits DISTINCT_NONE = new CQLLimits(NO_LIMIT, 1, true);

    public enum Kind { CQL_LIMIT, CQL_PAGING_LIMIT, CQL_GROUP_BY_LIMIT, CQL_GROUP_BY_PAGING_LIMIT }

    public static DataLimits cqlLimits(int cqlRowLimit)
    {
        return cqlRowLimit == NO_LIMIT ? NONE : new CQLLimits(cqlRowLimit);
    }

    public static DataLimits cqlLimits(int cqlRowLimit, int perPartitionLimit)
    {
        return cqlRowLimit == NO_LIMIT && perPartitionLimit == NO_LIMIT
             ? NONE
             : new CQLLimits(cqlRowLimit, perPartitionLimit);
    }

    private static DataLimits cqlLimits(int cqlRowLimit, int perPartitionLimit, boolean isDistinct)
    {
        return cqlRowLimit == NO_LIMIT && perPartitionLimit == NO_LIMIT && !isDistinct
             ? NONE
             : new CQLLimits(cqlRowLimit, perPartitionLimit, isDistinct);
    }

    public static DataLimits groupByLimits(int groupLimit,
                                           int groupPerPartitionLimit,
                                           int rowLimit,
                                           AggregationSpecification groupBySpec)
    {
        return new CQLGroupByLimits(groupLimit, groupPerPartitionLimit, rowLimit, groupBySpec);
    }

    public static DataLimits distinctLimits(int cqlRowLimit)
    {
        return CQLLimits.distinct(cqlRowLimit);
    }

    public abstract Kind kind();

    public abstract boolean isUnlimited();
    public abstract boolean isDistinct();

    public boolean isGroupByLimit()
    {
        return false;
    }

    public boolean isExhausted(Counter counter)
    {
        return counter.counted() < count();
    }

    public abstract DataLimits forPaging(int pageSize);
    public abstract DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining);

    public abstract DataLimits forShortReadRetry(int toFetch);

    /**
     * Creates a <code>DataLimits</code> instance to be used for paginating internally GROUP BY queries.
     *
     * @param state the <code>GroupMaker</code> state
     * @return a <code>DataLimits</code> instance to be used for paginating internally GROUP BY queries
     */
    public DataLimits forGroupByInternalPaging(GroupingState state)
    {
        throw new UnsupportedOperationException();
    }

    public abstract boolean hasEnoughLiveData(CachedPartition cached, int nowInSec, boolean countPartitionsWithOnlyStaticData);

    /**
     * Returns a new {@code Counter} for this limits.
     *
     * @param nowInSec the current time in second (to decide what is expired or not).
     * @param assumeLiveData if true, the counter will assume that every row passed is live and won't
     * thus check for liveness, otherwise it will. This should be {@code true} when used on a
     * {@code RowIterator} (since it only returns live rows), false otherwise.
     * @param countPartitionsWithOnlyStaticData if {@code true} the partitions with only static data should be counted
     * as 1 valid row.
     * @return a new {@code Counter} for this limits.
     */
    public abstract Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData);

    /**
     * The max number of results this limits enforces.
     * <p>
     * Note that the actual definition of "results" depends a bit: for "normal" queries it's a number of rows,
     * but for GROUP BY queries it's a number of groups.
     *
     * @return the maximum number of results this limits enforces.
     */
    public abstract int count();

    public abstract int perPartitionCount();

    /**
     * Returns equivalent limits but where any internal state kept to track where we are of paging and/or grouping is
     * discarded.
     */
    public abstract DataLimits withoutState();

    /**
     * Estimate the number of results that a full scan of the provided cfs would yield.
     */
    public abstract float estimateTotalResults(ColumnFamilyStore cfs);

    public abstract class Counter
    {
        protected final int nowInSec;
        protected final boolean assumeLiveData;

        protected Counter(int nowInSec, boolean assumeLiveData)
        {
            this.nowInSec = nowInSec;
            this.assumeLiveData = assumeLiveData;
        }

        abstract void newPartition(DecoratedKey partitionKey, Row staticRow);
        abstract Row newRow(Row row);           // Can return null, in which case row should not be returned.
        abstract Row newStaticRow(Row row);     // Can return EMPTY_STATIC_ROW, in which case row should not be returned.

        abstract void endOfPartition();
        public abstract void endOfIteration();

        /**
         * The number of results counted.
         * <p>
         * Note that the definition of "results" should be the same that for {@link #count}.
         *
         * @return the number of results counted.
         */
        public abstract int counted();

        public abstract int countedInCurrentPartition();

        /**
         * The number of rows counted.
         *
         * @return the number of rows counted.
         */
        public abstract int rowCounted();

        /**
         * The number of rows counted in the current partition.
         *
         * @return the number of rows counted in the current partition.
         */
        public abstract int rowCountedInCurrentPartition();

        public abstract boolean isDone();
        public abstract boolean isDoneForPartition();

        protected boolean isLive(Row row)
        {
            return assumeLiveData || row.hasLiveData(nowInSec);
        }
    }

    /**
     * Count the number of rows in the partitions. Note that unlike the truncate methods, the flow is not interrupted
     * when the counter is done.
     *
     * @param partitions - the partitions to count
     * @param counter - the counter that will receive the notifications
     *
     * @return the same flow but with the counter operations applied
     */
    public static CsFlow<FlowableUnfilteredPartition> countUnfiltered(CsFlow<FlowableUnfilteredPartition> partitions, Counter counter)
    {
        return partitions.doOnClose(counter::endOfIteration)
                         .map(partition -> countUnfiltered(counter, partition));
    }

    public static FlowableUnfilteredPartition countUnfiltered(Counter counter, FlowableUnfilteredPartition partition)
    {
        counter.newPartition(partition.partitionKey(), partition.staticRow);

        CsFlow<Unfiltered> content = partition.content
                                              .doOnClose(counter::endOfPartition)
                                              .map(unfiltered ->
                                                   {
                                                       if (unfiltered instanceof Row)
                                                           counter.newRow((Row) unfiltered);
                                                       return unfiltered;
                                                   });

        counter.newStaticRow(partition.staticRow);
        return new FlowableUnfilteredPartition(partition.header,
                                               partition.staticRow,
                                               content);
    }

    /**
     * Count the number of rows in the partitions and stop the flow once the counter is done.
     *
     * @param partitions - the partitions to count
     * @param nowInSec - the current time in seconds
     * @param countPartitionsWithOnlyStaticData if {@code true} the partitions with only static data should be counted
     * as 1 valid row.
     *
     * @return the truncated flow
     */
    public CsFlow<FlowableUnfilteredPartition> truncateUnfiltered(CsFlow<FlowableUnfilteredPartition> partitions, int nowInSec, boolean countPartitionsWithOnlyStaticData)
    {
        Counter counter = this.newCounter(nowInSec, false, countPartitionsWithOnlyStaticData);
        return truncateUnfiltered(partitions, counter);
    }

    public static CsFlow<FlowableUnfilteredPartition> truncateUnfiltered(CsFlow<FlowableUnfilteredPartition> partitions, Counter counter)
    {
        return partitions.takeUntilAndDoOnClose(counter::isDone,
                                                counter::endOfIteration)
                         .map(partition -> truncateUnfiltered(counter, partition));
    }

    public FlowableUnfilteredPartition truncateUnfiltered(FlowableUnfilteredPartition partition, int nowInSec, boolean countPartitionsWithOnlyStaticData)
    {
        Counter counter = this.newCounter(nowInSec, false, countPartitionsWithOnlyStaticData);
        return truncateUnfiltered(counter, partition);
    }

    public static FlowableUnfilteredPartition truncateUnfiltered(Counter counter, FlowableUnfilteredPartition partition)
    {
        counter.newPartition(partition.partitionKey(), partition.staticRow);

        CsFlow<Unfiltered> content = partition.content
                                              .takeUntilAndDoOnClose(counter::isDoneForPartition,
                                                                     counter::endOfPartition)
                                              .skippingMap(unfiltered ->
                                                       unfiltered instanceof Row
                                                               ? counter.newRow((Row) unfiltered)
                                                               : unfiltered);
        return new FlowableUnfilteredPartition(partition.header,
                                               counter.newStaticRow(partition.staticRow),
                                               content);
    }

    /**
     * Count the number of rows in the partitions and stop the flow once the counter is done.
     *
     * @param partitions - the partitions to count
     * @param nowInSec - the current time in seconds
     * @param countPartitionsWithOnlyStaticData if {@code true} the partitions with only static data should be counted
     * as 1 valid row.
     *
     * @return the truncated flow
     */
    public CsFlow<FlowablePartition> truncateFiltered(CsFlow<FlowablePartition> partitions, int nowInSec, boolean countPartitionsWithOnlyStaticData)
    {
        Counter counter = this.newCounter(nowInSec, true, countPartitionsWithOnlyStaticData);
        return truncateFiltered(partitions, counter);
    }

    public static CsFlow<FlowablePartition> truncateFiltered(CsFlow<FlowablePartition> partitions, Counter counter)
    {
        return partitions.takeUntilAndDoOnClose(counter::isDone,
                                                counter::endOfIteration)
                         .map(partition -> truncateFiltered(counter, partition));
    }

    public FlowablePartition truncateFiltered(FlowablePartition partition, int nowInSec, boolean countPartitionsWithOnlyStaticData)
    {
        Counter counter = this.newCounter(nowInSec, true, countPartitionsWithOnlyStaticData);
        return truncateFiltered(counter, partition);
    }

    public static FlowablePartition truncateFiltered(Counter counter, FlowablePartition partition)
    {
        counter.newPartition(partition.partitionKey(), partition.staticRow);

        CsFlow<Row> content = partition.content.takeUntilAndDoOnClose(counter::isDoneForPartition,
                                                                      counter::endOfPartition)
                                               .skippingMap(counter::newRow);
        return new FlowablePartition(partition.header,
                                     counter.newStaticRow(partition.staticRow),
                                     content);
    }

    /**
     * Limits used by CQL; this counts rows.
     */
    private static class CQLLimits extends DataLimits
    {
        protected final int rowLimit;
        protected final int perPartitionLimit;

        // Whether the query is a distinct query or not.
        protected final boolean isDistinct;

        private CQLLimits(int rowLimit)
        {
            this(rowLimit, NO_LIMIT);
        }

        private CQLLimits(int rowLimit, int perPartitionLimit)
        {
            this(rowLimit, perPartitionLimit, false);
        }

        private CQLLimits(int rowLimit, int perPartitionLimit, boolean isDistinct)
        {
            this.rowLimit = rowLimit;
            this.perPartitionLimit = perPartitionLimit;
            this.isDistinct = isDistinct;
        }

        private static CQLLimits distinct(int rowLimit)
        {
            return new CQLLimits(rowLimit, 1, true);
        }

        public Kind kind()
        {
            return Kind.CQL_LIMIT;
        }

        public boolean isUnlimited()
        {
            return rowLimit == NO_LIMIT && perPartitionLimit == NO_LIMIT;
        }

        public boolean isDistinct()
        {
            return isDistinct;
        }

        public DataLimits forPaging(int pageSize)
        {
            return new CQLLimits(pageSize, perPartitionLimit, isDistinct);
        }

        public DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            return new CQLPagingLimits(pageSize, perPartitionLimit, isDistinct, lastReturnedKey, lastReturnedKeyRemaining);
        }

        public DataLimits forShortReadRetry(int toFetch)
        {
            // When we do a short read retry, we're only ever querying the single partition on which we have a short read. So
            // we use toFetch as the row limit and use no perPartitionLimit (it would be equivalent in practice to use toFetch
            // for both argument or just for perPartitionLimit with no limit on rowLimit).
            return new CQLLimits(toFetch, NO_LIMIT, isDistinct);
        }

        @SuppressWarnings("resource") // cacheIter closed by the partition content blocking operation
        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec, boolean countPartitionsWithOnlyStaticData)
        {
            // We want the number of row that are currently live. Getting that precise number forces
            // us to iterate the cached partition in general, but we can avoid that if:
            //   - The number of rows with at least one non-expiring cell is greater than what we ask,
            //     in which case we know we have enough live.
            //   - The number of rows is less than requested, in which case we  know we won't have enough.
            if (cached.rowsWithNonExpiringCells() >= rowLimit)
                return true;

            if (cached.rowCount() < rowLimit)
                return false;

            // Otherwise, we need to re-count,
            FlowableUnfilteredPartition cachePart = cached.unfilteredPartition(ColumnFilter.selection(cached.columns()), Slices.ALL, false);
            DataLimits.Counter counter = newCounter(nowInSec, false, countPartitionsWithOnlyStaticData);
            FlowableUnfilteredPartition partition = DataLimits.truncateUnfiltered(counter, cachePart);

            // Consume the iterator until we've counted enough
            partition.content.process().blockingSingle(); // will also close cacheIter
            return counter.isDone();
        }

        public Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData)
        {
            return new CQLCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData);
        }

        public int count()
        {
            return rowLimit;
        }

        public int perPartitionCount()
        {
            return perPartitionLimit;
        }

        public DataLimits withoutState()
        {
            return this;
        }

        public float estimateTotalResults(ColumnFamilyStore cfs)
        {
            // TODO: we should start storing stats on the number of rows (instead of the number of cells)
            float rowsPerPartition = ((float) cfs.getMeanCells()) / cfs.metadata().regularColumns().size();
            return rowsPerPartition * (cfs.estimateKeys());
        }

        protected class CQLCounter extends Counter
        {
            protected int rowCounted;
            protected int rowInCurrentPartition;
            protected final boolean countPartitionsWithOnlyStaticData;

            protected boolean hasLiveStaticRow;

            public CQLCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData)
            {
                super(nowInSec, assumeLiveData);
                this.countPartitionsWithOnlyStaticData = countPartitionsWithOnlyStaticData;
            }

            @Override
            public void newPartition(DecoratedKey partitionKey, Row staticRow)
            {
                rowInCurrentPartition = 0;
                hasLiveStaticRow = !staticRow.isEmpty() && isLive(staticRow);
            }

            @Override
            public Row newRow(Row row)
            {
                if (isLive(row))
                    incrementRowCount();
                return row;
            }

            @Override
            public Row newStaticRow(Row row)
            {
                return row;
            }

            @Override
            public void endOfPartition()
            {
                // Normally, we don't count static rows as from a CQL point of view, it will be merge with other
                // rows in the partition. However, if we only have the static row, it will be returned as one row
                // so count it.
                if (countPartitionsWithOnlyStaticData && hasLiveStaticRow && rowInCurrentPartition == 0)
                    incrementRowCount();
            }

            @Override
            public void endOfIteration()
            {
                // nothing to do
            }

            protected void incrementRowCount()
            {
                ++rowCounted;
                ++rowInCurrentPartition;
            }

            public int counted()
            {
                return rowCounted;
            }

            public int countedInCurrentPartition()
            {
                return rowInCurrentPartition;
            }

            public int rowCounted()
            {
                return rowCounted;
            }

            public int rowCountedInCurrentPartition()
            {
                return rowInCurrentPartition;
            }

            public boolean isDone()
            {
                return rowCounted >= rowLimit;
            }

            public boolean isDoneForPartition()
            {
                return isDone() || rowInCurrentPartition >= perPartitionLimit;
            }

            @Override
            public String toString()
            {
                return String.format("[counted: %d, count: %d", counted(), count());
            }
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            if (rowLimit != NO_LIMIT)
            {
                sb.append("LIMIT ").append(rowLimit);
                if (perPartitionLimit != NO_LIMIT)
                    sb.append(' ');
            }

            if (perPartitionLimit != NO_LIMIT)
                sb.append("PER PARTITION LIMIT ").append(perPartitionLimit);

            return sb.toString();
        }
    }

    private static class CQLPagingLimits extends CQLLimits
    {
        private final ByteBuffer lastReturnedKey;
        private final int lastReturnedKeyRemaining;

        public CQLPagingLimits(int rowLimit, int perPartitionLimit, boolean isDistinct, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            super(rowLimit, perPartitionLimit, isDistinct);
            this.lastReturnedKey = lastReturnedKey;
            this.lastReturnedKeyRemaining = lastReturnedKeyRemaining;
        }

        @Override
        public Kind kind()
        {
            return Kind.CQL_PAGING_LIMIT;
        }

        @Override
        public DataLimits forPaging(int pageSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits withoutState()
        {
            return new CQLLimits(rowLimit, perPartitionLimit, isDistinct);
        }

        @Override
        public Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData)
        {
            return new PagingAwareCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData);
        }

        private class PagingAwareCounter extends CQLCounter
        {
            private PagingAwareCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData)
            {
                super(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData);
            }

            @Override
            public void newPartition(DecoratedKey partitionKey, Row staticRow)
            {
                if (partitionKey.getKey().equals(lastReturnedKey))
                {
                    rowInCurrentPartition = perPartitionLimit - lastReturnedKeyRemaining;
                    // lastReturnedKey is the last key for which we're returned rows in the first page.
                    // So, since we know we have returned rows, we know we have accounted for the static row
                    // if any already, so force hasLiveStaticRow to false so we make sure to not count it
                    // once more.
                    hasLiveStaticRow = false;
                }
                else
                {
                    super.newPartition(partitionKey, staticRow);
                }
            }
        }
    }

    /**
     * <code>CQLLimits</code> used for GROUP BY queries or queries with aggregates.
     * <p>Internally, GROUP BY queries are always paginated by number of rows to avoid OOMExceptions. By consequence,
     * the limits keep track of the number of rows as well as the number of groups.</p>
     * <p>A group can only be counted if the next group or the end of the data is reached.</p>
     */
    private static class CQLGroupByLimits extends CQLLimits
    {
        /**
         * The <code>GroupMaker</code> state
         */
        protected final GroupingState state;

        /**
         * The GROUP BY specification
         */
        protected final AggregationSpecification groupBySpec;

        /**
         * The limit on the number of groups
         */
        protected final int groupLimit;

        /**
         * The limit on the number of groups per partition
         */
        protected final int groupPerPartitionLimit;

        public CQLGroupByLimits(int groupLimit,
                                int groupPerPartitionLimit,
                                int rowLimit,
                                AggregationSpecification groupBySpec)
        {
            this(groupLimit, groupPerPartitionLimit, rowLimit, groupBySpec, GroupingState.EMPTY_STATE);
        }

        private CQLGroupByLimits(int groupLimit,
                                 int groupPerPartitionLimit,
                                 int rowLimit,
                                 AggregationSpecification groupBySpec,
                                 GroupingState state)
        {
            super(rowLimit, NO_LIMIT, false);
            this.groupLimit = groupLimit;
            this.groupPerPartitionLimit = groupPerPartitionLimit;
            this.groupBySpec = groupBySpec;
            this.state = state;
        }

        @Override
        public Kind kind()
        {
            return Kind.CQL_GROUP_BY_LIMIT;
        }

        @Override
        public boolean isGroupByLimit()
        {
            return true;
        }

        public boolean isUnlimited()
        {
            return groupLimit == NO_LIMIT && groupPerPartitionLimit == NO_LIMIT && rowLimit == NO_LIMIT;
        }

        public DataLimits forShortReadRetry(int toFetch)
        {
            return new CQLLimits(toFetch);
        }

        @Override
        public float estimateTotalResults(ColumnFamilyStore cfs)
        {
            // For the moment, we return the estimated number of rows as we have no good way of estimating 
            // the number of groups that will be returned. Hopefully, we should be able to fix
            // that problem at some point.
            return super.estimateTotalResults(cfs);
        }

        @Override
        public DataLimits forPaging(int pageSize)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} forPaging({})", hashCode(), pageSize);
            return new CQLGroupByLimits(pageSize,
                                        groupPerPartitionLimit,
                                        rowLimit,
                                        groupBySpec,
                                        state);
        }

        @Override
        public DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} forPaging({}, {}, {}) vs state {}/{}",
                             hashCode(),
                             pageSize,
                             lastReturnedKey == null ? "null" : ByteBufferUtil.bytesToHex(lastReturnedKey),
                             lastReturnedKeyRemaining,
                             state.partitionKey() == null ? "null" : ByteBufferUtil.bytesToHex(state.partitionKey()),
                             state.clustering() == null ? "null" : state.clustering().toBinaryString());
            return new CQLGroupByPagingLimits(pageSize,
                                              groupPerPartitionLimit,
                                              rowLimit,
                                              groupBySpec,
                                              state,
                                              lastReturnedKey,
                                              lastReturnedKeyRemaining);
        }

        @Override
        public DataLimits forGroupByInternalPaging(GroupingState state)
        {
            return new CQLGroupByLimits(rowLimit,
                                        groupPerPartitionLimit,
                                        rowLimit,
                                        groupBySpec,
                                        state);
        }

        @Override
        public Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData)
        {
            return new GroupByAwareCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData);
        }

        @Override
        public int count()
        {
            return groupLimit;
        }

        @Override
        public int perPartitionCount()
        {
            return groupPerPartitionLimit;
        }

        @Override
        public DataLimits withoutState()
        {
            return state == GroupingState.EMPTY_STATE
                 ? this
                 : new CQLGroupByLimits(groupLimit, groupPerPartitionLimit, rowLimit, groupBySpec);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            if (groupLimit != NO_LIMIT)
            {
                sb.append("GROUP LIMIT ").append(groupLimit);
                if (groupPerPartitionLimit != NO_LIMIT || rowLimit != NO_LIMIT)
                    sb.append(' ');
            }

            if (groupPerPartitionLimit != NO_LIMIT)
            {
                sb.append("GROUP PER PARTITION LIMIT ").append(groupPerPartitionLimit);
                if (rowLimit != NO_LIMIT)
                    sb.append(' ');
            }

            if (rowLimit != NO_LIMIT)
            {
                sb.append("LIMIT ").append(rowLimit);
            }

            return sb.toString();
        }

        @Override
        public boolean isExhausted(Counter counter)
        {
            return ((GroupByAwareCounter) counter).rowCounted < rowLimit
                    && counter.counted() < groupLimit;
        }

        protected class GroupByAwareCounter extends Counter
        {
            private final GroupMaker groupMaker;

            protected final boolean countPartitionsWithOnlyStaticData;

            /**
             * The key of the partition being processed.
             */
            protected DecoratedKey currentPartitionKey;

            /**
             * The number of rows counted so far.
             */
            protected int rowCounted;

            /**
             * The number of rows counted so far in the current partition.
             */
            protected int rowCountedInCurrentPartition;

            /**
             * The number of groups counted so far. A group is counted only once it is complete
             * (e.g the next one has been reached).
             */
            protected int groupCounted;

            /**
             * The number of groups in the current partition.
             */
            protected int groupInCurrentPartition;

            protected boolean hasGroupStarted;

            protected boolean hasLiveStaticRow;

            protected boolean hasReturnedRowsFromCurrentPartition;

            private GroupByAwareCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData)
            {
                super(nowInSec, assumeLiveData);
                this.groupMaker = groupBySpec.newGroupMaker(state);
                this.countPartitionsWithOnlyStaticData = countPartitionsWithOnlyStaticData;

                // If the end of the partition was reached at the same time than the row limit, the last group might
                // not have been counted yet. Due to that we need to guess, based on the state, if the previous group
                // is still open.
                hasGroupStarted = state.hasClustering();
            }

            @Override
            public void newPartition(DecoratedKey partitionKey, Row staticRow)
            {
                if (logger.isTraceEnabled())
                    logger.trace("{} - GroupByAwareCounter.applyToPartition {}", hashCode(),
                                 ByteBufferUtil.bytesToHex(partitionKey.getKey()));
                if (partitionKey.getKey().equals(state.partitionKey()))
                {
                    // The only case were we could have state.partitionKey() equals to the partition key
                    // is if some of the partition rows have been returned in the previous page but the
                    // partition was not exhausted (as the state partition key has not been updated yet).
                    // Since we know we have returned rows, we know we have accounted for
                    // the static row if any already, so force hasLiveStaticRow to false so we make sure to not count it
                    // once more.
                    hasLiveStaticRow = false;
                    hasReturnedRowsFromCurrentPartition = true;
                    hasGroupStarted = true;
                }
                else
                {
                    // We need to increment our count of groups if we have reached a new one and unless we had no new
                    // content added since we closed our last group (that is, if hasGroupStarted). Note that we may get
                    // here with hasGroupStarted == false in the following cases:
                    // * the partition limit was reached for the previous partition
                    // * the previous partition was containing only one static row
                    // * the rows of the last group of the previous partition were all marked as deleted
                    if (hasGroupStarted && groupMaker.isNewGroup(partitionKey, Clustering.STATIC_CLUSTERING))
                    {
                        incrementGroupCount();
                        // If we detect, before starting the new partition, that we are done, we need to increase
                        // the per partition group count of the previous partition as the next page will start from
                        // there.
                        if (isDone())
                            incrementGroupInCurrentPartitionCount();
                        hasGroupStarted = false;
                    }
                    hasReturnedRowsFromCurrentPartition = false;
                    hasLiveStaticRow = !staticRow.isEmpty() && isLive(staticRow);
                }
                currentPartitionKey = partitionKey;
                // If we are done we need to preserve the groupInCurrentPartition and rowCountedInCurrentPartition
                // because the pager need to retrieve the count associated to the last value it has returned.
                if (!isDone())
                {
                    groupInCurrentPartition = 0;
                    rowCountedInCurrentPartition = 0;
                }
            }

            @Override
            public Row newStaticRow(Row row)
            {
                if (logger.isTraceEnabled())
                    logger.trace("{} - GroupByAwareCounter.applyToStatic {}/{}",
                                 hashCode(),
                                 ByteBufferUtil.bytesToHex(currentPartitionKey.getKey()),
                                 row == null ? "null" : row.clustering().toBinaryString());

                // It's possible that we're "done" if the partition we just started bumped the number of groups (in
                // applyToPartition() above), in which case Transformation will still call this method. In that case, we
                // want to ignore the static row, it should (and will) be returned with the next page/group if needs be.
                if (isDone())
                {
                    hasLiveStaticRow = false; // The row has not been returned
                    return Rows.EMPTY_STATIC_ROW;
                }
                return row;
            }

            @Override
            public Row newRow(Row row)
            {
                if (logger.isTraceEnabled())
                    logger.trace("{} - GroupByAwareCounter.applyToRow {}/{}",
                                 hashCode(),
                                 ByteBufferUtil.bytesToHex(currentPartitionKey.getKey()),
                                 row.clustering().toBinaryString());

                // We want to check if the row belongs to a new group even if it has been deleted. The goal being
                // to minimize the chances of having to go through the same data twice if we detect on the next
                // non deleted row that we have reached the limit.
                if (groupMaker.isNewGroup(currentPartitionKey, row.clustering()))
                {
                    if (hasGroupStarted)
                    {
                        incrementGroupCount();
                        incrementGroupInCurrentPartitionCount();
                    }
                    hasGroupStarted = false;
                }

                // That row may have made us increment the group count, which may mean we're done for this partition, in
                // which case we shouldn't count this row (it won't be returned).
                if (isDoneForPartition())
                {
                    hasGroupStarted = false;
                    return null;
                }

                if (isLive(row))
                {
                    hasGroupStarted = true;
                    incrementRowCount();
                    hasReturnedRowsFromCurrentPartition = true;
                }

                return row;
            }

            @Override
            public int counted()
            {
                return groupCounted;
            }

            @Override
            public int countedInCurrentPartition()
            {
                return groupInCurrentPartition;
            }

            @Override
            public int rowCounted()
            {
                return rowCounted;
            }

            @Override
            public int rowCountedInCurrentPartition()
            {
                return rowCountedInCurrentPartition;
            }

            protected void incrementRowCount()
            {
                rowCountedInCurrentPartition++;
                ++rowCounted;
            }

            private void incrementGroupCount()
            {
                groupCounted++;
            }

            private void incrementGroupInCurrentPartitionCount()
            {
                groupInCurrentPartition++;
            }

            @Override
            public boolean isDoneForPartition()
            {
                return isDone() || groupInCurrentPartition >= groupPerPartitionLimit;
            }

            @Override
            public boolean isDone()
            {
                return groupCounted >= groupLimit;
            }

            @Override
            public void endOfPartition()
            {
                // Normally, we don't count static rows as from a CQL point of view, it will be merge with other
                // rows in the partition. However, if we only have the static row, it will be returned as one group
                // so count it.
                if (countPartitionsWithOnlyStaticData && hasLiveStaticRow && !hasReturnedRowsFromCurrentPartition)
                {
                    incrementRowCount();
                    incrementGroupCount();
                    incrementGroupInCurrentPartitionCount();
                    hasGroupStarted = false;
                }
            }

            @Override
            public void endOfIteration()
            {
                // Groups are only counted when the end of the group is reached.
                // The end of a group is detected by 2 ways:
                // 1) a new group is reached
                // 2) the end of the data is reached
                // We know that the end of the data is reached if the group limit has not been reached
                // and the number of rows counted is smaller than the internal page size.
                if (hasGroupStarted && groupCounted < groupLimit && rowCounted < rowLimit)
                {
                    incrementGroupCount();
                    incrementGroupInCurrentPartitionCount();
                }
            }
        }
    }

    private static class CQLGroupByPagingLimits extends CQLGroupByLimits
    {
        private final ByteBuffer lastReturnedKey;

        private final int lastReturnedKeyRemaining;

        public CQLGroupByPagingLimits(int groupLimit,
                                      int groupPerPartitionLimit,
                                      int rowLimit,
                                      AggregationSpecification groupBySpec,
                                      GroupingState state,
                                      ByteBuffer lastReturnedKey,
                                      int lastReturnedKeyRemaining)
        {
            super(groupLimit,
                  groupPerPartitionLimit,
                  rowLimit,
                  groupBySpec,
                  state);

            this.lastReturnedKey = lastReturnedKey;
            this.lastReturnedKeyRemaining = lastReturnedKeyRemaining;
        }

        @Override
        public Kind kind()
        {
            return Kind.CQL_GROUP_BY_PAGING_LIMIT;
        }

        @Override
        public DataLimits forPaging(int pageSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits forGroupByInternalPaging(GroupingState state)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData)
        {
            assert state == GroupingState.EMPTY_STATE || lastReturnedKey.equals(state.partitionKey());
            return new PagingGroupByAwareCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData);
        }

        @Override
        public DataLimits withoutState()
        {
            return new CQLGroupByLimits(groupLimit, groupPerPartitionLimit, rowLimit, groupBySpec);
        }

        private class PagingGroupByAwareCounter extends GroupByAwareCounter
        {
            private PagingGroupByAwareCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData)
            {
                super(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData);
            }

            @Override
            public void newPartition(DecoratedKey partitionKey, Row staticRow)
            {
                if (logger.isTraceEnabled())
                    logger.trace("{} - CQLGroupByPagingLimits.applyToPartition {}",
                                 hashCode(), ByteBufferUtil.bytesToHex(partitionKey.getKey()));

                if (partitionKey.getKey().equals(lastReturnedKey))
                {
                    currentPartitionKey = partitionKey;
                    groupInCurrentPartition = groupPerPartitionLimit - lastReturnedKeyRemaining;
                    hasReturnedRowsFromCurrentPartition = true;
                    hasLiveStaticRow = false;
                    hasGroupStarted = state.hasClustering();
                }
                else
                {
                    super.newPartition(partitionKey, staticRow);
                }
            }
        }
    }

    public static class Serializer extends VersionDependent<ReadVersion>
    {
        private Serializer(ReadVersion version)
        {
            super(version);
        }

        public void serialize(DataLimits limits, DataOutputPlus out, ClusteringComparator comparator) throws IOException
        {
            out.writeByte(limits.kind().ordinal());
            switch (limits.kind())
            {
                case CQL_LIMIT:
                case CQL_PAGING_LIMIT:
                    CQLLimits cqlLimits = (CQLLimits)limits;
                    out.writeUnsignedVInt(cqlLimits.rowLimit);
                    out.writeUnsignedVInt(cqlLimits.perPartitionLimit);
                    out.writeBoolean(cqlLimits.isDistinct);
                    if (limits.kind() == Kind.CQL_PAGING_LIMIT)
                    {
                        CQLPagingLimits pagingLimits = (CQLPagingLimits)cqlLimits;
                        ByteBufferUtil.writeWithVIntLength(pagingLimits.lastReturnedKey, out);
                        out.writeUnsignedVInt(pagingLimits.lastReturnedKeyRemaining);
                    }
                    break;
                case CQL_GROUP_BY_LIMIT:
                case CQL_GROUP_BY_PAGING_LIMIT:
                    CQLGroupByLimits groupByLimits = (CQLGroupByLimits) limits;
                    out.writeUnsignedVInt(groupByLimits.groupLimit);
                    out.writeUnsignedVInt(groupByLimits.groupPerPartitionLimit);
                    out.writeUnsignedVInt(groupByLimits.rowLimit);

                    AggregationSpecification groupBySpec = groupByLimits.groupBySpec;
                    AggregationSpecification.serializers.get(version).serialize(groupBySpec, out);

                    GroupingState.serializers.get(version).serialize(groupByLimits.state, out, comparator);

                    if (limits.kind() == Kind.CQL_GROUP_BY_PAGING_LIMIT)
                    {
                        CQLGroupByPagingLimits pagingLimits = (CQLGroupByPagingLimits) groupByLimits;
                        ByteBufferUtil.writeWithVIntLength(pagingLimits.lastReturnedKey, out);
                        out.writeUnsignedVInt(pagingLimits.lastReturnedKeyRemaining);
                     }
                     break;
            }
        }

        public DataLimits deserialize(DataInputPlus in, TableMetadata metadata) throws IOException
        {
            Kind kind = Kind.values()[in.readUnsignedByte()];
            switch (kind)
            {
                case CQL_LIMIT:
                case CQL_PAGING_LIMIT:
                {
                    int rowLimit = (int) in.readUnsignedVInt();
                    int perPartitionLimit = (int) in.readUnsignedVInt();
                    boolean isDistinct = in.readBoolean();
                    if (kind == Kind.CQL_LIMIT)
                        return cqlLimits(rowLimit, perPartitionLimit, isDistinct);
                    ByteBuffer lastKey = ByteBufferUtil.readWithVIntLength(in);
                    int lastRemaining = (int) in.readUnsignedVInt();
                    return new CQLPagingLimits(rowLimit, perPartitionLimit, isDistinct, lastKey, lastRemaining);
                }
                case CQL_GROUP_BY_LIMIT:
                case CQL_GROUP_BY_PAGING_LIMIT:
                {
                    int groupLimit = (int) in.readUnsignedVInt();
                    int groupPerPartitionLimit = (int) in.readUnsignedVInt();
                    int rowLimit = (int) in.readUnsignedVInt();

                    AggregationSpecification groupBySpec = AggregationSpecification.serializers.get(version).deserialize(in, metadata);

                    GroupingState state = GroupingState.serializers.get(version).deserialize(in, metadata.comparator);

                    if (kind == Kind.CQL_GROUP_BY_LIMIT)
                        return new CQLGroupByLimits(groupLimit,
                                                    groupPerPartitionLimit,
                                                    rowLimit,
                                                    groupBySpec,
                                                    state);

                    ByteBuffer lastKey = ByteBufferUtil.readWithVIntLength(in);
                    int lastRemaining = (int) in.readUnsignedVInt();
                    return new CQLGroupByPagingLimits(groupLimit,
                                                      groupPerPartitionLimit,
                                                      rowLimit,
                                                      groupBySpec,
                                                      state,
                                                      lastKey,
                                                      lastRemaining);
                }
            }
            throw new AssertionError();
        }

        public long serializedSize(DataLimits limits, ClusteringComparator comparator)
        {
            long size = TypeSizes.sizeof((byte) limits.kind().ordinal());
            switch (limits.kind())
            {
                case CQL_LIMIT:
                case CQL_PAGING_LIMIT:
                    CQLLimits cqlLimits = (CQLLimits) limits;
                    size += TypeSizes.sizeofUnsignedVInt(cqlLimits.rowLimit);
                    size += TypeSizes.sizeofUnsignedVInt(cqlLimits.perPartitionLimit);
                    size += TypeSizes.sizeof(cqlLimits.isDistinct);
                    if (limits.kind() == Kind.CQL_PAGING_LIMIT)
                    {
                        CQLPagingLimits pagingLimits = (CQLPagingLimits) cqlLimits;
                        size += ByteBufferUtil.serializedSizeWithVIntLength(pagingLimits.lastReturnedKey);
                        size += TypeSizes.sizeofUnsignedVInt(pagingLimits.lastReturnedKeyRemaining);
                    }
                    break;
                case CQL_GROUP_BY_LIMIT:
                case CQL_GROUP_BY_PAGING_LIMIT:
                    CQLGroupByLimits groupByLimits = (CQLGroupByLimits) limits;
                    size += TypeSizes.sizeofUnsignedVInt(groupByLimits.groupLimit);
                    size += TypeSizes.sizeofUnsignedVInt(groupByLimits.groupPerPartitionLimit);
                    size += TypeSizes.sizeofUnsignedVInt(groupByLimits.rowLimit);

                    AggregationSpecification groupBySpec = groupByLimits.groupBySpec;
                    size += AggregationSpecification.serializers.get(version).serializedSize(groupBySpec);

                    size += GroupingState.serializers.get(version).serializedSize(groupByLimits.state, comparator);

                    if (limits.kind() == Kind.CQL_GROUP_BY_PAGING_LIMIT)
                    {
                        CQLGroupByPagingLimits pagingLimits = (CQLGroupByPagingLimits) groupByLimits;
                        size += ByteBufferUtil.serializedSizeWithVIntLength(pagingLimits.lastReturnedKey);
                        size += TypeSizes.sizeofUnsignedVInt(pagingLimits.lastReturnedKeyRemaining);
                    }
                    break;
                default:
                    throw new AssertionError();
            }
            return size;
        }
    }
}
