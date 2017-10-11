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

import org.apache.cassandra.cql3.PageSize;
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
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * Object in charge of tracking if we have fetched enough data for a given query.
 * <p>
 * This is more complicated than a single count because we support {@code PER PARTITION}
 * limits, but also due to {@code GROUP BY} and paging.
 * </p>
 * <p>
 * Tracking happens by row count ({@see count()}) and bytes ({@see bytes()}), with the first exhausted limit
 * taking precedence.
 * </p>
 * <p>
 * When paging is used (see {@code forPaging} methods), the minimum number between the page size and the rows/bytes
 * limit is enforced, meaning that we'll never return more rows than requested.
 * </p>
 */
public abstract class DataLimits
{
    private static final Logger logger = LoggerFactory.getLogger(DataLimits.class);

    public static final Versioned<ReadVersion, Serializer> serializers = ReadVersion.versioned(Serializer::new);

    public static final int NO_ROWS_LIMIT = Integer.MAX_VALUE;
    public static final int NO_BYTES_LIMIT = Integer.MAX_VALUE;

    public static final DataLimits NONE = new CQLLimits(NO_ROWS_LIMIT)
    {
        @Override
        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            return false;
        }
    };

    // We currently deal with distinct queries by querying full partitions but limiting the result at 1 row per
    // partition (see SelectStatement.makeFilter). So an "unbounded" distinct is still actually doing some filtering.
    public static final DataLimits DISTINCT_NONE = new CQLLimits(NO_ROWS_LIMIT, 1, true);

    public enum Kind { CQL_LIMIT, CQL_PAGING_LIMIT, CQL_GROUP_BY_LIMIT, CQL_GROUP_BY_PAGING_LIMIT }

    public static DataLimits cqlLimits(int cqlRowLimit)
    {
        return cqlRowLimit == NO_ROWS_LIMIT ? NONE : new CQLLimits(cqlRowLimit);
    }

    public static DataLimits cqlLimits(int cqlRowLimit, int perPartitionLimit)
    {
        return cqlRowLimit == NO_ROWS_LIMIT && perPartitionLimit == NO_ROWS_LIMIT
             ? NONE
             : new CQLLimits(cqlRowLimit, perPartitionLimit);
    }

    private static DataLimits cqlLimits(int bytesLimit, int cqlRowLimit, int perPartitionLimit, boolean isDistinct)
    {
        return bytesLimit == NO_BYTES_LIMIT && cqlRowLimit == NO_ROWS_LIMIT && perPartitionLimit == NO_ROWS_LIMIT && !isDistinct
             ? NONE
             : new CQLLimits(bytesLimit, cqlRowLimit, perPartitionLimit, isDistinct);
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
        return counter.bytesCounted() < bytes() && counter.counted() < count();
    }

    public abstract DataLimits forPaging(PageSize pageSize);
    public abstract DataLimits forPaging(PageSize pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining);

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

    public abstract boolean hasEnoughLiveData(CachedPartition cached,
                                              int nowInSec,
                                              boolean countPartitionsWithOnlyStaticData,
                                              boolean enforceStrictLiveness);

    /**
     * Returns a new {@code Counter} for this limits.
     *
     * @param nowInSec the current time in second (to decide what is expired or not).
     * @param assumeLiveData if true, the counter will assume that every row passed is live and won't
     * thus check for liveness, otherwise it will. This should be {@code true} when used on a
     * {@code RowIterator} (since it only returns live rows), false otherwise.
     * @param countPartitionsWithOnlyStaticData if {@code true} the partitions with only static data should be counted
     * as 1 valid row.
     * @param enforceStrictLiveness whether the row should be purged if there is no PK liveness info,
     *                              normally retrieved from {@link TableMetadata#enforceStrictLiveness()}
     * @return a new {@code Counter} for this limits.
     */
    public abstract Counter newCounter(int nowInSec,
                                       boolean assumeLiveData,
                                       boolean countPartitionsWithOnlyStaticData,
                                       boolean enforceStrictLiveness);

    /**
     * The max number of bytes this limits enforces.
     * <p>
     * Note that if this value is set, less rows might be returned if the size of the current rows exceeds the bytes limit.
     *
     * @return the maximum number of bytes this limits enforces.
     */
    public abstract int bytes();

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
     * Returns equivalent limits but with updated count value.
     */
    public abstract DataLimits withCount(int count);

    /**
     * Duplicate the current limits with exact same state.
     */
    public abstract DataLimits duplicate();

    /**
     * Estimate the number of results that a full scan of the provided cfs would yield.
     */
    public abstract float estimateTotalResults(ColumnFamilyStore cfs);

    public abstract class Counter
    {
        protected final int nowInSec;
        protected final boolean assumeLiveData;
        private final boolean enforceStrictLiveness;

        protected Counter(int nowInSec, boolean assumeLiveData, boolean enforceStrictLiveness)
        {
            this.nowInSec = nowInSec;
            this.assumeLiveData = assumeLiveData;
            this.enforceStrictLiveness = enforceStrictLiveness;
        }

        abstract void newPartition(DecoratedKey partitionKey, Row staticRow);
        abstract Row newRow(Row row);           // Can return null, in which case row should not be returned.
        abstract Row newStaticRow(Row row);     // Can return EMPTY_STATIC_ROW, in which case row should not be returned.

        public abstract void endOfPartition();
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
         * The number of bytes for the counted rows.
         *
         * @return the number of bytes counted.
         */
        public abstract int bytesCounted();

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

        public abstract boolean isEmpty();
        public abstract boolean isDone();
        public abstract boolean isDoneForPartition();

        protected boolean isLive(Row row)
        {
            return assumeLiveData || row.hasLiveData(nowInSec, enforceStrictLiveness);
        }
    }

    /**
     * Count the number of rows in the partitions. Note that:
     * <ul>
     * <li>Unlike the truncate methods, the flow is not interrupted when the counter is done.</li>
     * <li>This method doesn't call {@link Counter#endOfIteration()}, as that's caller's responsibility.</li>
     * </ul>
     *
     * @param partitions the partitions to count
     * @param counter the counter that will receive the notifications
     *
     * @return the same flow but with the counter operations applied
     */
    public static Flow<FlowableUnfilteredPartition> countUnfilteredPartitions(Flow<FlowableUnfilteredPartition> partitions, Counter counter)
    {
        return partitions
            .map(partition -> countUnfilteredPartition(partition, counter));
    }

    /**
     * Count the number of rows in the given flow. Note that:
     * <ul>
     * <li>Unlike the truncate methods, the flow is not interrupted when the counter is done.</li>
     * <li>This method doesn't call {@link Counter#endOfPartition()}, as that's caller's responsibility.</li>
     * </ul>
     *
     * @param rows the rows to count
     * @param counter the counter that will receive the notifications
     *
     * @return the same flow but with the counter operations applied
     */
    public static Flow<Unfiltered> countUnfilteredRows(Flow<Unfiltered> rows, Counter counter)
    {
        return rows
            .map(unfiltered ->
            {
                if (unfiltered instanceof Row)
                    counter.newRow((Row) unfiltered);

                return unfiltered;
            });
    }

    private static FlowableUnfilteredPartition countUnfilteredPartition(FlowableUnfilteredPartition partition, Counter counter)
    {
        counter.newPartition(partition.partitionKey(), partition.staticRow);

        Flow<Unfiltered> content = countUnfilteredRows(partition.content, counter);

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
    public Flow<FlowableUnfilteredPartition> truncateUnfiltered(Flow<FlowableUnfilteredPartition> partitions, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
    {
        Counter counter = this.newCounter(nowInSec, false, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        return truncateUnfiltered(partitions, counter);
    }

    public static Flow<FlowableUnfilteredPartition> truncateUnfiltered(Flow<FlowableUnfilteredPartition> partitions, Counter counter)
    {
        return partitions.takeUntilAndDoOnClose(counter::isDone,
                                                counter::endOfIteration)
                         .map(partition -> truncateUnfiltered(counter, partition));
    }

    public FlowableUnfilteredPartition truncateUnfiltered(FlowableUnfilteredPartition partition, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
    {
        Counter counter = this.newCounter(nowInSec, false, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        return truncateUnfiltered(counter, partition);
    }

    public static FlowableUnfilteredPartition truncateUnfiltered(Counter counter, FlowableUnfilteredPartition partition)
    {
        counter.newPartition(partition.partitionKey(), partition.staticRow);

        Flow<Unfiltered> content = partition.content
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
    public Flow<FlowablePartition> truncateFiltered(Flow<FlowablePartition> partitions, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
    {
        Counter counter = this.newCounter(nowInSec, true, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        return truncateFiltered(partitions, counter);
    }

    public static Flow<FlowablePartition> truncateFiltered(Flow<FlowablePartition> partitions, Counter counter)
    {
        return partitions.takeUntilAndDoOnClose(counter::isDone,
                                                counter::endOfIteration)
                         .map(partition -> truncateFiltered(counter, partition));
    }

    public FlowablePartition truncateFiltered(FlowablePartition partition, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
    {
        Counter counter = this.newCounter(nowInSec, true, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        return truncateFiltered(counter, partition);
    }

    public static FlowablePartition truncateFiltered(Counter counter, FlowablePartition partition)
    {
        counter.newPartition(partition.partitionKey(), partition.staticRow);

        Flow<Row> content = partition.content.takeUntilAndDoOnClose(counter::isDoneForPartition,
                                                                    counter::endOfPartition)
                                             .skippingMap(counter::newRow);
        return new FlowablePartition(partition.header,
                                     counter.newStaticRow(partition.staticRow),
                                     content);
    }

    /**
     * Limits used by CQL; this counts rows or bytes read. Please note:
     * <ul>
     * <li>When paging on rows, the minimum number of rows between the current limit and the page size is used as actual limit.</li>
     * <li>When paging on bytes, the number of bytes takes precedence over the rows limit.</li>
     * </ul>
     */
    private static class CQLLimits extends DataLimits
    {
        protected final int bytesLimit;
        protected final int rowLimit;
        protected final int perPartitionLimit;

        // Whether the query is a distinct query or not.
        protected final boolean isDistinct;

        private CQLLimits(int rowLimit)
        {
            this(rowLimit, NO_ROWS_LIMIT);
        }

        private CQLLimits(int rowLimit, int perPartitionLimit)
        {
            this(NO_BYTES_LIMIT, rowLimit, perPartitionLimit, false);
        }

        private CQLLimits(int rowLimit, int perPartitionLimit, boolean isDistinct)
        {
            this(NO_BYTES_LIMIT, rowLimit, perPartitionLimit, isDistinct);
        }

        private CQLLimits(int bytesLimit, int rowLimit, int perPartitionLimit, boolean isDistinct)
        {
            this.bytesLimit = bytesLimit;
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
            return bytesLimit == NO_BYTES_LIMIT && rowLimit == NO_ROWS_LIMIT && perPartitionLimit == NO_ROWS_LIMIT;
        }

        public boolean isDistinct()
        {
            return isDistinct;
        }

        public DataLimits forPaging(PageSize pageSize)
        {
            return pageSize.isInBytes()
                   ? new CQLLimits(pageSize.rawSize(), rowLimit, perPartitionLimit, isDistinct)
                   : new CQLLimits(NO_BYTES_LIMIT, Math.min(rowLimit, pageSize.rawSize()), perPartitionLimit, isDistinct);
        }

        public DataLimits forPaging(PageSize pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            return pageSize.isInBytes()
                   ? new CQLPagingLimits(pageSize.rawSize(), rowLimit, perPartitionLimit, isDistinct, lastReturnedKey, lastReturnedKeyRemaining)
                   : new CQLPagingLimits(NO_BYTES_LIMIT, Math.min(rowLimit, pageSize.rawSize()), perPartitionLimit, isDistinct, lastReturnedKey, lastReturnedKeyRemaining);
        }

        public DataLimits forShortReadRetry(int toFetch)
        {
            return new CQLLimits(bytesLimit, toFetch, perPartitionLimit, isDistinct);
        }

        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
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
            DataLimits.Counter counter = newCounter(nowInSec, false, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
            FlowableUnfilteredPartition partition = DataLimits.truncateUnfiltered(counter, cachePart);

            // Consume the iterator until we've counted enough
            partition.content.process().blockingSingle(); // will also close cacheIter
            return counter.isDone();
        }

        public Counter newCounter(int nowInSec,
                                  boolean assumeLiveData,
                                  boolean countPartitionsWithOnlyStaticData,
                                  boolean enforceStrictLiveness)
        {
            return new CQLCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        }

        public int bytes()
        {
            return bytesLimit;
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

        public DataLimits withCount(int count)
        {
            return new CQLLimits(bytesLimit, count, perPartitionLimit, isDistinct);
        }

        public DataLimits duplicate()
        {
            return new CQLLimits(bytesLimit, rowLimit, perPartitionLimit, isDistinct);
        }

        public float estimateTotalResults(ColumnFamilyStore cfs)
        {
            // TODO: we should start storing stats on the number of rows (instead of the number of cells)
            float rowsPerPartition = ((float) cfs.getMeanCells()) / cfs.metadata().regularColumns().size();
            return rowsPerPartition * (cfs.estimateKeys());
        }

        protected class CQLCounter extends Counter
        {
            /**
             * Bytes and rows counted by this counter.
             */
            protected volatile int bytesCounted;
            protected volatile int rowCounted;

            /**
             * Rows counted in the current partition by this counter, plus any previously counted rows for the same partition.
             */
            protected volatile int rowCountedInCurrentPartition;
            protected volatile int previouslyCountedInCurrentPartition;

            protected final boolean countPartitionsWithOnlyStaticData;

            protected volatile int staticRowBytes;

            public CQLCounter(int nowInSec,
                              boolean assumeLiveData,
                              boolean countPartitionsWithOnlyStaticData,
                              boolean enforceStrictLiveness)
            {
                super(nowInSec, assumeLiveData, enforceStrictLiveness);
                this.countPartitionsWithOnlyStaticData = countPartitionsWithOnlyStaticData;
            }

            @Override
            public void newPartition(DecoratedKey partitionKey, Row staticRow)
            {
                rowCountedInCurrentPartition = 0;
                previouslyCountedInCurrentPartition = 0;
                staticRowBytes = !staticRow.isEmpty() && isLive(staticRow) ? staticRow.dataSize() : -1;
            }

            @Override
            public Row newRow(Row row)
            {
                if (isLive(row))
                    incrementRowCount(row.dataSize());
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
                if (countPartitionsWithOnlyStaticData
                    && staticRowBytes > 0
                    && (previouslyCountedInCurrentPartition + rowCountedInCurrentPartition) == 0)
                    incrementRowCount(staticRowBytes);
            }

            @Override
            public void endOfIteration()
            {
                if (logger.isTraceEnabled())
                    logger.trace("{} - counter done: {}", hashCode(), toString());
            }

            protected void incrementRowCount(int rowSize)
            {
                bytesCounted += rowSize;
                ++rowCounted;
                ++rowCountedInCurrentPartition;
            }

            public int counted()
            {
                return rowCounted;
            }

            public int countedInCurrentPartition()
            {
                return rowCountedInCurrentPartition;
            }

            public int bytesCounted()
            {
                return bytesCounted;
            }

            public int rowCounted()
            {
                return rowCounted;
            }

            public int rowCountedInCurrentPartition()
            {
                return rowCountedInCurrentPartition;
            }

            @Override
            public boolean isEmpty()
            {
                return bytesCounted == 0 && rowCounted == 0;
            }

            public boolean isDone()
            {
                return bytesCounted >= bytesLimit || rowCounted >= rowLimit;
            }

            public boolean isDoneForPartition()
            {
                return isDone() || (previouslyCountedInCurrentPartition + rowCountedInCurrentPartition) >= perPartitionLimit;
            }

            @Override
            public String toString()
            {
                return String.format("[counted(bytes,rows,perPartition): (%d,%d,%d), count(bytes,rows,perPartition): (%d,%d,%d)",
                                     bytesCounted(), rowCounted(), rowCountedInCurrentPartition(), bytes(), count(), perPartitionCount());
            }
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            if (bytesLimit != NO_BYTES_LIMIT)
            {
                sb.append("BYTES ").append(bytesLimit);
                if (rowLimit != NO_ROWS_LIMIT)
                    sb.append(' ');
            }

            if (rowLimit != NO_ROWS_LIMIT)
            {
                sb.append("LIMIT ").append(rowLimit);
                if (perPartitionLimit != NO_ROWS_LIMIT)
                    sb.append(' ');
            }

            if (perPartitionLimit != NO_ROWS_LIMIT)
                sb.append("PER PARTITION LIMIT ").append(perPartitionLimit);

            return sb.toString();
        }
    }

    private static class CQLPagingLimits extends CQLLimits
    {
        private final ByteBuffer lastReturnedKey;
        private final int lastReturnedKeyRemaining;

        public CQLPagingLimits(int bytesLimit, int rowLimit, int perPartitionLimit, boolean isDistinct, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            super(bytesLimit, rowLimit, perPartitionLimit, isDistinct);
            this.lastReturnedKey = lastReturnedKey;
            this.lastReturnedKeyRemaining = lastReturnedKeyRemaining;
        }

        @Override
        public Kind kind()
        {
            return Kind.CQL_PAGING_LIMIT;
        }

        @Override
        public DataLimits forPaging(PageSize pageSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits forPaging(PageSize pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits withoutState()
        {
            return new CQLLimits(bytesLimit, rowLimit, perPartitionLimit, isDistinct);
        }

        @Override
        public DataLimits withCount(int count)
        {
            return new CQLPagingLimits(bytesLimit, count, perPartitionLimit, isDistinct, lastReturnedKey, lastReturnedKeyRemaining);
        }

        @Override
        public DataLimits duplicate()
        {
            return new CQLPagingLimits(bytesLimit, rowLimit, perPartitionLimit, isDistinct, lastReturnedKey, lastReturnedKeyRemaining);
        }

        @Override
        public Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            return new PagingAwareCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        }

        private class PagingAwareCounter extends CQLCounter
        {
            private PagingAwareCounter(int nowInSec,
                                       boolean assumeLiveData,
                                       boolean countPartitionsWithOnlyStaticData,
                                       boolean enforceStrictLiveness)
            {
                super(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
            }

            @Override
            public void newPartition(DecoratedKey partitionKey, Row staticRow)
            {
                if (partitionKey.getKey().equals(lastReturnedKey))
                {
                    rowCountedInCurrentPartition = 0;
                    previouslyCountedInCurrentPartition = perPartitionLimit - lastReturnedKeyRemaining;
                    // lastReturnedKey is the last key for which we're returned rows in the first page.
                    // So, since we know we have returned rows, we know we have accounted for the static row
                    // if any already, so force hasLiveStaticRow to false so we make sure to not count it
                    // once more.
                    staticRowBytes = -1;
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
            this(groupLimit, groupPerPartitionLimit, NO_BYTES_LIMIT, rowLimit, groupBySpec, GroupingState.EMPTY_STATE);
        }

        private CQLGroupByLimits(int groupLimit,
                                 int groupPerPartitionLimit,
                                 int bytesLimit,
                                 int rowLimit,
                                 AggregationSpecification groupBySpec,
                                 GroupingState state)
        {
            super(bytesLimit, rowLimit, NO_ROWS_LIMIT, false);
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
            return groupLimit == NO_ROWS_LIMIT && groupPerPartitionLimit == NO_ROWS_LIMIT && super.isUnlimited();
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
        public DataLimits forPaging(PageSize pageSize)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} forPaging({})", hashCode(), pageSize);

            return pageSize.isInBytes()
                   ? new CQLGroupByLimits(groupLimit,
                                          groupPerPartitionLimit,
                                          pageSize.rawSize(),
                                          rowLimit,
                                          groupBySpec,
                                          state)
                   : new CQLGroupByLimits(Math.min(groupLimit, pageSize.rawSize()),
                                          groupPerPartitionLimit,
                                          NO_BYTES_LIMIT,
                                          rowLimit,
                                          groupBySpec,
                                          state);
        }

        @Override
        public DataLimits forPaging(PageSize pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} forPaging({}, {}, {}) vs state {}/{}",
                             hashCode(),
                             pageSize,
                             lastReturnedKey == null ? "null" : ByteBufferUtil.bytesToHex(lastReturnedKey),
                             lastReturnedKeyRemaining,
                             state.partitionKey() == null ? "null" : ByteBufferUtil.bytesToHex(state.partitionKey()),
                             state.clustering() == null ? "null" : state.clustering().toBinaryString());

            return pageSize.isInBytes()
                   ? new CQLGroupByPagingLimits(groupLimit,
                                                groupPerPartitionLimit,
                                                pageSize.rawSize(),
                                                rowLimit,
                                                groupBySpec,
                                                state,
                                                lastReturnedKey,
                                                lastReturnedKeyRemaining)
                   : new CQLGroupByPagingLimits(Math.min(groupLimit, pageSize.rawSize()),
                                                groupPerPartitionLimit,
                                                NO_BYTES_LIMIT,
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
                                        bytesLimit,
                                        rowLimit,
                                        groupBySpec,
                                        state);
        }

        @Override
        public Counter newCounter(int nowInSec,
                                  boolean assumeLiveData,
                                  boolean countPartitionsWithOnlyStaticData,
                                  boolean enforceStrictLiveness)
        {
            return new GroupByAwareCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
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
                 : new CQLGroupByLimits(groupLimit,
                     groupPerPartitionLimit,
                     bytesLimit,
                     rowLimit,
                     groupBySpec,
                     GroupingState.EMPTY_STATE);
        }

        @Override
        public DataLimits withCount(int count)
        {
            return new CQLGroupByLimits(count,
                     groupPerPartitionLimit,
                     bytesLimit,
                     rowLimit,
                     groupBySpec,
                     state);
        }

        @Override
        public DataLimits duplicate()
        {
            return new CQLGroupByLimits(groupLimit,
                     groupPerPartitionLimit,
                     bytesLimit,
                     rowLimit,
                     groupBySpec,
                     state);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            if (groupLimit != NO_ROWS_LIMIT)
            {
                sb.append("GROUP LIMIT ").append(groupLimit);
                if (groupPerPartitionLimit != NO_ROWS_LIMIT || rowLimit != NO_ROWS_LIMIT)
                    sb.append(' ');
            }

            if (groupPerPartitionLimit != NO_ROWS_LIMIT)
            {
                sb.append("GROUP PER PARTITION LIMIT ").append(groupPerPartitionLimit);
                if (bytesLimit != NO_BYTES_LIMIT)
                    sb.append(' ');
            }

            if (bytesLimit != NO_BYTES_LIMIT)
            {
                sb.append("BYTES LIMIT ").append(bytesLimit);
                if (rowLimit != NO_ROWS_LIMIT)
                    sb.append(' ');
            }

            if (rowLimit != NO_ROWS_LIMIT)
            {
                sb.append("LIMIT ").append(rowLimit);
            }

            return sb.toString();
        }

        @Override
        public boolean isExhausted(Counter counter)
        {
            return counter.bytesCounted() < bytesLimit
                && counter.rowCounted() < rowLimit
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
             * The number of bytes counted so far.
             */
            protected int bytesCounted;

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
             * The number of groups in the current partition, counted by this counter.
             */
            protected int groupCountedInCurrentPartition;

             /**
             * The number of groups in the current partition, as previously counted by a different counter.
             */
            protected int previouslyCountedInCurrentPartition;

            protected int staticRowBytes;

            protected boolean hasGroupStarted;

            protected boolean hasReturnedRowsFromCurrentPartition;

            private GroupByAwareCounter(int nowInSec,
                                        boolean assumeLiveData,
                                        boolean countPartitionsWithOnlyStaticData,
                                        boolean enforceStrictLiveness)
            {
                super(nowInSec, assumeLiveData, enforceStrictLiveness);
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
                    logger.trace("{} - GroupByAwareCounter.newPartition {} with state {}", hashCode(),
                                 ByteBufferUtil.bytesToHex(partitionKey.getKey()), state.partitionKey() != null ? ByteBufferUtil.bytesToHex(state.partitionKey()) : "null");

                if (partitionKey.getKey().equals(state.partitionKey()))
                {
                    // The only case were we could have state.partitionKey() equals to the partition key
                    // is if some of the partition rows have been returned in the previous page but the
                    // partition was not exhausted (as the state partition key has not been updated yet).
                    // Since we know we have returned rows, we know we have accounted for
                    // the static row if any already, so force hasLiveStaticRow to false so we make sure to not count it
                    // once more.
                    staticRowBytes = -1;
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
                    staticRowBytes = !staticRow.isEmpty() && isLive(staticRow) ? staticRow.dataSize() : -1;
                }
                currentPartitionKey = partitionKey;
                // If we are done we need to preserve the groupInCurrentPartition and rowCountedInCurrentPartition
                // because the pager need to retrieve the count associated to the last value it has returned.
                if (!isDone())
                {
                    previouslyCountedInCurrentPartition = 0;
                    groupCountedInCurrentPartition = 0;
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
                    staticRowBytes = -1; // The row has not been returned
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
                    incrementRowCount(row.dataSize());
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
                return groupCountedInCurrentPartition;
            }

            @Override
            public int bytesCounted()
            {
                return bytesCounted;
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

            protected void incrementRowCount(int rowSize)
            {
                bytesCounted += rowSize;
                rowCountedInCurrentPartition++;
                ++rowCounted;
            }

            private void incrementGroupCount()
            {
                groupCounted++;
            }

            private void incrementGroupInCurrentPartitionCount()
            {
                groupCountedInCurrentPartition++;
            }

            @Override
            public boolean isDoneForPartition()
            {
                return isDone() || (previouslyCountedInCurrentPartition + groupCountedInCurrentPartition) >= groupPerPartitionLimit;
            }

            @Override
            public boolean isEmpty()
            {
                return groupCounted == 0;
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
                if (countPartitionsWithOnlyStaticData && staticRowBytes > 0 && !hasReturnedRowsFromCurrentPartition)
                {
                    incrementRowCount(staticRowBytes);
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
                if (hasGroupStarted && groupCounted < groupLimit && bytesCounted < bytesLimit && rowCounted < rowLimit)
                {
                    incrementGroupCount();
                    incrementGroupInCurrentPartitionCount();
                }
            }

            @Override
            public String toString()
            {
                return String.format("[counted(bytes,groups,perPartition): (%d,%d,%d), count(bytes,groups,perPartition): (%d,%d,%d)",
                                     bytesCounted(), groupCounted, groupCountedInCurrentPartition, bytes(), count(), perPartitionCount());
            }
        }
    }

    private static class CQLGroupByPagingLimits extends CQLGroupByLimits
    {
        private final ByteBuffer lastReturnedKey;

        private final int lastReturnedKeyRemaining;

        public CQLGroupByPagingLimits(int groupLimit,
                                      int groupPerPartitionLimit,
                                      int bytesLimit,
                                      int rowLimit,
                                      AggregationSpecification groupBySpec,
                                      GroupingState state,
                                      ByteBuffer lastReturnedKey,
                                      int lastReturnedKeyRemaining)
        {
            super(groupLimit,
                  groupPerPartitionLimit,
                  bytesLimit,
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
        public DataLimits forPaging(PageSize pageSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits forPaging(PageSize pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits forGroupByInternalPaging(GroupingState state)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            assert state == GroupingState.EMPTY_STATE || lastReturnedKey.equals(state.partitionKey());
            return new PagingGroupByAwareCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        }

        @Override
        public DataLimits withoutState()
        {
            return new CQLGroupByLimits(groupLimit, groupPerPartitionLimit, rowLimit, groupBySpec);
        }

        @Override
        public DataLimits withCount(int count)
        {
            return new CQLGroupByPagingLimits(count, groupPerPartitionLimit, bytesLimit, rowLimit, groupBySpec, state, lastReturnedKey, lastReturnedKeyRemaining);
        }

        @Override
        public DataLimits duplicate()
        {
            return new CQLGroupByPagingLimits(groupLimit, groupPerPartitionLimit, bytesLimit, rowLimit, groupBySpec, state, lastReturnedKey, lastReturnedKeyRemaining);
        }

        private class PagingGroupByAwareCounter extends GroupByAwareCounter
        {
            private PagingGroupByAwareCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
            {
                super(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
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
                    groupCountedInCurrentPartition = 0;
                    previouslyCountedInCurrentPartition = groupPerPartitionLimit - lastReturnedKeyRemaining;
                    hasReturnedRowsFromCurrentPartition = true;
                    staticRowBytes = -1;
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
                    if (version.compareTo(ReadVersion.DSE_60) >= 0)
                        out.writeUnsignedVInt(cqlLimits.bytesLimit);
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
                    if (version.compareTo(ReadVersion.DSE_60) >= 0)
                        out.writeUnsignedVInt(groupByLimits.bytesLimit);

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
            // Passing bytes as page size is not yet supported for internode communication, so just assume no limit.
            Kind kind = Kind.values()[in.readUnsignedByte()];
            switch (kind)
            {
                case CQL_LIMIT:
                case CQL_PAGING_LIMIT:
                {
                    int rowLimit = (int) in.readUnsignedVInt();
                    int perPartitionLimit = (int) in.readUnsignedVInt();
                    int bytesLimit =  version.compareTo(ReadVersion.DSE_60) >= 0 ? (int)in.readUnsignedVInt() : NO_BYTES_LIMIT;
                    boolean isDistinct = in.readBoolean();
                    if (kind == Kind.CQL_LIMIT)
                        return cqlLimits(bytesLimit, rowLimit, perPartitionLimit, isDistinct);
                    ByteBuffer lastKey = ByteBufferUtil.readWithVIntLength(in);
                    int lastRemaining = (int) in.readUnsignedVInt();
                    return new CQLPagingLimits(bytesLimit, rowLimit, perPartitionLimit, isDistinct, lastKey, lastRemaining);
                }
                case CQL_GROUP_BY_LIMIT:
                case CQL_GROUP_BY_PAGING_LIMIT:
                {
                    int groupLimit = (int) in.readUnsignedVInt();
                    int groupPerPartitionLimit = (int) in.readUnsignedVInt();
                    int rowLimit = (int) in.readUnsignedVInt();
                    int bytesLimit =  version.compareTo(ReadVersion.DSE_60) >= 0 ? (int)in.readUnsignedVInt() : NO_BYTES_LIMIT;

                    AggregationSpecification groupBySpec = AggregationSpecification.serializers.get(version).deserialize(in, metadata);

                    GroupingState state = GroupingState.serializers.get(version).deserialize(in, metadata.comparator);

                    if (kind == Kind.CQL_GROUP_BY_LIMIT)
                        return new CQLGroupByLimits(groupLimit,
                                                    groupPerPartitionLimit,
                                                    bytesLimit,
                                                    rowLimit,
                                                    groupBySpec,
                                                    state);

                    ByteBuffer lastKey = ByteBufferUtil.readWithVIntLength(in);
                    int lastRemaining = (int) in.readUnsignedVInt();
                    return new CQLGroupByPagingLimits(groupLimit,
                                                      groupPerPartitionLimit,
                                                      bytesLimit,
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
                    if (version.compareTo(ReadVersion.DSE_60) >= 0)
                        size += TypeSizes.sizeofUnsignedVInt(cqlLimits.bytesLimit);
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
                    if (version.compareTo(ReadVersion.DSE_60) >= 0)
                        size += TypeSizes.sizeofUnsignedVInt(groupByLimits.bytesLimit);

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
