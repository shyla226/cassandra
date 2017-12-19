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

package org.apache.cassandra.db.partitions;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.ArrayBackedRow;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.PartitionHeader;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowAndDeletionMergeIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIndexedListIterator;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.flow.Flow;


public class ArrayBackedPartition implements Partition
{
    private final static Logger logger = LoggerFactory.getLogger(ArrayBackedPartition.class);

    public static final class Holder
    {
        RegularAndStaticColumns columns;
        DeletionInfo deletionInfo;
        Row[] rows;
        int length;
        Row staticRow;
        EncodingStats stats;

        Holder(RegularAndStaticColumns columns, Row[] data, int length,  DeletionInfo deletionInfo, Row staticRow, EncodingStats stats)
        {
            this.columns = columns;
            this.rows = data;
            this.length = length;
            this.deletionInfo = deletionInfo;
            this.staticRow = staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
            this.stats = stats;
        }

        Holder(RegularAndStaticColumns columns, int initialRows, Row staticRow, EncodingStats stats)
        {
            this.columns = columns;
            this.rows = new Row[Math.max(initialRows, 2)];
            this.length = 0;
            this.deletionInfo = DeletionInfo.LIVE;
            this.staticRow = staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
            this.stats = stats;
        }

        @Inline
        void maybeGrow()
        {
            if (length == rows.length)
            {
                Row[] newRows;
                newRows = new Row[Math.max(8, length * 2)];

                System.arraycopy(rows, 0, newRows, 0, length);
                rows = newRows;
            }
        }
    }

    protected final DecoratedKey partitionKey;
    protected final Holder holder;
    protected final TableMetadata metadata;

    public static ArrayBackedPartition create(UnfilteredRowIterator iterator)
    {
        return create(iterator, INITIAL_ROW_CAPACITY);
    }

    public static ArrayBackedPartition create(UnfilteredRowIterator iterator, int initialRowCapacity)
    {
        assert initialRowCapacity > 0;
        Row[] rows = new Row[initialRowCapacity];
        int length = 0;

        MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(iterator.partitionLevelDeletion(), iterator.metadata().comparator, iterator.isReverseOrder());

        while (iterator.hasNext())
        {
            Unfiltered unfiltered = iterator.next();
            if (unfiltered.kind() == Unfiltered.Kind.ROW)
            {
                //grow
                if (length == rows.length)
                {
                    Row[] newRows = new Row[Math.max(8, length * 2)];
                    System.arraycopy(rows, 0, newRows, 0, length);
                    rows = newRows;
                }

                rows[length++] = (Row) unfiltered;
            }
            else
            {
                deletionBuilder.add((RangeTombstoneMarker) unfiltered);
            }
        }

        if (iterator.isReverseOrder() && length > 0)
        {
            Row tmp;
            for (int i = 0; i < length; i++)
            {
                tmp = rows[i];
                int swapidx = length - 1 - i;
                rows[i] = rows[swapidx];
                rows[swapidx] = tmp;
            }
        }

        Holder holder = new Holder(iterator.metadata().regularAndStaticColumns(), rows, length, deletionBuilder.build(), iterator.staticRow(), iterator.stats());

        return new ArrayBackedPartition(iterator.partitionKey(), holder, iterator.metadata());
    }

    public static Flow<Partition> create(FlowableUnfilteredPartition partition)
    {
        return create(partition, INITIAL_ROW_CAPACITY);
    }

    public static Flow<Partition> create(Flow<FlowableUnfilteredPartition> partitions)
    {
        return create(partitions, INITIAL_ROW_CAPACITY);
    }

    public static Flow<Partition> create(Flow<FlowableUnfilteredPartition> partitions, int initialRowCapacity)
    {
        return partitions.flatMap(partition -> create(partition, initialRowCapacity));
    }

    public static Flow<Partition> create(FlowableUnfilteredPartition partition, int initialRowCapacity)
    {
        PartitionHeader header = partition.header();
        ClusteringComparator comparator = header.metadata.comparator;
        Row staticRow = partition.staticRow();

        assert initialRowCapacity > 0;
        Holder h = new Holder(header.columns,  initialRowCapacity, staticRow, header.stats);
        MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(header.partitionLevelDeletion, comparator, header.isReverseOrder);

        // Note that multiple subscribers would share the builder, i.e. Holder should not be reused.
        return partition.content()
                        .process(unfiltered ->
                                 {
                                     if (unfiltered.kind() == Unfiltered.Kind.ROW)
                                     {
                                         h.maybeGrow();
                                         h.rows[h.length++] = (Row) unfiltered;
                                     }
                                     else
                                     {
                                         deletionBuilder.add((RangeTombstoneMarker) unfiltered);
                                     }
                                 })
                        .skippingMap(VOID ->
                                     {
                                         DeletionInfo deletion = deletionBuilder.build();
                                         // At this point we can easily check if the partition is empty and skip such partitions.
                                         if (h.length == 0 && deletion.isLive() && staticRow.isEmpty())
                                             return null;

                                         h.deletionInfo = deletion;

                                         if (partition.isReverseOrder() && h.length > 0)
                                         {
                                             Row tmp;
                                             for (int i = 0; i < h.length / 2; i++)
                                             {
                                                 tmp = h.rows[i];
                                                 int swapidx = h.length - 1 - i;
                                                 h.rows[i] = h.rows[swapidx];
                                                 h.rows[swapidx] = tmp;
                                             }
                                         }

                                         return new ArrayBackedPartition(header.partitionKey, h, partition.metadata());
                                     });
    }

    protected ArrayBackedPartition(DecoratedKey partitionKey, Holder holder, TableMetadata metadata)
    {
        this.partitionKey = partitionKey;
        this.holder = holder;
        this.metadata = metadata;
    }

    protected Holder holder()
    {
        return holder;
    }

    public TableMetadata metadata()
    {
        return metadata;
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return holder.deletionInfo.getPartitionDeletion();
    }

    public RegularAndStaticColumns columns()
    {
        return holder().columns;
    }

    public EncodingStats stats()
    {
        return holder().stats;
    }

    public int rowCount()
    {
        return holder().length;
    }

    public Row lastRow()
    {
        Holder holder = holder();
        return holder.length > 0 ? holder.rows[holder.length - 1] : null;
    }

    public DeletionInfo deletionInfo()
    {
        return holder().deletionInfo;
    }

    public Row staticRow()
    {
        return holder().staticRow;
    }

    public boolean isEmpty()
    {
        final Holder holder = holder();
        return holder.deletionInfo.isLive() && holder.length == 0 && holder.staticRow.isEmpty();
    }


    public boolean hasRows()
    {
        return holder().length > 0;
    }

    public Iterator<Row> iterator()
    {
        return new AbstractIndexedListIterator<Row>(holder.length, 0)
        {
            protected Row get(int index)
            {
                return holder.rows[index];
            }
        };
    }


    public Row getRow(Clustering clustering)
    {
        Row row = searchIterator(ColumnFilter.selection(columns()), false).next(clustering);

        // Note that for statics, this will never return null, this will return an empty row. However,
        // it's more consistent for this method to return null if we don't really have a static row.
        return row == null || (clustering == Clustering.STATIC_CLUSTERING && row.isEmpty()) ? null : row;
    }

    protected void append(Row row)
    {
        holder.maybeGrow();
        holder.rows[holder.length++] = row;
    }

    public SearchIterator<Clustering, Row> searchIterator(ColumnFilter columns, boolean reversed)
    {
        final Holder holder = holder();

        return new SearchIterator<Clustering, Row>()
        {
            private final SearchIterator<ClusteringPrefix, Row> rawIter = new ArrayBackedSearchIterator(reversed);
            private final DeletionTime partitionDeletion = holder.deletionInfo.getPartitionDeletion();

            public Row next(Clustering clustering)
            {
                if (clustering == Clustering.STATIC_CLUSTERING)
                    return staticRow(columns, true);

                Row row = rawIter.next(clustering);
                RangeTombstone rt = holder.deletionInfo.rangeCovering(clustering);

                // A search iterator only return a row, so it doesn't allow to directly account for deletion that should apply to to row
                // (the partition deletion or the deletion of a range tombstone that covers it). So if needs be, reuse the row deletion
                // to carry the proper deletion on the row.
                DeletionTime activeDeletion = partitionDeletion;
                if (rt != null && rt.deletionTime().supersedes(activeDeletion))
                    activeDeletion = rt.deletionTime();

                if (row == null)
                    return activeDeletion.isLive() ? null : ArrayBackedRow.emptyDeletedRow(clustering, Row.Deletion.regular(activeDeletion));

                return row.filter(columns, activeDeletion, true, metadata());
            }
        };
    }

    public int indexOf(ClusteringPrefix clustering)
    {
        return Arrays.binarySearch(holder.rows, 0, holder.length, clustering, metadata.comparator);
    }

    public UnfilteredRowIterator unfilteredIterator()
    {
        return unfilteredIterator(ColumnFilter.selection(columns()), Slices.ALL, false);
    }

    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        Row staticRow = staticRow(selection, false);
        if (slices.size() == 0)
        {
            DeletionTime partitionDeletion = holder.deletionInfo.getPartitionDeletion();
            return UnfilteredRowIterators.noRowsIterator(metadata(), partitionKey(), staticRow, partitionDeletion, reversed);
        }

        return slices.size() == 1
               ? sliceIterator(selection, slices.get(0), reversed, staticRow)
               : new SlicesIterator(selection, slices, reversed, staticRow);
    }

    public FlowableUnfilteredPartition unfilteredPartition()
    {
        return FlowablePartitions.fromIterator(unfilteredIterator());
    }

    public FlowableUnfilteredPartition unfilteredPartition(ColumnFilter selection, Slices slices, boolean reversed)
    {
        return FlowablePartitions.fromIterator(unfilteredIterator(selection, slices, reversed));
    }

    private UnfilteredRowIterator sliceIterator(ColumnFilter selection, Slice slice, boolean reversed, Row staticRow)
    {
        ClusteringBound start = slice.start() == ClusteringBound.BOTTOM ? null : slice.start();
        ClusteringBound end = slice.end() == ClusteringBound.TOP ? null : slice.end();
        Iterator<Row> rowIter = slice(start, end, reversed);
        Iterator<RangeTombstone> deleteIter = holder.deletionInfo.rangeIterator(slice, reversed);
        return merge(rowIter, deleteIter, selection, reversed, staticRow);
    }

    private Iterator<Row> slice(ClusteringBound start, ClusteringBound end, boolean reversed)
    {
        if (holder.length == 0)
            return Collections.emptyIterator();

        int startAt = start == null ? 0 : indexOf(start);
        if (startAt < 0)
            startAt = -startAt - 1;

        int endAt = end == null ? holder.length - 1 : indexOf(end);
        if (endAt < 0)
            endAt = -endAt - 2;

        if (endAt < 0 || endAt < startAt)
            return Collections.emptyIterator();

        int startAtIdx = startAt;
        int endAtIdx = endAt;
        assert endAtIdx >= startAtIdx;
        return new AbstractIndexedListIterator<Row>(endAtIdx + 1, startAtIdx)
        {
            protected Row get(int index)
            {
                int idx = reversed ? endAtIdx - (index - startAtIdx) : index;
                return holder.rows[idx];
            }
        };
    }

    private Row staticRow(ColumnFilter columns, boolean setActiveDeletionToRow)
    {
        DeletionTime partitionDeletion = holder().deletionInfo.getPartitionDeletion();
        if (columns.fetchedColumns().statics.isEmpty() || (holder().staticRow.isEmpty() && partitionDeletion.isLive()))
            return Rows.EMPTY_STATIC_ROW;

        Row row = holder().staticRow.filter(columns, partitionDeletion, setActiveDeletionToRow, metadata());
        return row == null ? Rows.EMPTY_STATIC_ROW : row;
    }

    private RowAndDeletionMergeIterator merge(Iterator<Row> rowIter, Iterator<RangeTombstone> deleteIter,
                                              ColumnFilter selection, boolean reversed, Row staticRow)
    {
        return new RowAndDeletionMergeIterator(metadata(), partitionKey(), holder().deletionInfo.getPartitionDeletion(),
                                               selection, staticRow, reversed, holder().stats,
                                               rowIter, deleteIter, canHaveShadowedData());
    }


    protected boolean canHaveShadowedData()
    {
        return false;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("[%s] key=%s partition_deletion=%s columns=%s",
                                metadata(),
                                metadata().partitionKeyType.getString(partitionKey().getKey()),
                                partitionLevelDeletion(),
                                columns()));

        if (staticRow() != Rows.EMPTY_STATIC_ROW)
            sb.append("\n    ").append(staticRow().toString(metadata(), true));

        try (UnfilteredRowIterator iter = unfilteredIterator())
        {
            while (iter.hasNext())
                sb.append("\n    ").append(iter.next().toString(metadata(), true));
        }

        return sb.toString();
    }


    class SlicesIterator extends ArrayBackedUnfilteredRowIterator
    {
        private final Slices slices;

        private int idx;
        private Iterator<Unfiltered> currentSlice;
        private final ColumnFilter selection;

        private SlicesIterator(ColumnFilter selection,
                               Slices slices,
                               boolean isReversed,
                               Row staticRow)
        {
            super(isReversed, staticRow, selection.fetchedColumns());
            this.selection = selection;
            this.slices = slices;
        }

        protected Unfiltered computeNext()
        {
            while (true)
            {
                if (currentSlice == null)
                {
                    if (idx >= slices.size())
                        return endOfData();

                    int sliceIdx = reversed ? slices.size() - idx - 1 : idx;
                    currentSlice = sliceIterator(selection, slices.get(sliceIdx), reversed, Rows.EMPTY_STATIC_ROW);
                    idx++;
                }

                if (currentSlice.hasNext())
                    return currentSlice.next();

                currentSlice = null;
            }
        }
    }

    class ArrayBackedUnfilteredRowIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
    {
        final Holder holder;
        final boolean reversed;
        final Row staticRow;
        final RegularAndStaticColumns columns;

        int index;
        ArrayBackedUnfilteredRowIterator(boolean reversed, Row staticRow, RegularAndStaticColumns columns)
        {
            this.holder = holder();
            this.reversed = reversed;
            this.index = reversed ? holder.length - 1 : 0;
            this.staticRow = staticRow;
            this.columns = columns;
        }

        public TableMetadata metadata()
        {
            return metadata;
        }

        public boolean isReverseOrder()
        {
            return reversed;
        }

        public RegularAndStaticColumns columns()
        {
            return columns;
        }

        public DecoratedKey partitionKey()
        {
            return partitionKey;
        }

        public Row staticRow()
        {
            return staticRow;
        }

        public DeletionTime partitionLevelDeletion()
        {
            return holder.deletionInfo.getPartitionDeletion();
        }

        public EncodingStats stats()
        {
            return holder.stats;
        }

        protected Unfiltered computeNext()
        {
            if (index < 0 || index >= holder.length)
                return endOfData();

            Unfiltered next = holder.rows[index];
            index += reversed ? -1 : +1;

            return next;
        }
    }

    class ArrayBackedSearchIterator implements SearchIterator<ClusteringPrefix, Row>
    {
        final boolean reversed;
        int index;

        ArrayBackedSearchIterator(boolean reversed)
        {
            this.reversed = reversed;
            this.index = reversed ? holder.length - 1 : 0;
        }

        // Perform a binary search of the data
        public Row next(ClusteringPrefix key)
        {
            int start = reversed ? 0 : index;
            int end = reversed ? index : holder.length;

            index = Arrays.binarySearch(holder.rows, start, end, key, metadata.comparator);
            if (index < 0)
            {
                index = -index - 1;
                return null;
            }

            return holder.rows[index++];
        }
    }
}
