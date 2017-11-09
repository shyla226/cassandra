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

package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.format.AbstractReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.btree.BTree;

class ReverseReader extends AbstractReader
{
    protected ReusablePartitionData buffer;
    protected Iterator<Unfiltered> iterator;

    // Set in loadFromDisk () and used in setIterator to handle range tombstone extending on multiple index block. See
    // loadFromDisk for details. Note that those are always false for non-indexed readers.
    protected boolean skipFirstIteratedItem;
    protected boolean skipLastIteratedItem;

    final BigTableReader sstable;

    Slice slice = Slice.ALL;

    ReverseReader(BigTableReader sstable,
                  Slices slices,
                  FileDataInput file,
                  boolean shouldCloseFile,
                  SerializationHelper helper)
    {
        super(sstable, slices, file, shouldCloseFile, helper, true);
        this.sstable = sstable;
    }

    protected ReusablePartitionData createBuffer(int blocksCount)
    {
        int estimatedRowCount = 16;
        int columnCount = metadata.regularColumns().size();
        if (columnCount == 0 || metadata.clusteringColumns().isEmpty())
        {
            estimatedRowCount = 1;
        }
        else
        {
            try
            {
                // To avoid wasted resizing we guess-estimate the number of rows we're likely to read. For that
                // we use the stats on the number of rows per partition for that sstable.
                // FIXME: so far we only keep stats on cells, so to get a rough estimate on the number of rows,
                // we divide by the number of regular columns the table has. We should fix once we collect the
                // stats on rows
                int estimatedRowsPerPartition = (int) (sstable.getEstimatedColumnCount().percentile(0.75) /
                                                       columnCount);
                estimatedRowCount = Math.max(estimatedRowsPerPartition / blocksCount, 1);
            }
            catch (IllegalStateException e)
            {
                // The EstimatedHistogram mean() method can throw this (if it overflows). While such overflow
                // shouldn't happen, it's not worth taking the risk of letting the exception bubble up.
            }
        }
        return new ReusablePartitionData(metadata, estimatedRowCount);
    }

    public boolean setForSlice(Slice slice) throws IOException
    {
        this.slice = slice;
        return super.setForSlice(slice);
    }

    protected void setIterator(Slice slice)
    {
        assert buffer != null;
        iterator = buffer.built.unfilteredIterator(ColumnFilter.all(metadata),
                                                   Slices.with(metadata.comparator, slice),
                                                   true);

        if (!iterator.hasNext())
            return;

        if (skipFirstIteratedItem)
            iterator.next();

        if (skipLastIteratedItem)
            iterator = new SkipLastIterator(iterator);
    }

    protected Unfiltered nextInSlice() throws IOException
    {
        return iterator.hasNext() ? iterator.next() : null;
    }

    protected boolean preSliceStep() throws IOException
    {
        // No skipping pre-slice, this iterator reads the whole block till the end of the first slice, and reuses
        // the constructed buffer for other slices.
        return true;
    }

    enum PrepStage
    {
        OPEN,
        PROCESSING,
        CLOSE,
        DONE
    }
    PrepStage prepStage = PrepStage.OPEN;

    protected boolean slicePrepStep() throws IOException
    {
        if (buffer == null)
            buffer = createBuffer(1);
        return prepStep(false, false);
    }

    protected boolean prepStep(boolean hasNextBlock, boolean hasPreviousBlock) throws IOException
    {
        // If we have read the data, just create the iterator for the slice. Otherwise, read the data.
        switch (prepStage)
        {
        case OPEN:
            skipLastIteratedItem = false;
            skipFirstIteratedItem = false;
            buffer.reset();
            // If we have an open marker, it's either one from what we just skipped or it's one that open in the next (or
            // one of the next) index block (if openMarker == openMarkerAtStartOfBlock).
            if (openMarker != null)
            {
                // We have to feed a marker to the buffer, because that marker is likely to be close later and ImmtableBTreePartition
                // doesn't take kindly to marker that comes without their counterpart. If that's the last block we're gonna read (for
                // the current slice at least) it's easy because we'll want to return that open marker at the end of the data in this
                // block anyway, so we have nothing more to do than adding it to the buffer.
                // If it's not the last block however, it means this marker is really
                // open in a next block and so while we do need to add it the buffer for the reason mentioned above, we don't
                // want to "return" it just yet, we'll wait until we reach it in the next blocks. That's why we trigger
                // skipLastIteratedItem in that case (this is first item of the block, but we're iterating in reverse order
                // so it will be last returned by the iterator).
                buffer.add(new RangeTombstoneBoundMarker(start, openMarker));
                skipLastIteratedItem = hasNextBlock;
            }
            // fall through
            prepStage = PrepStage.PROCESSING;
        case PROCESSING:
            // Now deserialize everything until we reach our requested end (if we have one)
            // See SSTableIterator.ForwardRead.computeNext() for why this is a strict inequality below: this is the same
            // reasoning here.
            if (deserializer.hasNext()
                && deserializer.compareNextTo(end) < 0
                && !stopReadingDisk())
            {
                Unfiltered unfiltered = deserializer.readNext();
                // We may get empty row for the same reason expressed on UnfilteredSerializer.deserializeOne.
                if (!unfiltered.isEmpty())
                    buffer.add(unfiltered);

                if (unfiltered.isRangeTombstoneMarker())
                    updateOpenMarker((RangeTombstoneMarker) unfiltered);
                return false;
            }

            // else fall through
            prepStage = PrepStage.CLOSE;
        case CLOSE:
            // If we have an open marker, we should close it before finishing
            if (openMarker != null)
            {
                // This is the reverse problem than the one at the start of the block. Namely, if it's the first block
                // we deserialize for the slice (the one covering the slice end basically), then it's easy, we just want
                // to add the close marker to the buffer and return it normally.
                // If it's note our first block (for the slice) however, it means that marker closed in a previously read
                // block and we have already returned it. So while we should still add it to the buffer for the sake of
                // not breaking ImmutableBTreePartition, we should skip it when returning from the iterator, hence the
                // skipFirstIteratedItem (this is the last item of the block, but we're iterating in reverse order so it will
                // be the first returned by the iterator).
                buffer.add(new RangeTombstoneBoundMarker(end, openMarker));
                skipFirstIteratedItem = hasPreviousBlock;
            }

            buffer.build();
            // fall through
            prepStage = PrepStage.DONE;
        case DONE:
            setIterator(slice);
            // exit switch
        }
        return true;
    }

    protected RangeTombstoneMarker sliceStartMarker()
    {
        return null;
    }

    protected RangeTombstoneMarker sliceEndMarker()
    {
        return null;
    }

    protected boolean stopReadingDisk() throws IOException
    {
        return false;
    }

    static class ReusablePartitionData
    {
        private final TableMetadata metadata;

        private MutableDeletionInfo.Builder deletionBuilder;
        private MutableDeletionInfo deletionInfo;
        private BTree.Builder<Row> rowBuilder;
        private ImmutableBTreePartition built;

        private ReusablePartitionData(TableMetadata metadata,
                                      int initialRowCapacity)
        {
            this.metadata = metadata;
            this.rowBuilder = BTree.builder(metadata.comparator, initialRowCapacity);
        }


        public void add(Unfiltered unfiltered)
        {
            if (unfiltered.isRow())
                rowBuilder.add((Row)unfiltered);
            else
                deletionBuilder.add((RangeTombstoneMarker)unfiltered);
        }

        public void reset()
        {
            built = null;
            rowBuilder = BTree.builder(metadata.comparator);
            deletionBuilder = MutableDeletionInfo.builder(DeletionTime.LIVE, metadata.comparator, false);
        }

        public void build()
        {
            deletionInfo = deletionBuilder.build();
            built = new ImmutableBTreePartition(metadata, null, null, Rows.EMPTY_STATIC_ROW, rowBuilder.build(),
                                                deletionInfo, EncodingStats.NO_STATS);
            deletionBuilder = null;
        }
    }

    static class SkipLastIterator extends AbstractIterator<Unfiltered>
    {
        private final Iterator<Unfiltered> iterator;

        private SkipLastIterator(Iterator<Unfiltered> iterator)
        {
            this.iterator = iterator;
        }

        protected Unfiltered computeNext()
        {
            if (!iterator.hasNext())
                return endOfData();

            Unfiltered next = iterator.next();
            return iterator.hasNext() ? next : endOfData();
        }
    }
}
