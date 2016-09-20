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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapAllocator;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * A thread-safe and atomic Partition implementation.
 *
 * Operations (in particular addAll) on this implementation are atomic and
 * isolated (in the sense of ACID). Typically a addAll is guaranteed that no
 * other thread can see the state where only parts but not all rows have
 * been added.
 */
public class AtomicBTreePartition extends AbstractBTreePartition
{
    public static final long EMPTY_SIZE = ObjectSizes.measure(new AtomicBTreePartition(CFMetaData.createFake("keyspace", "table"),
            DatabaseDescriptor.getPartitioner().decorateKey(ByteBuffer.allocate(1)),
            null));

    private final MemtableAllocator allocator;
    private final TreeMap<Clustering, Row> rows;
    private PartitionColumns columns = PartitionColumns.NONE;
    private DeletionInfo deletionInfo = DeletionInfo.LIVE;
    private Row staticRow = Rows.EMPTY_STATIC_ROW;
    private EncodingStats stats = EncodingStats.NO_STATS;

    public AtomicBTreePartition(CFMetaData metadata, DecoratedKey partitionKey, MemtableAllocator allocator)
    {
        // involved in potential bug? partition columns may be a subset if we alter columns while it's in memtable
        super(metadata, partitionKey);
        this.allocator = allocator;
        rows = new TreeMap<>(metadata.comparator);
    }

    protected boolean canHaveShadowedData()
    {
        return true;
    }

    /**
     * Adds a given update to this in-memtable partition.
     *
     * @return an array containing first the difference in size seen after merging the updates, and second the minimum
     * time detla between updates.
     */
    public long[] addAllWithSizeDelta(final PartitionUpdate update, OpOrder.Group writeOp, UpdateTransaction indexer)
    {
        RowUpdater updater = new RowUpdater(this, allocator, writeOp, indexer);
        try
        {
            indexer.start();


            if (!update.deletionInfo().getPartitionDeletion().isLive())
                indexer.onPartitionDeletion(update.deletionInfo().getPartitionDeletion());

            if (update.deletionInfo().hasRanges())
                update.deletionInfo().rangeIterator(false).forEachRemaining(indexer::onRangeTombstone);

            if (update.deletionInfo().mayModify(this.deletionInfo))
            {
                DeletionInfo inputDeletionInfoCopy = update.deletionInfo().copy(HeapAllocator.instance);
                DeletionInfo newDeletionInfo = deletionInfo.mutableCopy().add(inputDeletionInfoCopy);
                updater.allocated(newDeletionInfo.unsharedHeapSize() - deletionInfo.unsharedHeapSize());
                deletionInfo = newDeletionInfo;
            }

            columns = update.columns().mergeTo(this.columns);
            Row newStatic = update.staticRow();
            staticRow = newStatic.isEmpty()
                        ? this.staticRow
                        : (this.staticRow.isEmpty() ? updater.apply(newStatic) : updater.apply(staticRow, newStatic));
            for (Row row : update)
            {
                // TODO if we stick with a TreeMap, we should customize it to avoid doing a separate lookup and then insert per row
                Row existing = rows.get(row.clustering());
                if (existing == null)
                    rows.put(row.clustering(), updater.apply(row));
                else
                    rows.put(row.clustering(), updater.apply(existing, row));
            }

            // TODO should account for TreeMap usage of on-heap memory somehow through updater.allocated()?

            stats = stats.mergeWith(update.stats());
            updater.finish();
            return new long[]{updater.dataSize, updater.colUpdateTimeDelta};
        }
        finally
        {
            indexer.commit();
        }
    }

    @Override
    public DeletionInfo deletionInfo()
    {
        return allocator.ensureOnHeap().applyToDeletionInfo(super.deletionInfo());
    }

    @Override
    public Row staticRow()
    {
        return allocator.ensureOnHeap().applyToStatic(super.staticRow());
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return allocator.ensureOnHeap().applyToPartitionKey(super.partitionKey());
    }

    @Override
    public Row getRow(Clustering clustering)
    {
        return allocator.ensureOnHeap().applyToRow(super.getRow(clustering));
    }

    @Override
    public Row lastRow()
    {
        return allocator.ensureOnHeap().applyToRow(super.lastRow());
    }

    @Override
    public SearchIterator<Clustering, Row> searchIterator(ColumnFilter columns, boolean reversed)
    {
        return allocator.ensureOnHeap().applyToPartition(super.searchIterator(columns, reversed));
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        return allocator.ensureOnHeap().applyToPartition(super.unfilteredIterator(selection, slices, reversed));
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator()
    {
        return allocator.ensureOnHeap().applyToPartition(super.unfilteredIterator());
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(Holder current, ColumnFilter selection, Slices slices, boolean reversed)
    {
        return allocator.ensureOnHeap().applyToPartition(super.unfilteredIterator(current, selection, slices, reversed));
    }

    @Override
    public Iterator<Row> iterator()
    {
        return allocator.ensureOnHeap().applyToPartition(super.iterator());
    }

    // TODO remove this at the Interface level
    public Holder holder()
    {
        return null;
    }

    // the function we provide to the btree utilities to perform any column replacements
    private static final class RowUpdater implements UpdateFunction<Row, Row>
    {
        final AtomicBTreePartition updating;
        final MemtableAllocator allocator;
        final OpOrder.Group writeOp;
        final UpdateTransaction indexer;
        final int nowInSec;
        Row.Builder regularBuilder;
        long dataSize;
        long heapSize;
        long colUpdateTimeDelta = Long.MAX_VALUE;
        final MemtableAllocator.DataReclaimer reclaimer;

        private RowUpdater(AtomicBTreePartition updating, MemtableAllocator allocator, OpOrder.Group writeOp, UpdateTransaction indexer)
        {
            this.updating = updating;
            this.allocator = allocator;
            this.writeOp = writeOp;
            this.indexer = indexer;
            this.nowInSec = FBUtilities.nowInSeconds();
            this.reclaimer = allocator.reclaimer();
        }

        private Row.Builder builder(Clustering clustering)
        {
            boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;
            // We know we only insert/update one static per PartitionUpdate, so no point in saving the builder
            if (isStatic)
                return allocator.rowBuilder(writeOp);

            if (regularBuilder == null)
                regularBuilder = allocator.rowBuilder(writeOp);
            return regularBuilder;
        }

        public Row apply(Row insert)
        {
            Row data = Rows.copy(insert, builder(insert.clustering())).build();
            indexer.onInserted(insert);

            this.dataSize += data.dataSize();
            this.heapSize += data.unsharedHeapSizeExcludingData();
            return data;
        }

        public Row apply(Row existing, Row update)
        {
            Row.Builder builder = builder(existing.clustering());
            colUpdateTimeDelta = Math.min(colUpdateTimeDelta, Rows.merge(existing, update, builder, nowInSec));

            Row reconciled = builder.build();

            indexer.onUpdated(existing, reconciled);

            dataSize += reconciled.dataSize() - existing.dataSize();
            heapSize += reconciled.unsharedHeapSizeExcludingData() - existing.unsharedHeapSizeExcludingData();
            reclaimer.reclaim(existing);

            return reconciled;
        }

        public boolean abortEarly()
        {
            return false;
        }

        public void allocated(long heapSize)
        {
            this.heapSize += heapSize;
        }

        protected void finish()
        {
            allocator.onHeap().adjust(heapSize, writeOp);
            reclaimer.commit();
        }
    }
}
