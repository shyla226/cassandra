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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.reactivex.Single;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
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
    public static final long EMPTY_SIZE = ObjectSizes.measure(new AtomicBTreePartition(null,
            DatabaseDescriptor.getPartitioner().decorateKey(ByteBuffer.allocate(1)),
            null));

    private final MemtableAllocator allocator;

    private volatile Holder ref;

    private final TableMetadataRef metadata;

    public AtomicBTreePartition(TableMetadataRef metadata, DecoratedKey partitionKey, MemtableAllocator allocator)
    {
        // involved in potential bug? partition columns may be a subset if we alter columns while it's in memtable
        super(partitionKey);
        this.metadata = metadata;
        this.allocator = allocator;
        this.ref = EMPTY;
    }

    public TableMetadata metadata()
    {
        return metadata.get();
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
    public Single<long[]> addAllWithSizeDelta(final PartitionUpdate update, OpOrder.Group writeOp, UpdateTransaction indexer)
    {
        RowUpdater updater = new RowUpdater(this, allocator, writeOp, indexer);
        try
        {
            indexer.start();

            if (!update.deletionInfo().getPartitionDeletion().isLive())
                indexer.onPartitionDeletion(update.deletionInfo().getPartitionDeletion());

            if (update.deletionInfo().hasRanges())
                update.deletionInfo().rangeIterator(false).forEachRemaining(indexer::onRangeTombstone);

            DeletionInfo newDeletionInfo = ref.deletionInfo;
            if (update.deletionInfo().mayModify(ref.deletionInfo))
            {
                DeletionInfo inputDeletionInfoCopy = update.deletionInfo().copy(HeapAllocator.instance);
                newDeletionInfo = ref.deletionInfo.mutableCopy().add(inputDeletionInfoCopy);
                updater.allocated(newDeletionInfo.unsharedHeapSize() - ref.deletionInfo.unsharedHeapSize());
            }

            RegularAndStaticColumns newColumns = update.columns().mergeTo(ref.columns);
            Row newStatic = update.staticRow();
            newStatic = newStatic.isEmpty()
                      ? ref.staticRow
                      : (ref.staticRow.isEmpty() ? updater.apply(newStatic) : updater.apply(ref.staticRow, newStatic));

            Object[] tree = BTree.update(ref.tree, update.metadata().comparator, update, update.rowCount(), updater);
            EncodingStats newStats = ref.stats.mergeWith(update.stats());

            this.ref = new Holder(newColumns, tree, newDeletionInfo, newStatic, newStats);

            updater.finish();
        }
        finally
        {
            return indexer.commit().toSingleDefault(new long[]{updater.dataSize, updater.colUpdateTimeDelta});
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

    public Holder holder()
    {
        return ref;
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
        List<Row> inserted; // TODO: replace with walk of aborted BTree

        private RowUpdater(AtomicBTreePartition updating, MemtableAllocator allocator, OpOrder.Group writeOp, UpdateTransaction indexer)
        {
            this.updating = updating;
            this.allocator = allocator;
            this.writeOp = writeOp;
            this.indexer = indexer;
            this.nowInSec = FBUtilities.nowInSeconds();
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
            if (inserted == null)
                inserted = new ArrayList<>();
            inserted.add(reconciled);

            return reconciled;
        }

        protected void reset()
        {
            this.dataSize = 0;
            this.heapSize = 0;
            if (inserted != null)
                inserted.clear();
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
        }
    }
}
