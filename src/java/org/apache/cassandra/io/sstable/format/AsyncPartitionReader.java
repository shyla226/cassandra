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

package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.PartitionHeader;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.Rebufferer.NotInCacheException;
import org.apache.cassandra.io.util.Rebufferer.ReaderConstraint;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowSource;
import org.apache.cassandra.utils.flow.FlowSubscriber;
import org.apache.cassandra.utils.flow.FlowSubscriptionRecipient;

/**
 * Asynchronous partition reader.
 *
 * This is only used to asyncronously create a {@link FlowableUnfilteredPartition}
 * via the {@link #create(SSTableReader, SSTableReadsListener, DecoratedKey, Slices, ColumnFilter, boolean, boolean)} method below.
 * This creates a Flow<FlowableUnfilteredPartition> which when requested reads the header and static row of the
 * partition and constructs a Flow<Unfiltered> which can retrieve the partition rows.
 *
 * All reads proceed optimistically, i.e. they first proceed as if all data is already in the chunk cache. If this is the
 * case, the read can complete without delay and the requested data is passed on immediately. Otherwise the read will
 * trigger a {@link NotInCacheException} which is caught, and a retry is registered once the data is fetched
 * from disk.  This requires, on retry, first calling the {@link AbstractSSTableIterator#resetReaderState()}
 * In order to start from the last finished item.
 *
 * The state logic is very straight fwd since Flow gives us guarantees that
 * request/close will not be called until after a previous call finishes.
 *
 * We only need to track if we are waiting for data since we need to reset the reader state in that case.
 */
class AsyncPartitionReader
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncPartitionReader.class);

    final SSTableReadsListener listener;
    final DecoratedKey key;
    final ColumnFilter selectedColumns;
    final SSTableReader table;
    final boolean reverse;
    final SerializationHelper helper;
    final boolean closeDataFile;
    final boolean lowerBoundAllowed;
    Slices slices;

    volatile RowIndexEntry indexEntry = null;
    volatile FileDataInput dfile = null;
    volatile UnfilteredRowIterator ssTableIterator = null;
    volatile long filePos = -1;

    private AsyncPartitionReader(SSTableReader table,
                                 SSTableReadsListener listener,
                                 DecoratedKey key,
                                 RowIndexEntry indexEntry,
                                 FileDataInput dfile,
                                 Slices slices,
                                 ColumnFilter selectedColumns,
                                 boolean reverse,
                                 boolean lowerBoundAllowed)
    {
        this.table = table;
        this.listener = listener;
        this.indexEntry = indexEntry;
        this.dfile = dfile;
        this.key = key;
        this.selectedColumns = selectedColumns;
        this.slices = slices;
        this.reverse = reverse;
        this.helper = new SerializationHelper(table.metadata(), table.descriptor.version.encodingVersion(), SerializationHelper.Flag.LOCAL, selectedColumns);
        this.closeDataFile = (dfile == null);
        this.lowerBoundAllowed = lowerBoundAllowed;
    }

    /**
     * Whether or not lower bound optimisation can be applied to current sstable:
     *   * This is a partition read (no clustering names filter is present)
     *   * Table is non-compact and has no tombstones
     *   * Table has no static columns (as static column read requires table scan/read)
     */
    boolean canUseLowerBound()
    {
        return lowerBoundAllowed &&
               (!table.mayHaveTombstones() && !table.metadata().isCompactTable() && !table.header.hasStatic() && selectedColumns.fetchedColumns().statics.isEmpty());
    }

    /**
     * Returns the clustering Lower Bound from SSTable metadata.
     */
    private ClusteringBound getMetadataLowerBound()
    {
        final StatsMetadata m = table.getSSTableMetadata();
        List<ByteBuffer> vals = reverse ? m.maxClusteringValues : m.minClusteringValues;
        return ClusteringBound.inclusiveOpen(reverse, vals.toArray(new ByteBuffer[0]));
    }

    /**
     * Initializes iterator lazily.
     * @return null if table index entry is empty and partition is not present.
     */
    UnfilteredRowIterator initializeIterator() throws IOException
    {
        // If this is a retry the indexEntry may be already read.
        if (indexEntry == null)
        {
            indexEntry = table.getPosition(key, SSTableReader.Operator.EQ, listener, ReaderConstraint.ASYNC);

            if (indexEntry == null)
                return null;
        }

        assert ssTableIterator == null;

        if (dfile == null)
            dfile = table.getFileDataInput(indexEntry.position, ReaderConstraint.ASYNC);
        else
            dfile.seek(indexEntry.position);

        UnfilteredRowIterator ret;
        if (slices == null && selectedColumns == null)
            ret = table.simpleIterator(dfile, key, indexEntry, false);
        else
            ret = table.iterator(dfile, key, indexEntry, slices, selectedColumns, reverse, ReaderConstraint.ASYNC);
        filePos = dfile.getFilePointer();

        return ret;
    }

    /**
     * Creates a FUP from a AsyncPartitionReader
     */
    public static Flow<FlowableUnfilteredPartition> create(SSTableReader table,
                                                           SSTableReadsListener listener,
                                                           DecoratedKey key,
                                                           Slices slices,
                                                           ColumnFilter selectedColumns,
                                                           boolean reverse,
                                                           boolean lowerBoundAllowed)
    {
        return new AsyncPartitionReader(table, listener, key, null, null, slices, selectedColumns, reverse, lowerBoundAllowed).partitions();
    }

    /**
     * Create a FUP from the given index entry with no other constraint.
     */
    public static Flow<FlowableUnfilteredPartition> create(SSTableReader table,
                                                           FileDataInput dfile,
                                                           SSTableReadsListener listener,
                                                           IndexFileEntry indexEntry)
    {
        return new AsyncPartitionReader(table, listener, indexEntry.key, indexEntry.entry, dfile, null, null, false, false).partitions();
    }

    /**
     * Create a FUP from the given index entry with the given constraints.
     */
    public static Flow<FlowableUnfilteredPartition> create(SSTableReader table,
                                                           FileDataInput dfile,
                                                           SSTableReadsListener listener,
                                                           IndexFileEntry indexEntry,
                                                           Slices slices,
                                                           ColumnFilter selectedColumns,
                                                           boolean reversed)
    {
        return new AsyncPartitionReader(table, listener, indexEntry.key, indexEntry.entry, dfile, slices, selectedColumns, reversed, false).partitions();
    }

    public Flow<FlowableUnfilteredPartition> partitions()
    {
        if (canUseLowerBound())
        {
            PartitionHeader header = new PartitionHeader(table.metadata(),
                                                         key,
                                                         DeletionTime.LIVE,
                                                         selectedColumns.fetchedColumns(),
                                                         reverse,
                                                         table.stats);
            return Flow.just(new PartitionSubscription(header, Rows.EMPTY_STATIC_ROW, true));
        }
        else
        {
            return new PartitionReader();
        }
    }

    private static void readWithRetry(Reader reader, boolean isRetry, TPCScheduler onReadyExecutor)
    {
        try
        {
            reader.performRead(isRetry);
        }
        catch (NotInCacheException e)
        {
            // Retry the request once data is in the cache
            e.accept(() -> readWithRetry(reader, true, onReadyExecutor),
                     (t) ->
                     {
                         // Calling completeExceptionally() wraps the original exception into a CompletionException even
                         // though the documentation says otherwise
                         if (t instanceof CompletionException && t.getCause() != null)
                             t = t.getCause();

                         reader.onError(t);
                         return null;
                     },
                     onReadyExecutor);
        }
        catch (Throwable t)
        {
            reader.onError(t);
        }
    }

    interface Reader
    {
        void performRead(boolean isRetry) throws Exception;
        void onError(Throwable t);
    }

    class PartitionReader extends FlowSource<FlowableUnfilteredPartition> implements Reader
    {
        final TPCScheduler onReadyExecutor = TPC.bestTPCScheduler();
        boolean issued = false;

        /**
         * This method must be async-read-safe.
         */
        public void performRead(boolean isRetry) throws Exception
        {
            assert !issued;

            // Short-circuit the empty partition case
            if ((ssTableIterator = initializeIterator()) == null)
            {
                subscriber.onComplete();
                return;
            }

            PartitionHeader header = new PartitionHeader(ssTableIterator.metadata(),
                                                         ssTableIterator.partitionKey(),
                                                         ssTableIterator.partitionLevelDeletion(),
                                                         ssTableIterator.columns(),
                                                         ssTableIterator.isReverseOrder(),
                                                         ssTableIterator.stats());
            PartitionSubscription partitionContent = new PartitionSubscription(header, ssTableIterator.staticRow(), false);

            issued = true;
            subscriber.onFinal(partitionContent);
        }

        public void onError(Throwable t)
        {
            subscriber.onError(t);
        }

        public void requestNext()
        {
            readWithRetry(this, false, onReadyExecutor);
        }

        public void close() throws Exception
        {
            // If we have issued a FUP, we have passed control over the opened dfile and sstableIterator to it.
            // If didn't get around to issuing, we need to close anything partially open.
            if (issued || dfile == null)
                return;

            if (closeDataFile)
                dfile.close();
            assert ssTableIterator == null;
        }

        public String toString()
        {
            return Flow.formatTrace("PartitionReader:" + table, subscriber);
        }
    }

    class PartitionSubscription extends FlowableUnfilteredPartition.FlowSource
    implements FlowableUnfilteredPartition, Reader
    {
        final TPCScheduler onReadyExecutor = TPC.bestTPCScheduler();

        //Used to track the work done iterating (hasNext vs next)
        //Since we could have an async break in either place
        volatile boolean needsHasNextCheck = true;
        boolean provideLowerBound;

        protected PartitionSubscription(PartitionHeader header, Row staticRow, boolean provideLowerBound)
        {
            super(header, staticRow);
            this.provideLowerBound = provideLowerBound;
        }

        /**
         * This method must be async-read-safe.
         */
        public void performRead(boolean isRetry)
        {
            try
            {
                if (ssTableIterator == null)
                {
                    // Short-circuit empty partition
                    if ((ssTableIterator = initializeIterator()) == null)
                    {
                        subscriber.onComplete();
                        return;
                    }
                }

                //If this was an async response
                //Make sure the state is reset
                if (isRetry)
                {
                    ssTableIterator.resetReaderState();
                    dfile.seek(filePos);
                }

                if (needsHasNextCheck)
                {
                    filePos = dfile.getFilePointer();

                    boolean hasNext = ssTableIterator.hasNext();
                    if (!hasNext)
                    {
                        subscriber.onComplete();
                        return;
                    }

                    needsHasNextCheck = false;
                }

                filePos = dfile.getFilePointer();
                Unfiltered next = ssTableIterator.next();
                needsHasNextCheck = true;

                subscriber.onNext(next);
            }
            catch (NotInCacheException nice)
            {
                throw nice;
            }
            catch(Throwable t)
            {
                subscriber.onError(t);
            }
        }

        public void onError(Throwable t)
        {
            subscriber.onError(t);
        }

        public void requestNext()
        {
            readWithRetry(this, false, onReadyExecutor);
        }

        public void close() throws Exception
        {
            try
            {
                if (ssTableIterator != null)
                    ssTableIterator.close();
            }
            finally
            {
                if (closeDataFile && dfile != null)
                    dfile.close();
            }
        }

        public String toString()
        {
            return Flow.formatTrace("PartitionSubscription:" + table);
        }

        @Override
        public FlowableUnfilteredPartition skipLowerBound()
        {
            assert subscriber == null;
            this.provideLowerBound = false;
            return this;
        }

        public void requestFirst(FlowSubscriber<Unfiltered> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
        {
            subscribe(subscriber, subscriptionRecipient);
            if (provideLowerBound)
                subscriber.onNext(new RangeTombstoneBoundMarker(getMetadataLowerBound(), DeletionTime.LIVE));
            else
                requestNext();
        }
    }
}
