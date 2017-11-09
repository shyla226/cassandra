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
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.Rebufferer.NotInCacheException;
import org.apache.cassandra.io.util.Rebufferer.ReaderConstraint;
import org.apache.cassandra.utils.ByteBufferUtil;
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
 * from disk.  This requires, on retry, first calling the {@link AbstractReader#resetReaderState()}
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
    final boolean lowerBoundAllowed;
    Slices slices;

    // We want to avoid separately opening and closing the data file for the partition and content readers and thus
    // we try to pass control of which reader is responsible for closing it. To make sure we don't double-close
    // when the partition reader is closed concurrently with the row reader being opened, we track the ownership
    // changes atomically using the enum below.
    enum DataFileOwner
    {
        EXTERNAL,
        PARTITION_READER,
        PARTITION_SUBSCRIPTION,
        NONE
    }
    final AtomicReference<DataFileOwner> dataFileOwner;

    volatile RowIndexEntry indexEntry = null;
    volatile FileDataInput dfile = null;
    volatile long filePos = -1;
    volatile State state;
    Row staticRow = Rows.EMPTY_STATIC_ROW;
    DeletionTime partitionLevelDeletion = DeletionTime.LIVE;


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
        this.lowerBoundAllowed = lowerBoundAllowed;
        this.dataFileOwner = new AtomicReference<>(dfile != null
                                                   ? DataFileOwner.EXTERNAL
                                                   : DataFileOwner.NONE);
        this.state = indexEntry == null ? State.STARTING : State.HAVE_INDEX_ENTRY;
    }

    /**
     * Creates a FUP from a AsyncPartitionReader
     *
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
        return new AsyncPartitionReader(table, listener, indexEntry.key, indexEntry.entry, dfile, Slices.ALL, ColumnFilter.all(table.metadata()), false, false).partitions();
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
        return ClusteringBound.inclusiveOpen(reverse, vals.toArray(new ByteBuffer[vals.size()]));
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
        catch (IOException | IndexOutOfBoundsException e)
        {
            reader.table().markSuspect();
            reader.onError(new CorruptSSTableException(e, reader.table().getFilename()));
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
        SSTableReader table();
    }

    enum State
    {
        STARTING,
        HAVE_INDEX_ENTRY,
        HAVE_DFILE,
        HAVE_SKIPPED_KEY,
        HAVE_DELETION_TIME,
        PREPARED
    }

    /**
     * Called (and continued after NotInCacheException) to prepare the partition-level data and to seek to the
     * first row in non-indexed partitions.
     * Returns false if row isn't present.
     */
    public boolean prepareRow() throws Exception
    {
        if (filePos != -1)
            dfile.seek(filePos);

        switch (state)
        {
        case STARTING:
            assert indexEntry == null;
            indexEntry = table.getExactPosition(key, listener, ReaderConstraint.ASYNC);

            if (indexEntry == null)
            {
                state = State.PREPARED;
                return false;
            }
            state = State.HAVE_INDEX_ENTRY;
            // else fall through
        case HAVE_INDEX_ENTRY:
            boolean needSeekAtPartitionStart = !indexEntry.isIndexed() || !selectedColumns.fetchedColumns().statics.isEmpty();
            if (!needSeekAtPartitionStart)
            {
                partitionLevelDeletion = indexEntry.deletionTime();
                staticRow = Rows.EMPTY_STATIC_ROW;
                state = State.PREPARED;
                return true;  // no need to open or read data file, issue directly
            }

            if (dataFileOwner.compareAndSet(DataFileOwner.NONE, DataFileOwner.PARTITION_READER))
                dfile = table.openDataReader(ReaderConstraint.ASYNC);   // This does not include a read and can't throw

            filePos = indexEntry.position;
            state = State.HAVE_DFILE;
            dfile.seek(filePos);
            // fall through
        case HAVE_DFILE:
            ByteBufferUtil.skipShortLength(dfile); // Skip partition key
            filePos = dfile.getFilePointer();
            state = State.HAVE_SKIPPED_KEY;
            // fall through
        case HAVE_SKIPPED_KEY:
            partitionLevelDeletion = DeletionTime.serializer.deserialize(dfile);
            filePos = dfile.getFilePointer();
            state = State.HAVE_DELETION_TIME;
            // fall through
        case HAVE_DELETION_TIME:
            staticRow = SSTableReader.readStaticRow(table, dfile, helper, selectedColumns.fetchedColumns().statics);
            filePos = dfile.getFilePointer();
            state = State.PREPARED;
            // we're done
            return true;
        case PREPARED:
        default:
            throw new AssertionError();
        }
    }

    class PartitionReader extends FlowSource<FlowableUnfilteredPartition> implements Reader
    {
        final TPCScheduler onReadyExecutor = TPC.bestTPCScheduler();

        /**
         * This method must be async-read-safe.
         */
        public void performRead(boolean isRetry) throws Exception
        {
            if (!prepareRow())
            {
                subscriber.onComplete();
                return;
            }

            PartitionHeader header = new PartitionHeader(table.metadata(),
                                                         key,
                                                         partitionLevelDeletion,
                                                         selectedColumns.fetchedColumns(),
                                                         reverse,
                                                         table.stats());

            PartitionSubscription partitionContent = new PartitionSubscription(header, staticRow, false);
            subscriber.onFinal(partitionContent);
        }

        public void onError(Throwable t)
        {
            subscriber.onError(t);
        }

        public SSTableReader table()
        {
            return table;
        }

        public void requestNext()
        {
            readWithRetry(this, false, onReadyExecutor);
        }

        public void close() throws Exception
        {
            // If partition hasn't been opened, we need to close the data file
            if (dataFileOwner.compareAndSet(DataFileOwner.PARTITION_READER, DataFileOwner.NONE))
                dfile.close();
        }


        public String toString()
        {
            return Flow.formatTrace("PartitionReader:" + table);
        }
    }

    class PartitionSubscription extends FlowableUnfilteredPartition.FlowSource implements Reader
    {
        final TPCScheduler onReadyExecutor = TPC.bestTPCScheduler();
        SSTableReader.PartitionReader sstableReader;
        boolean provideLowerBound;

        protected PartitionSubscription(PartitionHeader header, Row staticRow, boolean provideLowerBound)
        {
            // The constructor is not allowed to throw
            super(header, staticRow);
            this.provideLowerBound = provideLowerBound;
        }

        private FileDataInput openFile()
        {
            // Normally the partition reader will still be open and we can transfer the dfile's ownership from it.
            if (dataFileOwner.compareAndSet(DataFileOwner.PARTITION_READER, DataFileOwner.PARTITION_SUBSCRIPTION))
                return dfile;

            // If not, identify what situation we are in.
            switch (dataFileOwner.get())
            {
            case EXTERNAL:
                return dfile;         // we are good, no need to close
            case PARTITION_SUBSCRIPTION:
                return dfile;         // already ours, possibly needed async seek to content
            case NONE:
                // partition reader closed, we need to reopen file
                dataFileOwner.set(DataFileOwner.PARTITION_SUBSCRIPTION);
                return dfile = table.openDataReader(ReaderConstraint.ASYNC);
            default:
                throw new AssertionError(); // if it was this, compareAndSet should have worked
            }
        }

        /**
         * This method must be async-read-safe.
         */
        public void performRead(boolean isRetry) throws Exception
        {
            if (sstableReader == null)
            {
                if (state != State.PREPARED)
                {
                    // This is a delayed initialization.
                    if (!prepareRow())
                    {
                        // Partition not found in this table.
                        subscriber.onComplete();
                        return;
                    }
                }

                openFile();
                if (filePos != -1)
                    dfile.seek(filePos);

                sstableReader = table.reader(dfile,  false, indexEntry, helper, slices, reverse, ReaderConstraint.ASYNC);
            }
            else if (isRetry)
            {
                //If this was an async response
                //Make sure the state is reset
                sstableReader.resetReaderState();
            }

            Unfiltered next = sstableReader.next();
            if (next != null)
                subscriber.onNext(next);
            else
                subscriber.onComplete();
        }

        public void onError(Throwable t)
        {
            subscriber.onError(t);
        }

        public SSTableReader table()
        {
            return table;
        }

        @Override
        public void requestNext()
        {
            readWithRetry(this, false, onReadyExecutor);
        }

        public void close() throws Exception
        {
            try
            {
                if (sstableReader != null)
                    sstableReader.close();
            }
            finally
            {
                if (dataFileOwner.compareAndSet(DataFileOwner.PARTITION_SUBSCRIPTION, DataFileOwner.NONE))
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

        @Override
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
