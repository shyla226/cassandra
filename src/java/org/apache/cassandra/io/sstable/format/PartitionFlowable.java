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
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Rebufferer.NotInCacheException;
import org.apache.cassandra.io.util.Rebufferer.ReaderConstraint;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.CsFlow;
import org.apache.cassandra.utils.flow.CsSubscriber;
import org.apache.cassandra.utils.flow.CsSubscription;

/**
 * Internal representation of a partition in flowable form.
 * The first item is *ALWAYS* the partition header.
 * The second item is *ALWAYS* the static row.
 * Followed by all the partition rows.
 *
 * All data fetched through the {@link org.apache.cassandra.cache.ChunkCache}
 * Since we could require data that's not yet in the Cache we catch any
 * {@link NotInCacheException}s and register a retry once the data is fetched
 * from disk.  This requires, on retry, first calling the {@link AbstractSSTableIterator#resetReaderState()}
 * In order to start from the last finished item.
 *
 * The state logic is very straight fwd since CsFlow gives us garuntees that
 * request/close will not be called until after a previous call finishes.
 *
 * We only need to track if we are waiting for data since we need to reset the reader state.
 */
class PartitionFlowable extends CsFlow<Unfiltered>
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionFlowable.class);

    PartitionSubscription subscr;
    final SSTableReadsListener listener;
    final DecoratedKey key;
    final ColumnFilter selectedColumns;
    final SSTableReader table;
    final boolean reverse;
    final ReaderConstraint rc;

    Slices slices;

    private PartitionFlowable(SSTableReader table, SSTableReadsListener listener, DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reverse)
    {
        this.table = table;
        this.listener = listener;
        this.key = key;
        this.selectedColumns = selectedColumns;
        this.slices = slices;
        this.reverse = reverse;
        this.subscr = null;
        this.rc = table.dataFile.mmapped() ? ReaderConstraint.NONE : ReaderConstraint.IN_CACHE_ONLY;
    }

    public static CsFlow<FlowableUnfilteredPartition> create(SSTableReader table, SSTableReadsListener listener, DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reverse)
    {
        PartitionFlowable flowable = new PartitionFlowable(table, listener, key, slices, selectedColumns, reverse);

        return flowable.take(2)
                       .toList()
                       .map(head ->
                            {
                                if (head.size() != 2)
                                    return new FlowablePartitions.EmptyFlowableUnfilteredPartition(new PartitionHeader(table.metadata(), key, DeletionTime.LIVE, RegularAndStaticColumns.NONE, reverse, EncodingStats.NO_STATS));

                                return new FlowableUnfilteredPartition((PartitionHeader) head.get(0), (Row) head.get(1), flowable);
                            });
    }

    @Override
    public CsSubscription subscribe(CsSubscriber<Unfiltered> s)
    {
        if (subscr == null)
            subscr = new PartitionSubscription(s);
        else
            subscr.replaceSubscriber(s);

        return subscr;
    }

    public CsSubscription getSubscription()
    {
        return subscr;
    }

    class PartitionSubscription implements CsSubscription
    {
        volatile FileDataInput dfile = null;
        volatile AbstractSSTableIterator ssTableIterator = null;

        //Used to track the work done iterating (hasNext vs next)
        //Since we could have an async break in either place
        volatile boolean needsHasNextCheck = true;

        volatile long filePos = -1;
        RowIndexEntry indexEntry;
        DeletionTime partitionLevelDeletion;
        Row staticRow = Rows.EMPTY_STATIC_ROW;

        SerializationHelper helper;

        //Force all disk callbacks through the same thread
        private final Executor onReadyExecutor = TPC.bestTPCScheduler().getExecutor();

        AtomicInteger count = new AtomicInteger(0);
        volatile CsSubscriber<Unfiltered> s;

        PartitionSubscription(CsSubscriber<Unfiltered> s)
        {
            this.s = s;
            this.helper = new SerializationHelper(table.metadata(), table.descriptor.version.encodingVersion(), SerializationHelper.Flag.LOCAL, selectedColumns);
        }

        public void replaceSubscriber(CsSubscriber<Unfiltered> s)
        {
            this.s = s;
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(dfile);
            FileUtils.closeQuietly(ssTableIterator);

            dfile = null;
            ssTableIterator = null;
        }

        @Override
        public void request()
        {
            switch (count.getAndIncrement())
            {
                case 0:
                    perform(this::issueHeader, false);
                    break;
                case 1:
                    perform(indexEntry.isIndexed() ? this::issueStaticRowIndexed : this::issueStaticRowUnindexed, false);
                    break;
                case 2:
                    s.onComplete();
                    break;
                default:
                    perform(this::issueNextUnfiltered, false);
            }
        }

        void perform(Consumer<Boolean> action, boolean isRetry)
        {
            try
            {
                action.accept(isRetry);
            }
            catch (NotInCacheException e)
            {
                // Retry the request once data is in the cache
                e.accept(() -> perform(action, true),
                         (t) ->
                         {
                             // Calling completeExceptionally() wraps the original exception into a CompletionException even
                             // though the documentation says otherwise
                             if (t instanceof CompletionException && t.getCause() != null)
                                 t = t.getCause();

                             s.onError(t);
                         },
                         onReadyExecutor);
            }
            catch (Throwable t)
            {
                s.onError(t);
            }
        }

        private void issueHeader(Boolean isRetry)
        {
            try
            {
                indexEntry = table.getPosition(key, SSTableReader.Operator.EQ, listener, rc);
                if (indexEntry == null)
                {
                    s.onComplete();
                    return;
                }

                if (indexEntry.isIndexed())
                {
                    partitionLevelDeletion = indexEntry.deletionTime();
                    filePos = indexEntry.position;
                }
                else
                {
                    try (FileDataInput dfile = table.getFileDataInput(indexEntry.position, rc))
                    {
                        ByteBufferUtil.skipShortLength(dfile); // Skip partition key
                        partitionLevelDeletion = DeletionTime.serializer.deserialize(dfile);
                        filePos = dfile.getFilePointer();
                    }
                    catch (IOException e)
                    {
                        table.markSuspect();
                        throw new CorruptSSTableException(e, table.getFilename());
                    }
                }

                s.onNext(new PartitionHeader(table.metadata(), key, partitionLevelDeletion, selectedColumns.fetchedColumns(), reverse, table.stats()));
            }
            catch (NotInCacheException nice)
            {
                throw nice;
            }
            catch (Throwable t)
            {
                s.onError(t);
            }
        }

        private void issueStaticRowIndexed(Boolean isRetry)
        {
            try
            {
                Columns statics = selectedColumns.fetchedColumns().statics;
                assert indexEntry != null;

                if (table.header.hasStatic())
                {
                    try (FileDataInput dfile = table.getFileDataInput(indexEntry.position, rc))
                    {
                        // We haven't read partition header
                        ByteBufferUtil.skipShortLength(dfile); // Skip partition key
                        DeletionTime.serializer.skip(dfile); // Skip deletion

                        if (statics.isEmpty())
                            UnfilteredSerializer.serializers.get(table.descriptor.version.encodingVersion()).skipStaticRow(dfile, table.header, helper);
                        else
                            staticRow = UnfilteredSerializer.serializers.get(table.descriptor.version.encodingVersion()).deserializeStaticRow(dfile, table.header, helper);

                        filePos = dfile.getFilePointer();
                    }
                    catch (IOException e)
                    {
                        table.markSuspect();
                        throw new CorruptSSTableException(e, table.getFilename());
                    }
                }

                s.onNext(staticRow);
            }
            catch (NotInCacheException nice)
            {
                throw nice;
            }
            catch (Throwable t)
            {
                s.onError(t);
            }
        }

        private void issueStaticRowUnindexed(Boolean isRetry)
        {
            try
            {
                Columns statics = selectedColumns.fetchedColumns().statics;
                assert indexEntry != null;

                if (table.header.hasStatic())
                {
                    try (FileDataInput dfile = table.getFileDataInput(filePos, rc))
                    {
                        // Read and/or go to position after static row.
                        if (statics.isEmpty())
                            UnfilteredSerializer.serializers.get(table.descriptor.version.encodingVersion()).skipStaticRow(dfile, table.header, helper);
                        else
                            staticRow = UnfilteredSerializer.serializers.get(table.descriptor.version.encodingVersion()).deserializeStaticRow(dfile, table.header, helper);

                        filePos = dfile.getFilePointer();
                    }
                    catch (IOException e)
                    {
                        table.markSuspect();
                        throw new CorruptSSTableException(e, table.getFilename());
                    }
                }

                s.onNext(staticRow);
            }
            catch (NotInCacheException nice)
            {
                throw nice;
            }
            catch (Throwable t)
            {
                s.onError(t);
            }
        }

        AbstractSSTableIterator maybeInitIterator()
        {
            if (ssTableIterator == null)
            {
                assert indexEntry != null;

                dfile = table.getFileDataInput(filePos, rc);
                ssTableIterator = table.iterator(dfile, key, indexEntry, slices, selectedColumns, reverse, partitionLevelDeletion, staticRow);

                //The FP may have moved during init
                filePos = dfile.getFilePointer();
            }

            return ssTableIterator;
        }

        private void issueNextUnfiltered(Boolean isRetry)
        {
            try
            {
                AbstractSSTableIterator iter = maybeInitIterator();

                //If this was an async response
                //Make sure the state is reset
                if (isRetry)
                {
                    iter.resetReaderState();
                    dfile.seek(filePos);
                }

                if (needsHasNextCheck)
                {
                    filePos = dfile.getFilePointer();

                    boolean hasNext = iter.hasNext();
                    if (!hasNext)
                    {
                        s.onComplete();
                        return;
                    }

                    needsHasNextCheck = false;
                }

                filePos = dfile.getFilePointer();
                needsHasNextCheck = true;

                Unfiltered next = iter.next();
                s.onNext(next);
            }
            catch (NotInCacheException nice)
            {
                throw nice;
            }
            catch (Throwable t)
            {
                s.onError(t);
            }
        }
    }
}
