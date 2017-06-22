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

import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.PartitionHeader;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.Rebufferer.NotInCacheException;
import org.apache.cassandra.io.util.Rebufferer.ReaderConstraint;
import org.apache.cassandra.utils.flow.CsFlow;
import org.apache.cassandra.utils.flow.CsSubscriber;
import org.apache.cassandra.utils.flow.CsSubscription;

/**
 * Internal representation of a partition in flowable form.
 * The first item is *ALWAYS* the partition header.
 * The second item is *ALWAYS* the static row.
 * Followed by all the partition rows.
 *
 * This is only used to asyncronously create a {@link FlowableUnfilteredPartition}
 * via the {@link #create(SSTableReader, SSTableReadsListener, DecoratedKey, Slices, ColumnFilter, boolean)} method below
 *
 * All data fetched through the {@link org.apache.cassandra.cache.ChunkCache}
 * Since we could require data that's not yet in the Cache we catch any
 * {@link NotInCacheException}s and register a retry once the data is fetched
 * from disk.  This requires, on retry, first calling the {@link AbstractSSTableIterator#resetReaderState()}
 * In order to start from the last finished item.
 *
 * The state logic is very straight fwd since CsFlow gives us guarantees that
 * request/close will not be called until after a previous call finishes.
 *
 * We only need to track if we are waiting for data since we need to reset the reader state in that case.
 */
class PartitionFlowable extends CsFlow<FlowableUnfilteredPartition>
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionFlowable.class);

    final SSTableReadsListener listener;
    final DecoratedKey key;
    final ColumnFilter selectedColumns;
    final SSTableReader table;
    final boolean reverse;
    final ReaderConstraint rc;
    final SerializationHelper helper;
    Slices slices;

    volatile RowIndexEntry indexEntry = null;
    volatile FileDataInput dfile = null;
    volatile AbstractSSTableIterator ssTableIterator = null;
    volatile long filePos = -1;

    private PartitionFlowable(SSTableReader table, SSTableReadsListener listener, DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reverse)
    {
        this.table = table;
        this.listener = listener;
        this.key = key;
        this.selectedColumns = selectedColumns;
        this.slices = slices;
        this.reverse = reverse;
        this.helper = new SerializationHelper(table.metadata(), table.descriptor.version.encodingVersion(), SerializationHelper.Flag.LOCAL, selectedColumns);
        this.rc = table.dataFile.mmapped() ? ReaderConstraint.NONE : ReaderConstraint.IN_CACHE_ONLY;
    }

    /**
     * Creates a FUP from a PartitionFlowable
     *
     * This is the only supported accessor of this class.
     */
    public static CsFlow<FlowableUnfilteredPartition> create(SSTableReader table, SSTableReadsListener listener, DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reverse)
    {
        return new PartitionFlowable(table, listener, key, slices, selectedColumns, reverse);
    }

    public CsSubscription subscribe(CsSubscriber<FlowableUnfilteredPartition> subscriber) throws Exception
    {
        assert indexEntry == null;
        return new PartitionReader(subscriber);
    }

    abstract class Base<T> implements CsSubscription
    {
        CsSubscriber<T> subscriber;

        //Force all disk callbacks through the same thread
        private final Executor onReadyExecutor = TPC.bestTPCScheduler().getExecutor();

        abstract void performRead(boolean isRetry) throws Exception;

        Base(CsSubscriber<T> s)
        {
            this.subscriber = s;
        }

        private void perform(boolean isRetry)
        {
            try
            {
                performRead(isRetry);
            }
            catch (NotInCacheException e)
            {
                // Retry the request once data is in the cache
                e.accept(() -> perform(true),
                         (t) ->
                         {
                             // Calling completeExceptionally() wraps the original exception into a CompletionException even
                             // though the documentation says otherwise
                             if (t instanceof CompletionException && t.getCause() != null)
                                 t = t.getCause();

                             subscriber.onError(t);
                         },
                         onReadyExecutor);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
            }
        }

        public void request()
        {
            perform(false);
        }

        public Throwable addSubscriberChainFromSource(Throwable throwable)
        {
            return CsFlow.wrapException(throwable, this);
        }
    }

    class PartitionReader extends Base<FlowableUnfilteredPartition>
    {
        boolean issued = false;

        PartitionReader(CsSubscriber<FlowableUnfilteredPartition> subscriber)
        {
            super(subscriber);
        }

        void performRead(boolean isRetry) throws Exception
        {
            if (issued)
            {
                subscriber.onComplete();    // our job is done, we have already issued partition.
                return;
            }

            // If this is a retry the indexEntry may be already read.
            if (indexEntry == null)
            {
                indexEntry = table.getPosition(key, SSTableReader.Operator.EQ, listener, rc);

                if (indexEntry == null)
                {
                    subscriber.onComplete();
                    return;
                }
            }
            else
                assert isRetry;

            if (dfile == null)
                dfile = table.getFileDataInput(indexEntry.position, rc);

            // This is the last stage that can fail in-cache read.
            assert ssTableIterator == null;
            ssTableIterator = (AbstractSSTableIterator) table.iterator(dfile, key, indexEntry, slices, selectedColumns, reverse);
            filePos = dfile.getFilePointer();

            PartitionHeader header = new PartitionHeader(ssTableIterator.metadata(),
                                                         ssTableIterator.partitionKey(),
                                                         ssTableIterator.partitionLevelDeletion(),
                                                         ssTableIterator.columns(),
                                                         ssTableIterator.isReverseOrder(),
                                                         ssTableIterator.stats());

            CsFlow<Unfiltered> content = new CsFlow<Unfiltered>()
            {
                public CsSubscription subscribe(CsSubscriber<Unfiltered> subscriber) throws Exception
                {
                    return new PartitionSubscription(subscriber);
                }
            };
            issued = true;
            subscriber.onNext(new FlowableUnfilteredPartition(header, ssTableIterator.staticRow(), content));
        }

        public void close() throws Exception
        {
            // If we didn't get around to issuing a FUP, we need to close anything partially open.
            if (issued || dfile == null)
                return;

            try
            {
                if (ssTableIterator != null)
                    ssTableIterator.close();
            }
            finally
            {
                dfile.close();
            }
        }


        public String toString()
        {
            return CsFlow.formatTrace("PartitionReader:" + table, subscriber);
        }
    }

    class PartitionSubscription extends Base<Unfiltered>
    {
        //Used to track the work done iterating (hasNext vs next)
        //Since we could have an async break in either place
        volatile boolean needsHasNextCheck = true;

        PartitionSubscription(CsSubscriber<Unfiltered> s)
        {
            super(s);
        }

        void performRead(boolean isRetry)
        {
            try
            {
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
            catch (Throwable t)
            {
                subscriber.onError(t);
            }
        }

        public void close() throws Exception
        {
            try
            {
                ssTableIterator.close();
            }
            finally
            {
                dfile.close();
            }
        }

        public String toString()
        {
            return CsFlow.formatTrace("PartitionSubscription:" + table, subscriber);
        }
    }
}
