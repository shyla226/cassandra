package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.PlatformDependent;
import io.reactivex.Flowable;
import io.reactivex.internal.subscriptions.SubscriptionArbiter;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.AbstractSSTableIterator;
import org.apache.cassandra.db.columniterator.SSTableIterator;
import org.apache.cassandra.db.columniterator.SSTableReversedIterator;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Rebufferer.NotInCacheException;
import org.apache.cassandra.io.util.Rebufferer.ReaderConstraint;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.Ref;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Internal representation of a partition in flowable form.
 * The first item is *ALWAYS* the partition header.
 * The second item is *ALWAYS* the static row.
 * Followed by all the partition rows.
 *
 */
class PartitionFlowable extends Flowable<Unfiltered>
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionFlowable.class);

    PartitionSubscription subscr;
    final DecoratedKey key;
    final ColumnFilter selectedColumns;
    final BigTableReader table;
    final boolean reverse;
    final int offset;
    final long limit;

    Slices slices;

    public PartitionFlowable(BigTableReader table, DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reverse, long limit)
    {
        this.table = table;
        this.key = key;
        this.selectedColumns = selectedColumns;
        this.slices = slices;
        this.reverse = reverse;
        this.subscr = null;
        this.offset = 0;
        this.limit = limit;
    }


    public PartitionFlowable(PartitionFlowable o, int offset)
    {
        this.table = o.table;
        this.key = o.key;
        this.selectedColumns = o.selectedColumns;
        this.slices = o.slices;
        this.reverse = o.reverse;
        this.subscr = new PartitionSubscription(o.subscr, offset);
        this.offset = offset;
        this.limit = Long.MAX_VALUE;
    }


    @Override
    protected void subscribeActual(Subscriber<? super Unfiltered> s)
    {
        if (subscr == null)
            subscr = new PartitionSubscription(s);
        else
            subscr.setSubscriber(s);

        s.onSubscribe(subscr);
    }

    enum State
    {
        READY,
        WORKING,
        WAITING,
        DONE_WORKING,
        CLOSING,
        CLOSED
    }

    class PartitionSubscription implements Subscription
    {
        final Ref<SSTableReader> tableRef;
        FileDataInput dfile = null;
        AbstractSSTableIterator ssTableIterator = null;

        //Used to track the work done iterating (hasNext vs next)
        //Since we could have an async break in either place
        boolean needsHasNextCheck = true;

        long filePos = -1;
        RowIndexEntry<?> indexEntry;
        DeletionTime partitionLevelDeletion;
        Row staticRow = Rows.EMPTY_STATIC_ROW;

        SerializationHelper helper;

        //Force all disk callbacks through the same thread
        private final Executor onReadyExecutor = NettyRxScheduler.instance().getExecutor();

        volatile State state = State.READY;
        AtomicReferenceFieldUpdater<PartitionSubscription, State> stateUpdater = PlatformDependent.newAtomicReferenceFieldUpdater(PartitionSubscription.class, "state");
        Subscriber<? super Unfiltered> s;

        AtomicInteger count = new AtomicInteger(0);
        AtomicLong requests = new AtomicLong(0);

        PartitionSubscription(Subscriber<? super Unfiltered> s)
        {
            this.s = s;
            this.tableRef = table.ref();
            this.helper = new SerializationHelper(table.metadata(), table.descriptor.version.encodingVersion(), SerializationHelper.Flag.LOCAL, selectedColumns);
        }


        PartitionSubscription(PartitionSubscription p, int offset)
        {
            this.s = null;
            this.tableRef = table.ref();
            this.helper = p.helper;
            this.count.set(offset);
            this.indexEntry = p.indexEntry;
            this.filePos = p.filePos;
            this.partitionLevelDeletion = p.partitionLevelDeletion;
            this.staticRow = p.staticRow;
        }

        void setSubscriber(Subscriber<? super Unfiltered> s)
        {
            assert this.s == null;
            this.s = s;
        }

        @Override
        public void cancel()
        {
            switch (state)
            {
                case DONE_WORKING:
                case WORKING:
                case READY:
                    close();
                    break;
                case WAITING:
                    stateUpdater.compareAndSet(this, State.WAITING, State.CLOSING);
                    break;
                case CLOSING:
                case CLOSED:
                    break;
                default:
                    throw new IllegalStateException();
            }
        }

        private void close()
        {
            assert state != State.CLOSED : "Already closed";
            stateUpdater.set(this, State.CLOSED);
            tableRef.close();
            FileUtils.closeQuietly(dfile);
            FileUtils.closeQuietly(ssTableIterator);
        }

        public void pull()
        {
            //Avoid doing any work beyond our limit
            if (count.get() >= limit)
                return;

            switch (count.getAndIncrement())
            {
                case 0:
                    perform(this::issueHeader, s);
                    break;
                case 1:
                    perform(indexEntry.isIndexed() ? this::issueStaticRowIndexed : this::issueStaticRowUnindexed, s);
                    break;
                default:
                    perform(this::issueNextUnfiltered, s);
                    break;
            }
        }

        void perform(Consumer<ReaderConstraint> action, Subscriber caller)
        {
            try
            {
                action.accept(ReaderConstraint.IN_CACHE_ONLY);

                switch (state)
                {
                    case WORKING:
                        stateUpdater.compareAndSet(this, State.WORKING, State.DONE_WORKING);
                        break;
                    case WAITING:
                        break;
                    case CLOSING:
                        close();
                        return;
                    case READY:
                    case CLOSED:
                        return;
                    default:
                        throw new IllegalStateException("" + state);
                }

                //If this is an async wait callback then start the request
                //chain again
                if (state == State.WAITING && caller == s)
                    request(0, State.WAITING);
            }
            catch (NotInCacheException e)
            {
                if (state != State.WAITING)
                {
                    boolean f = stateUpdater.compareAndSet(this, State.WORKING, State.WAITING);
                    assert f : state;
                }

                // Retry the request once data is in the cache
                e.accept(() -> perform(action, caller), onReadyExecutor);
            }
        }

        @Override
        public void request(long howMany)
        {
            request(howMany, State.READY);
        }


        public void request(long howMany, State expectedState)
        {
            if (howMany > 0)
                requests.addAndGet(howMany);

            //logger.info("key={} requested={}, total={}, expected={}, current={} count={} limit={}", key, howMany, requests.get(), expectedState, state, count, limit);

            while (requests.get() > 0 && stateUpdater.compareAndSet(this, expectedState, State.WORKING))
            {
                long r = requests.decrementAndGet();
                assert r >= 0 : "" + howMany + " " + expectedState;
                pull();

                //pull may not have finished working if we hit an async wait
                //so we only put the state back to ready if it's DONE_WORKING.
                //It could be in WAITING state which we will just stop and let
                //the callback handle it.
                stateUpdater.compareAndSet(this, State.DONE_WORKING, expectedState);
            }

            // When finishing a callback request
            // We must ensure we put it back into the ready state
            // So RX requests can start working again
            if (expectedState != State.READY)
            {
                stateUpdater.compareAndSet(this, expectedState, State.READY);
            }
        }

        private void issueHeader(ReaderConstraint rc)
        {
            assert state != State.CLOSED;

            try
            {
                indexEntry = table.getPosition(key, SSTableReader.Operator.EQ, rc);
                if (indexEntry == null)
                {
                    cancel();
                    s.onNext(new PartitionHeader(table.metadata(), key, DeletionTime.LIVE, RegularAndStaticColumns.NONE, reverse, table.stats()));
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
                cancel();
                s.onError(t);
            }
        }

        private void issueStaticRowIndexed(ReaderConstraint rc)
        {
            assert state != State.CLOSED;

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
                cancel();
                s.onError(t);
            }
        }

        private void issueStaticRowUnindexed(ReaderConstraint rc)
        {
            assert state != State.CLOSED;

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
                cancel();
                s.onError(t);
            }
        }


        AbstractSSTableIterator maybeInitIterator(ReaderConstraint rc)
        {
            if (ssTableIterator == null)
            {
                assert indexEntry != null;

                dfile = table.getFileDataInput(filePos, rc);

                ssTableIterator = reverse
                                  ? new SSTableReversedIterator(table, dfile, key, indexEntry, slices, selectedColumns, table.getIndexFile(), partitionLevelDeletion, staticRow)
                                  : new SSTableIterator(table, dfile, key, indexEntry, slices, selectedColumns, table.getIndexFile(), partitionLevelDeletion, staticRow);

                //The FP may have moved during init
                filePos = dfile.getFilePointer();
            }

            return ssTableIterator;
        }

        private void issueNextUnfiltered(ReaderConstraint rc)
        {
            try
            {
                assert state != State.CLOSED;

                AbstractSSTableIterator iter = maybeInitIterator(rc);

                //If this was an async response
                //Make sure the state is reset
                if (state.equals(State.WAITING))
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
                        cancel();
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
                cancel();
                s.onError(t);
            }
        }
    }
}
