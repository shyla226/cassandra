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
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.Striped;

import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.SingleEmitter;
import io.reactivex.SingleSource;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.Schedulable;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.WriteVerbs.WriteVersion;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.flow.RxThreads;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class CounterMutation implements IMutation, Schedulable
{
    public static final Versioned<WriteVersion, Serializer<CounterMutation>> serializers = WriteVersion.versioned(CounterMutationSerializer::new);

    /**
     *   These are the striped locks that are shared by all threads that perform counter mutations. Locks are
     *   taken by metadata id, partition and clustering keys, and the counter column name,
     *   see {@link CounterMutation#getCounterLockKeys()}.
     *   <p>
     *   We use semaphores rather than locks because the same thread may have several counter mutations
     *   ongoing at the same time in an asynchronous fashion. So reentrant locks would not protect against
     *   the same thread starting a new counter mutation before finishing a previous one on the same counter.
     *   <p>
     *   If we were sure that at any given time a partition key is handled by only a TPC thread, then we
     *   could make LOCKS smaller and thread local. However, there are cases when this is currently
     *   not true, such us before StorageService is initialized or when the topology changes. We may
     *   be able to improve on this in APOLLO-694, which will deal with topology changes.
     */
    private static final Striped<Semaphore> LOCKS = Striped.semaphore(TPC.getNumCores() * 1024, 1);

    private final Mutation mutation;
    private final ConsistencyLevel consistency;

    public CounterMutation(Mutation mutation, ConsistencyLevel consistency)
    {
        this.mutation = mutation;
        this.consistency = consistency;
    }

    public String getKeyspaceName()
    {
        return mutation.getKeyspaceName();
    }

    public Collection<TableId> getTableIds()
    {
        return mutation.getTableIds();
    }

    public Collection<PartitionUpdate> getPartitionUpdates()
    {
        return mutation.getPartitionUpdates();
    }

    public Mutation getMutation()
    {
        return mutation;
    }

    public DecoratedKey key()
    {
        return mutation.key();
    }

    public ConsistencyLevel consistency()
    {
        return consistency;
    }

    /**
     * Applies the counter mutation, returns the result Mutation (for replication to other nodes).
     *
     * 1. Grabs the striped cell-level locks in the proper order
     * 2. Gets the current values of the counters-to-be-modified from the counter cache
     * 3. Reads the rest of the current values (cache misses) from the CF
     * 4. Writes the updated counter values
     * 5. Updates the counter cache
     * 6. Releases the lock(s)
     *
     * See CASSANDRA-4775 and CASSANDRA-6504 for further details.
     *
     * @return the applied resulting Mutation
     */
    public Single<Mutation> applyCounterMutation()
    {
        return applyCounterMutation(System.nanoTime());
    }

    public StagedScheduler getScheduler()
    {
        return mutation.getScheduler();
    }

    public TracingAwareExecutor getOperationExecutor()
    {
        return mutation.getOperationExecutor();
    }

    private Single<Mutation> applyCounterMutation(long startTime)
    {
        return RxThreads.subscribeOn(
        acquireLocks(startTime).flatMap(locks -> Single.using(() -> locks,
                                                                  this::applyCounterMutationInternal,
                                                                  this::releaseLocks)),
        getScheduler(),
        TPCTaskType.COUNTER_ACQUIRE_LOCK);
    }

    /**
     * Helper method for {@link #applyCounterMutation(long)}. Performs the actual work with the locks
     * available.
     *
     * @param locks - the locks, we don't use them at the moment. They are accepted as a parameter
     *              to allow using a method reference in the calling site.
     *
     * @return a single that will complete when the mutation is applied
     */
    private SingleSource<Mutation> applyCounterMutationInternal(List<Semaphore> locks)
    {
        final Mutation result = new Mutation(getKeyspaceName(), key());
        Completable ret = Completable.concat(getPartitionUpdates()
                                            .stream()
                                            .map(this::processModifications)
                                            .map(single -> single.flatMapCompletable(upd -> Completable.fromRunnable(() -> result.add(upd))))
                                            .collect(Collectors.toList()));
        return ret.andThen(result.applyAsync())
                  .toSingleDefault(result);
    }

    public void apply()
    {
        TPCUtils.blockingGet(applyCounterMutation());
    }

    public Completable applyAsync()
    {
        return applyCounterMutation().toCompletable();
    }

    /**
     * Acquire locks asynchronously and publish them when they are available.
     * @return a single that will publish the locks when they are aquired
     */
    private Single<List<Semaphore>> acquireLocks(long startTime)
    {
        List<Semaphore> locks = new ArrayList<>();
        return Single.create(source -> acquireLocks(source, locks, startTime));
    }

    /**
     * Helper method for acquireLocks(). Acquires the locks and publishes them if successful, otherwise
     * retryed again after a small delay.
     *
     * @param startTime the time at which the operation started, we give up after a timeout
     * @param source the subsriber to the single that will receive the locks
     * @param locks a list that will be populated with acquired locks, if all acquisitions are successful
     */
    private void acquireLocks(final SingleEmitter<List<Semaphore>> source, List<Semaphore> locks, long startTime)
    {
        assert TPC.isTPCThread() : "Only TPC threads can acquire locks for counter mutations";

        Tracing.trace("Acquiring counter locks");
        for (Semaphore lock : LOCKS.bulkGet(getCounterLockKeys()))
        {
            if (!lock.tryAcquire())
            {
                // we failed to acquire a semaphore permit, so unlock everything we've acquired so far
                releaseLocks(locks);

                long timeout = getTimeout();
                if ((System.nanoTime() - startTime) > TimeUnit.MILLISECONDS.toNanos(timeout))
                {
                    Tracing.trace("Failed to acquire locks for counter mutation for longer than {} millis, giving up", timeout);
                    Keyspace keyspace = Keyspace.open(getKeyspaceName());
                    source.onError(new WriteTimeoutException(WriteType.COUNTER, consistency(), 0, consistency().blockFor(keyspace)));
                }
                else
                {
                    Tracing.trace("Failed to acquire counter locks, scheduling retry");
                    // TODO: 1 microsecond is an arbitrary value that was chosen to avoid spinning the CPU too much, we
                    // should perform some tests to see if there is an impact in changing this value (APOLLO-799)
                    mutation.getScheduler().scheduleDirect(() -> acquireLocks(source, locks, startTime),
                                                           TPCTaskType.COUNTER_ACQUIRE_LOCK,
                                                           1,
                                                           TimeUnit.MICROSECONDS);
                }
                return;
            }
            locks.add(lock);
        }

        source.onSuccess(locks);
    }

    private void releaseLocks(List<Semaphore> locks)
    {
        for (Semaphore lock : locks)
        {
            assert lock.availablePermits() == 0 : "Attempted to release a lock that was not acquired: " + lock.availablePermits();
            lock.release();
        }

        locks.clear();
    }

    /**
     * Returns a wrapper for the Striped#bulkGet() call (via Keyspace#counterLocksFor())
     * Striped#bulkGet() depends on Object#hashCode(), so here we make sure that the cf id and the partition key
     * all get to be part of the hashCode() calculation.
     */
    private Iterable<Object> getCounterLockKeys()
    {
        return Iterables.concat(Iterables.transform(getPartitionUpdates(), new Function<PartitionUpdate, Iterable<Object>>()
        {
            public Iterable<Object> apply(final PartitionUpdate update)
            {
                return Iterables.concat(Iterables.transform(update, new Function<Row, Iterable<Object>>()
                {
                    public Iterable<Object> apply(final Row row)
                    {
                        return Iterables.concat(Iterables.transform(row, new Function<ColumnData, Object>()
                        {
                            public Object apply(final ColumnData data)
                            {
                                return Objects.hashCode(update.metadata().id, key(), row.clustering(), data.column());
                            }
                        }));
                    }
                }));
            }
        }));
    }

    private Single<PartitionUpdate> processModifications(PartitionUpdate changes)
    {
        ColumnFamilyStore cfs = Keyspace.open(getKeyspaceName()).getColumnFamilyStore(changes.metadata().id);

        List<PartitionUpdate.CounterMark> marks = changes.collectCounterMarks();

        if (CacheService.instance.counterCache.getCapacity() != 0)
        {
            Tracing.trace("Fetching {} counter values from cache", marks.size());
            updateWithCurrentValuesFromCache(marks, cfs);
            if (marks.isEmpty())
                return Single.just(changes);
        }

        Tracing.trace("Reading {} counter values from the CF", marks.size());
        return updateWithCurrentValuesFromCFS(marks, cfs).andThen(Single.fromCallable(() -> {
            // What's remain is new counters
            for (PartitionUpdate.CounterMark mark : marks)
                updateWithCurrentValue(mark, ClockAndCount.BLANK, cfs);
            return changes;
        }));
    }

    private void updateWithCurrentValue(PartitionUpdate.CounterMark mark, ClockAndCount currentValue, ColumnFamilyStore cfs)
    {
        long clock = Math.max(FBUtilities.timestampMicros(), currentValue.clock + 1L);
        long count = currentValue.count + CounterContext.instance().total(mark.value());

        mark.setValue(CounterContext.instance().createGlobal(CounterId.getLocalId(), clock, count));

        // Cache the newly updated value
        cfs.putCachedCounter(key().getKey(), mark.clustering(), mark.column(), mark.path(), ClockAndCount.create(clock, count));
    }

    // Returns the count of cache misses.
    private void updateWithCurrentValuesFromCache(List<PartitionUpdate.CounterMark> marks, ColumnFamilyStore cfs)
    {
        Iterator<PartitionUpdate.CounterMark> iter = marks.iterator();
        while (iter.hasNext())
        {
            PartitionUpdate.CounterMark mark = iter.next();
            ClockAndCount cached = cfs.getCachedCounter(key().getKey(), mark.clustering(), mark.column(), mark.path());
            if (cached != null)
            {
                updateWithCurrentValue(mark, cached, cfs);
                iter.remove();
            }
        }
    }

    // Reads the missing current values from the CFS.
    private Completable updateWithCurrentValuesFromCFS(List<PartitionUpdate.CounterMark> marks, ColumnFamilyStore cfs)
    {
        ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
        BTreeSet.Builder<Clustering> names = BTreeSet.builder(cfs.metadata().comparator);
        for (PartitionUpdate.CounterMark mark : marks)
        {
            if (mark.clustering() != Clustering.STATIC_CLUSTERING)
                names.add(mark.clustering());
            if (mark.path() == null)
                builder.add(mark.column());
            else
                builder.select(mark.column(), mark.path());
        }

        int nowInSec = FBUtilities.nowInSeconds();
        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(names.build(), false);
        SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(cfs.metadata(), nowInSec, key(), builder.build(), filter);
        PeekingIterator<PartitionUpdate.CounterMark> markIter = Iterators.peekingIterator(marks.iterator());

        return Completable.using(() -> cmd.executionController(),
                                 controller -> cmd.deferredQuery(cfs, controller)
                                                  .flatMapCompletable(p -> {
                                                      FlowablePartition partition = FlowablePartitions.filter(p, nowInSec);
                                                      updateForRow(markIter, partition.staticRow, cfs);

                                                      return partition.content.takeWhile((row) -> markIter.hasNext())
                                                                              .processToRxCompletable(row -> updateForRow(markIter, row, cfs));
                                        }),
                                 controller -> controller.close());
    }

    private int compare(Clustering c1, Clustering c2, ColumnFamilyStore cfs)
    {
        if (c1 == Clustering.STATIC_CLUSTERING)
            return c2 == Clustering.STATIC_CLUSTERING ? 0 : -1;
        if (c2 == Clustering.STATIC_CLUSTERING)
            return 1;

        return cfs.getComparator().compare(c1, c2);
    }

    private void updateForRow(PeekingIterator<PartitionUpdate.CounterMark> markIter, Row row, ColumnFamilyStore cfs)
    {
        int cmp = 0;
        // If the mark is before the row, we have no value for this mark, just consume it
        while (markIter.hasNext() && (cmp = compare(markIter.peek().clustering(), row.clustering(), cfs)) < 0)
            markIter.next();

        if (!markIter.hasNext())
            return;

        while (cmp == 0)
        {
            PartitionUpdate.CounterMark mark = markIter.next();
            Cell cell = mark.path() == null ? row.getCell(mark.column()) : row.getCell(mark.column(), mark.path());
            if (cell != null)
            {
                updateWithCurrentValue(mark, CounterContext.instance().getLocalClockAndCount(cell.value()), cfs);
                markIter.remove();
            }
            if (!markIter.hasNext())
                return;

            cmp = compare(markIter.peek().clustering(), row.clustering(), cfs);
        }
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getCounterWriteRpcTimeout();
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean shallow)
    {
        return String.format("CounterMutation(%s, %s)", mutation.toString(shallow), consistency);
    }

    private static class CounterMutationSerializer extends VersionDependent<WriteVersion> implements Serializer<CounterMutation>
    {
        private CounterMutationSerializer(WriteVersion version)
        {
            super(version);
        }

        public void serialize(CounterMutation cm, DataOutputPlus out) throws IOException
        {
            Mutation.serializers.get(version).serialize(cm.mutation, out);
            out.writeUTF(cm.consistency.name());
        }

        public CounterMutation deserialize(DataInputPlus in) throws IOException
        {
            Mutation m = Mutation.serializers.get(version).deserialize(in);
            ConsistencyLevel consistency = Enum.valueOf(ConsistencyLevel.class, in.readUTF());
            return new CounterMutation(m, consistency);
        }

        public long serializedSize(CounterMutation cm)
        {
            return Mutation.serializers.get(version).serializedSize(cm.mutation)
                   + TypeSizes.sizeof(cm.consistency.name());
        }
    }
}
