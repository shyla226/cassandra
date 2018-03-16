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

package org.apache.cassandra.db.view;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import io.reactivex.Single;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.ArrayBackedRow;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowTransform;

public class ViewUpdateFlow
{
    // Group PartitionUpdate
    private static Function<PartitionUpdate, DecoratedKey> byKey = p -> p.partitionKey();
    private static Function<PartitionUpdate, TableId> byCf = p -> p.metadata().id;

    /**
     * Asynchronously computes the mutations necessary for updating a view after a given base table update.
     *
     * This method performs a read in the affected partitions, so it should be used when potentially updating
     * existing rows.
     */
    public static Single<Collection<Mutation>> forUpdate(PartitionUpdate update, TableMetadataRef basetableMetadataRef,
                                                         Collection<View> views, int nowInSec)
    {
        return createViewUpdateFlow(createBaseTableUpdateFlow(update, basetableMetadataRef, views, nowInSec),
                                    update.partitionKey(), views, nowInSec);
    }

    /**
     * Asynchronously computes the mutations that should be built for a given base table row iterator.
     *
     * This method assumes there are no pre-existing rows in the provided base table rows, so it should only
     * be used used when rebuilding a view.
     */
    public static Single<Collection<Mutation>> forRebuild(UnfilteredRowIterator update, TableMetadataRef basetableMetadataRef,
                                                          Collection<View> views, int nowInSec)
    {
        return createViewUpdateFlow(createBaseTableUpdateFlowNoExisting(update, basetableMetadataRef, nowInSec),
                                    update.partitionKey(), views, nowInSec);
    }

    /**
     * Given a base table update flow, asynchronously computes the view mutations that should be written for this base
     * table update.
     */
    private static Single<Collection<Mutation>> createViewUpdateFlow(Flow<RowUpdate> baseTableUpdateFlow,
                                                                     DecoratedKey partitionKey,
                                                                     Collection<View> views, int nowInSec)
    {
        List<ViewUpdateGenerator> generators = new ArrayList<>(views.size());
        for (View view : views)
            generators.add(new ViewUpdateGenerator(view, partitionKey, nowInSec));

        return baseTableUpdateFlow.flatMap(affectedRow -> createViewUpdateFlow(generators, affectedRow))
                                  .toList()
                                  .mapToRxSingle(viewUpdates -> createMutations(viewUpdates));
    }

    /**
     * Creates a {@link Flow<RowUpdate>} that will stream the state of the rows before and after this update.
     *
     * This method perform a read in the affected partitions to check the row previous state.
     */
    private static Flow<RowUpdate> createBaseTableUpdateFlow(PartitionUpdate update, TableMetadataRef basetableMetadataRef,
                                                             Collection<View> views, int nowInSec)
    {
        SinglePartitionReadCommand command = readExistingRowsCommand(update, views, nowInSec);
        long start = System.nanoTime();
        Flow<RowUpdate> existingAndMerged = command == null ? Flow.empty()
                : command.executeLocally().flatMap(fup -> new BaseTableUpdateFlow(update.unfilteredIterator(), fup, basetableMetadataRef, nowInSec));
        return existingAndMerged.doOnClose(() -> Keyspace.openAndGetStore(update.metadata()).metric.viewReadTime.update(System.nanoTime() - start, TimeUnit.NANOSECONDS));
    }

    /**
     * Creates a {@link Flow<RowUpdate>} that will stream the state of the rows after this update.
     *
     * This method does not perform a read to check the rows state before the update, so it should only be used for view rebuild.
     */
    private static Flow<RowUpdate> createBaseTableUpdateFlowNoExisting(UnfilteredRowIterator update, TableMetadataRef basetableMetadataRef, int nowInSec)
    {
        FlowableUnfilteredPartition emptyFup = FlowablePartitions.empty(basetableMetadataRef.get(), update.partitionKey(), false);
        return new BaseTableUpdateFlow(update, emptyFup, basetableMetadataRef, nowInSec);
    }

    /**
     * Create updates to be made to the view given a base table row before and after an update.
     * NOTE: this is not thread-safe
     */
    public static Flow<PartitionUpdate> createViewUpdateFlow(List<ViewUpdateGenerator> generators, RowUpdate update)
    {
        assert !generators.isEmpty();
        if (generators.size() == 1)
            return generators.get(0).createViewUpdates(update.before, update.after);

        return Flow.fromIterable(generators)
                   .flatMap(generator -> generator.createViewUpdates(update.before, update.after));
    }

    /**
     * Returns the command to use to read the existing rows required to generate view updates for the provided base
     * base updates.
     *
     * @param updates the base table updates being applied.
     * @param views the views potentially affected by {@code updates}.
     * @param nowInSec the current time in seconds.
     * @return the command to use to read the base table rows required to generate view updates for {@code updates}.
     */
    private static SinglePartitionReadCommand readExistingRowsCommand(PartitionUpdate updates, Collection<View> views, int nowInSec)
    {
        Slices.Builder sliceBuilder = null;
        DeletionInfo deletionInfo = updates.deletionInfo();
        TableMetadata metadata = updates.metadata();
        DecoratedKey key = updates.partitionKey();
        // TODO: This is subtle: we need to gather all the slices that we have to fetch between partition del, range tombstones and rows.
        if (!deletionInfo.isLive())
        {
            sliceBuilder = new Slices.Builder(metadata.comparator);
            // Everything covered by a deletion might invalidate an existing view entry, which means we must read it to know. In practice
            // though, the views involved might filter some base table clustering columns, in which case we can restrict what we read
            // using those restrictions.
            // If there is a partition deletion, then we can simply take each slices from each view select filter. They may overlap but
            // the Slices.Builder handles that for us. Note that in many case this will just involve reading everything (as soon as any
            // view involved has no clustering restrictions for instance).
            // For range tombstone, we should theoretically take the difference between the range tombstoned and the slices selected
            // by every views, but as we don't an easy way to compute that right now, we keep it simple and just use the tombstoned
            // range.
            // TODO: we should improve that latter part.
            if (!deletionInfo.getPartitionDeletion().isLive())
            {
                for (View view : views)
                    sliceBuilder.addAll(view.getSelectStatement().clusteringIndexFilterAsSlices());
            }
            else
            {
                assert deletionInfo.hasRanges();
                Iterator<RangeTombstone> iter = deletionInfo.rangeIterator(false);
                while (iter.hasNext())
                    sliceBuilder.add(iter.next().deletedSlice());
            }
        }

        // We need to read every row that is updated, unless we can prove that it has no impact on any view entries.

        // If we had some slices from the deletions above, we'll continue using that. Otherwise, it's more efficient to build
        // a names query.
        BTreeSet.Builder<Clustering> namesBuilder = sliceBuilder == null ? BTreeSet.builder(metadata.comparator) : null;
        for (Row row : updates)
        {
            // Don't read the existing state if we can prove the update won't affect any views
            if (!affectsAnyViews(key, row, views))
                continue;

            if (namesBuilder == null)
                sliceBuilder.add(Slice.make(row.clustering()));
            else
                namesBuilder.add(row.clustering());
        }

        NavigableSet<Clustering> names = namesBuilder == null ? null : namesBuilder.build();
        // If we have a slice builder, it means we had some deletions and we have to read. But if we had
        // only row updates, it's possible none of them affected the views, in which case we have nothing
        // to do.
        if (names != null && names.isEmpty())
            return null;

        ClusteringIndexFilter clusteringFilter = names == null
                                                 ? new ClusteringIndexSliceFilter(sliceBuilder.build(), false)
                                                 : new ClusteringIndexNamesFilter(names, false);
        // since unselected columns also affect view liveness, we need to query all base columns if base and view have same key columns.
        // If we have more than one view, we should merge the queried columns by each views but to keep it simple we just
        // include everything. We could change that in the future.
        ColumnFilter queriedColumns = views.size() == 1 && metadata.enforceStrictLiveness()
                                      ? Iterables.getOnlyElement(views).getSelectStatement().queriedColumns()
                                      : ColumnFilter.all(metadata);
        // Note that the views could have restrictions on regular columns, but even if that's the case we shouldn't apply those
        // when we read, because even if an existing row doesn't match the view filter, the update can change that in which
        // case we'll need to know the existing content. There is also no easy way to merge those RowFilter when we have multiple views.
        // TODO: we could still make sense to special case for when there is a single view and a small number of updates (and
        // no deletions). Indeed, in that case we could check whether any of the update modify any of the restricted regular
        // column, and if that's not the case we could use view filter. We keep it simple for now though.
        RowFilter rowFilter = RowFilter.NONE;
        return SinglePartitionReadCommand.create(metadata, nowInSec, queriedColumns, rowFilter, DataLimits.NONE, key, clusteringFilter);
    }

    private static boolean affectsAnyViews(DecoratedKey partitionKey, Row update, Collection<View> views)
    {
        for (View view : views)
        {
            if (view.mayBeAffectedBy(partitionKey, update))
                return true;
        }
        return false;
    }

    /**
     * To generate mutation using PartitionUpdates with the same partition key
     *
     * @param updates
     * @return
     */
    private static Collection<Mutation> createMutations(List<PartitionUpdate> updates)
    {
        if (updates.isEmpty())
            return Collections.emptyList();
        String keyspaceName = updates.get(0).metadata().keyspace;

        Map<DecoratedKey, List<PartitionUpdate>> updatesByKey = updates.stream().collect(Collectors.groupingBy(byKey));
        List<Mutation> mutations = new ArrayList<>();
        for (Map.Entry<DecoratedKey, List<PartitionUpdate>> updatesWithSameKey : updatesByKey.entrySet())
        {
            DecoratedKey key = updatesWithSameKey.getKey();
            Mutation mutation = new Mutation(keyspaceName, key);
            Map<TableId, List<PartitionUpdate>> updatesByCf = updatesWithSameKey.getValue().stream().collect(Collectors.groupingBy(byCf));
            for(List<PartitionUpdate> updatesToMerge : updatesByCf.values())
                mutation.add(PartitionUpdate.merge(updatesToMerge));
            mutations.add(mutation);
        }
        return mutations;
    }

    /**
     * A {@link Flow<RowUpdate>} containing the state of base table rows before and after
     * an update.
     *
     * This flow works as as following:
     *   1. {@link #requestNext()}: downstream subscriber requests next element
     *   2. {@link #processNext()}
     *      2.1: When {@link #cachedExisting} and {@link #getNext()} are not null, the next {@link RowUpdate} is
     *      supplied via {@link org.apache.cassandra.utils.flow.FlowSubscriber#onNext(Object)}
     *      2.2: When either {@link #cachedExisting} or {@link #getNext()} is null, and source is not finished, then
     *      call {@link org.apache.cassandra.utils.flow.FlowSubscription#requestNext()} for next existing Unfiltered from source to process.
     *      2.3: When either {@link #cachedExisting} or {@link #getNext()} is null, and source is finished, then call
     *      {@link complete()} to consume remaining {@link #updatesIter} and tell subscriber to complete.
     *   3. {@link #onNext(Unfiltered)} is called when there is a new element from upstream, which will in turn cache it on {@link #cachedExisting}
     *      and potentially supply the next {@link RowUpdate} to downstream via {@link org.apache.cassandra.utils.flow.FlowSubscriber#onNext(Object)}.
     *   4. When the source {@link Flow<Unfiltered>} with existing unfiltereds is finished, {@link #onComplete()} is called.
     *      This will set the {@link #finishedExistings} flag what will call {@link #complete()} and 
     *      return only elements from the source {@link #updatesIter}.
     */
    static class BaseTableUpdateFlow extends FlowTransform<Unfiltered, RowUpdate>
    {
        private final TableMetadataRef baseTableMetadataRef;
        private final UnfilteredRowIterator updates;
        private final PeekingIterator<Unfiltered> updatesIter;
        private final DeletionTracker existingsDeletion;
        private final DeletionTracker updatesDeletion;
        private int nowInSec;
        private Unfiltered cachedExisting = null;
        private boolean finishedExistings = false;

        private BaseTableUpdateFlow(UnfilteredRowIterator updates,
                                    FlowableUnfilteredPartition existings,
                                    TableMetadataRef tableMetadataRef,
                                    int nowInSec)
        {
            super(existings.content());
            this.baseTableMetadataRef = tableMetadataRef;
            existingsDeletion = new DeletionTracker(existings.partitionLevelDeletion());
            updatesDeletion = new DeletionTracker(updates.partitionLevelDeletion());
            this.nowInSec = nowInSec;
            this.updates = updates;
            updatesIter = Iterators.peekingIterator(updates);
        }

        @Override
        public void requestNext()
        {
            processNext();
        }

        private void processNext()
        {
            // There are 3 cases,
            // 1. We don't have next element for subscriber and source is not finished,
            //    then we ask source for next element to process
            // 2. We don't have next element for subscriber and source is finished,
            //    then consume the remaining updateIterator and complete
            // 3. We have next element for subscriber, just deliver it.
            RowUpdate next = peekExisting() == null ? null : getNext();
            if (next == null && !finishedExistings)
                source.requestNext();
            else if (next == null && finishedExistings)
                complete();
            else
                subscriber.onNext(next);
        }

        @Override
        public void onNext(Unfiltered item)
        {
            // send to subscriber
            cachedExisting = item;
            processNext();
        }

        @Override
        public void onFinal(Unfiltered item)
        {
            // source is done, should not request more from source
            finishedExistings = true;
            cachedExisting = item;
            processNext();
        }

        @Override
        public void onComplete()
        {
            // source is done, should not request more from source
            finishedExistings = true;
            processNext();
        }

        private void complete()
        {
            assert peekExisting() == null && finishedExistings;
            // No more existings, exhaust updates iterator.
            while (updatesIter.hasNext())
            {
                Unfiltered update = updatesIter.next();
                // If it's a range tombstone, it removes nothing pre-exisiting, so we can ignore it for view updates
                if (!update.isRangeTombstoneMarker())
                {
                    Row updateRow = (Row) update;
                    subscriber.onNext(RowUpdate.create(emptyRow(updateRow.clustering(),
                                                                existingsDeletion.currentDeletion()),
                                                       updateRow,
                                                       nowInSec));
                    return;
                }
            }
            // No more updates, complete flow.
            subscriber.onComplete();
        }

        private RowUpdate getNext()
        {
            assert peekExisting() != null;
            while (updatesIter.hasNext())
            {
                Unfiltered existing = peekExisting();
                Unfiltered update = updatesIter.peek();

                Row existingRow;
                Row updateRow;
                int cmp = baseTableMetadataRef.get().comparator.compare(update, existing);
                if (cmp < 0)
                {
                    // We have an update where there was nothing before
                    if (update.isRangeTombstoneMarker())
                    {
                        updatesDeletion.update(updatesIter.next());
                        continue;
                    }

                    updateRow = ((Row)updatesIter.next()).withRowDeletion(updatesDeletion.currentDeletion());
                    existingRow = emptyRow(updateRow.clustering(), existingsDeletion.currentDeletion());
                }
                else if (cmp > 0)
                {
                    Unfiltered nextExisting = consumeExisting();
                    // We have something existing but no update (which will happen either because it's a range tombstone marker in
                    // existing, or because we've fetched the existing row due to some partition/range deletion in the updates)
                    if (existing.isRangeTombstoneMarker())
                    {
                        existingsDeletion.update(nextExisting);
                        return null; //request next existing from source
                    }

                    existingRow = ((Row) nextExisting).withRowDeletion(existingsDeletion.currentDeletion());
                    updateRow = emptyRow(existingRow.clustering(), updatesDeletion.currentDeletion());

                    // The way we build the read command used for existing rows, we should always have updatesDeletion.currentDeletion()
                    // that is not live, since we wouldn't have read the existing row otherwise. And we could assert that, but if we ever
                    // change the read method so that it can slightly over-read in some case, that would be an easily avoiding bug lurking,
                    // so we just handle the case.
                    if (updateRow == null)
                    {
                        return null; //request next existing from source
                    }
                }
                else
                {
                    // We're updating a row that had pre-existing data
                    if (update.isRangeTombstoneMarker())
                    {
                        assert existing.isRangeTombstoneMarker();
                        updatesDeletion.update(updatesIter.next());
                        existingsDeletion.update(consumeExisting());
                        return null; //request next existing from source
                    }

                    assert !existing.isRangeTombstoneMarker();
                    existingRow = ((Row)consumeExisting()).withRowDeletion(existingsDeletion.currentDeletion());
                    updateRow = ((Row)updatesIter.next()).withRowDeletion(updatesDeletion.currentDeletion());
                }

                return RowUpdate.create(existingRow, updateRow, nowInSec);
            }

            Unfiltered existing = consumeExisting();

            // We only care about more existing rows if the update deletion isn't live, i.e. if we had a partition deletion
            if (!updatesDeletion.currentDeletion().isLive())
            {
                // If it's a range tombstone, we don't care, we're only looking for existing entry that gets deleted by
                // the new partition deletion
                if (!existing.isRangeTombstoneMarker())
                {
                    Row existingRow = (Row)existing;
                    return RowUpdate.create(existingRow, emptyRow(existingRow.clustering(),
                                                                  updatesDeletion.currentDeletion()),
                                            nowInSec);
                }
            }

            return null; //request next existing from source
        }

        private Unfiltered peekExisting()
        {
            return cachedExisting;
        }

        /**
         * Clear existing and request next from source
         * @return
         */
        private Unfiltered consumeExisting()
        {
            Unfiltered toReturn = cachedExisting;
            cachedExisting = null;
            return toReturn;

        }

        public void close() throws Exception
        {
            updates.close();
        }
    }

    private static Row emptyRow(Clustering clustering, DeletionTime deletion)
    {
        // Returning null for an empty row is slightly ugly, but the case where there is no pre-existing row is fairly common
        // (especially when building the view), so we want to avoid a dummy allocation of an empty row every time.
        // And MultiViewUpdateBuilder knows how to deal with that.
        return deletion.isLive() ? null : ArrayBackedRow.emptyDeletedRow(clustering, Row.Deletion.regular(deletion));
    }


    /**
     * A simple helper that tracks for a given {@code UnfilteredRowIterator} what is the current deletion at any time of the
     * iteration. It will be the currently open range tombstone deletion if there is one and the partition deletion otherwise.
     */
    private static class DeletionTracker
    {
        private final DeletionTime partitionDeletion;
        private DeletionTime deletion;

        public DeletionTracker(DeletionTime partitionDeletion)
        {
            this.partitionDeletion = partitionDeletion;
            this.deletion = partitionDeletion;
        }

        public void update(Unfiltered marker)
        {
            assert marker instanceof RangeTombstoneMarker;
            RangeTombstoneMarker rtm = (RangeTombstoneMarker)marker;
            this.deletion = rtm.isOpen(false)
                            ? rtm.openDeletionTime(false)
                            : partitionDeletion;
        }

        public DeletionTime currentDeletion()
        {
            return deletion;
        }
    }

    public static class RowUpdate
    {
        /**
         * The base table row as it is before an update.
         */
        public final Row before;
        /**
         * The base table row after the update is applied (note that this is not just the new update, but rather the resulting row).
         */
        public final Row after;

        private RowUpdate(Row before, Row after)
        {
            this.before = before;
            this.after = after;
        }

        public static RowUpdate create(Row existingBaseRow, Row updateBaseRow, int nowInSec)
        {
            // Having existing empty is useful, it just means we'll insert a brand new entry for updateBaseRow,
            // but if we have no update at all, we shouldn't get there.
            assert !updateBaseRow.isEmpty();

            // We allow existingBaseRow to be null, which we treat the same as being empty as an small optimization
            // to avoid allocating empty row objects when we know there was nothing existing.
            Row mergedBaseRow = existingBaseRow == null ? updateBaseRow
                                                        : Rows.merge(existingBaseRow, updateBaseRow, nowInSec);
            return new RowUpdate(existingBaseRow, mergedBaseRow);
        }
    }
}
