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

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Iterables;

import io.reactivex.Completable;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Groups all the views for a given table.
 */
public class TableViews extends AbstractCollection<View>
{
    private final TableMetadataRef baseTableMetadata;

    // We need this to be thread-safe, but the number of times this is changed (when a view is created in the keyspace)
    // is massively exceeded by the number of times it's read (for every mutation on the keyspace), so a copy-on-write
    // list is the best option.
    private final List<View> views = new CopyOnWriteArrayList<>();

    private static final int VIEW_REBUILD_BATCH_SIZE = Integer.getInteger("cassandra.view.rebuild.batch", 100);

    public TableViews(TableId id)
    {
        baseTableMetadata = Schema.instance.getTableMetadataRef(id);
    }

    public int size()
    {
        return views.size();
    }

    public Iterator<View> iterator()
    {
        return views.iterator();
    }

    public boolean contains(String viewName)
    {
        return Iterables.any(views, view -> view.name.equals(viewName));
    }

    public boolean add(View view)
    {
        // We should have validated that there is no existing view with this name at this point
        assert !contains(view.name);
        return views.add(view);
    }

    public Iterable<ColumnFamilyStore> allViewsCfs()
    {
        Keyspace keyspace = Keyspace.open(baseTableMetadata.keyspace);
        return Iterables.transform(views, view -> keyspace.getColumnFamilyStore(view.getDefinition().name));
    }

    public void forceBlockingFlush()
    {
        for (ColumnFamilyStore viewCfs : allViewsCfs())
            viewCfs.forceBlockingFlush();
    }

    public void dumpMemtables()
    {
        for (ColumnFamilyStore viewCfs : allViewsCfs())
            viewCfs.dumpMemtable();
    }

    public void truncateBlocking(CommitLogPosition replayAfter, long truncatedAt)
    {
        for (ColumnFamilyStore viewCfs : allViewsCfs())
        {
            viewCfs.discardSSTables(truncatedAt);
            TPCUtils.blockingAwait(SystemKeyspace.saveTruncationRecord(viewCfs, truncatedAt, replayAfter));
        }
    }

    public void removeByName(String viewName)
    {
        views.removeIf(v -> v.name.equals(viewName));
    }

    /**
     * Calculates and pushes updates to the views replicas. The replicas are determined by
     * {@link ViewUtils#getViewNaturalEndpoint(String, Token, Token)}.
     *
     * @param update an update on the base table represented by this object.
     * @param writeCommitLog whether we should write the commit log for the view updates.
     * @param baseComplete time from epoch in ms that the local base mutation was (or will be) completed
     */
    public Completable pushViewReplicaUpdates(PartitionUpdate update, boolean writeCommitLog, AtomicLong baseComplete)
    {
        assert update.metadata().id.equals(baseTableMetadata.id);

        Collection<View> views = updatedViews(update);
        if (views.isEmpty())
            return Completable.complete();

        // Read modified rows
        int nowInSec = FBUtilities.nowInSeconds();
        long queryStartNanoTime = System.nanoTime();

        return ViewUpdateFlow.forUpdate(update, baseTableMetadata, views, nowInSec)
                             .flatMapCompletable(mutations ->
                                                   {
                                                       if (!mutations.isEmpty())
                                                           return StorageProxy.mutateMV(update.partitionKey().getKey(), mutations, writeCommitLog, baseComplete, queryStartNanoTime);
                                                       else
                                                           return Completable.complete();
                                                   });
    }

    /**
     * Given some updates on the base table of this object and the existing values for the rows affected by that update,
     * generates the mutation to be applied to the provided views.
     *
     * @param views the views potentially affected by {@code updates}.
     * @param updates the base table updates being applied.
     * @param nowInSec the current time in seconds.
     * @return the mutations to apply to the {@code views}. This can be empty.
     */
    public Iterator<Collection<Mutation>> generateViewUpdates(Collection<View> views,
                                                              UnfilteredRowIterator updates,
                                                              int nowInSec)
    {
        assert updates.metadata().id.equals(baseTableMetadata.id);
        CloseableIterator<UnfilteredRowIterator> updateIterators = ThrottledUnfilteredIterator.throttle(updates, VIEW_REBUILD_BATCH_SIZE);
        return new Iterator<Collection<Mutation>>()
        {
            @Override
            public boolean hasNext()
            {
                return updateIterators.hasNext();
            }

            @Override
            public Collection<Mutation> next()
            {
                // This runs on ViewBuildExecutor, and there is no asynchronous fetching of existing data
                return ViewUpdateFlow.forRebuild(updateIterators.next(), baseTableMetadata, views, nowInSec).blockingGet();
            }
        };
    }

    /**
     * Return the views that are potentially updated by the provided updates.
     *
     * @param updates the updates applied to the base table.
     * @return the views affected by {@code updates}.
     */
    public Collection<View> updatedViews(PartitionUpdate updates)
    {
        List<View> matchingViews = new ArrayList<>(views.size());

        for (View view : views)
        {
            ReadQuery selectQuery = view.getReadQuery();
            if (!selectQuery.selectsKey(updates.partitionKey()))
                continue;

            matchingViews.add(view);
        }
        return matchingViews;
    }
}
