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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import io.reactivex.schedulers.Schedulers;

import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.cache.RowCacheSentinel;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.lifecycle.*;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.ReadRepairDecision;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.Threads;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * A read command that selects a (part of a) single partition.
 */
public class SinglePartitionReadCommand extends ReadCommand
{
    private static final SelectionDeserializer<SinglePartitionReadCommand> selectionDeserializer = new Deserializer();
    public static final Versioned<ReadVersion, Serializer<SinglePartitionReadCommand>> serializers = ReadVersion.versioned(v -> new ReadCommandSerializer<>(v, selectionDeserializer));

    private final DecoratedKey partitionKey;
    private final ClusteringIndexFilter clusteringIndexFilter;

    // We access the scheduler/operationExecutor multiple times for each command (at least twice for every replica
    // involved in the request and response executor in Messaging, plus potentially in AbstractReadExecutor when
    // speculating) and re-doing their computation is unnecessary so caching their value here. Note that we don't
    // serialize those in any way, they are just recomputed in the ctor.
    private final transient TPCScheduler scheduler;
    private final transient TracingAwareExecutor operationExecutor;

    /**
     * Race condition when ReadCommand is re-used.
     *
     * It's ok to get a smaller oldestUnrepairedTombstone value when ReadCommand is reused, but the concurrent update
     * to this variable is not safe, we might lost update.
     *
     * SEE APOLLO-1084
     */
    private int oldestUnrepairedTombstone = Integer.MAX_VALUE;

    private SinglePartitionReadCommand(DigestVersion digestVersion,
                                       TableMetadata metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       RowFilter rowFilter,
                                       DataLimits limits,
                                       DecoratedKey partitionKey,
                                       ClusteringIndexFilter clusteringIndexFilter,
                                       IndexMetadata index)
    {
        super(digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, index);
        assert partitionKey.getPartitioner() == metadata.partitioner;
        this.partitionKey = partitionKey;
        this.clusteringIndexFilter = clusteringIndexFilter;

        this.scheduler = TPC.getForKey(Keyspace.open(metadata().keyspace), partitionKey);
        this.operationExecutor = scheduler.forTaskType(TPCTaskType.READ);
    }

    public Request.Dispatcher<SinglePartitionReadCommand, ReadResponse> dispatcherTo(Collection<InetAddress> endpoints)
    {
        return Verbs.READS.SINGLE_READ.newDispatcher(endpoints, this);
    }

    public Request<SinglePartitionReadCommand, ReadResponse> requestTo(InetAddress endpoint)
    {
        return Verbs.READS.SINGLE_READ.newRequest(endpoint, this);
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param columnFilter the column filter to use for the query.
     * @param rowFilter the row filter to use for the query.
     * @param limits the limits to use for the query.
     * @param partitionKey the partition key for the partition to query.
     * @param clusteringIndexFilter the clustering index filter to use for the query.
     * @param indexMetadata explicitly specified index to use for the query.
     *
     * @return a newly created read command.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata,
                                                    int nowInSec,
                                                    ColumnFilter columnFilter,
                                                    RowFilter rowFilter,
                                                    DataLimits limits,
                                                    DecoratedKey partitionKey,
                                                    ClusteringIndexFilter clusteringIndexFilter,
                                                    IndexMetadata indexMetadata)
    {
        return new SinglePartitionReadCommand(null,
                                              metadata,
                                              nowInSec,
                                              columnFilter,
                                              rowFilter,
                                              limits,
                                              partitionKey,
                                              clusteringIndexFilter,
                                              indexMetadata);
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param columnFilter the column filter to use for the query.
     * @param rowFilter the row filter to use for the query.
     * @param limits the limits to use for the query.
     * @param partitionKey the partition key for the partition to query.
     * @param clusteringIndexFilter the clustering index filter to use for the query.
     *
     * @return a newly created read command.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata,
                                                    int nowInSec,
                                                    ColumnFilter columnFilter,
                                                    RowFilter rowFilter,
                                                    DataLimits limits,
                                                    DecoratedKey partitionKey,
                                                    ClusteringIndexFilter clusteringIndexFilter)
    {
        return create(metadata,
                      nowInSec,
                      columnFilter,
                      rowFilter,
                      limits,
                      partitionKey,
                      clusteringIndexFilter,
                      findIndex(metadata, rowFilter));
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param columnFilter the column filter to use for the query.
     * @param filter the clustering index filter to use for the query.
     *
     * @return a newly created read command. The returned command will use no row filter and have no limits.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata,
                                                    int nowInSec,
                                                    DecoratedKey key,
                                                    ColumnFilter columnFilter,
                                                    ClusteringIndexFilter filter)
    {
        return create(metadata, nowInSec, columnFilter, RowFilter.NONE, DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new read command that queries a single partition in its entirety.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     *
     * @return a newly created read command that queries all the rows of {@code key}.
     */
    public static SinglePartitionReadCommand fullPartitionRead(TableMetadata metadata, int nowInSec, DecoratedKey key)
    {
        return create(metadata, nowInSec, key, Slices.ALL);
    }

    /**
     * Creates a new read command that queries a single partition in its entirety.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     *
     * @return a newly created read command that queries all the rows of {@code key}.
     */
    public static SinglePartitionReadCommand fullPartitionRead(TableMetadata metadata, int nowInSec, ByteBuffer key)
    {
        return create(metadata, nowInSec, metadata.partitioner.decorateKey(key), Slices.ALL);
    }

    /**
     * Creates a new single partition slice command for the provided single slice.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slice the slice of rows to query.
     *
     * @return a newly created read command that queries {@code slice} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, DecoratedKey key, Slice slice)
    {
        return create(metadata, nowInSec, key, Slices.with(metadata.comparator, slice));
    }

    /**
     * Creates a new single partition slice command for the provided slices.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slices the slices of rows to query.
     *
     * @return a newly created read command that queries the {@code slices} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, DecoratedKey key, Slices slices)
    {
        ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(slices, false);
        return create(metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.NONE, DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new single partition slice command for the provided slices.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slices the slices of rows to query.
     *
     * @return a newly created read command that queries the {@code slices} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, ByteBuffer key, Slices slices)
    {
        return create(metadata, nowInSec, metadata.partitioner.decorateKey(key), slices);
    }

    /**
     * Creates a new single partition name command for the provided rows.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param names the clustering for the rows to query.
     *
     * @return a newly created read command that queries the {@code names} in {@code key}. The returned query will
     * query every columns (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, DecoratedKey key, NavigableSet<Clustering> names)
    {
        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(names, false);
        return create(metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.NONE, DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new single partition name command for the provided row.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param name the clustering for the row to query.
     *
     * @return a newly created read command that queries {@code name} in {@code key}. The returned query will
     * query every columns (without limit or row filtering).
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, DecoratedKey key, Clustering name)
    {
        return create(metadata, nowInSec, key, FBUtilities.singleton(name, metadata.comparator));
    }

    public SinglePartitionReadCommand createDigestCommand(DigestVersion digestVersion)
    {
        return new SinglePartitionReadCommand(digestVersion,
                                              metadata(),
                                              nowInSec(),
                                              columnFilter(),
                                              rowFilter(),
                                              limits(),
                                              partitionKey(),
                                              clusteringIndexFilter(),
                                              indexMetadata());
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public ClusteringIndexFilter clusteringIndexFilter()
    {
        return clusteringIndexFilter;
    }

    public ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key)
    {
        return clusteringIndexFilter;
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getReadRpcTimeout();
    }

    public boolean isReversed()
    {
        return clusteringIndexFilter.isReversed();
    }

    public boolean selectsKey(DecoratedKey key)
    {
        if (!this.partitionKey().equals(key))
            return false;

        return rowFilter().partitionKeyRestrictionsAreSatisfiedBy(key, metadata().partitionKeyType);
    }

    public boolean selectsClustering(DecoratedKey key, Clustering clustering)
    {
        if (clustering == Clustering.STATIC_CLUSTERING)
            return !columnFilter().fetchedColumns().statics.isEmpty();

        if (!clusteringIndexFilter().selects(clustering))
            return false;

        return rowFilter().clusteringKeyRestrictionsAreSatisfiedBy(clustering);
    }

    /**
     * Returns a new command suitable to paging from the last returned row.
     *
     * @param lastReturned the last row returned by the previous page. The newly created command
     * will only query row that comes after this (in query order). This can be {@code null} if this
     * is the first page.
     * @param limits the limits to use for the page to query.
     * @param inclusive - whether or not we want to include the lastReturned in the newly returned page of results.
     *
     * @return the newly create command.
     */
    public SinglePartitionReadCommand forPaging(Clustering lastReturned, DataLimits limits, boolean inclusive)
    {
        // We shouldn't have set digest yet when reaching that point
        assert !isDigestQuery();
        return create(metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      limits,
                      partitionKey(),
                      lastReturned == null ? clusteringIndexFilter() : clusteringIndexFilter.forPaging(metadata().comparator, lastReturned, inclusive));
    }

    public SinglePartitionReadCommand withUpdatedLimit(DataLimits newLimits)
    {
        return new SinglePartitionReadCommand(digestVersion(),
                                              metadata(),
                                              nowInSec(),
                                              columnFilter(),
                                              rowFilter(),
                                              newLimits,
                                              partitionKey(),
                                              clusteringIndexFilter(),
                                              indexMetadata());
    }

    public Flow<FlowablePartition> execute(ReadContext ctx) throws RequestExecutionException
    {
        return StorageProxy.read(Group.one(this), ctx);
    }

    public SinglePartitionPager getPager(PagingState pagingState, ProtocolVersion protocolVersion)
    {
        return getPager(this, pagingState, protocolVersion);
    }

    private static SinglePartitionPager getPager(SinglePartitionReadCommand command, PagingState pagingState, ProtocolVersion protocolVersion)
    {
        return new SinglePartitionPager(command, pagingState, protocolVersion);
    }

    protected void recordLatency(TableMetrics metric, long latencyNanos)
    {
        metric.readLatency.addNano(latencyNanos);
    }

    public Flow<FlowableUnfilteredPartition> queryStorage(final ColumnFamilyStore cfs, ReadExecutionController executionController)
    {
        if (cfs.isRowCacheEnabled())
            return getThroughCache(cfs, executionController);
        else
            return deferredQuery(cfs, executionController);
    }

    public Flow<FlowableUnfilteredPartition> deferredQuery(final ColumnFamilyStore cfs, ReadExecutionController executionController)
    {
        SSTableReadMetricsCollector metricsCollector = new SSTableReadMetricsCollector();

        return Threads.deferOnCore(() -> queryMemtableAndDisk(cfs, executionController, metricsCollector)
                                         .doOnClose(() -> updateMetrics(cfs.metric, metricsCollector)),
                                   TPC.getCoreForKey(cfs.keyspace, partitionKey),
                                   TPCTaskType.READ);
    }

    private void updateMetrics(TableMetrics metrics, SSTableReadMetricsCollector metricsCollector)
    {
        int mergedSSTablesIterated = metricsCollector.getMergedSSTables();
        metrics.updateSSTableIterated(mergedSSTablesIterated);
        Tracing.trace("Merged data from memtables and {} sstables", mergedSSTablesIterated);
    }

    /**
     * Fetch the rows requested if in cache; if not, read it from disk and cache it.
     * <p>
     * If the partition is cached, and the filter given is within its bounds, we return
     * from cache, otherwise from disk.
     * <p>
     * If the partition is is not cached, we figure out what filter is "biggest", read
     * that from disk, then filter the result and either cache that or return it.
     */
    private Flow<FlowableUnfilteredPartition> getThroughCache(ColumnFamilyStore cfs, ReadExecutionController executionController)
    {
        assert !cfs.isIndex(); // CASSANDRA-5732
        assert cfs.isRowCacheEnabled() : String.format("Row cache is not enabled on table [%s]", cfs.name);

        RowCacheKey key = new RowCacheKey(metadata(), partitionKey());

        // Attempt a sentinel-read-cache sequence.  if a write invalidates our sentinel, we'll return our
        // (now potentially obsolete) data, but won't cache it. see CASSANDRA-3862
        // TODO: don't evict entire partitions on writes (#2864)
        IRowCacheEntry cached = CacheService.instance.rowCache.get(key);
        if (cached != null)
        {
            if (cached instanceof RowCacheSentinel)
            {
                // Some other read is trying to cache the value, just do a normal non-caching read
                Tracing.trace("Row cache miss (race)");
                cfs.metric.rowCacheMiss.inc();
                return deferredQuery(cfs, executionController);
            }

            CachedPartition cachedPartition = (CachedPartition)cached;
            if (cfs.isFilterFullyCoveredBy(clusteringIndexFilter(), limits(), cachedPartition, nowInSec(), metadata().enforceStrictLiveness()))
            {
                cfs.metric.rowCacheHit.inc();
                Tracing.trace("Row cache hit");
                FlowableUnfilteredPartition ret = clusteringIndexFilter().getFlowableUnfilteredPartition(columnFilter(), cachedPartition);
                cfs.metric.updateSSTableIterated(0);
                return Flow.just(ret);
            }

            cfs.metric.rowCacheHitOutOfRange.inc();
            Tracing.trace("Ignoring row cache as cached value could not satisfy query");
            return deferredQuery(cfs, executionController);
        }

        cfs.metric.rowCacheMiss.inc();
        Tracing.trace("Row cache miss");

        // Note that on tables with no clustering keys, any positive value of
        // rowsToCache implies caching the full partition
        boolean cacheFullPartitions = metadata().clusteringColumns().size() > 0 ?
                                      metadata().params.caching.cacheAllRows() :
                                      metadata().params.caching.cacheRows();

        // To be able to cache what we read, what we read must at least covers what the cache holds, that
        // is the 'rowsToCache' first rows of the partition. We could read those 'rowsToCache' first rows
        // systematically, but we'd have to "extend" that to whatever is needed for the user query that the
        // 'rowsToCache' first rows don't cover and it's not trivial with our existing filters. So currently
        // we settle for caching what we read only if the user query does query the head of the partition since
        // that's the common case of when we'll be able to use the cache anyway. One exception is if we cache
        // full partitions, in which case we just always read it all and cache.
        if (cacheFullPartitions || clusteringIndexFilter().isHeadFilter())
        {
            RowCacheSentinel sentinel = new RowCacheSentinel();
            boolean sentinelSuccess = CacheService.instance.rowCache.putIfAbsent(key, sentinel);

            if (sentinelSuccess)
            {
                int rowsToCache = metadata().params.caching.rowsPerPartitionToCache();
                final boolean enforceStrictLiveness = metadata().enforceStrictLiveness();

                Flow<FlowableUnfilteredPartition> iter = SinglePartitionReadCommand.fullPartitionRead(metadata(),
                                                                                                      nowInSec(),
                                                                                                      partitionKey())
                                                                                   .deferredQuery(cfs,
                                                                                                    executionController);

                return iter.flatMap(partition -> {

                    Flow.Tee<Unfiltered> tee = partition.content.tee();
                    FlowableUnfilteredPartition toCache = partition.withContent(tee.child(0));
                    FlowableUnfilteredPartition toReturn = partition.withContent(tee.child(1));

                    toCache = DataLimits.cqlLimits(rowsToCache).truncateUnfiltered(toCache, nowInSec(), false, enforceStrictLiveness);
                    Flow<CachedBTreePartition> cachedPartition = CachedBTreePartition.create(toCache, nowInSec());

                    // reduceToFuture initiates processing on this branch. It will immediately return until we have a request
                    // from the other branch of the tee. When that closes, the rest of the cache processing will be done.
                    cachedPartition.doOnError(error -> cfs.invalidateCachedPartition(key))
                                   .reduceToFuture(null, (VOID, c) ->
                                        {
                                            if (!c.isEmpty())
                                            {
                                                Tracing.trace("Caching {} rows", c.rowCount());
                                                CacheService.instance.rowCache.replace(key, sentinel, c);
                                            }
                                            else
                                                cfs.invalidateCachedPartition(key);

                                            return null;
                                        });

                    // We now return the other branch of the tee, filtered to what the query wants.
                    return Flow.just(clusteringIndexFilter().filterNotIndexed(columnFilter(), toReturn));
                });
            }
        }
        else
            Tracing.trace("Fetching data but not populating cache as query does not query from the start of the partition");

        return deferredQuery(cfs, executionController);
    }

    /**
     * Queries both memtable and sstables to fetch the result of this query.
     * <p>
     * Please note that this method:
     *   1) does not check the row cache.
     *   2) does not apply the query limit, nor the row filter (and so ignore 2ndary indexes).
     *      Those are applied in {@link ReadCommand#executeLocally}.
     *   3) does not record some of the read metrics (latency, scanned cells histograms) nor
     *      throws TombstoneOverwhelmingException.
     * It is publicly exposed because there is a few places where that is exactly what we want,
     * but it should be used only where you know you don't need thoses things.
     * <p>
     * Also note that one must have created a {@code ReadExecutionController} on the queried table and we require it as
     * a parameter to enforce that fact, even though it's not explicitlly used by the method.
     */
    private Flow<FlowableUnfilteredPartition> queryMemtableAndDisk(ColumnFamilyStore cfs,
                                                                   ReadExecutionController executionController,
                                                                   SSTableReadMetricsCollector metricsCollector)
    {
        assert executionController != null && executionController.validForReadOn(cfs);

        Tracing.trace("Executing single-partition query on {}", cfs.name);

         /*
         * We have 2 main strategies:
         *   1) We query memtables and sstables simultaneously. This is our most generic strategy and the one we use
         *      unless we have a names filter that we know we can optimize further.
         *   2) If we have a name filter (so we query specific rows), we can make a bet: that all column for all queried row
         *      will have data in the most recent sstable(s), thus saving us from reading older ones. This does imply we
         *      have a way to guarantee we have all the data for what is queried, which is only possible for name queries
         *      and if we have neither non-frozen collections/UDTs nor counters (indeed, for a non-frozen collection or UDT,
         *      we can't guarantee an older sstable won't have some elements that weren't in the most recent sstables,
         *      and counters are intrinsically a collection of shards and so have the same problem).
         */
        if (clusteringIndexFilter().kind() == ClusteringIndexFilter.Kind.NAMES && !queriesMulticellType(cfs.metadata()))
            return queryMemtableAndSSTablesInTimestampOrder(cfs,
                                                            (ClusteringIndexNamesFilter) clusteringIndexFilter(),
                                                            metricsCollector);
        else
            return queryMemtableAndDiskInternal(cfs, metricsCollector);
    }

    @Override
    protected int oldestUnrepairedTombstone()
    {
        return oldestUnrepairedTombstone;
    }

    // variables mutated in lambda
    class MutableState
    {
        // for queryMemtableAndDiskInternal
        long mostRecentPartitionTombstone = Long.MIN_VALUE;
        long minTimestamp = Long.MIN_VALUE;
        int allTableCount = 0;
        int includedDueToTombstones = 0;
        int nonIntersectingSSTables = 0;

        // queryMemtableAndSSTablesInTimestampOrder
        boolean onlyUnrepaired = true;
        ImmutableBTreePartition timeOrderedResults = null;
        ClusteringIndexNamesFilter namesFilter = null;
    }

    private Flow<FlowableUnfilteredPartition> queryMemtableAndDiskInternal(ColumnFamilyStore cfs,
                                                                           SSTableReadMetricsCollector metricsCollector)
    {
        // We now build a flow of FlowableUnfilteredPartition sourced from three separate lists:
        // - memtables
        // - sstables that contain sliced data, but only newer than the most recent partition tombstone
        // - sstables that contain tombstones newer than the data from above

        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, partitionKey()));

        List<Flow<FlowableUnfilteredPartition>> iterators = new ArrayList<>(3);

        MutableState state = new MutableState();

        iterators.add(Flow.fromIterable(view.memtables)
                          .flatMap(memtable ->
                          {
                              state.minTimestamp = Math.min(state.minTimestamp, memtable.getMinTimestamp());
                              return memtable.getPartition(partitionKey());
                          })
                          .skippingMap(p ->
                          {
                              if (p == null)
                                  return null;

                              oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone,
                                                                   p.stats().minLocalDeletionTime);

                              state.mostRecentPartitionTombstone = Math.max(state.mostRecentPartitionTombstone,
                                                                      p.partitionLevelDeletion().markedForDeleteAt());

                              return clusteringIndexFilter().getFlowableUnfilteredPartition(columnFilter(), p);
                          }));

        /*
         * We can't eliminate full sstables based on the timestamp of what we've already read like
         * in collectTimeOrderedData, but we still want to eliminate sstable whose maxTimestamp < mostRecentTombstone
         * we've read. We still rely on the sstable ordering by maxTimestamp since if
         *   maxTimestamp_s1 > maxTimestamp_s0,
         * we're guaranteed that s1 cannot have a row tombstone such that
         *   timestamp(tombstone) > maxTimestamp_s0
         * since we necessarily have
         *   timestamp(tombstone) <= maxTimestamp_s1
         * In other words, iterating in maxTimestamp order allow to do our mostRecentPartitionTombstone elimination
         * in one pass, and minimize the number of sstables for which we read a partition tombstone.
         */

        Collections.sort(view.sstables, SSTableReader.maxTimestampComparator);
        List<SSTableReader> skippedSSTablesWithTombstones = null;
        List<SSTableReader> filteredSSTables = new ArrayList<>(view.sstables.size());

        for (SSTableReader sstable : view.sstables)
        {
            ++state.allTableCount;

            if (!shouldInclude(sstable))
            {
                state.nonIntersectingSSTables++;
                if (sstable.mayHaveTombstones())
                { // if sstable has tombstones we need to check after one pass if it can be safely skipped
                    if (skippedSSTablesWithTombstones == null)
                        skippedSSTablesWithTombstones = new ArrayList<>();
                    skippedSSTablesWithTombstones.add(sstable);
                }
                continue;
            }
            filteredSSTables.add(sstable);
        }

        // Operating over this flow executes the operations in each takeWhile below only after the preceding items have
        // been processed, which ensures that the relevant fields are properly set.

        iterators.add(Flow.fromIterable(filteredSSTables)
                          .takeWhile(sstable -> sstable.getMaxTimestamp() >= state.mostRecentPartitionTombstone)
                          .flatMap(sstable ->
                          {
                              state.minTimestamp = Math.min(state.minTimestamp, sstable.getMinTimestamp());

                              if (!sstable.isRepaired())
                                  oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone,
                                                                       sstable.getMinLocalDeletionTime());

                              return makeFlowable(sstable, metricsCollector);
                          })
                          .map(fup ->
                          {
                              state.mostRecentPartitionTombstone = Math.max(state.mostRecentPartitionTombstone,
                                                                      fup.header.partitionLevelDeletion.markedForDeleteAt());
                              return fup;
                          }));

        // These one must be added at the end, so that minTimestamp (which is also affected by mostRecentPartitionTombstone)
        // is calculated correctly for them.
        if (skippedSSTablesWithTombstones != null)
        {
            iterators.add(Flow.fromIterable(skippedSSTablesWithTombstones)
                              .takeWhile(sstable -> sstable.getMaxTimestamp() > state.minTimestamp)
                              .flatMap(sstable ->
                              {
                                  // This has no sliced data so we no longer need to update minTimestamp

                                  state.includedDueToTombstones++;

                                  if (!sstable.isRepaired())
                                      oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone,
                                                                           sstable.getMinLocalDeletionTime());

                                  return makeFlowable(sstable, metricsCollector);
                              }));
        }

        return Flow.concat(iterators)
                   .toList()
                   .map(sources -> mergeResult(cfs,
                                               sources,
                                               state.nonIntersectingSSTables,
                                               state.allTableCount,
                                               state.includedDueToTombstones));
    }

    private FlowableUnfilteredPartition mergeResult(ColumnFamilyStore cfs,
                                                    List<FlowableUnfilteredPartition> fups,
                                                    int nonIntersectingSSTables,
                                                    int allTableCount,
                                                    int includedDueToTombstones)
    {
        if (Tracing.isTracing())
            Tracing.trace("Skipped {}/{} non-slice-intersecting sstables, included {} due to tombstones",
                          nonIntersectingSSTables, allTableCount, includedDueToTombstones);

        if (fups.isEmpty())
            return FlowablePartitions.empty(metadata(), partitionKey(), clusteringIndexFilter().isReversed());

        StorageHook.instance.reportRead(metadata().id, partitionKey());
        cfs.metric.samplers.get(TableMetrics.Sampler.READS).addSample(partitionKey.getKey(), partitionKey.hashCode(), 1);

        return FlowablePartitions.merge(fups, nowInSec(), null);
    }

    private boolean shouldInclude(SSTableReader sstable)
    {
        // If some static columns are queried, we should always include the sstable: the clustering values stats of the sstable
        // don't tell us if the sstable contains static values in particular.
        // TODO: we could record if a sstable contains any static value at all.
        if (!columnFilter().fetchedColumns().statics.isEmpty())
            return true;

        return clusteringIndexFilter().shouldInclude(sstable);
    }

    private Flow<FlowableUnfilteredPartition> makeFlowable(final SSTableReader sstable,
                                                           final ClusteringIndexNamesFilter clusterFilter,
                                                           SSTableReadMetricsCollector metricsCollector)
    {
        return sstable.flow(partitionKey(), clusterFilter.getSlices(metadata()), columnFilter(), isReversed(), metricsCollector);
    }

    private Flow<FlowableUnfilteredPartition> makeFlowable(final SSTableReader sstable,
                                                           SSTableReadMetricsCollector metricsCollector)
    {
        return sstable.flow(partitionKey(), clusteringIndexFilter().getSlices(metadata()), columnFilter(), isReversed(), metricsCollector);
    }

    private boolean queriesMulticellType(TableMetadata table)
    {
        if (!table.hasMulticellOrCounterColumn)
            return false;

        for (ColumnMetadata column : columnFilter().fetchedColumns())
        {
            if (column.type.isMultiCell() || column.type.isCounter())
                return true;
        }
        return false;
    }

    /**
     * Do a read by querying the memtable(s) first, and then each relevant sstables sequentially by order of the sstable
     * max timestamp.
     *
     * This is used for names query in the hope of only having to query the 1 or 2 most recent query and then knowing nothing
     * more recent could be in the older sstables (which we can only guarantee if we know exactly which row we queries, and if
     * no collection or counters are included).
     * This method assumes the filter is a {@code ClusteringIndexNamesFilter}.
     */
    private Flow<FlowableUnfilteredPartition> queryMemtableAndSSTablesInTimestampOrder(ColumnFamilyStore cfs,
                                                                                       final ClusteringIndexNamesFilter initFilter,
                                                                                       SSTableReadMetricsCollector metricsCollector)
    {
        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, partitionKey()));

        // variables mutated in lambda

        MutableState state = new MutableState();
        state.namesFilter = initFilter;

        Flow<FlowableUnfilteredPartition> pipeline =
            Flow.fromIterable(view.memtables)
                .flatMap(memtable -> memtable.getPartition(partitionKey()))
                .skippingMap(p ->
                             {
                                 if (p == null)
                                     return null;

                                 oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, p.stats().minLocalDeletionTime);

                                 return state.namesFilter.getFlowableUnfilteredPartition(columnFilter(), p);
                             });

        if (!view.sstables.isEmpty())
        {
            /* sort the SSTables on disk */
            Collections.sort(view.sstables, SSTableReader.maxTimestampComparator);

            Flow<FlowableUnfilteredPartition> sstablePipeline =
                Flow.fromIterable(view.sstables)
                    .takeWhile(sstable ->
                    {
                        // if we've already seen a partition tombstone with a timestamp greater
                        // than the most recent update to this sstable, we're done, since the rest of the sstables
                        // will also be older
                        if (state.timeOrderedResults != null &&
                            sstable.getMaxTimestamp() < state.timeOrderedResults.partitionLevelDeletion()
                                                                                .markedForDeleteAt())
                            return false;

                        long currentMaxTs = sstable.getMaxTimestamp();
                        state.namesFilter = reduceFilter(state.namesFilter,
                                                         state.timeOrderedResults,
                                                         currentMaxTs);
                        // If the filter is empty, we have constructed our result; stop processing sstables.
                        return (state.namesFilter != null);
                    })
                    .flatMap(sstable ->
                    {
                        if (!shouldInclude(sstable))
                        {
                            // This means that nothing queried by the filter can be in the sstable, but tombstones can still span over
                            // queried data. We can completely skip a table that doesn't have any tombstones, though.
                            if (!sstable.mayHaveTombstones())
                                return Flow.empty();
                        }

                        Tracing.trace("Merging data from sstable {}", sstable.descriptor.generation);

                        if (sstable.isRepaired())
                            state.onlyUnrepaired = false;
                        else
                            oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone,
                                                                 sstable.getMinLocalDeletionTime());

                        return makeFlowable(sstable, state.namesFilter, metricsCollector);
                    });

            pipeline = Flow.concat(pipeline, sstablePipeline);
        }

        return pipeline.flatMap(p -> mergeToMemory(state.timeOrderedResults, p, initFilter))
                       .process(p -> state.timeOrderedResults = p)
                       .map(v -> outputTimeOrderedResult(cfs,
                                                         state.timeOrderedResults,
                                                         metricsCollector,
                                                         state.onlyUnrepaired));
    }

    private FlowableUnfilteredPartition outputTimeOrderedResult(ColumnFamilyStore cfs,
                                                                ImmutableBTreePartition timeOrderedResult,
                                                                SSTableReadMetricsCollector metricsCollector,
                                                                boolean onlyUnrepaired)
    {
        if (timeOrderedResult == null || timeOrderedResult.isEmpty())
            return FlowablePartitions.empty(metadata(), partitionKey(), clusteringIndexFilter().isReversed());

        DecoratedKey key = timeOrderedResult.partitionKey();
        cfs.metric.samplers.get(TableMetrics.Sampler.READS).addSample(key.getKey(), key.hashCode(), 1);
        StorageHook.instance.reportRead(cfs.metadata.id, partitionKey());

        // "hoist up" the requested data into a more recent sstable
        if (metricsCollector.getMergedSSTables() > cfs.getMinimumCompactionThreshold()
            && onlyUnrepaired
            && !cfs.isAutoCompactionDisabled()
            && cfs.getCompactionStrategyManager().shouldDefragment())
        {
            // !!WARNING!!   if we stop copying our data to a heap-managed object,
            //               we will need to track the lifetime of this mutation as well
            Tracing.trace("Defragmenting requested data");

            Schedulers.io().scheduleDirect(
                new TPCRunnable(() ->
                                {
                                    try (UnfilteredRowIterator iter = timeOrderedResult.unfilteredIterator(columnFilter(), Slices.ALL, false))
                                    {
                                        final Mutation mutation = new Mutation(PartitionUpdate.fromIterator(iter, columnFilter()));
                                        // skipping commitlog and index updates is fine since we're just de-fragmenting existing data
                                        // Fire and forget
                                        Keyspace.open(mutation.getKeyspaceName()).apply(mutation, false, false, true).subscribe();
                                    }
                                },
                                ExecutorLocals.create(),
                                TPCTaskType.WRITE_DEFRAGMENT,
                                TPC.getNumCores()));
        }

        return timeOrderedResult.unfilteredPartition(columnFilter(), Slices.ALL, clusteringIndexFilter().isReversed());
    }

    private Flow<ImmutableBTreePartition> mergeToMemory(ImmutableBTreePartition prev, FlowableUnfilteredPartition partition, ClusteringIndexNamesFilter filter)
    {
        int maxRows = Math.max(filter.requestedRows().size(), 1);

        FlowableUnfilteredPartition merged;
        if (prev == null || prev.isEmpty())
            merged = partition;
        else
            merged = FlowablePartitions.merge(ImmutableList.of(partition,
                                                               prev.unfilteredPartition(columnFilter(), Slices.ALL, filter.isReversed())),
                                              nowInSec(),
                                              null);

        return ImmutableBTreePartition.create(merged, maxRows, true);
    }

    private ClusteringIndexNamesFilter reduceFilter(ClusteringIndexNamesFilter filter, Partition result, long sstableTimestamp)
    {
        if (result == null)
            return filter;

        SearchIterator<Clustering, Row> searchIter = result.searchIterator(columnFilter(), false);

        RegularAndStaticColumns columns = columnFilter().fetchedColumns();
        NavigableSet<Clustering> clusterings = filter.requestedRows();

        // We want to remove rows for which we have values for all requested columns. We have to deal with both static and regular rows.
        // TODO: we could also remove a selected column if we've found values for every requested row but we'll leave
        // that for later.

        boolean removeStatic = false;
        if (!columns.statics.isEmpty())
        {
            Row staticRow = searchIter.next(Clustering.STATIC_CLUSTERING);
            removeStatic = staticRow != null && canRemoveRow(staticRow, columns.statics, sstableTimestamp);
        }

        NavigableSet<Clustering> toRemove = null;
        for (Clustering clustering : clusterings)
        {
            Row row = searchIter.next(clustering);
            if (row == null || !canRemoveRow(row, columns.regulars, sstableTimestamp))
                continue;

            if (toRemove == null)
                toRemove = new TreeSet<>(result.metadata().comparator);
            toRemove.add(clustering);
        }

        if (!removeStatic && toRemove == null)
            return filter;

        // Check if we have everything we need
        boolean hasNoMoreStatic = columns.statics.isEmpty() || removeStatic;
        boolean hasNoMoreClusterings = clusterings.isEmpty() || (toRemove != null && toRemove.size() == clusterings.size());
        if (hasNoMoreStatic && hasNoMoreClusterings)
            return null;

        if (toRemove != null)
        {
            BTreeSet.Builder<Clustering> newClusterings = BTreeSet.builder(result.metadata().comparator);
            newClusterings.addAll(Sets.difference(clusterings, toRemove));
            clusterings = newClusterings.build();
        }
        return new ClusteringIndexNamesFilter(clusterings, filter.isReversed());
    }

    private boolean canRemoveRow(Row row, Columns requestedColumns, long sstableTimestamp)
    {
        // We can remove a row if it has data that is more recent that the next sstable to consider for the data that the query
        // cares about. And the data we care about is 1) the row timestamp (since every query cares if the row exists or not)
        // and 2) the requested columns.
        if (row.primaryKeyLivenessInfo().isEmpty() || row.primaryKeyLivenessInfo().timestamp() <= sstableTimestamp)
            return false;

        for (ColumnMetadata column : requestedColumns)
        {
            Cell cell = row.getCell(column);
            if (cell == null || cell.timestamp() <= sstableTimestamp)
                return false;
        }
        return true;
    }

    public boolean queriesOnlyLocalData()
    {
        return StorageProxy.isLocalToken(metadata().keyspace, partitionKey.getToken());
    }

    @Override
    public boolean selectsFullPartition()
    {
        return metadata().isStaticCompactTable() ||
               (clusteringIndexFilter.selectsAllPartition() && !rowFilter().hasExpressionOnClusteringOrRegularColumns());
    }

    @Override
    public ReadContext.Builder applyDefaults(ReadContext.Builder ctx)
    {
        // Single partition reads use both digests and read-repair (if enabled) by default (contrarily to range queries
        // which ignore them).
        return ctx.useDigests()
                  .readRepairDecision(ReadRepairDecision.newDecision(metadata()));
    }

    @Override
    public String toString()
    {
        return String.format("Read(%s columns=%s rowFilter=%s limits=%s key=%s filter=%s, nowInSec=%d)",
                             metadata().toString(),
                             columnFilter(),
                             rowFilter(),
                             limits(),
                             metadata().partitionKeyType.getString(partitionKey().getKey()),
                             clusteringIndexFilter.toString(metadata()),
                             nowInSec());
    }

    protected void appendCQLWhereClause(StringBuilder sb)
    {
        sb.append(" WHERE ");

        sb.append(ColumnMetadata.toCQLString(metadata().partitionKeyColumns())).append(" = ");
        DataRange.appendKeyString(sb, metadata().partitionKeyType, partitionKey().getKey());

        // We put the row filter first because the clustering index filter can end by "ORDER BY"
        if (!rowFilter().isEmpty())
            sb.append(" AND ").append(rowFilter());

        String filterString = clusteringIndexFilter().toCQLString(metadata());
        if (!filterString.isEmpty())
            sb.append(" AND ").append(filterString);
    }

    protected void serializeSelection(DataOutputPlus out, ReadVersion version) throws IOException
    {
        metadata().partitionKeyType.writeValue(partitionKey().getKey(), out);
        ClusteringIndexFilter.serializers.get(version).serialize(clusteringIndexFilter(), out);
    }

    protected long selectionSerializedSize(ReadVersion version)
    {
        return metadata().partitionKeyType.writtenLength(partitionKey().getKey())
             + ClusteringIndexFilter.serializers.get(version).serializedSize(clusteringIndexFilter());
    }

    public boolean isLimitedToOnePartition()
    {
        return true;
    }

    public StagedScheduler getScheduler()
    {
        return scheduler;
    }

    public TracingAwareExecutor getOperationExecutor()
    {
        return operationExecutor;
    }

    /**
     * Groups multiple single partition read commands.
     */
    public static class Group implements ReadQuery
    {
        public final List<SinglePartitionReadCommand> commands;
        private final DataLimits limits;
        private final int nowInSec;
        private final boolean selectsFullPartitions;
        private final boolean enforceStrictLiveness;

        public Group(List<SinglePartitionReadCommand> commands, DataLimits limits)
        {
            assert !commands.isEmpty();
            this.commands = commands;
            this.limits = limits;
            SinglePartitionReadCommand firstCommand = commands.get(0);
            this.nowInSec = firstCommand.nowInSec();
            this.selectsFullPartitions = firstCommand.selectsFullPartition();
            this.enforceStrictLiveness = firstCommand.metadata().enforceStrictLiveness();
            for (int i = 1; i < commands.size(); i++)
                assert commands.get(i).nowInSec() == nowInSec;
        }

        public static Group one(SinglePartitionReadCommand command)
        {
            return new Group(Collections.singletonList(command), command.limits());
        }

        public Flow<FlowablePartition> execute(ReadContext ctx) throws RequestExecutionException
        {
            return StorageProxy.read(this, ctx);
        }

        public int nowInSec()
        {
            return nowInSec;
        }

        public DataLimits limits()
        {
            return limits;
        }

        public TableMetadata metadata()
        {
            return commands.get(0).metadata();
        }

        public boolean isEmpty()
        {
            return false;
        }

        @Override
        public ReadContext.Builder applyDefaults(ReadContext.Builder ctx)
        {
            return commands.get(0).applyDefaults(ctx);
        }

        @Override
        public boolean selectsFullPartition()
        {
            return selectsFullPartitions;
        }

        public ReadExecutionController executionController()
        {
            // Note that the only difference between the command in a group must be the partition key on which
            // they applied. So as far as ReadOrderGroup is concerned, we can use any of the commands to start one.
            return commands.get(0).executionController();
        }

        public Flow<FlowablePartition> executeInternal(Monitor monitor)
        {
            return limits.truncateFiltered(FlowablePartitions.filterAndSkipEmpty(executeLocally(monitor, false),
                                                                                 nowInSec()),
                                                         nowInSec(),
                                                         selectsFullPartitions,
                                                         enforceStrictLiveness);
        }

        public Flow<FlowableUnfilteredPartition> executeLocally(Monitor monitor)
        {
            return executeLocally(monitor, true);
        }

        /**
         * Implementation of {@link ReadQuery#executeLocally()}.
         *
         * @param sort - whether to sort the inner commands by partition key, required for merging the iterator
         *               later on. This will be false when called by {@link ReadQuery#executeInternal(Monitor)}
         *               because in this case it is safe to do so as there is no merging involved and we don't want to
         *               change the old behavior which was to not sort by partition.
         *
         * @return - the iterator that can be used to retrieve the query result.
         */
        private Flow<FlowableUnfilteredPartition> executeLocally(Monitor monitor, boolean sort)
        {
            if (commands.size() == 1)
                return commands.get(0).executeLocally(monitor);

            List<SinglePartitionReadCommand> commands = this.commands;
            if (sort)
            {
                commands = new ArrayList<>(commands);
                commands.sort(Comparator.comparing(SinglePartitionReadCommand::partitionKey));
            }

            return Flow.fromIterable(commands)
                       .flatMap(command -> command.executeLocally(monitor));
        }

        public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion)
        {
            if (commands.size() == 1)
                return SinglePartitionReadCommand.getPager(commands.get(0), pagingState, protocolVersion);

            return new MultiPartitionPager(this, pagingState, protocolVersion);
        }

        public boolean selectsKey(DecoratedKey key)
        {
            return Iterables.any(commands, c -> c.selectsKey(key));
        }

        public boolean selectsClustering(DecoratedKey key, Clustering clustering)
        {
            return Iterables.any(commands, c -> c.selectsClustering(key, clustering));
        }

        public boolean queriesOnlyLocalData()
        {
            for (SinglePartitionReadCommand cmd : commands)
            {
                if (!cmd.queriesOnlyLocalData())
                    return false;
            }

            return true;
        }

        public String toCQLString()
        {
            // A group is really just a SELECT ... IN (x, y, z) where the IN is on the partition key. Rebuilding that
            // just too much work, so we simply concat the string for each commnand.
            return Joiner.on("; ").join(Iterables.transform(commands, ReadCommand::toCQLString));
        }

        @Override
        public String toString()
        {
            return commands.toString();
        }
    }

    private static class Deserializer extends SelectionDeserializer<SinglePartitionReadCommand>
    {
        public SinglePartitionReadCommand deserialize(DataInputPlus in,
                                                      ReadVersion version,
                                                      DigestVersion digestVersion,
                                                      TableMetadata metadata,
                                                      int nowInSec,
                                                      ColumnFilter columnFilter,
                                                      RowFilter rowFilter,
                                                      DataLimits limits,
                                                      IndexMetadata index)
        throws IOException
        {
            DecoratedKey key = metadata.partitioner.decorateKey(metadata.partitionKeyType.readValue(in, DatabaseDescriptor.getMaxValueSize()));
            ClusteringIndexFilter filter = ClusteringIndexFilter.serializers.get(version).deserialize(in, metadata);
            return new SinglePartitionReadCommand(digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, key, filter, index);
        }
    }

    /**
     * {@code SSTableReaderListener} used to collect metrics about SSTable read access.
     */
    private static final class SSTableReadMetricsCollector implements SSTableReadsListener
    {
        /**
         * The number of SSTables that need to be merged. This counter is only updated for single partition queries
         * since this has been the behavior so far.
         */
        private int mergedSSTables;

        @Override
        public void onSSTableSelected(SSTableReader sstable, RowIndexEntry indexEntry, SelectionReason reason)
        {
            sstable.incrementReadCount();
            mergedSSTables++;
        }

        /**
         * Returns the number of SSTables that need to be merged.
         * @return the number of SSTables that need to be merged.
         */
        public int getMergedSSTables()
        {
            return mergedSSTables;
        }
    }
}
