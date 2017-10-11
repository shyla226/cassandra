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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * A read command that selects a (part of a) range of partitions.
 */
public class PartitionRangeReadCommand extends ReadCommand
{
    private static final SelectionDeserializer<PartitionRangeReadCommand> selectionDeserializer = new Deserializer();
    public static final Versioned<ReadVersion, Serializer<PartitionRangeReadCommand>> serializers = ReadVersion.versioned(v -> new ReadCommandSerializer<>(v, selectionDeserializer));

    private final DataRange dataRange;
    /**
     * Race condition when ReadCommand is re-used.
     *
     * It's ok to get a smaller oldestUnrepairedTombstone value when ReadCommand is reused, but the concurrent update
     * to this variable is not safe, we might lost update.
     *
     * SEE APOLLO-1084
     */
    private int oldestUnrepairedTombstone = Integer.MAX_VALUE;

    // We access the scheduler/operationExecutor multiple times for each command (at least twice for every replica
    // involved in the request and response executor in Messaging) and re-doing their computation is unnecessary so
    // caching their value here. Note that we don't serialize those in any way, they are just recomputed in the ctor.
    private final transient TPCScheduler scheduler;
    private final transient TracingAwareExecutor operationExecutor;

    private PartitionRangeReadCommand(DigestVersion digestVersion,
                                      TableMetadata metadata,
                                      int nowInSec,
                                      ColumnFilter columnFilter,
                                      RowFilter rowFilter,
                                      DataLimits limits,
                                      DataRange dataRange,
                                      IndexMetadata index)
    {
        super(digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, index);
        this.dataRange = dataRange;

        this.scheduler = TPC.bestTPCScheduler();
        this.operationExecutor = scheduler.forTaskType(TPCTaskType.READ_RANGE);
    }

    protected PartitionRangeReadCommand(TableMetadata metadata,
                                        int nowInSec,
                                        ColumnFilter columnFilter,
                                        RowFilter rowFilter,
                                        DataLimits limits,
                                        DataRange dataRange,
                                        IndexMetadata index)
    {
        this(null, metadata, nowInSec, columnFilter, rowFilter, limits, dataRange, index);
    }

    public static PartitionRangeReadCommand create(TableMetadata metadata,
                                                   int nowInSec,
                                                   ColumnFilter columnFilter,
                                                   RowFilter rowFilter,
                                                   DataLimits limits,
                                                   DataRange dataRange)
    {
        return new PartitionRangeReadCommand(metadata,
                                             nowInSec,
                                             columnFilter,
                                             rowFilter,
                                             limits,
                                             dataRange,
                                             findIndex(metadata, rowFilter));
    }

    /**
     * Creates a new read command that query all the data in the table.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     *
     * @return a newly created read command that queries everything in the table.
     */
    public static PartitionRangeReadCommand allDataRead(TableMetadata metadata, int nowInSec)
    {
        return new PartitionRangeReadCommand(metadata,
                                             nowInSec,
                                             ColumnFilter.all(metadata),
                                             RowFilter.NONE,
                                             DataLimits.NONE,
                                             DataRange.allData(metadata.partitioner),
                                             null);
    }

    public static PartitionRangeReadCommand fullRangeRead(TableMetadata metadata, DataRange range, int nowInSec)
    {
        return new PartitionRangeReadCommand(metadata,
                                             nowInSec,
                                             ColumnFilter.all(metadata),
                                             RowFilter.NONE,
                                             DataLimits.NONE,
                                             range,
                                             null);
    }

    public Request.Dispatcher<? extends PartitionRangeReadCommand, ReadResponse> dispatcherTo(Collection<InetAddress> endpoints)
    {
        return Verbs.READS.RANGE_READ.newDispatcher(endpoints, this);
    }

    public Request<? extends PartitionRangeReadCommand, ReadResponse> requestTo(InetAddress endpoint)
    {
        return Verbs.READS.RANGE_READ.newRequest(endpoint, this);
    }

    public DataRange dataRange()
    {
        return dataRange;
    }

    public ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key)
    {
        return dataRange.clusteringIndexFilter(key);
    }

    /**
     * Returns an equivalent command but that only queries data within the provided range.
     *
     * @param range the sub-range to restrict the command to. This method <b>assumes</b> that this is a proper sub-range
     * of the command this is applied to.
     * @param isRangeContinuation whether {@code range} is a direct continuation of whatever previous range we have
     * queried. This matters for the {@code DataLimits} that may contain states when we do paging and in the context of
     * parallel queries: that state only make sense if the range queried is indeed the follow-up of whatever range we've
     * previously query (that yield said state). In practice this means that ranges for which {@code isRangeContinuation}
     * is false may have to be slightly pessimistic when counting data and may include a little bit than necessary, and
     * this should be dealt with post-query (in the case of {@code StorageProxy.getRangeSlice()}, which uses this method
     * for replica queries, this is dealt with by re-counting results on the coordinator). Note that if this is the
     * first range we queried, then the {@code DataLimits} will have not state and the value of this parameter doesn't
     * matter.
     */
    public PartitionRangeReadCommand forSubRange(AbstractBounds<PartitionPosition> range, boolean isRangeContinuation)
    {
        DataRange newRange = dataRange().forSubRange(range);
        // If we're not a continuation of whatever range we've previously queried, we should ignore the states of the
        // DataLimits as it's either useless, or misleading. This is particularly important for GROUP BY queries, where
        // DataLimits.CQLGroupByLimits.GroupByAwareCounter assumes that if GroupingState.hasClustering(), then we're in
        // the middle of a group, but we can't make that assumption if we query and range "in advance" of where we are
        // on the ring.
        return new PartitionRangeReadCommand(digestVersion(),
                                             metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             isRangeContinuation ? limits() : limits().withoutState(),
                                             newRange,
                                             indexMetadata());
    }

    public PartitionRangeReadCommand createDigestCommand(DigestVersion digestVersion)
    {
        return new PartitionRangeReadCommand(digestVersion,
                                             metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             limits(),
                                             dataRange(),
                                             indexMetadata());
    }

    public PartitionRangeReadCommand withUpdatedLimit(DataLimits newLimits)
    {
        return new PartitionRangeReadCommand(metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             newLimits,
                                             dataRange(),
                                             indexMetadata());
    }

    public PartitionRangeReadCommand withUpdatedLimitsAndDataRange(DataLimits newLimits, DataRange newDataRange)
    {
        return new PartitionRangeReadCommand(metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             newLimits,
                                             newDataRange,
                                             indexMetadata());
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getRangeRpcTimeout();
    }

    public boolean isReversed()
    {
        return dataRange.isReversed();
    }

    public boolean selectsKey(DecoratedKey key)
    {
        if (!dataRange().contains(key))
            return false;

        return rowFilter().partitionKeyRestrictionsAreSatisfiedBy(key, metadata().partitionKeyType);
    }

    public boolean selectsClustering(DecoratedKey key, Clustering clustering)
    {
        if (clustering == Clustering.STATIC_CLUSTERING)
            return !columnFilter().fetchedColumns().statics.isEmpty();

        if (!dataRange().clusteringIndexFilter(key).selects(clustering))
            return false;
        return rowFilter().clusteringKeyRestrictionsAreSatisfiedBy(clustering);
    }

    public Flow<FlowablePartition> execute(ReadContext ctx) throws RequestExecutionException
    {
        return StorageProxy.getRangeSlice(this, ctx);
    }

    public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion)
    {
        return new PartitionRangeQueryPager(this, pagingState, protocolVersion);
    }

    protected void recordLatency(TableMetrics metric, long latencyNanos)
    {
        metric.rangeLatency.addNano(latencyNanos);
    }

    public Flow<FlowableUnfilteredPartition> queryStorage(final ColumnFamilyStore cfs, ReadExecutionController executionController)
    {
        ColumnFamilyStore.ViewFragment view = cfs.select(View.selectLive(dataRange().keyRange()));
        Tracing.trace("Executing seq scan across {} sstables for {}", view.sstables.size(), dataRange().keyRange().getString(metadata().partitionKeyType));

        // fetch data from current memtable, historical memtables, and SSTables in the correct order.
        final List<Flow<FlowableUnfilteredPartition>> iterators = new ArrayList<>(Iterables.size(view.memtables) + view.sstables.size());

        for (Memtable memtable : view.memtables)
        {
            Flow<FlowableUnfilteredPartition> iter = memtable.makePartitionIterator(columnFilter(), dataRange());
            oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, memtable.getMinLocalDeletionTime());
            iterators.add(iter);
        }

        SSTableReadsListener readCountUpdater = newReadCountUpdater();
        for (SSTableReader sstable : view.sstables)
        {
            iterators.add(sstable.getAsyncScanner(columnFilter(), dataRange(), readCountUpdater));
            if (!sstable.isRepaired())
                oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, sstable.getMinLocalDeletionTime());
        }

        // iterators can be empty for offline tools
        if (iterators.isEmpty())
            return Flow.empty();

        if (cfs.isRowCacheEnabled())
        {
            return FlowablePartitions.mergePartitions(iterators, nowInSec(), null)
                                     .map(partition ->
                                          {
                                              // Note that we rely on the fact that until we start iterating the partition no really costly operation is done.
                                              DecoratedKey dk = partition.header.partitionKey;

                                              // Check if this partition is in the rowCache and if it is, if  it covers our filter
                                              CachedPartition cached = cfs.getRawCachedPartition(dk);
                                              ClusteringIndexFilter filter = dataRange().clusteringIndexFilter(dk);

                                              if (cached != null && cfs.isFilterFullyCoveredBy(filter, limits(), cached, nowInSec(), metadata().enforceStrictLiveness()))
                                              {
                                                  // The partition will not be used, so abort it.
                                                  partition.unused();

                                                  return filter.getFlowableUnfilteredPartition(columnFilter(), cached);
                                              }

                                              return partition;
                                          });
        }
        else
        {
            return FlowablePartitions.mergePartitions(iterators, nowInSec(), null);
        }

    }

    /**
     * Creates a new {@code SSTableReadsListener} to update the SSTables read counts.
     * @return a new {@code SSTableReadsListener} to update the SSTables read counts.
     */
    private static SSTableReadsListener newReadCountUpdater()
    {
        return new SSTableReadsListener()
                {
                    @Override
                    public void onScanningStarted(SSTableReader sstable)
                    {
                        sstable.incrementReadCount();
                    }
                };
    }

    @Override
    protected int oldestUnrepairedTombstone()
    {
        return oldestUnrepairedTombstone;
    }

    protected void appendCQLWhereClause(StringBuilder sb)
    {
        if (dataRange.isUnrestricted() && rowFilter().isEmpty())
            return;

        sb.append(" WHERE ");
        // We put the row filter first because the data range can end by "ORDER BY"
        if (!rowFilter().isEmpty())
        {
            sb.append(rowFilter());
            if (!dataRange.isUnrestricted())
                sb.append(" AND ");
        }
        if (!dataRange.isUnrestricted())
            sb.append(dataRange.toCQLString(metadata()));
    }

    public Flow<FlowablePartition> withLimitsAndPostReconciliation(Flow<FlowablePartition> partitions)
    {
        return limits().truncateFiltered(postReconciliationProcessing(partitions), nowInSec(), selectsFullPartition(), metadata().enforceStrictLiveness());
    }

    /**
     * Allow to post-process the result of the query after it has been reconciled on the coordinator
     * but before it is passed to the CQL layer to return the ResultSet.
     *
     * See CASSANDRA-8717 for why this exists.
     */
    public Flow<FlowablePartition> postReconciliationProcessing(Flow<FlowablePartition> partitions)
    {
        ColumnFamilyStore cfs = Keyspace.open(metadata().keyspace).getColumnFamilyStore(metadata().name);
        Index index = getIndex(cfs);
        return index == null ? partitions : index.postProcessorFor(this).apply(partitions, this);
    }

    public boolean queriesOnlyLocalData()
    {
        return StorageProxy.isLocalRange(metadata().keyspace, dataRange.keyRange());
    }

    @Override
    public boolean selectsFullPartition()
    {
        return metadata().isStaticCompactTable() ||
               (dataRange.selectsAllPartition() && !rowFilter().hasExpressionOnClusteringOrRegularColumns());
    }

    @Override
    public String toString()
    {
        return String.format("Read(%s columns=%s rowfilter=%s limits=%s %s)",
                             metadata().toString(),
                             columnFilter(),
                             rowFilter(),
                             limits(),
                             dataRange().toString(metadata()));
    }

    protected void serializeSelection(DataOutputPlus out, ReadVersion version) throws IOException
    {
        DataRange.serializers.get(version).serialize(dataRange(), out, metadata());
    }

    protected long selectionSerializedSize(ReadVersion version)
    {
        return DataRange.serializers.get(version).serializedSize(dataRange(), metadata());
    }

    /*
     * We are currently using PartitionRangeReadCommand for most index queries, even if they are explicitly restricted
     * to a single partition key. Return true if that is the case.
     *
     * See CASSANDRA-11617 and CASSANDRA-11872 for details.
     */
    public boolean isLimitedToOnePartition()
    {
        return dataRange.keyRange instanceof Bounds
               && dataRange.startKey().kind() == PartitionPosition.Kind.ROW_KEY
               && dataRange.startKey().equals(dataRange.stopKey());
    }

    public StagedScheduler getScheduler()
    {
        return scheduler;
    }

    public TracingAwareExecutor getOperationExecutor()
    {
        return operationExecutor;
    }

    private static class Deserializer extends SelectionDeserializer<PartitionRangeReadCommand>
    {
        public PartitionRangeReadCommand deserialize(DataInputPlus in,
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
            DataRange range = DataRange.serializers.get(version).deserialize(in, metadata);
            return new PartitionRangeReadCommand(digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, range, index);
        }
    }
}
