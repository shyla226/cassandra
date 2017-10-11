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
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.functions.Function;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.flow.Flow;
import io.reactivex.Single;
import org.apache.cassandra.concurrent.Schedulable;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.selection.ResultBuilder;
import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.exceptions.UnknownIndexException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.VersionDependent;

/**
 * General interface for storage-engine read commands (common to both range and
 * single partition commands).
 * <p>
 * This contains all the information needed to do a local read.
 */
public abstract class ReadCommand implements ReadQuery, Schedulable
{
    protected static final Logger logger = LoggerFactory.getLogger(ReadCommand.class);

    private final TableMetadata metadata;
    private final int nowInSec;

    private final ColumnFilter columnFilter;
    private final RowFilter rowFilter;
    private final DataLimits limits;

    @Nullable
    private final IndexMetadata index;

    /** The version of the digest this must generate if it is a digest query, {@code null} if it isn't a digest query */
    @Nullable
    private final DigestVersion digestVersion;

    protected static abstract class SelectionDeserializer<T extends ReadCommand>
    {
        public abstract T deserialize(DataInputPlus in,
                                      ReadVersion version,
                                      DigestVersion digestVersion,
                                      TableMetadata metadata,
                                      int nowInSec,
                                      ColumnFilter columnFilter,
                                      RowFilter rowFilter,
                                      DataLimits limits,
                                      IndexMetadata index) throws IOException;
    }

    protected ReadCommand(DigestVersion digestVersion,
                          TableMetadata metadata,
                          int nowInSec,
                          ColumnFilter columnFilter,
                          RowFilter rowFilter,
                          DataLimits limits,
                          IndexMetadata index)
    {
        this.digestVersion = digestVersion;
        this.metadata = metadata;
        this.nowInSec = nowInSec;
        this.columnFilter = columnFilter;
        this.rowFilter = rowFilter;
        this.limits = limits;
        this.index = index;
    }

    protected abstract void serializeSelection(DataOutputPlus out, ReadVersion version) throws IOException;
    protected abstract long selectionSerializedSize(ReadVersion version);

    public abstract boolean isLimitedToOnePartition();

    public abstract Request.Dispatcher<? extends ReadCommand, ReadResponse> dispatcherTo(Collection<InetAddress> endpoints);
    public abstract Request<? extends ReadCommand, ReadResponse> requestTo(InetAddress endpoint);

    /**
     * Creates a new <code>ReadCommand</code> instance with new limits.
     *
     * @param newLimits the new limits
     * @return a new <code>ReadCommand</code> with the updated limits
     */
    public abstract ReadCommand withUpdatedLimit(DataLimits newLimits);

    /**
     * The metadata for the table queried.
     *
     * @return the metadata for the table queried.
     */
    public TableMetadata metadata()
    {
        return metadata;
    }

    public boolean isEmpty()
    {
        return false;
    }

    /**
     * The time in seconds to use as "now" for this query.
     * <p>
     * We use the same time as "now" for the whole query to avoid considering different
     * values as expired during the query, which would be buggy (would throw of counting amongst other
     * things).
     *
     * @return the time (in seconds) to use as "now".
     */
    public int nowInSec()
    {
        return nowInSec;
    }

    /**
     * The configured timeout for this command.
     *
     * @return the configured timeout for this command.
     */
    public abstract long getTimeout();

    /**
     * A filter on which (non-PK) columns must be returned by the query.
     *
     * @return which columns must be fetched by this query.
     */
    public ColumnFilter columnFilter()
    {
        return columnFilter;
    }

    /**
     * Filters/Resrictions on CQL rows.
     * <p>
     * This contains the restrictions that are not directly handled by the
     * {@code ClusteringIndexFilter}. More specifically, this includes any non-PK column
     * restrictions and can include some PK columns restrictions when those can't be
     * satisfied entirely by the clustering index filter (because not all clustering columns
     * have been restricted for instance). If there is 2ndary indexes on the table,
     * one of this restriction might be handled by a 2ndary index.
     *
     * @return the filter holding the expression that rows must satisfy.
     */
    public RowFilter rowFilter()
    {
        return rowFilter;
    }

    /**
     * The limits set on this query.
     *
     * @return the limits set on this query.
     */
    public DataLimits limits()
    {
        return limits;
    }

    /**
     * Whether this query is a digest one or not.
     *
     * @return Whether this query is a digest query.
     */
    public boolean isDigestQuery()
    {
        return digestVersion != null;
    }

    /**
     * If the query is a digest one, the requested digest version.
     *
     * @return the requested digest version if the query is a digest, {@code null} otherwise.
     */
    @Nullable
    public DigestVersion digestVersion()
    {
        return digestVersion;
    }

    /**
     * Returns a digest command for this command using the provided digest version.
     *
     * @param digestVersion the version of the digest that the new command must generate.
     * @return a newly created digest command, otherwise equivalent to this command.
     */
    public abstract ReadCommand createDigestCommand(DigestVersion digestVersion);

    /**
     * Index (metadata) chosen for this query. Can be null.
     *
     * @return index (metadata) chosen for this query
     */
    @Nullable
    public IndexMetadata indexMetadata()
    {
        return index;
    }

    /**
     * The clustering index filter this command to use for the provided key.
     * <p>
     * Note that that method should only be called on a key actually queried by this command
     * and in practice, this will almost always return the same filter, but for the sake of
     * paging, the filter on the first key of a range command might be slightly different.
     *
     * @param key a partition key queried by this command.
     *
     * @return the {@code ClusteringIndexFilter} to use for the partition of key {@code key}.
     */
    public abstract ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key);

    @VisibleForTesting
    public abstract Flow<FlowableUnfilteredPartition> queryStorage(ColumnFamilyStore cfs, ReadExecutionController executionController);

    protected abstract int oldestUnrepairedTombstone();


    /**
     * Whether the underlying {@code ClusteringIndexFilter} is reversed or not.
     *
     * @return whether the underlying {@code ClusteringIndexFilter} is reversed or not.
     */
    public abstract boolean isReversed();

    /**
     * Create a read response and takes care of eventually closing the iterator.
     *
     * Digest responses calculate the digest in the constructor and close the iterator immediately,
     * whilst data responses may keep it open until the iterator is closed by the final handler, e.g.
     * {@link org.apache.cassandra.cql3.statements.SelectStatement#processPartition(FlowablePartition, QueryOptions, ResultBuilder, int)}
     *
     * @param partitions - the partitions to be processed in order to create the response
     * @param forLocalDelivery - if the response is to be delivered locally (optimized path)
     * @return An appropriate response, either of type digest or data.
     */
    Single<ReadResponse> createResponse(Flow<FlowableUnfilteredPartition> partitions, boolean forLocalDelivery)
    {
        return isDigestQuery()
               ? ReadResponse.createDigestResponse(partitions, this)
               : ReadResponse.createDataResponse(partitions, this, forLocalDelivery);
    }

    long indexSerializedSize()
    {
        return null != index
               ? IndexMetadata.serializer.serializedSize(index)
               : 0;
    }

    public Index getIndex()
    {
        return getIndex(Keyspace.openAndGetStore(metadata));
    }

    public Index getIndex(ColumnFamilyStore cfs)
    {
        return null != index
               ? cfs.indexManager.getIndex(index)
               : null;
    }

    static IndexMetadata findIndex(TableMetadata table, RowFilter rowFilter)
    {
        if (table.indexes.isEmpty() || rowFilter.isEmpty())
            return null;

        ColumnFamilyStore cfs = Keyspace.openAndGetStore(table);

        Index index = cfs.indexManager.getBestIndexFor(rowFilter);

        return null != index
               ? index.getIndexMetadata()
               : null;
    }

    /**
     * If the index manager for the CFS determines that there's an applicable
     * 2i that can be used to execute this command, call its (optional)
     * validation method to check that nothing in this command's parameters
     * violates the implementation specific validation rules.
     */
    public void maybeValidateIndex()
    {
        Index index = getIndex(Keyspace.openAndGetStore(metadata));
        if (null != index)
            index.validate(this);
    }

    /**
     * Executes this command on the local host.
     *
     * @return an iterator over the result of executing this command locally.
     */
    // The result iterator is closed upon exceptions (we know it's fine to potentially not close the intermediary
    // iterators created inside the try as long as we do close the original resultIterator), or by closing the result.
    @Override
    public Flow<FlowableUnfilteredPartition> executeLocally(Monitor monitor)
    {
        long startTimeNanos = System.nanoTime();
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(metadata);
        Index index = getIndex(cfs);

        Index.Searcher pickSearcher = null;
        if (index != null)
        {
            if (!cfs.indexManager.isIndexQueryable(index))
                throw new IndexNotAvailableException(index);

            pickSearcher = index.searcherFor(this);
            Tracing.trace("Executing read on {}.{} using index {}", cfs.metadata.keyspace, cfs.metadata.name, index.getIndexMetadata().name);
        }

        Index.Searcher searcher = pickSearcher;
        Flow<FlowableUnfilteredPartition> flow = applyController(
            controller ->
            {
                Flow<FlowableUnfilteredPartition> r = searcher == null
                                                        ? queryStorage(cfs, controller)
                                                        : searcher.search(controller);

                if (monitor != null)
                    r = monitor.withMonitoring(r);

                r = withoutPurgeableTombstones(r, cfs);
                r = withMetricsRecording(r, cfs.metric, startTimeNanos);

                // If we've used a 2ndary index, we know the result already satisfy the primary expression used, so
                // no point in checking it again.
                RowFilter updatedFilter = searcher == null
                                          ? rowFilter()
                                          : index.getPostIndexQueryFilter(rowFilter());

                // TODO: We'll currently do filtering by the rowFilter here because it's convenient. However,
                // we'll probably want to optimize by pushing it down the layer (like for dropped columns) as it
                // would be more efficient (the sooner we discard stuff we know we don't care, the less useless
                // processing we do on it).
                r = updatedFilter.filter(r, cfs.metadata(), nowInSec());
                return limits().truncateUnfiltered(r, nowInSec(), selectsFullPartition(), metadata().enforceStrictLiveness());
            });

        return flow;
    }

    public Flow<FlowableUnfilteredPartition> applyController(Function<ReadExecutionController, Flow<FlowableUnfilteredPartition>> op)
    {
        return Flow.using(() -> ReadExecutionController.forCommand(this),
                          op,
                            controller -> controller.close());
    }

    protected abstract void recordLatency(TableMetrics metric, long latencyNanos);

    public Flow<FlowablePartition> executeInternal(Monitor monitor)
    {
        return FlowablePartitions.filterAndSkipEmpty(executeLocally(monitor), nowInSec());
    }

    public ReadExecutionController executionController()
    {
        return ReadExecutionController.forCommand(this);
    }

    /**
     * Wraps the provided iterator so that metrics on what is scanned by the command are recorded.
     * This also log warning/trow TombstoneOverwhelmingException if appropriate.
     */
    private Flow<FlowableUnfilteredPartition> withMetricsRecording(Flow<FlowableUnfilteredPartition> partitions,
                                                                   final TableMetrics metric,
                                                                   final long startTimeNanos)
    {
        class MetricRecording
        {
            private final int failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
            private final int warningThreshold = DatabaseDescriptor.getTombstoneWarnThreshold();

            private final boolean respectTombstoneThresholds = !SchemaConstants.isSystemKeyspace(ReadCommand.this.metadata().keyspace);
            private final boolean enforceStrictLiveness = metadata.enforceStrictLiveness();

            private int liveRows = 0;
            private int tombstones = 0;

            private DecoratedKey currentKey;

            public FlowableUnfilteredPartition countPartition(FlowableUnfilteredPartition iter)
            {
                currentKey = iter.header.partitionKey;
                countRow(iter.staticRow);

                return new FlowableUnfilteredPartition(iter.header,
                                                       iter.staticRow,
                                                       iter.content.map(this::countUnfiltered));
            }

            public Unfiltered countUnfiltered(Unfiltered unfiltered)
            {
                if (unfiltered.isRow())
                    countRow((Row) unfiltered);
                else
                    countTombstone(unfiltered.clustering());

                return unfiltered;
            }

            public void countRow(Row row)
            {
                Boolean hasLiveCells = row.reduceCells(Boolean.FALSE, (ret, cell) -> {
                    if (!cell.isLive(nowInSec))
                    {
                        countTombstone(row.clustering());
                        return ret;
                    }

                    return Boolean.TRUE; // cell is live
                });

                /**
                 * This duplicates the logic of {@link AbstractRow#hasLiveData(int, boolean)} to avoid
                 * iterating twice on the cells since it is inefficient.
                 */
                if ((hasLiveCells && !enforceStrictLiveness) || row.primaryKeyLivenessInfo().isLive(nowInSec))
                    ++liveRows;
            }

            private void countTombstone(ClusteringPrefix clustering)
            {
                ++tombstones;
                if (tombstones > failureThreshold && respectTombstoneThresholds)
                {
                    String query = ReadCommand.this.toCQLString();
                    Tracing.trace("Scanned over {} tombstones for query {}; query aborted (see tombstone_failure_threshold)", failureThreshold, query);
                    metric.tombstoneFailures.inc();
                    throw new TombstoneOverwhelmingException(tombstones, query, ReadCommand.this.metadata(), currentKey, clustering);
                }
            }

            public void onComplete()
            {
                recordLatency(metric, System.nanoTime() - startTimeNanos);

                metric.tombstoneScannedHistogram.update(tombstones);
                metric.liveScannedHistogram.update(liveRows);

                boolean warnTombstones = tombstones > warningThreshold && respectTombstoneThresholds;
                if (warnTombstones)
                {
                    String msg = String.format("Read %d live rows and %d tombstone cells for query %1.512s (see tombstone_warn_threshold)", liveRows, tombstones, ReadCommand.this.toCQLString());
                    ClientWarn.instance.warn(msg);
                    if (tombstones < failureThreshold)
                    {
                        metric.tombstoneWarnings.inc();
                    }

                    logger.warn(msg);
                }

                Tracing.trace("Read {} live and {} tombstone cells{}", liveRows, tombstones, (warnTombstones ? " (see tombstone_warn_threshold)" : ""));
            }
        };

        final MetricRecording metricsRecording = new MetricRecording();
        partitions = partitions.map(metricsRecording::countPartition);
        partitions = partitions.doOnClose(metricsRecording::onComplete);
        return partitions;
    }

    protected abstract void appendCQLWhereClause(StringBuilder sb);

    static class PurgeOp
    {
        private final DeletionPurger purger;
        private final int nowInSec;
        private final boolean enforceStrictLiveness;

        public PurgeOp(int nowInSec,
                       int gcBefore,
                       Supplier<Integer> oldestUnrepairedTombstone,
                       boolean onlyPurgeRepairedTombstones,
                       boolean enforceStrictLiveness)
        {
            this.nowInSec = nowInSec;
            this.purger = (timestamp, localDeletionTime) ->
                          !(onlyPurgeRepairedTombstones && localDeletionTime >= oldestUnrepairedTombstone.get())
                          && localDeletionTime < gcBefore;
            this.enforceStrictLiveness = enforceStrictLiveness;
        }

        public FlowableUnfilteredPartition purgePartition(FlowableUnfilteredPartition partition)
        {
            PartitionHeader header = partition.header;

            if (purger.shouldPurge(header.partitionLevelDeletion))
                header = header.with(DeletionTime.LIVE);

            FlowableUnfilteredPartition purged = new FlowableUnfilteredPartition(header,
                                                                                 applyToStatic(partition.staticRow),
                                                                                 partition.content.skippingMap(this::purgeUnfiltered));
            return purged;
        }

        public Unfiltered purgeUnfiltered(Unfiltered next)
        {
            return next.purge(purger, nowInSec, enforceStrictLiveness);
        }

        public Row applyToStatic(Row row)
        {
            Row purged = row.purge(purger, nowInSec, enforceStrictLiveness);
            return purged != null ? purged : Rows.EMPTY_STATIC_ROW;
        }
    }


    // Skip purgeable tombstones. We do this because it's safe to do (post-merge of the memtable and sstable at least), it
    // can save us some bandwidth, and avoid making us throw a TombstoneOverwhelmingException for purgeable tombstones (which
    // are to some extend an artifact of compaction lagging behind and hence counting them is somewhat unintuitive).
    protected Flow<FlowableUnfilteredPartition> withoutPurgeableTombstones(Flow<FlowableUnfilteredPartition> iterator, ColumnFamilyStore cfs)
    {
        return iterator.map(new PurgeOp(nowInSec(),
                                        cfs.gcBefore(nowInSec()),
                                        this::oldestUnrepairedTombstone,
                                        cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones(),
                                        cfs.metadata().enforceStrictLiveness())
                            ::purgePartition);
    }

    /**
     * Recreate the CQL string corresponding to this query.
     * <p>
     * Note that in general the returned string will not be exactly the original user string, first
     * because there isn't always a single syntax for a given query,  but also because we don't have
     * all the information needed (we know the non-PK columns queried but not the PK ones as internally
     * we query them all). So this shouldn't be relied too strongly, but this should be good enough for
     * debugging purpose which is what this is for.
     */
    public String toCQLString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(columnFilter());
        sb.append(" FROM ").append(metadata().keyspace).append('.').append(metadata.name);
        appendCQLWhereClause(sb);

        if (limits() != DataLimits.NONE)
            sb.append(' ').append(limits());
        return sb.toString();
    }

    protected static class ReadCommandSerializer<T extends ReadCommand> extends VersionDependent<ReadVersion> implements Serializer<T>
    {
        private final SelectionDeserializer<T> selectionDeserializer;

        protected ReadCommandSerializer(ReadVersion version, SelectionDeserializer<T> selectionDeserializer)
        {
            super(version);
            this.selectionDeserializer = selectionDeserializer;
        }

        private static int digestFlag(boolean isDigest)
        {
            return isDigest ? 0x01 : 0;
        }

        private static boolean isDigest(int flags)
        {
            return (flags & 0x01) != 0;
        }

        // We don't set this flag anymore, but still look if we receive a
        // command with it set in case someone is using thrift a mixed 3.0/4.0+
        // cluster (which is unsupported). This is also a reminder for not
        // re-using this flag until we drop 3.0/3.X compatibility (since it's
        // used by these release for thrift and would thus confuse things)
        private static boolean isForThrift(int flags)
        {
            return (flags & 0x02) != 0;
        }

        private static int indexFlag(boolean hasIndex)
        {
            return hasIndex ? 0x04 : 0;
        }

        private static boolean hasIndex(int flags)
        {
            return (flags & 0x04) != 0;
        }

        private int digestVersionInt(DigestVersion digestVersion)
        {
            return version.compareTo(ReadVersion.DSE_60) < 0
                   ? MessagingVersion.OSS_30.protocolVersion().handshakeVersion
                   : digestVersion.ordinal();
        }

        private DigestVersion fromDigestVersionInt(int digestVersion)
        {
            // Before DSE_60, the version is the messagingVersion to use for the digest. After DSE_60, it's directly the
            // ordinal of the digestVersion.
            if (version.compareTo(ReadVersion.DSE_60) >= 0)
                return DigestVersion.values()[digestVersion];

            MessagingVersion ms = MessagingVersion.fromHandshakeVersion(digestVersion);
            return ms.<ReadVersion>groupVersion(Verbs.Group.READS).digestVersion;
        }

        public void serialize(T command, DataOutputPlus out) throws IOException
        {
            // The "kind" of the command: 0->single partition, 1->partition range. Was never really needed since we
            // can distinguish based on the Verb used and so removed in newer versions.
            if (version.compareTo(ReadVersion.DSE_60) < 0)
                out.writeByte(command instanceof SinglePartitionReadCommand ? 0 : 1);

            out.writeByte(digestFlag(command.isDigestQuery()) | indexFlag(null != command.indexMetadata()));
            if (command.isDigestQuery())
                out.writeUnsignedVInt(digestVersionInt(command.digestVersion()));
            command.metadata().id.serialize(out);
            out.writeInt(command.nowInSec());
            ColumnFilter.serializers.get(version).serialize(command.columnFilter(), out);
            RowFilter.serializers.get(version).serialize(command.rowFilter(), out);
            DataLimits.serializers.get(version).serialize(command.limits(), out, command.metadata().comparator);
            if (null != command.indexMetadata())
                IndexMetadata.serializer.serialize(command.indexMetadata(), out);

            command.serializeSelection(out, version);
        }

        public T deserialize(DataInputPlus in) throws IOException
        {
            if (version.compareTo(ReadVersion.DSE_60) < 0)
                in.readByte(); // See comment in serialize(), we don't need this.

            int flags = in.readByte();
            boolean isDigest = isDigest(flags);
            // Shouldn't happen or it's a user error (see comment above) but
            // better complain loudly than doing the wrong thing.
            if (isForThrift(flags))
                throw new IllegalStateException("Received a command with the thrift flag set. "
                                              + "This means thrift is in use in a mixed 3.0/3.X and 4.0+ cluster, "
                                              + "which is unsupported. Make sure to stop using thrift before "
                                              + "upgrading to 4.0");

            boolean hasIndex = hasIndex(flags);
            DigestVersion digestVersion = isDigest ? fromDigestVersionInt((int)in.readUnsignedVInt()) : null;
            TableMetadata metadata = Schema.instance.getExistingTableMetadata(TableId.deserialize(in));
            int nowInSec = in.readInt();

            ColumnFilter columnFilter = ColumnFilter.serializers.get(version).deserialize(in, metadata);
            RowFilter rowFilter = RowFilter.serializers.get(version).deserialize(in, metadata);
            DataLimits limits = DataLimits.serializers.get(version).deserialize(in, metadata);

            IndexMetadata index = hasIndex ? deserializeIndexMetadata(in, metadata) : null;

            return selectionDeserializer.deserialize(in, version, digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, index);
        }

        private IndexMetadata deserializeIndexMetadata(DataInputPlus in, TableMetadata metadata) throws IOException
        {
            try
            {
                return IndexMetadata.serializer.deserialize(in, metadata);
            }
            catch (UnknownIndexException e)
            {
                logger.info("Couldn't find a defined index on {}.{} with the id {}. " +
                            "If an index was just created, this is likely due to the schema not " +
                            "being fully propagated. Local read will proceed without using the " +
                            "index. Please wait for schema agreement after index creation.",
                            metadata.keyspace, metadata.name, e.indexId);
                return null;
            }
        }

        public long serializedSize(T command)
        {
            return 1 // flags
                   + (version.compareTo(ReadVersion.DSE_60) < 0 ? 1 : 0) // kind for older versions
                   + (command.isDigestQuery() ? TypeSizes.sizeofUnsignedVInt(digestVersionInt(command.digestVersion())) : 0)
                   + command.metadata().id.serializedSize()
                   + TypeSizes.sizeof(command.nowInSec())
                   + ColumnFilter.serializers.get(version).serializedSize(command.columnFilter())
                   + RowFilter.serializers.get(version).serializedSize(command.rowFilter())
                   + DataLimits.serializers.get(version).serializedSize(command.limits(), command.metadata().comparator)
                   + command.selectionSerializedSize(version)
                   + command.indexSerializedSize();
        }
    }
}
