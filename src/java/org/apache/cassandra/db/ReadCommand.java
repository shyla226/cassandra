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
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import io.reactivex.Single;

import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.selection.ResultBuilder;
import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.ProtocolVersion;
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
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * General interface for storage-engine read commands (common to both range and
 * single partition commands).
 * <p>
 * This contains all the information needed to do a local read.
 */
public abstract class ReadCommand implements ReadQuery
{
    protected static final Logger logger = LoggerFactory.getLogger(ReadCommand.class);

    public static final Versioned<ReadVersion, Serializer<ReadCommand>> serializers = ReadVersion.versioned(ReadCommandSerializer::new);

    private final Kind kind;
    private final TableMetadata metadata;
    private final int nowInSec;

    private final ColumnFilter columnFilter;
    private final RowFilter rowFilter;
    private final DataLimits limits;

    // SecondaryIndexManager will attempt to provide the most selective of any available indexes
    // during execution. Here we also store an the results of that lookup to repeating it over
    // the lifetime of the command.
    protected Optional<IndexMetadata> index = Optional.empty();

    // Flag to indicate whether the index manager has been queried to select an index for this
    // command. This is necessary as the result of that lookup may be null, in which case we
    // still don't want to repeat it.
    private boolean indexManagerQueried = false;

    /** The version of the digest this must generate if it is a digest query, {@code null} if it isn't a digest query */
    @Nullable
    private final DigestVersion digestVersion;

    protected static abstract class SelectionDeserializer
    {
        public abstract ReadCommand deserialize(DataInputPlus in, ReadVersion version, DigestVersion digestVersion, TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, Optional<IndexMetadata> index) throws IOException;
    }

    protected enum Kind
    {
        SINGLE_PARTITION (SinglePartitionReadCommand.selectionDeserializer),
        PARTITION_RANGE  (PartitionRangeReadCommand.selectionDeserializer);

        private final SelectionDeserializer selectionDeserializer;

        Kind(SelectionDeserializer selectionDeserializer)
        {
            this.selectionDeserializer = selectionDeserializer;
        }
    }

    protected ReadCommand(Kind kind,
                          DigestVersion digestVersion,
                          TableMetadata metadata,
                          int nowInSec,
                          ColumnFilter columnFilter,
                          RowFilter rowFilter,
                          DataLimits limits)
    {
        this.kind = kind;
        this.digestVersion = digestVersion;
        this.metadata = metadata;
        this.nowInSec = nowInSec;
        this.columnFilter = columnFilter;
        this.rowFilter = rowFilter;
        this.limits = limits;
    }

    protected abstract void serializeSelection(DataOutputPlus out, ReadVersion version) throws IOException;
    protected abstract long selectionSerializedSize(ReadVersion version);

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

    protected abstract Flowable<FlowableUnfilteredPartition> queryStorage(ColumnFamilyStore cfs, ReadExecutionController executionController);

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
     * Digest responses calculate the digest in the construstor and close the iterator immediately,
     * whilst data responses may keep it open until the iterator is closed by the final handler, e.g.
     * {@link org.apache.cassandra.cql3.statements.SelectStatement#processPartition(RowIterator, QueryOptions, ResultBuilder, int)}
     * @param iterator - the iterator containing the results, the response will take ownership
     *
     * @return An appropriate response, either of type digest or data.
     */
    public ReadResponse createResponse(UnfilteredPartitionIterator iterator)
    {
        return isDigestQuery()
             ? ReadResponse.createDigestResponse(iterator, this)
             : ReadResponse.createDataResponse(iterator, this);
    }

    public long indexSerializedSize()
    {
        if (index.isPresent())
            return IndexMetadata.serializer.serializedSize(index.get());
        else
            return 0;
    }

    public Index getIndex()
    {
        return getIndex(Keyspace.openAndGetStore(metadata));
    }

    public Index getIndex(ColumnFamilyStore cfs)
    {
        // if we've already consulted the index manager, and it returned a valid index
        // the result should be cached here.
        if(index.isPresent())
            return cfs.indexManager.getIndex(index.get());

        // if no cached index is present, but we've already consulted the index manager
        // then no registered index is suitable for this command, so just return null.
        if (indexManagerQueried)
            return null;

        // do the lookup, set the flag to indicate so and cache the result if not null
        Index selected = cfs.indexManager.getBestIndexFor(this);
        indexManagerQueried = true;

        if (selected == null)
            return null;

        index = Optional.of(selected.getIndexMetadata());
        return selected;
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
    @SuppressWarnings("resource")
    @Override
    public Single<UnfilteredPartitionIterator> executeLocally(Monitor monitor)
    {
        Single s = Single.defer(
        () ->
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
            ReadExecutionController controller = ReadExecutionController.forCommand(this);
            try
            {
                Single<UnfilteredPartitionIterator> resultIterator = searcher == null
                                                                   ? Single.just(FlowablePartitions.toPartitions(queryStorage(cfs, controller), cfs.metadata()))
                                                                   : searcher.search(controller);

                return resultIterator.map(r ->
                                          {
                                              if (monitor != null)
                                                  r = monitor.withMonitoring(r);

                                              r = withMetricsRecording(withoutPurgeableTombstones(r, cfs), cfs.metric, startTimeNanos);

                                              // If we've used a 2ndary index, we know the result already satisfy the primary expression used, so
                                              // no point in checking it again.
                                              RowFilter updatedFilter = searcher == null
                                                                        ? rowFilter()
                                                                        : index.getPostIndexQueryFilter(rowFilter());

                                              // TODO: We'll currently do filtering by the rowFilter here because it's convenient. However,
                                              // we'll probably want to optimize by pushing it down the layer (like for dropped columns) as it
                                              // would be more efficient (the sooner we discard stuff we know we don't care, the less useless
                                              // processing we do on it).
                                              final UnfilteredPartitionIterator res = limits().filter(updatedFilter.filter(r, nowInSec()), nowInSec());

                                              // TODO - to be removed after final implementation with MergeFlowable is integrated.
                                              // Closing the execution controller is too important for it to be buried in a transformation,
                                              // so I've temporarily wrapped the iterator - I understand that in the final flowable implementation
                                              // we'll convert a Flowable<Unfiltered> to an iterator or use the Flowable directly, so the idea
                                              // is to close the controller when the subscription to the flowable is cancelled, i.e. when the
                                              // iterator is closed.
                                              // I have checked that all the callers of executeLocally() indeed close the iterator.
                                              return new UnfilteredPartitionIterator()
                                              {

                                                  public boolean hasNext()
                                                  {
                                                      return res.hasNext();
                                                  }

                                                  public UnfilteredRowIterator next()
                                                  {
                                                      return res.next();
                                                  }

                                                  public TableMetadata metadata()
                                                  {
                                                      return res.metadata();
                                                  }

                                                  public void close()
                                                  {
                                                      controller.close();
                                                      res.close();
                                                  }
                                              };
                                          });
            }
            catch (Throwable t)
            {
                controller.close(); // idempotent
                return Single.error(t);
            }
        });


        if (this instanceof SinglePartitionReadCommand)
            s = s.subscribeOn(NettyRxScheduler.getForKey(metadata.keyspace, ((SinglePartitionReadCommand)this).partitionKey(), false));

        return s;
    }

    protected abstract void recordLatency(TableMetrics metric, long latencyNanos);

    public Single<PartitionIterator> executeInternal(Monitor monitor)
    {
        return executeLocally(monitor).map(p -> UnfilteredPartitionIterators.filter(p, nowInSec()));
    }

    public ReadExecutionController executionController()
    {
        return ReadExecutionController.forCommand(this);
    }

    /**
     * Wraps the provided iterator so that metrics on what is scanned by the command are recorded.
     * This also log warning/trow TombstoneOverwhelmingException if appropriate.
     */
    private UnfilteredPartitionIterator withMetricsRecording(UnfilteredPartitionIterator iter, final TableMetrics metric, final long startTimeNanos)
    {
        class MetricRecording extends Transformation<UnfilteredRowIterator>
        {
            private final int failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
            private final int warningThreshold = DatabaseDescriptor.getTombstoneWarnThreshold();

            private final boolean respectTombstoneThresholds = !SchemaConstants.isSystemKeyspace(ReadCommand.this.metadata().keyspace);

            private int liveRows = 0;
            private int tombstones = 0;

            private DecoratedKey currentKey;

            @Override
            public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator iter)
            {
                currentKey = iter.partitionKey();
                return Transformation.apply(iter, this);
            }

            @Override
            public Row applyToStatic(Row row)
            {
                return applyToRow(row);
            }

            @Override
            public Row applyToRow(Row row)
            {
                boolean hasLiveCells = false;
                for (Cell cell : row.cells())
                {
                    if (!cell.isLive(ReadCommand.this.nowInSec()))
                        countTombstone(row.clustering());
                    else if (!hasLiveCells)
                        hasLiveCells = true;
                }

                if (hasLiveCells || row.primaryKeyLivenessInfo().isLive(nowInSec))
                    ++ liveRows;

                return row;
            }

            @Override
            public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
            {
                countTombstone(marker.clustering());
                return marker;
            }

            private void countTombstone(ClusteringPrefix clustering)
            {
                ++tombstones;
                if (tombstones > failureThreshold && respectTombstoneThresholds)
                {
                    String query = ReadCommand.this.toCQLString();
                    Tracing.trace("Scanned over {} tombstones for query {}; query aborted (see tombstone_failure_threshold)", failureThreshold, query);
                    throw new TombstoneOverwhelmingException(tombstones, query, ReadCommand.this.metadata(), currentKey, clustering);
                }
            }

            @Override
            public void onClose()
            {
                recordLatency(metric, System.nanoTime() - startTimeNanos);

                metric.tombstoneScannedHistogram.update(tombstones);
                metric.liveScannedHistogram.update(liveRows);

                boolean warnTombstones = tombstones > warningThreshold && respectTombstoneThresholds;
                if (warnTombstones)
                {
                    String msg = String.format("Read %d live rows and %d tombstone cells for query %1.512s (see tombstone_warn_threshold)", liveRows, tombstones, ReadCommand.this.toCQLString());
                    ClientWarn.instance.warn(msg);
                    logger.warn(msg);
                }

                Tracing.trace("Read {} live and {} tombstone cells{}", liveRows, tombstones, (warnTombstones ? " (see tombstone_warn_threshold)" : ""));
            }
        };

        return Transformation.apply(iter, new MetricRecording());
    }

    protected abstract void appendCQLWhereClause(StringBuilder sb);

    // Skip purgeable tombstones. We do this because it's safe to do (post-merge of the memtable and sstable at least), it
    // can save us some bandwith, and avoid making us throw a TombstoneOverwhelmingException for purgeable tombstones (which
    // are to some extend an artefact of compaction lagging behind and hence counting them is somewhat unintuitive).
    protected UnfilteredPartitionIterator withoutPurgeableTombstones(UnfilteredPartitionIterator iterator, ColumnFamilyStore cfs)
    {
        class WithoutPurgeableTombstones extends PurgeFunction
        {
            public WithoutPurgeableTombstones()
            {
                super(nowInSec(), cfs.gcBefore(nowInSec()), oldestUnrepairedTombstone(), cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones());
            }

            protected Predicate<Long> getPurgeEvaluator()
            {
                return time -> true;
            }
        }
        return Transformation.apply(iterator, new WithoutPurgeableTombstones());
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

    private static class ReadCommandSerializer extends VersionDependent<ReadVersion> implements Serializer<ReadCommand>
    {
        private ReadCommandSerializer(ReadVersion version)
        {
            super(version);
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

        public void serialize(ReadCommand command, DataOutputPlus out) throws IOException
        {
            out.writeByte(command.kind.ordinal());
            out.writeByte(digestFlag(command.isDigestQuery()) | indexFlag(command.index.isPresent()));
            if (command.isDigestQuery())
                out.writeUnsignedVInt(digestVersionInt(command.digestVersion()));
            command.metadata.id.serialize(out);
            out.writeInt(command.nowInSec());
            ColumnFilter.serializers.get(version).serialize(command.columnFilter(), out);
            RowFilter.serializers.get(version).serialize(command.rowFilter(), out);
            DataLimits.serializers.get(version).serialize(command.limits(), out, command.metadata.comparator);
            if (command.index.isPresent())
                IndexMetadata.serializer.serialize(command.index.get(), out);

            command.serializeSelection(out, version);
        }

        public ReadCommand deserialize(DataInputPlus in) throws IOException
        {
            Kind kind = Kind.values()[in.readByte()];
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
            DataLimits limits = DataLimits.serializers.get(version).deserialize(in, metadata.comparator);
            Optional<IndexMetadata> index = hasIndex
                                          ? deserializeIndexMetadata(in, metadata)
                                          : Optional.empty();

            return kind.selectionDeserializer.deserialize(in, version, digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, index);
        }

        private Optional<IndexMetadata> deserializeIndexMetadata(DataInputPlus in, TableMetadata metadata) throws IOException
        {
            try
            {
                return Optional.of(IndexMetadata.serializer.deserialize(in, metadata));
            }
            catch (UnknownIndexException e)
            {
                logger.info("Couldn't find a defined index on {}.{} with the id {}. " +
                            "If an index was just created, this is likely due to the schema not " +
                            "being fully propagated. Local read will proceed without using the " +
                            "index. Please wait for schema agreement after index creation.",
                            metadata.keyspace, metadata.name, e.indexId);
                return Optional.empty();
            }
        }

        public long serializedSize(ReadCommand command)
        {
            return 2 // kind + flags
                   + (command.isDigestQuery() ? TypeSizes.sizeofUnsignedVInt(digestVersionInt(command.digestVersion())) : 0)
                   + command.metadata.id.serializedSize()
                   + TypeSizes.sizeof(command.nowInSec())
                   + ColumnFilter.serializers.get(version).serializedSize(command.columnFilter())
                   + RowFilter.serializers.get(version).serializedSize(command.rowFilter())
                   + DataLimits.serializers.get(version).serializedSize(command.limits(), command.metadata.comparator)
                   + command.selectionSerializedSize(version)
                   + command.indexSerializedSize();
        }
    }
}
