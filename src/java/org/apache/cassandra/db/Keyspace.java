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

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import io.reactivex.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCBoundaries;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.view.ViewManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.UnknownKeyspaceException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * It represents a Keyspace.
 */
public class Keyspace
{
    private static final Logger logger = LoggerFactory.getLogger(Keyspace.class);

    private static final String TEST_FAIL_WRITES_KS = System.getProperty("cassandra.test.fail_writes_ks", "");
    private static final boolean TEST_FAIL_WRITES = !TEST_FAIL_WRITES_KS.isEmpty();

    public final KeyspaceMetrics metric;

    // It is possible to call Keyspace.open without a running daemon, so it makes sense to ensure
    // proper directories here as well as in CassandraDaemon.
    static
    {
        if (DatabaseDescriptor.isDaemonInitialized() || DatabaseDescriptor.isToolInitialized())
            DatabaseDescriptor.createAllDirectories();
    }

    private volatile KeyspaceMetadata metadata;

    //OpOrder is defined globally since we need to order writes across
    //Keyspaces in the case of Views (batchlog of view mutations)
    public static final OpOrder writeOrder = TPC.newOpOrder(Keyspace.class);

    // this is set during draining and it indicates that no more mutations should be accepted
    private volatile OpOrder.Barrier writeBarrier = null;

    /* ColumnFamilyStore per column family */
    private final ConcurrentMap<TableId, ColumnFamilyStore> columnFamilyStores = new ConcurrentHashMap<>();
    private volatile AbstractReplicationStrategy replicationStrategy;
    public final ViewManager viewManager;

    private volatile TPCBoundaries tpcBoundaries;
    private long boundariesForRingVersion = -1;

    private static volatile boolean initialized = false;

    public static void setInitialized()
    {
        initialized = true;
    }

    @VisibleForTesting
    public static boolean isInitialized()
    {
        return initialized;
    }

    public static Keyspace open(String keyspaceName)
    {
        if (initialized || SchemaConstants.isSystemKeyspace(keyspaceName))
            return open(keyspaceName, Schema.instance, true);

        throw new IllegalStateException(String.format("Cannot open non-system keyspace %s as server is not yet initialized.", keyspaceName));
    }

    // to only be used by org.apache.cassandra.tools.Standalone* classes
    public static Keyspace openWithoutSSTables(String keyspaceName)
    {
        return open(keyspaceName, Schema.instance, false);
    }

    private static Keyspace open(String keyspaceName, Schema schema, boolean loadSSTables)
    {
        Keyspace keyspaceInstance = schema.getKeyspaceInstance(keyspaceName);

        if (keyspaceInstance == null)
        {
            // instantiate the Keyspace.  we could use putIfAbsent but it's important to making sure it is only done once
            // per keyspace, so we synchronize and re-check before doing it.
            synchronized (Keyspace.class)
            {
                keyspaceInstance = schema.getKeyspaceInstance(keyspaceName);
                if (keyspaceInstance == null)
                {
                    // open and store the keyspace
                    keyspaceInstance = new Keyspace(keyspaceName, loadSSTables);
                    schema.storeKeyspaceInstance(keyspaceInstance);
                }
            }
        }
        return keyspaceInstance;
    }

    public static Keyspace clear(String keyspaceName)
    {
        return clear(keyspaceName, Schema.instance);
    }

    public static Keyspace clear(String keyspaceName, Schema schema)
    {
        synchronized (Keyspace.class)
        {
            Keyspace t = schema.removeKeyspaceInstance(keyspaceName);
            if (t != null)
            {
                for (ColumnFamilyStore cfs : t.getColumnFamilyStores())
                    t.unloadCf(cfs);
                t.metric.release();
            }
            return t;
        }
    }

    public static ColumnFamilyStore openAndGetStore(TableMetadataRef tableRef)
    {
        return open(tableRef.keyspace).getColumnFamilyStore(tableRef.id);
    }

    public static ColumnFamilyStore openAndGetStore(TableMetadata table)
    {
        return open(table.keyspace).getColumnFamilyStore(table.id);
    }

    /**
     * Removes every SSTable in the directory from the appropriate Tracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    public static void removeUnreadableSSTables(File directory)
    {
        for (Keyspace keyspace : Keyspace.all())
        {
            for (ColumnFamilyStore baseCfs : keyspace.getColumnFamilyStores())
            {
                for (ColumnFamilyStore cfs : baseCfs.concatWithIndexes())
                    cfs.maybeRemoveUnreadableSSTables(directory);
            }
        }
    }

    public void setMetadata(KeyspaceMetadata metadata)
    {
        this.metadata = metadata;
        createReplicationStrategy(metadata);
    }

    public KeyspaceMetadata getMetadata()
    {
        return metadata;
    }

    public Collection<ColumnFamilyStore> getColumnFamilyStores()
    {
        return Collections.unmodifiableCollection(columnFamilyStores.values());
    }

    public ColumnFamilyStore getColumnFamilyStore(String cfName)
    {
        TableMetadata table = Schema.instance.getTableMetadata(getName(), cfName);
        if (table == null)
            throw new IllegalArgumentException(String.format("Unknown keyspace/cf pair (%s.%s)", getName(), cfName));
        return getColumnFamilyStore(table.id);
    }

    public ColumnFamilyStore getColumnFamilyStore(TableId id)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(id);
        if (cfs == null)
            throw new UnknownTableException("Cannot find table, it may have been dropped", id);
        return cfs;
    }

    public boolean hasColumnFamilyStore(TableId id)
    {
        return columnFamilyStores.containsKey(id);
    }

    /**
     * Take a snapshot of the specific column family, or the entire set of column families
     * if columnFamily is null with a given timestamp
     *
     * @param snapshotName     the tag associated with the name of the snapshot.  This value may not be null
     * @param columnFamilyName the column family to snapshot or all on null
     * @param skipFlush Skip blocking flush of memtable
     * @param alreadySnapshotted the set of sstables that have already been snapshotted (to avoid duplicate hardlinks in
     *                           some edge cases)
     * @throws IOException if the column family doesn't exist
     */
    public Set<SSTableReader> snapshot(String snapshotName, String columnFamilyName, boolean skipFlush, Set<SSTableReader> alreadySnapshotted) throws IOException
    {
        assert snapshotName != null;
        assert alreadySnapshotted != null;

        boolean tookSnapShot = false;
        Set<SSTableReader> snapshotSSTables = new HashSet<>();
        // copy so we can update after each CF snapshot without modifying the original
        alreadySnapshotted = new HashSet<>(alreadySnapshotted);
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
        {
            if (columnFamilyName == null || cfStore.name.equals(columnFamilyName))
            {
                tookSnapShot = true;
                Set<SSTableReader> newSnapshots = cfStore.snapshot(snapshotName, null, false, skipFlush, alreadySnapshotted);
                snapshotSSTables.addAll(newSnapshots);
                alreadySnapshotted.addAll(newSnapshots);
            }
        }

        if ((columnFamilyName != null) && !tookSnapShot)
            throw new IOException("Failed taking snapshot. Table " + columnFamilyName + " does not exist.");

        return snapshotSSTables;
    }

    /**
     * Take a snapshot of the specific column family, or the entire set of column families
     * if columnFamily is null with a given timestamp
     *
     * @param snapshotName     the tag associated with the name of the snapshot.  This value may not be null
     * @param columnFamilyName the column family to snapshot or all on null
     * @throws IOException if the column family doesn't exist
     */
    public void snapshot(String snapshotName, String columnFamilyName) throws IOException
    {
        snapshot(snapshotName, columnFamilyName, false, new HashSet<>());
    }

    /**
     * @param clientSuppliedName may be null.
     * @return the name of the snapshot
     */
    public static String getTimestampedSnapshotName(String clientSuppliedName)
    {
        String snapshotName = Long.toString(System.currentTimeMillis());
        if (clientSuppliedName != null && !clientSuppliedName.equals(""))
        {
            snapshotName = snapshotName + "-" + clientSuppliedName;
        }
        return snapshotName;
    }

    public static String getTimestampedSnapshotNameWithPrefix(String clientSuppliedName, String prefix)
    {
        return prefix + "-" + getTimestampedSnapshotName(clientSuppliedName);
    }

    /**
     * Check whether snapshots already exists for a given name.
     *
     * @param snapshotName the user supplied snapshot name
     * @return true if the snapshot exists
     */
    public boolean snapshotExists(String snapshotName)
    {
        assert snapshotName != null;
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
        {
            if (cfStore.snapshotExists(snapshotName))
                return true;
        }
        return false;
    }

    /**
     * Clear all the snapshots for a given keyspace.
     *
     * @param snapshotName the user supplied snapshot name. It empty or null,
     *                     all the snapshots will be cleaned
     */
    public static void clearSnapshot(String snapshotName, String keyspace)
    {
        List<File> snapshotDirs = Directories.getKSChildDirectories(keyspace, ColumnFamilyStore.getInitialDirectories());
        Directories.clearSnapshot(snapshotName, snapshotDirs);
    }

    /**
     * @return A list of open SSTableReaders
     */
    public List<SSTableReader> getAllSSTables(SSTableSet sstableSet)
    {
        List<SSTableReader> list = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
            Iterables.addAll(list, cfStore.getSSTables(sstableSet));
        return list;
    }

    private Keyspace(String keyspaceName, boolean loadSSTables)
    {
        metadata = Schema.instance.getKeyspaceMetadata(keyspaceName);
        // with the current synchronization mechanism, the ks metadata may disappear whilst
        // opening a keyspace. I think keyspace creation (and deletion) should be synchronized
        // differently, i.e. using the same lock for modifying the schema (SystemKeyspace.class)
        // in 5.1.
        if (metadata == null)
            throw new UnknownKeyspaceException(keyspaceName);

        createReplicationStrategy(metadata);

        this.metric = new KeyspaceMetrics(this);
        this.viewManager = new ViewManager(this);
        for (TableMetadata cfm : metadata.tablesAndViews())
        {
            if (cfm == null) // unsure how this can happen but it did (APOLLO-395)
                throw new IllegalStateException("Unexpected null metadata for keyspace " + keyspaceName);

            logger.trace("Initializing {}.{}", getName(), cfm.name);
            initCf(Schema.instance.getTableMetadataRef(cfm.id), loadSSTables);
        }
        this.viewManager.reload();
    }

    // Only used for mocking Keyspace
    private Keyspace(KeyspaceMetadata metadata)
    {
        this.metadata = metadata;
        createReplicationStrategy(metadata);
        this.metric = new KeyspaceMetrics(this);
        this.viewManager = new ViewManager(this);
    }

    public static Keyspace mockKS(KeyspaceMetadata metadata)
    {
        return new Keyspace(metadata);
    }

    public TPCBoundaries getTPCBoundaries()
    {
        TPCBoundaries boundaries = tpcBoundaries;
        if (boundaries == null || boundariesForRingVersion < StorageService.instance.getTokenMetadata().getRingVersion())
        {
            if (!StorageService.instance.isInitialized())
                return TPCBoundaries.NONE;

            synchronized (this)
            {
                boundaries = tpcBoundaries;
                if (boundaries == null || boundariesForRingVersion < StorageService.instance.getTokenMetadata().getRingVersion())
                {
                    boundariesForRingVersion = StorageService.instance.getTokenMetadata().getRingVersion();
                    tpcBoundaries = boundaries = computeTPCBoundaries();
                    logger.debug("Computed TPC core assignments for {}: {}", getName(), boundaries);
                }
            }
        }
        return boundaries;
    }

    private TPCBoundaries computeTPCBoundaries()
    {
        if (SchemaConstants.isSystemKeyspace(metadata.name))
            return TPCBoundaries.NONE;

        List<Range<Token>> localRanges = StorageService.getStartupTokenRanges(this);
        return localRanges == null ? TPCBoundaries.NONE : TPCBoundaries.compute(localRanges, TPC.getNumCores());
    }

    private void createReplicationStrategy(KeyspaceMetadata ksm)
    {
        replicationStrategy = AbstractReplicationStrategy.createReplicationStrategy(ksm.name,
                                                                                    ksm.params.replication.klass,
                                                                                    StorageService.instance.getTokenMetadata(),
                                                                                    DatabaseDescriptor.getEndpointSnitch(),
                                                                                    ksm.params.replication.options);
    }

    // best invoked on the compaction mananger.
    public void dropCf(TableId tableId)
    {
        assert columnFamilyStores.containsKey(tableId);
        ColumnFamilyStore cfs = columnFamilyStores.remove(tableId);
        if (cfs == null)
            return;

        cfs.getCompactionStrategyManager().shutdown();
        CompactionManager.instance.interruptCompactionForCFs(cfs.concatWithIndexes());
        // wait for any outstanding reads/writes that might affect the CFS
        cfs.keyspace.writeOrder.awaitNewBarrier();
        cfs.readOrdering.awaitNewBarrier();

        unloadCf(cfs);
    }

    // disassociate a cfs from this keyspace instance.
    private void unloadCf(ColumnFamilyStore cfs)
    {
        cfs.forceBlockingFlush();
        cfs.invalidate();
    }

    /**
     * Registers a custom cf instance with this keyspace.
     * This is required for offline tools what use non-standard directories.
     */
    public void initCfCustom(ColumnFamilyStore newCfs)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(newCfs.metadata.id);

        if (cfs == null)
        {
            // CFS being created for the first time, either on server startup or new CF being added.
            // We don't worry about races here; startup is safe, and adding multiple idential CFs
            // simultaneously is a "don't do that" scenario.
            ColumnFamilyStore oldCfs = columnFamilyStores.putIfAbsent(newCfs.metadata.id, newCfs);
            // CFS mbean instantiation will error out before we hit this, but in case that changes...
            if (oldCfs != null)
                throw new IllegalStateException("added multiple mappings for cf id " + newCfs.metadata.id);
        }
        else
        {
            throw new IllegalStateException("CFS is already initialized: " + cfs.name);
        }
    }

    /**
     * adds a cf to internal structures, ends up creating disk files).
     */
    public void initCf(TableMetadataRef metadata, boolean loadSSTables)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(metadata.id);

        if (cfs == null)
        {
            // CFS being created for the first time, either on server startup or new CF being added.
            // We don't worry about races here; startup is safe, and adding multiple idential CFs
            // simultaneously is a "don't do that" scenario.
            ColumnFamilyStore oldCfs = columnFamilyStores.putIfAbsent(metadata.id, ColumnFamilyStore.createColumnFamilyStore(this, metadata, loadSSTables));
            // CFS mbean instantiation will error out before we hit this, but in case that changes...
            if (oldCfs != null)
                throw new IllegalStateException("added multiple mappings for cf id " + metadata.id);
        }
        else
        {
            // re-initializing an existing CF.  This will happen if you cleared the schema
            // on this node and it's getting repopulated from the rest of the cluster.
            assert cfs.name.equals(metadata.name);
            cfs.reload();
        }
    }

    /**
     * Close this keyspace to further mutations, called when draining or shutting down.
     *
     * A final write barrier is issued and returned. After this barrier is set, new mutations
     * will be rejected, see {@link Keyspace#apply(Mutation, boolean, boolean, boolean)}.
     */
    public OpOrder.Barrier stopMutations()
    {
        assert writeBarrier == null : "Keyspace has already been closed to mutations";
        writeBarrier = writeOrder.newBarrier();
        writeBarrier.issue();
        return writeBarrier;
    }

    public Completable apply(final Mutation mutation, final boolean writeCommitLog)
    {
        return apply(mutation, writeCommitLog, true, true);
    }

    /**
     * Applies the provided mutation.
     *
     * @param mutation       the row to write.  Must not be modified after calling apply, since commitlog append
     *                       may happen concurrently, depending on the CL Executor type.
     * @param writeCommitLog false to disable commitlog append entirely
     * @param updateIndexes  false to disable index updates (used by CollationController "defragmenting")
     * @param isDroppable    true if this should throw WriteTimeoutException if it does not acquire lock within write_request_timeout_in_ms
     * @throws ExecutionException
     */
    public Completable apply(final Mutation mutation, final boolean writeCommitLog, boolean updateIndexes, boolean isDroppable)
    {
        if (TEST_FAIL_WRITES && metadata.name.equals(TEST_FAIL_WRITES_KS))
            return Completable.error(new InternalRequestExecutionException(RequestFailureReason.UNKNOWN, "Testing write failures"));

        if (writeBarrier != null)
            return failDueToWriteBarrier(mutation);

        final boolean requiresViewUpdate = updateIndexes && viewManager.updatesAffectView(Collections.singleton(mutation), false);
        return requiresViewUpdate ? applyWithViews(mutation, writeCommitLog, updateIndexes, isDroppable)
                                  : applyNoViews(mutation, writeCommitLog, updateIndexes);

    }

    private Completable applyWithViews(Mutation mutation, boolean writeCommitLog, boolean updateIndexes, boolean isDroppable)
    {
        Supplier<CompletableFuture<Void>> update = () -> TPCUtils.toFuture(Completable.using(writeOrder::start,
                                                                                             opGroup -> applyInternal(opGroup, mutation, writeCommitLog, updateIndexes, true),
                                                                                             opGroup -> opGroup.close()));

        return TPCUtils.toCompletable(viewManager.updateWithLocks(mutation, update, isDroppable));
    }

    private Completable applyNoViews(Mutation mutation, boolean writeCommitLog, boolean updateIndexes)
    {
        return Completable.using(writeOrder::start,
                                 opGroup -> applyInternal(opGroup, mutation, writeCommitLog, updateIndexes, false),
                                 OpOrder.Group::close);
    }

    private Completable applyInternal(OpOrder.Group opGroup, Mutation mutation, boolean writeCommitLog, boolean updateIndexes, boolean requiresViewUpdate)
    {
        if (writeBarrier != null && !writeBarrier.isAfter(opGroup))
            return failDueToWriteBarrier(mutation);

        if (!writeCommitLog)
            return postCommitLogApply(opGroup, mutation, CommitLogPosition.NONE, updateIndexes, requiresViewUpdate);

        return CommitLog.instance.add(mutation)
                                 .flatMapCompletable(position -> postCommitLogApply(opGroup, mutation, position, updateIndexes, requiresViewUpdate));
    }

    /**
     * Apply a mutation after it has been added to the commit log.
     *
     * @param opGroup the {@link OpOrder.Group} protecting the application. That group must be close once this method
     *                return in <b>all</b> cases, but doing so is the responsibility of the caller/creator of the group.
     * @param mutation the mutation to apply.
     * @param commitLogPosition the position from the commit log addition. This can be {@link CommitLogPosition#NONE} if
     *                          we are not writing to the commit log for this mutation.
     * @param updateIndexes {@code false} to disable index updates.
     * @param requiresViewUpdate whether the mutation has materialized view associated to it that should be applied.
     * @return
     */
    private Completable postCommitLogApply(OpOrder.Group opGroup, Mutation mutation, CommitLogPosition commitLogPosition, boolean updateIndexes, boolean requiresViewUpdate)
    {
        if (logger.isTraceEnabled())
            logger.trace("Got CL position {} for mutation {} (view updates: {})",
                         commitLogPosition, mutation, requiresViewUpdate);

        List<Completable> memtablePutCompletables = new ArrayList<>(mutation.getPartitionUpdates().size());

        for (PartitionUpdate upd : mutation.getPartitionUpdates())
        {
            ColumnFamilyStore cfs = columnFamilyStores.get(upd.metadata().id);
            if (cfs == null)
            {
                logger.error("Attempting to mutate non-existant table {} ({}.{})", upd.metadata().id, upd.metadata().keyspace, upd.metadata().name);
                continue;
            }

            // TODO this probably doesn't need to be atomic after TPC
            AtomicLong baseComplete = new AtomicLong(Long.MAX_VALUE);

            Completable viewUpdateCompletable = null;
            if (requiresViewUpdate)
            {
                Tracing.trace("Creating materialized view mutations from base table replica");

                viewUpdateCompletable = viewManager.forTable(upd.metadata().id)
                                                   .pushViewReplicaUpdates(upd, commitLogPosition != CommitLogPosition.NONE, baseComplete)
                                                   .doOnError(exc ->
                                                              {
                                                                  JVMStabilityInspector.inspectThrowable(exc);
                                                                  logger.error(String.format("Unknown exception caught while attempting to update MaterializedView! %s.%s",
                                                                                             upd.metadata().keyspace, upd.metadata().name), exc);
                                                              });
            }

            Tracing.trace("Adding to {} memtable", upd.metadata().name);
            UpdateTransaction indexTransaction = updateIndexes
                                                 ? cfs.indexManager.newUpdateTransaction(upd, opGroup, FBUtilities.nowInSeconds())
                                                 : UpdateTransaction.NO_OP;

            CommitLogPosition pos = commitLogPosition == CommitLogPosition.NONE ? null : commitLogPosition;
            Completable memtableCompletable = cfs.apply(upd, indexTransaction, opGroup, pos);
            if (requiresViewUpdate)
            {
                memtableCompletable = memtableCompletable.doOnComplete(() -> baseComplete.set(System.currentTimeMillis()));
                memtablePutCompletables.add(viewUpdateCompletable);
            }
            memtablePutCompletables.add(memtableCompletable);
        }

        // avoid the expensive concat call if there's only 1 completable
        if (memtablePutCompletables.size() == 1)
            return memtablePutCompletables.get(0);
        else
            return Completable.concat(memtablePutCompletables);
    }

    private Completable failDueToWriteBarrier(Mutation mutation)
    {
        assert writeBarrier != null : "Expected non null write barrier";

        if (SchemaConstants.isSystemKeyspace(mutation.getKeyspaceName()))
        {
            logger.warn("Attempted to apply system mutation {} during shutdown but keyspace was already closed to mutations",
                         mutation);
            return Completable.complete();
        }

        logger.debug(FBUtilities.Debug.getStackTrace());
        logger.error("Attempted to apply user mutation {} during shutdown but keyspace was already closed to mutations",
                    mutation);
        return Completable.error(new InternalRequestExecutionException(RequestFailureReason.UNKNOWN, "Keyspace closed to new mutations"));
    }

    public AbstractReplicationStrategy getReplicationStrategy()
    {
        return replicationStrategy;
    }

    public List<CompletableFuture<CommitLogPosition>> flush()
    {
        List<CompletableFuture<CommitLogPosition>> futures = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfs : columnFamilyStores.values())
            futures.add(cfs.forceFlush());
        return futures;
    }

    public Iterable<ColumnFamilyStore> getValidColumnFamilies(boolean allowIndexes,
                                                              boolean autoAddIndexes,
                                                              String... cfNames) throws IOException
    {
        Set<ColumnFamilyStore> valid = new HashSet<>();

        if (cfNames.length == 0)
        {
            // all stores are interesting
            for (ColumnFamilyStore cfStore : getColumnFamilyStores())
            {
                valid.add(cfStore);
                if (autoAddIndexes)
                    valid.addAll(getIndexColumnFamilyStores(cfStore));
            }
            return valid;
        }

        // include the specified stores and possibly the stores of any of their indexes
        for (String cfName : cfNames)
        {
            if (SecondaryIndexManager.isIndexColumnFamily(cfName))
            {
                if (!allowIndexes)
                {
                    logger.warn("Operation not allowed on secondary Index table ({})", cfName);
                    continue;
                }
                String baseName = SecondaryIndexManager.getParentCfsName(cfName);
                String indexName = SecondaryIndexManager.getIndexName(cfName);

                ColumnFamilyStore baseCfs = getColumnFamilyStore(baseName);
                Index index = baseCfs.indexManager.getIndexByName(indexName);
                if (index == null)
                    throw new IllegalArgumentException(String.format("Invalid index specified: %s/%s.",
                                                                     baseCfs.metadata.name,
                                                                     indexName));

                if (index.getBackingTable().isPresent())
                    valid.add(index.getBackingTable().get());
            }
            else
            {
                ColumnFamilyStore cfStore = getColumnFamilyStore(cfName);
                valid.add(cfStore);
                if (autoAddIndexes)
                    valid.addAll(getIndexColumnFamilyStores(cfStore));
            }
        }

        return valid;
    }

    private Set<ColumnFamilyStore> getIndexColumnFamilyStores(ColumnFamilyStore baseCfs)
    {
        Set<ColumnFamilyStore> stores = new HashSet<>();
        for (ColumnFamilyStore indexCfs : baseCfs.indexManager.getAllIndexColumnFamilyStores())
        {
            logger.info("adding secondary index table {} to operation", indexCfs.metadata.name);
            stores.add(indexCfs);
        }
        return stores;
    }

    public static Iterable<Keyspace> all()
    {
        return toKeyspaces(Schema.instance.getKeyspaces());
    }

    public static Iterable<Keyspace> nonSystem()
    {
        return toKeyspaces(Schema.instance.getNonSystemKeyspaces());
    }

    public static Iterable<Keyspace> nonLocalStrategy()
    {
        return toKeyspaces(Schema.instance.getNonLocalStrategyKeyspaces());
    }

    public static Iterable<Keyspace> system()
    {
        return toKeyspaces(SchemaConstants.SYSTEM_KEYSPACE_NAMES);
    }

    /**
     * Convert a list of ks names to ks instances, if possible. If we fail to open the ks,
     * it is suppressed. Even though we receive ksName for keyspaces with valid metadata, if
     * there is a race with dropping the keyspace, the metadata and ks instance may be deleted
     * after the ks names were returned, see APOLLO-395.
     *
     * @param ksNames - the list of keyspace names to convert
     *
     * @return - the list of keyspace instances, may be empty
     */
    private static Iterable<Keyspace> toKeyspaces(Collection<String> ksNames)
    {
        return ksNames.stream()
                      .map(ksName -> {
                          try
                          {
                              return Keyspace.open(ksName);
                          }
                          catch (UnknownKeyspaceException ex)
                          {
                              logger.info("Could not open keyspace {}, it was probably dropped.", ex.keyspaceName);
                              return null;
                          }
                          catch (Throwable t)
                          {
                              JVMStabilityInspector.inspectThrowable(t);
                              logger.error("Failed to open keyspace {} due to unexpected exception", ksName, t);
                              return null;
                          }
                      })
                      .filter(Objects::nonNull)
                      .collect(Collectors.toList());
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(name='" + getName() + "')";
    }

    public String getName()
    {
        return metadata.name;
    }
}
