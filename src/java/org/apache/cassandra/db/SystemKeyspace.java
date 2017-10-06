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

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;

import io.reactivex.Single;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.WriteVerbs.WriteVersion;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.CompactionHistoryTabularData;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.net.*;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.ExecutableLock;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternalAsync;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;

public final class SystemKeyspace
{
    private SystemKeyspace()
    {
    }

    private static final Logger logger = LoggerFactory.getLogger(SystemKeyspace.class);

    public static final LocalPartitioner BATCH_PARTITIONER = new LocalPartitioner(TimeUUIDType.instance);
    public static final String BATCHES = "batches";
    public static final String PAXOS = "paxos";
    public static final String BUILT_INDEXES = "IndexInfo";
    private static final String LOCAL = "local";
    private static final String PEERS = "peers";
    public static final String PEER_EVENTS = "peer_events";
    public static final String RANGE_XFERS = "range_xfers";
    public static final String COMPACTION_HISTORY = "compaction_history";
    public static final String SSTABLE_ACTIVITY = "sstable_activity";
    public static final String SIZE_ESTIMATES = "size_estimates";
    public static final String AVAILABLE_RANGES = "available_ranges";
    public static final String TRANSFERRED_RANGES = "transferred_ranges";
    public static final String VIEWS_BUILDS_IN_PROGRESS = "views_builds_in_progress";
    public static final String BUILT_VIEWS = "built_views";
    public static final String PREPARED_STATEMENTS = "prepared_statements";
    public static final String REPAIRS = "repairs";

    public static final TableMetadata Batches =
        parse(BATCHES,
              "batches awaiting replay",
              "CREATE TABLE %s ("
              + "id timeuuid,"
              + "mutations list<blob>,"
              + "version int,"
              + "PRIMARY KEY ((id)))")
              .partitioner(BATCH_PARTITIONER)
              .compaction(CompactionParams.scts(singletonMap("min_threshold", "2")))
              .compression(CompressionParams.forSystemTables())
              .build();

    public static final TableMetadata Paxos =
        parse(PAXOS,
              "in-progress paxos proposals",
              "CREATE TABLE %s ("
              + "row_key blob,"
              + "cf_id UUID,"
              + "in_progress_ballot timeuuid,"
              + "most_recent_commit blob,"
              + "most_recent_commit_at timeuuid,"
              + "most_recent_commit_version int,"
              + "proposal blob,"
              + "proposal_ballot timeuuid,"
              + "proposal_version int,"
              + "PRIMARY KEY ((row_key), cf_id))")
              .compaction(CompactionParams.lcs(emptyMap()))
              .compression(CompressionParams.forSystemTables())
              .build();

    private static final TableMetadata BuiltIndexes =
        parse(BUILT_INDEXES,
              "built column indexes",
              "CREATE TABLE \"%s\" ("
              + "table_name text," // table_name here is the name of the keyspace - don't be fooled
              + "index_name text,"
              + "PRIMARY KEY ((table_name), index_name)) "
              + "WITH COMPACT STORAGE")
              .build();

    private static final TableMetadata Local =
        parse(LOCAL,
              "information about the local node",
              "CREATE TABLE %s ("
              + "key text,"
              + "bootstrapped text,"
              + "broadcast_address inet,"
              + "cluster_name text,"
              + "cql_version text,"
              + "data_center text,"
              + "gossip_generation int,"
              + "host_id uuid,"
              + "listen_address inet,"
              + "native_protocol_version text,"
              + "partitioner text,"
              + "rack text,"
              + "release_version text,"
              + "rpc_address inet,"
              + "schema_version uuid,"
              + "tokens set<varchar>,"
              + "truncated_at map<uuid, blob>,"
              + "native_transport_address inet,"
              + "native_transport_port int,"
              + "native_transport_port_ssl int,"
              + "storage_port int,"
              + "storage_port_ssl int,"
              + "jmx_port int,"

              // DSE-specific extra columns:
              + "dse_version text,"
              + "graph boolean,"
              + "server_id text,"
              + "workload text,"
              + "workloads frozen<set<text>>,"

              + "PRIMARY KEY ((key)))")
              .recordDeprecatedSystemColumn("thrift_version", UTF8Type.instance)
              .build();

    private static final TableMetadata Peers =
        parse(PEERS,
              "information about known peers in the cluster",
              "CREATE TABLE %s ("
              + "peer inet,"
              + "data_center text,"
              + "host_id uuid,"
              + "preferred_ip inet,"
              + "rack text,"
              + "release_version text,"
              + "rpc_address inet,"
              + "schema_version uuid,"
              + "tokens set<varchar>,"
              + "native_transport_address inet,"
              + "native_transport_port int,"
              + "native_transport_port_ssl int,"
              + "storage_port int,"
              + "storage_port_ssl int,"
              + "jmx_port int,"

              // DSE-specific extra columns:
              + "dse_version text,"
              + "graph boolean,"
              + "server_id text,"
              + "workload text,"
              + "workloads frozen<set<text>>,"

              + "PRIMARY KEY ((peer)))")
              .build();

    private static final TableMetadata PeerEvents =
        parse(PEER_EVENTS,
              "events related to peers",
              "CREATE TABLE %s ("
              + "peer inet,"
              + "hints_dropped map<uuid, int>,"
              + "PRIMARY KEY ((peer)))")
              .build();

    private static final TableMetadata RangeXfers =
        parse(RANGE_XFERS,
              "ranges requested for transfer",
              "CREATE TABLE %s ("
              + "token_bytes blob,"
              + "requested_at timestamp,"
              + "PRIMARY KEY ((token_bytes)))")
              .build();

    private static final TableMetadata CompactionHistory =
        parse(COMPACTION_HISTORY,
              "week-long compaction history",
              "CREATE TABLE %s ("
              + "id uuid,"
              + "bytes_in bigint,"
              + "bytes_out bigint,"
              + "columnfamily_name text,"
              + "compacted_at timestamp,"
              + "keyspace_name text,"
              + "rows_merged map<int, bigint>,"
              + "PRIMARY KEY ((id)))")
              .defaultTimeToLive((int) TimeUnit.DAYS.toSeconds(7))
              .build();

    private static final TableMetadata SSTableActivity =
        parse(SSTABLE_ACTIVITY,
              "historic sstable read rates",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "columnfamily_name text,"
              + "generation int,"
              + "rate_120m double,"
              + "rate_15m double,"
              + "PRIMARY KEY ((keyspace_name, columnfamily_name, generation)))")
              .build();

    private static final TableMetadata SizeEstimates =
        parse(SIZE_ESTIMATES,
              "per-table primary range size estimates",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "table_name text,"
              + "range_start text,"
              + "range_end text,"
              + "mean_partition_size bigint,"
              + "partitions_count bigint,"
              + "PRIMARY KEY ((keyspace_name), table_name, range_start, range_end))")
              .build();

    private static final TableMetadata AvailableRanges =
        parse(AVAILABLE_RANGES,
              "available keyspace/ranges during bootstrap/replace that are ready to be served",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "ranges set<blob>,"
              + "PRIMARY KEY ((keyspace_name)))")
              .build();

    private static final TableMetadata TransferredRanges =
        parse(TRANSFERRED_RANGES,
              "record of transferred ranges for streaming operation",
              "CREATE TABLE %s ("
              + "operation text,"
              + "peer inet,"
              + "keyspace_name text,"
              + "ranges set<blob>,"
              + "PRIMARY KEY ((operation, keyspace_name), peer))")
              .build();

    private static final TableMetadata ViewsBuildsInProgress =
        parse(VIEWS_BUILDS_IN_PROGRESS,
              "views builds current progress",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "view_name text,"
              + "last_token varchar,"
              + "generation_number int,"
              + "PRIMARY KEY ((keyspace_name), view_name))")
              .build();

    private static final TableMetadata BuiltViews =
        parse(BUILT_VIEWS,
              "built views",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "view_name text,"
              + "status_replicated boolean,"
              + "PRIMARY KEY ((keyspace_name), view_name))")
              .build();

    private static final TableMetadata PreparedStatements =
        parse(PREPARED_STATEMENTS,
              "prepared statements",
              "CREATE TABLE %s ("
              + "prepared_id blob,"
              + "logged_keyspace text,"
              + "query_string text,"
              + "PRIMARY KEY ((prepared_id)))")
              .build();

    private static final TableMetadata Repairs =
        parse(REPAIRS,
              "repairs",
              "CREATE TABLE %s ("
              + "parent_id timeuuid, "
              + "started_at timestamp, "
              + "last_update timestamp, "
              + "repaired_at timestamp, "
              + "state int, "
              + "coordinator inet, "
              + "participants set<inet>, "
              + "ranges set<blob>, "
              + "cfids set<uuid>, "
              + "PRIMARY KEY (parent_id))").build();


    /** A semaphore for global synchronization on SystemKeyspace static methods */
    private static final ExecutableLock GLOBAL_LOCK = new ExecutableLock();

    /** Changed by {@link #beginStartupBlocking()} and {@link #finishStartupBlocking()} */
    private static StartupState startupState = StartupState.NONE;

    /**
     * The local host id.
     *
     * Set during start-up and only changed when replacing a node, again during startup.
     */
    private static UUID localHostId = null;

    /**
     * The bootstrap state.
     *
     * Set during start-up, see {@link #beginStartupBlocking()} and {@link #finishStartupBlocking()}.
     */
    private static volatile BootstrapState bootstrapState = BootstrapState.NEEDS_BOOTSTRAP;

    /**
     * Cache for information read from System PEERS.
     *
     * Set during start-up, see {@link #beginStartupBlocking()} and {@link #finishStartupBlocking()}.
     */
    private static volatile ConcurrentMap<InetAddress, PeerInfo> peers = null;

    private static TableMetadata.Builder parse(String table, String description, String cql)
    {
        return parse(table, description, cql, Collections.emptyList());
    }

    private static TableMetadata.Builder parse(String table, String description, String cql, Collection<UserType> types)
    {
        return CreateTableStatement.parse(format(cql, table), SchemaConstants.SYSTEM_KEYSPACE_NAME, types)
                                   .id(TableId.forSystemTable(SchemaConstants.SYSTEM_KEYSPACE_NAME, table))
                                   .dcLocalReadRepairChance(0.0)
                                   .gcGraceSeconds(0)
                                   .memtableFlushPeriod((int) TimeUnit.HOURS.toMillis(1))
                                   .comment(description);
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SchemaConstants.SYSTEM_KEYSPACE_NAME, KeyspaceParams.local(), tables(), Views.none(), Types.none(), functions());
    }

    private static Tables tables()
    {
        return Tables.of(BuiltIndexes,
                         Batches,
                         Paxos,
                         Local,
                         Peers,
                         PeerEvents,
                         RangeXfers,
                         CompactionHistory,
                         SSTableActivity,
                         SizeEstimates,
                         AvailableRanges,
                         TransferredRanges,
                         ViewsBuildsInProgress,
                         BuiltViews,
                         PreparedStatements,
                         Repairs);
    }

    private static Functions functions()
    {
        return Functions.builder()
                        .add(UuidFcts.all())
                        .add(TimeFcts.all())
                        .add(BytesConversionFcts.all())
                        .add(AggregateFcts.all())
                        .add(CastFcts.all())
                        .add(OperationFcts.all())
                        .build();
    }

    /**
     * @return  a list of tables names that should always be readable to authenticated users
     *          (since they are used by many tools such as nodetool, cqlsh, bulkloader).
     */
    public static List<String> readableSystemResources()
    {
        return Arrays.asList(SystemKeyspace.LOCAL, SystemKeyspace.PEERS);
    }

    /**
     * The truncation records map a table to its truncation position and time, if any TRUNCATE was performed.
     *
     * Set during start-up, see {@link #beginStartupBlocking()} and {@link #finishStartupBlocking()}.
     */
    private static ConcurrentMap<TableId, Pair<CommitLogPosition, Long>> truncationRecords = null;

    public enum BootstrapState
    {
        NEEDS_BOOTSTRAP,
        COMPLETED,
        IN_PROGRESS,
        DECOMMISSIONED
    }

    private enum StartupState
    {
        NONE,
        STARTED,
        COMPLETED
    };

    /**
     * Called very early on during startup, before any CL replay, persists the local metadata
     * and initializes the internal cache.
     * <p>
     * We need to persist the local metadata by calling {@link #persistLocalMetadata()} as soon as possible
     * after startup checks. This should be the first write to SystemKeyspace (CASSANDRA-11742).
     * <p>
     * We need to initialize the cache very early, because of early calls to {@link StorageService#populateTokenMetadata()},
     * and to {@link #loadDcRackInfo()}. Note that DseSimpleSnitch.getDataCenter() calls {@link #loadDcRackInfo()},
     * and that {@link #persistLocalMetadata()} calls {@link IEndpointSnitch#getDatacenter(InetAddress)}, as well as
     * {@link IEndpointSnitch#getRack(InetAddress)}. Therefore, the {@link #PEERS} cache must be loaded before calling
     * {@link #persistLocalMetadata()}, whereas the data cached from {@link #LOCAL} is loaded afterwards, given that
     * {@link #persistLocalMetadata()} updates {@link #LOCAL}.
     * <p>
     * After CL replay, the cache is loaded again, see {@link #finishStartupBlocking()}.
     * <p>
     * This method is idem-potent for consistency with {@link #finishStartupBlocking()}.
     */
    public static void beginStartupBlocking()
    {
        TPCUtils.withLockBlocking(GLOBAL_LOCK, () -> {
            if (startupState != StartupState.NONE)
                return null;

            peers = TPCUtils.blockingGet(readPeerInfo());

            TPCUtils.blockingAwait(SystemKeyspace.persistLocalMetadata());

            truncationRecords = TPCUtils.blockingGet(readTruncationRecords());
            bootstrapState = TPCUtils.blockingGet(loadBootstrapState());

            startupState = StartupState.STARTED;
            return null;
        });
    }

    /**
     * Called during startup after CL replay, persist other data (e.g. the schema mutations)
     * and re-load the cache to pick up any CL changes.
     *
     * This method is idem-potent as StorageService calls it in initServer purely for the benefit
     * of tests (CassandraDaemon has already called it during startup).
     */
    public static void finishStartupBlocking()
    {
        TPCUtils.withLockBlocking(GLOBAL_LOCK, () -> {
            if (startupState == StartupState.COMPLETED)
                return null;

            SchemaKeyspace.saveSystemKeyspacesSchema();

            peers = TPCUtils.blockingGet(readPeerInfo());
            truncationRecords = TPCUtils.blockingGet(readTruncationRecords());
            bootstrapState = TPCUtils.blockingGet(loadBootstrapState());

            startupState = StartupState.COMPLETED;
            return null;
        });
    }

    /**
     * Reset the startup state and remove the internal caches, this is used exclusively for testing.
     */
    @VisibleForTesting
    public static void resetStartupBlocking()
    {
        TPCUtils.withLockBlocking(GLOBAL_LOCK, () -> {
            peers = null;
            truncationRecords =null;
            bootstrapState = null;
            startupState = StartupState.NONE;
            return null;
        });
    }

    /**
     * The peers cache is loaded with other caches during {@link #beginStartupBlocking()}
     * and {@link #finishStartupBlocking()} but for tools we don't want to unnecessarily
     * start-up services unless it is required (CASSANDRA-9054). Some snitches however do
     * require peers info and {@link DatabaseDescriptor#toolInitialization()} does load
     * the snitches, so we load the peers cache on demand if it is safe to do so (not on
     * a TPC thread), see APOLLO-1210.
     *
     * WARNING: this method should not be called by a method already holding the {@link #GLOBAL_LOCK}.
     */
    private static void checkPeersCache()
    {
        if (peers != null)
            return;

        if (TPC.isTPCThread())
            throw new TPCUtils.WouldBlockException(String.format("Reading system peers would block %s, call startup methods first",
                                                                 Thread.currentThread().getName()));

        TPCUtils.withLockBlocking(GLOBAL_LOCK, () -> {
            if (peers == null)
                peers = TPCUtils.blockingGet(readPeerInfo());
            return null;
        });
    }

    private static void verify(boolean test, String error)
    {
        if (!test)
            throw new IllegalStateException(error);
    }

    public static CompletableFuture<Void> persistLocalMetadata()
    {
        String req = "INSERT INTO system.%s (" +
                     "key," +
                     "cluster_name," +
                     "release_version," +
                     "cql_version," +
                     "native_protocol_version," +
                     "data_center," +
                     "rack," +
                     "partitioner," +
                     "rpc_address," +
                     "broadcast_address," +
                     "listen_address," +
                     "native_transport_address," +
                     "native_transport_port," +
                     "native_transport_port_ssl," +
                     "storage_port," +
                     "storage_port_ssl," +
                     "jmx_port" +
                     ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        return TPCUtils.toFutureVoid(executeOnceInternal(format(req, LOCAL),
                                                         LOCAL,
                                                         DatabaseDescriptor.getClusterName(),
                                                         FBUtilities.getReleaseVersionString(),
                                                         QueryProcessor.CQL_VERSION.toString(),
                                                         String.valueOf(ProtocolVersion.CURRENT.asInt()),
                                                         snitch.getDatacenter(FBUtilities.getBroadcastAddress()),
                                                         snitch.getRack(FBUtilities.getBroadcastAddress()),
                                                         DatabaseDescriptor.getPartitioner().getClass().getName(),
                                                         DatabaseDescriptor.getNativeTransportAddress(),
                                                         FBUtilities.getBroadcastAddress(),
                                                         FBUtilities.getLocalAddress(),
                                                         DatabaseDescriptor.getNativeTransportAddress(),
                                                         DatabaseDescriptor.getNativeTransportPort(),
                                                         DatabaseDescriptor.getNativeTransportPortSSL(),
                                                         DatabaseDescriptor.getStoragePort(),
                                                         DatabaseDescriptor.getSSLStoragePort(),
                                                         DatabaseDescriptor.getJMXPort().orElse(null)));
    }

    public static CompletableFuture<Void> updateCompactionHistory(String ksname,
                                                                              String cfname,
                                                                              long compactedAt,
                                                                              long bytesIn,
                                                                              long bytesOut,
                                                                              Map<Integer, Long> rowsMerged)
    {
        // don't write anything when the history table itself is compacted, since that would in turn cause new compactions
        if (ksname.equals("system") && cfname.equals(COMPACTION_HISTORY))
            return TPCUtils.completedFuture();

        String req = "INSERT INTO system.%s (id, keyspace_name, columnfamily_name, compacted_at, bytes_in, bytes_out, rows_merged) VALUES (?, ?, ?, ?, ?, ?, ?)";
        return TPCUtils.toFutureVoid(executeInternalAsync(format(req, COMPACTION_HISTORY),
                                                          UUIDGen.getTimeUUID(),
                                                          ksname,
                                                          cfname,
                                                          ByteBufferUtil.bytes(compactedAt),
                                                          bytesIn,
                                                          bytesOut,
                                                          rowsMerged));
    }

    public static CompletableFuture<TabularData> getCompactionHistory()
    {
        return TPCUtils.toFuture(executeInternalAsync(format("SELECT * from system.%s", COMPACTION_HISTORY)))
                       .thenApply(resultSet-> {
                           try
                           {
                               return CompactionHistoryTabularData.from(resultSet);
                           }
                           catch (OpenDataException ex)
                           {
                               throw new CompletionException(ex);
                           }
                       });
    }

    public static CompletableFuture<Boolean> isViewBuilt(String keyspaceName, String viewName)
    {
        String req = "SELECT view_name FROM %s.\"%s\" WHERE keyspace_name=? AND view_name=?";
        return TPCUtils.toFuture(executeInternalAsync(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName))
                       .thenApply(result -> !result.isEmpty());
    }

    public static CompletableFuture<Boolean> isViewStatusReplicated(String keyspaceName, String viewName)
    {
        String req = "SELECT status_replicated FROM %s.\"%s\" WHERE keyspace_name=? AND view_name=?";
        return TPCUtils.toFuture(executeInternalAsync(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName))
                       .thenApply(result -> {
                           if (result.isEmpty())
                               return false;

                           UntypedResultSet.Row row = result.one();
                           return row.has("status_replicated") && row.getBoolean("status_replicated");
                       });
    }

    public static CompletableFuture<Void> setViewBuilt(String keyspaceName, String viewName, boolean replicated)
    {
        return isViewBuilt(keyspaceName, viewName)
               .thenCompose(built -> {
                   if (built)
                       return isViewStatusReplicated(keyspaceName, viewName)
                              .thenCompose(replicatedResult -> {
                                  if (replicatedResult == replicated)
                                      return TPCUtils.completedFuture();
                                  else
                                      return doSetViewBuilt(keyspaceName, viewName, replicated);
                       });
                   else
                       return doSetViewBuilt(keyspaceName, viewName, replicated);
               });
    }

    private static CompletableFuture<Void> doSetViewBuilt(String keyspaceName, String viewName, boolean replicated)
    {
        String req = "INSERT INTO %s.\"%s\" (keyspace_name, view_name, status_replicated) VALUES (?, ?, ?)";
        return TPCUtils.toFuture(executeInternalAsync(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS), keyspaceName, viewName, replicated))
                       .thenCompose(resultSet -> forceFlush(BUILT_VIEWS));
    }

    public static CompletableFuture<Void> setViewRemoved(String keyspaceName, String viewName)
    {
        String buildReq = "DELETE FROM %S.%s WHERE keyspace_name = ? AND view_name = ? IF EXISTS";
        return TPCUtils.toFuture(executeInternalAsync(String.format(buildReq, SchemaConstants.SYSTEM_KEYSPACE_NAME, VIEWS_BUILDS_IN_PROGRESS),
                                                      keyspaceName,
                                                      viewName))
                       .thenCompose(r1 -> forceFlush(VIEWS_BUILDS_IN_PROGRESS))
                       .thenCompose(r2 -> {
                           String builtReq = "DELETE FROM %s.\"%s\" WHERE keyspace_name = ? AND view_name = ? IF EXISTS";
                           return TPCUtils.toFuture(executeInternalAsync(String.format(builtReq, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_VIEWS),
                                                                         keyspaceName,
                                                                         viewName));})
                       .thenCompose(r3 -> forceFlush(BUILT_VIEWS));
    }

    public static CompletableFuture<Void> beginViewBuild(String ksname, String viewName, int generationNumber)
    {
        String req = "INSERT INTO system.%s (keyspace_name, view_name, generation_number) VALUES (?, ?, ?)";
        return TPCUtils.toFutureVoid(executeInternalAsync(format(req, VIEWS_BUILDS_IN_PROGRESS), ksname, viewName, generationNumber));
    }

    public static CompletableFuture<Void> finishViewBuildStatus(String ksname, String viewName)
    {
        // We flush the view built first, because if we fail now, we'll restart at the last place we checkpointed
        // view build.
        // If we flush the delete first, we'll have to restart from the beginning.
        // Also, if writing to the built_view succeeds, but the view_builds_in_progress deletion fails, we will be able
        // to skip the view build next boot.
        String req = "DELETE FROM system.%s WHERE keyspace_name = ? AND view_name = ? IF EXISTS";
        return setViewBuilt(ksname, viewName, false)
               .thenCompose(r1 -> TPCUtils.toFuture(executeInternalAsync(String.format(req, VIEWS_BUILDS_IN_PROGRESS), ksname, viewName)))
               .thenCompose(r2 -> forceFlush(VIEWS_BUILDS_IN_PROGRESS));
    }

    public static CompletableFuture<Void> setViewBuiltReplicated(String ksname, String viewName)
    {
        return setViewBuilt(ksname, viewName, true);
    }

    public static CompletableFuture<Void> updateViewBuildStatus(String ksname, String viewName, Token token)
    {
        String req = "INSERT INTO system.%s (keyspace_name, view_name, last_token) VALUES (?, ?, ?)";
        Token.TokenFactory factory = ViewsBuildsInProgress.partitioner.getTokenFactory();
        return TPCUtils.toFutureVoid(executeInternalAsync(format(req, VIEWS_BUILDS_IN_PROGRESS), ksname, viewName, factory.toString(token)));
    }

    public static CompletableFuture<Pair<Integer, Token>> getViewBuildStatus(String ksname, String viewName)
    {
        String req = "SELECT generation_number, last_token FROM system.%s WHERE keyspace_name = ? AND view_name = ?";
        return TPCUtils.toFuture(executeInternalAsync(format(req, VIEWS_BUILDS_IN_PROGRESS), ksname, viewName))
                       .thenApply(queryResultSet -> {
                           if (queryResultSet == null || queryResultSet.isEmpty())
                               return null;

                           UntypedResultSet.Row row = queryResultSet.one();

                           Integer generation = null;
                           Token lastKey = null;
                           if (row.has("generation_number"))
                               generation = row.getInt("generation_number");
                           if (row.has("last_key"))
                           {
                               Token.TokenFactory factory = ViewsBuildsInProgress.partitioner.getTokenFactory();
                               lastKey = factory.fromString(row.getString("last_key"));
                           }

                           return Pair.create(generation, lastKey);
                       });
    }

    public static CompletableFuture<Void> saveTruncationRecord(ColumnFamilyStore cfs, long truncatedAt, CommitLogPosition position)
    {
        return TPCUtils.withLock(GLOBAL_LOCK, () -> {
            String req = "UPDATE system.%s SET truncated_at = truncated_at + ? WHERE key = '%s'";
            return TPCUtils.toFuture(executeInternalAsync(format(req, LOCAL, LOCAL), truncationAsMapEntry(cfs, truncatedAt, position)))
                           .thenCompose(resultSet -> {
                               if (truncationRecords != null)
                                   truncationRecords.put(cfs.metadata.id, Pair.create(position, truncatedAt));
                               return forceFlush(LOCAL);
                           });
        });
    }

    /**
     * This method is used to remove information about truncation time for specified column family,
     * if a truncation record is found. This is called when invalidating a CFS. The truncation records
     * must be available, that is {@link #finishStartupBlocking()} must have been called, which is done
     * immediately after replaying the CL at startup.
     */
    public static CompletableFuture<Void> maybeRemoveTruncationRecord(TableId id)
    {
        return TPCUtils.withLock(GLOBAL_LOCK, () -> {
            Pair<CommitLogPosition, Long> truncationRecord = getTruncationRecords().get(id);
            if (truncationRecord == null)
                return TPCUtils.completedFuture();
            else
                return removeTruncationRecord(id);
      });
    }

    /**
     * This removes the truncation record for a table and assumes that such record exists on disk.
     * This is called during CL reply, before the truncation records are loaded into memory.
     */
    public static CompletableFuture<Void> removeTruncationRecord(TableId id)
    {
        String req = "DELETE truncated_at[?] from system.%s WHERE key = '%s'";
        return TPCUtils.toFuture(executeInternalAsync(String.format(req, LOCAL, LOCAL), id.asUUID()))
                       .thenCompose(resultSet -> {
                           if (truncationRecords != null)
                               truncationRecords.remove(id);
                           return forceFlush(LOCAL);
                       });
    }

    private static Map<UUID, ByteBuffer> truncationAsMapEntry(ColumnFamilyStore cfs, long truncatedAt, CommitLogPosition position)
    {
        try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
        {
            CommitLogPosition.serializer.serialize(position, out);
            out.writeLong(truncatedAt);
            return singletonMap(cfs.metadata.id.asUUID(), out.asNewBuffer());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static long getTruncatedAt(TableId id)
    {
        Pair<CommitLogPosition, Long> record = getTruncationRecord(id);
        return record == null ? Long.MIN_VALUE : record.right;
    }

    private static Pair<CommitLogPosition, Long> getTruncationRecord(TableId id)
    {
        return getTruncationRecords().get(id);
    }

    private static Map<TableId, Pair<CommitLogPosition, Long>> getTruncationRecords()
    {
        verify(truncationRecords != null, "startup methods not yet called");
        return truncationRecords;
    }

    public static CompletableFuture<ConcurrentMap<TableId, Pair<CommitLogPosition, Long>>> readTruncationRecords()
    {
        return TPCUtils.toFuture(executeInternalAsync(format("SELECT truncated_at FROM system.%s WHERE key = '%s'", LOCAL, LOCAL)))
                       .thenApply(rows -> {
                           ConcurrentMap<TableId, Pair<CommitLogPosition, Long>> records = new ConcurrentHashMap<>();

                           if (!rows.isEmpty() && rows.one().has("truncated_at"))
                           {
                               Map<UUID, ByteBuffer> map = rows.one().getMap("truncated_at", UUIDType.instance, BytesType.instance);
                               for (Map.Entry<UUID, ByteBuffer> entry : map.entrySet())
                                   records.put(TableId.fromUUID(entry.getKey()), truncationRecordFromBlob(entry.getValue()));
                           }

                           return records;
                       });
    }

    private static Pair<CommitLogPosition, Long> truncationRecordFromBlob(ByteBuffer bytes)
    {
        try (RebufferingInputStream in = new DataInputBuffer(bytes, true))
        {
            return Pair.create(CommitLogPosition.serializer.deserialize(in), in.available() > 0 ? in.readLong() : Long.MIN_VALUE);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Record tokens being used by another node
     */
    public static CompletableFuture<Void> updateTokens(InetAddress ep, Collection<Token> tokens)
    {
        return TPCUtils.withLock(GLOBAL_LOCK, () -> {
            if (ep.equals(FBUtilities.getBroadcastAddress()))
                return TPCUtils.completedFuture();

            String req = "INSERT INTO system.%s (peer, tokens) VALUES (?, ?)";
            logger.debug("PEERS TOKENS for {} = {}", ep, tokensAsSet(tokens));
            return TPCUtils.toFutureVoid(executeInternalAsync(format(req, PEERS), ep, tokensAsSet(tokens)));
        });
    }

    private static CompletableFuture<ConcurrentMap<InetAddress, PeerInfo>> readPeerInfo()
    {
        String req = "SELECT * from system." + PEERS;
        return TPCUtils.toFuture(executeInternalAsync(req))
                       .thenApply(results -> {
                           ConcurrentMap<InetAddress, PeerInfo> ret = new ConcurrentHashMap<>();
                           for (UntypedResultSet.Row row : results)
                               ret.put(row.getInetAddress("peer"), new PeerInfo(row));
                           return ret;
                       });
    }

    public static CompletableFuture<Void> updatePreferredIP(InetAddress ep, InetAddress preferred_ip)
    {
        return TPCUtils.withLock(GLOBAL_LOCK, () -> {
            InetAddress current = getPreferredIPIfAvailable(ep);
            if (current == preferred_ip)
                return TPCUtils.completedFuture();

            String req = "INSERT INTO system.%s (peer, preferred_ip) VALUES (?, ?)";
            return TPCUtils.toFuture(executeInternalAsync(format(req, PEERS), ep, preferred_ip))
                           .thenCompose(r ->
                                        {
                                            if (peers != null)
                                                peers.computeIfAbsent(ep, key -> new PeerInfo()).setPreferredIp(preferred_ip);
                                            return forceFlush(PEERS);
                                        });
        });
    }

    public static CompletableFuture<Void> updatePeerInfo(InetAddress ep, String columnName, Object value)
    {
        return TPCUtils.withLock(GLOBAL_LOCK, () -> {
            if (ep.equals(FBUtilities.getBroadcastAddress()))
                return TPCUtils.completedFuture();

            String req = "INSERT INTO system.%s (peer, %s) VALUES (?, ?)";
            return TPCUtils.toFuture(executeInternalAsync(format(req, PEERS, columnName), ep, value))
                           .thenAccept(resultSet -> {
                               if (peers != null)
                                   peers.computeIfAbsent(ep, key -> new PeerInfo()).setValue(columnName, value);
                           });
        });
    }

    public static CompletableFuture<UntypedResultSet> loadPeerInfo(InetAddress ep, String columnName)
    {
        String req = "SELECT %s FROM system.%s WHERE peer = '%s'";
        return TPCUtils.toFuture(executeInternalAsync(format(req, columnName, PEERS, ep.getHostAddress())));
    }

    public static CompletableFuture<Void> updateHintsDropped(InetAddress ep, UUID timePeriod, int value)
    {
        return TPCUtils.withLock(GLOBAL_LOCK, () -> {
            // with 30 day TTL
            String req = "UPDATE system.%s USING TTL 2592000 SET hints_dropped[ ? ] = ? WHERE peer = ?";
            return TPCUtils.toFutureVoid(executeInternalAsync(format(req, PEER_EVENTS), timePeriod, value, ep));
        });
    }

    public static CompletableFuture<Void> updateSchemaVersion(UUID version)
    {
        return updateLocalInfo("schema_version", version);
    }

    public static CompletableFuture<Void> updateLocalInfo(String columnName, Object value)
    {
        if (columnName.equals("host_id"))
        {
            if (!(value instanceof UUID))
                throw new IllegalArgumentException("Expected UUID for host_id column");

            return setLocalHostId((UUID) value).thenAccept(uuid -> {});
        }
        else if (columnName.equals("bootstrapped"))
        {
            if (!(value instanceof BootstrapState))
                throw new IllegalArgumentException("Expected BootstrapState for bootstrapped column");

            return setBootstrapState((BootstrapState) value).thenAccept(state -> {});
        }
        else if (columnName.equals("truncated_at"))
        {
            throw new IllegalArgumentException("Truncation records should be updated one by one via saveTruncationRecord");
        }
        else
        {
            return TPCUtils.withLock(GLOBAL_LOCK, () ->
            {
                final String req = "INSERT INTO system.%s (key, %s) VALUES ('%s', ?)";
                return TPCUtils.toFutureVoid(executeInternalAsync(format(req, LOCAL, columnName, LOCAL), value));
            });
        }
    }

    public static CompletableFuture<UntypedResultSet> loadLocalInfo(String columnName)
    {
        String req = "SELECT %s FROM system.%s WHERE key = '%s'";
        return TPCUtils.toFuture(executeInternalAsync(format(req, columnName, LOCAL, LOCAL)));
    }

    private static Set<String> tokensAsSet(Collection<Token> tokens)
    {
        if (tokens.isEmpty())
            return Collections.emptySet();
        Token.TokenFactory factory = StorageService.instance.getTokenFactory();
        Set<String> s = new HashSet<>(tokens.size());
        for (Token tk : tokens)
            s.add(factory.toString(tk));
        return s;
    }

    private static Collection<Token> deserializeTokens(Collection<String> tokensStrings)
    {
        Token.TokenFactory factory = StorageService.instance.getTokenFactory();
        List<Token> tokens = new ArrayList<>(tokensStrings.size());
        for (String tk : tokensStrings)
            tokens.add(factory.fromString(tk));
        return tokens;
    }

    /**
     * Remove stored tokens being used by another node
     */
    public static CompletableFuture<Void> removeEndpoint(InetAddress ep)
    {
        return TPCUtils.withLock(GLOBAL_LOCK, () -> {
            String req = "DELETE FROM system.%s WHERE peer = ?";
            return TPCUtils.toFuture(executeInternalAsync(format(req, PEERS), ep))
                           .thenCompose(resultSet -> {
                               if (peers != null)
                                   peers.remove(ep);
                               return forceFlush(PEERS);
                           });
        });
    }

    /**
     * This method is used to update the System Keyspace with the new tokens for this node
    */
    public static CompletableFuture<Void> updateTokens(Collection<Token> tokens)
    {
        verify(!tokens.isEmpty(), "removeEndpoint should be used instead");
        return TPCUtils.withLock(GLOBAL_LOCK, () ->
            getSavedTokens().thenCompose(savedTokens -> {
                if (tokens.containsAll(savedTokens) && tokens.size() == savedTokens.size())
                    return TPCUtils.completedFuture();

                String req = "INSERT INTO system.%s (key, tokens) VALUES ('%s', ?)";
                return TPCUtils.toFuture(executeInternalAsync(format(req, LOCAL, LOCAL), tokensAsSet(tokens)))
                               .thenCompose(resultSet -> forceFlush(LOCAL));
            })
        );
    }

    private static CompletableFuture<Void> forceFlush(String cfname)
    {
        if (!DatabaseDescriptor.isUnsafeSystem())
            return Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                           .getColumnFamilyStore(cfname)
                           .forceFlush()
                           .thenApply(pos -> null);

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Return a map of stored tokens to IP addresses
     */
    public static CompletableFuture<SetMultimap<InetAddress, Token>> loadTokens()
    {
        return TPCUtils.toFuture(executeInternalAsync("SELECT peer, tokens FROM system." + PEERS))
                       .thenApply(resultSet -> {
                           SetMultimap<InetAddress, Token> tokenMap = HashMultimap.create();
                           for (UntypedResultSet.Row row : resultSet)
                           {
                               InetAddress peer = row.getInetAddress("peer");
                               if (row.has("tokens"))
                                   tokenMap.putAll(peer, deserializeTokens(row.getSet("tokens", UTF8Type.instance)));
                           }
                           return tokenMap;
                         });
    }

    /**
     * Return a map of stored host_ids to IP addresses
     */
    public static Map<InetAddress, UUID> getHostIds()
    {
        checkPeersCache();
        Map<InetAddress, UUID> hostIdMap = new HashMap<>();

        for (Map.Entry<InetAddress, PeerInfo> entry : peers.entrySet())
            hostIdMap.put(entry.getKey(), entry.getValue().hostId);

        return hostIdMap;
    }

    /**
     * Get preferred IP for given endpoint if it is known. Otherwise this returns given endpoint itself.
     *
     * @param ep endpoint address to check
     * @return Preferred IP for given endpoint if present, otherwise returns given ep
     */
    public static InetAddress getPreferredIP(InetAddress ep)
    {
        checkPeersCache();
        return getPreferredIPIfAvailable(ep);
    }

    private static InetAddress getPreferredIPIfAvailable(InetAddress ep)
    {
        PeerInfo info = peers == null ? null : peers.get(ep);
        return info == null || info.preferredIp == null ? ep : info.preferredIp;
    }

    /**
     * Return a map of IP addresses containing a map of dc and rack info
     */
    public static Map<InetAddress, Map<String,String>> loadDcRackInfo()
    {
        checkPeersCache();
        Map<InetAddress, Map<String, String>> result = new HashMap<>();

        for (Map.Entry<InetAddress, PeerInfo> entry : peers.entrySet())
        {
            if (entry.getValue().rack != null && entry.getValue().dc != null)
            {
                Map<String, String> dcRack = new HashMap<>();
                dcRack.put("data_center", entry.getValue().dc);
                dcRack.put("rack", entry.getValue().rack);
                result.put(entry.getKey(), dcRack);
            }
        }

        return result;
    }

    /**
     * Get release version for given endpoint.
     * If release version is unknown, then this returns null.
     *
     * @param ep endpoint address to check
     * @return Release version or null if version is unknown.
     */
    public static CassandraVersion getReleaseVersion(InetAddress ep)
    {
        checkPeersCache();

        if (FBUtilities.getBroadcastAddress().equals(ep))
            return new CassandraVersion(FBUtilities.getReleaseVersionString());

        PeerInfo info = peers.get(ep);
        return info == null ? null : info.version;
    }

    /**
     * One of three things will happen if you try to read the system keyspace:
     * 1. files are present and you can read them: great
     * 2. no files are there: great (new node is assumed)
     * 3. files are present but you can't read them: bad
     * @throws ConfigurationException
     */
    public static CompletableFuture<Void> checkHealth() throws ConfigurationException
    {
        Keyspace keyspace;
        try
        {
            keyspace = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        }
        catch (AssertionError err)
        {
            // this happens when a user switches from OPP to RP.
            ConfigurationException ex = new ConfigurationException("Could not read system keyspace!");
            ex.initCause(err);
            throw ex;
        }
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(LOCAL);

        String req = "SELECT cluster_name FROM system.%s WHERE key='%s'";
        return TPCUtils.toFuture(executeInternalAsync(format(req, LOCAL, LOCAL)))
                       .thenAccept(result -> {
                           if (result.isEmpty() || !result.one().has("cluster_name"))
                           {
                               // this is a brand new node
                               if (!cfs.getLiveSSTables().isEmpty())
                                   throw new ConfigurationException("Found system keyspace files, but they couldn't be loaded!");

                               // no system files.  this is a new node.
                               return;
                           }

                           String savedClusterName = result.one().getString("cluster_name");
                           if (!DatabaseDescriptor.getClusterName().equals(savedClusterName))
                               throw new ConfigurationException("Saved cluster name " + savedClusterName + " != configured name " + DatabaseDescriptor.getClusterName());
                       });
    }

    public static CompletableFuture<Collection<Token>> getSavedTokens()
    {
        String req = "SELECT tokens FROM system.%s WHERE key='%s'";
        return TPCUtils.toFuture(executeInternalAsync(format(req, LOCAL, LOCAL)))
                       .thenApply(result -> result.isEmpty() || !result.one().has("tokens")
                                            ? Collections.<Token>emptyList()
                                            : deserializeTokens(result.one().getSet("tokens", UTF8Type.instance)));
    }

    public static CompletableFuture<Integer> incrementAndGetGeneration()
    {
        String req = "SELECT gossip_generation FROM system.%s WHERE key='%s'";
        return TPCUtils.toFuture(executeInternalAsync(format(req, LOCAL, LOCAL)))
                       .thenCompose(result -> {
                           int generation;
                           if (result.isEmpty() || !result.one().has("gossip_generation"))
                           {
                               // seconds-since-epoch isn't a foolproof new generation
                               // (where foolproof is "guaranteed to be larger than the last one seen at this ip address"),
                               // but it's as close as sanely possible
                               generation = (int) (System.currentTimeMillis() / 1000);
                           }
                           else
                           {
                               // Other nodes will ignore gossip messages about a node that have a lower generation than previously seen.
                               final int storedGeneration = result.one().getInt("gossip_generation") + 1;
                               final int now = (int) (System.currentTimeMillis() / 1000);
                               if (storedGeneration >= now)
                               {
                                   logger.warn("Using stored Gossip Generation {} as it is greater than current system time {}.  See CASSANDRA-3654 if you experience problems",
                                               storedGeneration, now);
                                   generation = storedGeneration;
                               }
                               else
                               {
                                   generation = now;
                               }
                           }

                           String insert = "INSERT INTO system.%s (key, gossip_generation) VALUES ('%s', ?)";
                           return TPCUtils.toFuture(executeInternalAsync(format(insert, LOCAL, LOCAL), generation))
                                          .thenCompose(r -> forceFlush(LOCAL))
                                          .thenApply(r -> generation);
                      });
    }

    private static CompletableFuture<BootstrapState> loadBootstrapState()
    {
        String req = "SELECT bootstrapped FROM system.%s WHERE key='%s'";
        return TPCUtils.toFuture(executeInternalAsync(format(req, LOCAL, LOCAL)))
                       .thenApply(result -> {
                           if (result.isEmpty() || !result.one().has("bootstrapped"))
                               return BootstrapState.NEEDS_BOOTSTRAP;
                           else
                               return BootstrapState.valueOf(result.one().getString("bootstrapped"));
        });
    }

    public static BootstrapState getBootstrapState()
    {
        return bootstrapState;
    }

    public static boolean bootstrapComplete()
    {
        return getBootstrapState() == BootstrapState.COMPLETED;
    }

    public static boolean bootstrapInProgress()
    {
        return getBootstrapState() == BootstrapState.IN_PROGRESS;
    }

    public static boolean wasDecommissioned()
    {
        return getBootstrapState() == BootstrapState.DECOMMISSIONED;
    }

    public static CompletableFuture<Void> setBootstrapState(BootstrapState state)
    {
        return TPCUtils.withLock(GLOBAL_LOCK, () -> {
            if (getBootstrapState() == state)
                return TPCUtils.completedFuture();

            String req = "INSERT INTO system.%s (key, bootstrapped) VALUES ('%s', ?)";
            return TPCUtils.toFuture(executeInternalAsync(format(req, LOCAL, LOCAL), state.name()))
                           .thenCompose(resultSet -> {
                               bootstrapState = state;
                               return forceFlush(LOCAL);
                           });
        });
    }

    public static CompletableFuture<Boolean> isIndexBuilt(String keyspaceName, String indexName)
    {
        String req = "SELECT index_name FROM %s.\"%s\" WHERE table_name=? AND index_name=?";
        return TPCUtils.toFuture(executeInternalAsync(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, indexName))
                       .thenApply(result -> !result.isEmpty());
    }

    public static CompletableFuture<Void> setIndexBuilt(String keyspaceName, String indexName)
    {
        String req = "INSERT INTO %s.\"%s\" (table_name, index_name) VALUES (?, ?) IF NOT EXISTS;";
        return TPCUtils.toFuture(executeInternalAsync(String.format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, indexName))
               .thenCompose(resultSet -> forceFlush(BUILT_INDEXES));
    }

    public static CompletableFuture<Void> setIndexRemoved(String keyspaceName, String indexName)
    {
        return isIndexBuilt(keyspaceName, indexName).thenCompose(built -> {
           if (!built)
               return TPCUtils.completedFuture(); // index was not built

            String req = "DELETE FROM %s.\"%s\" WHERE table_name = ? AND index_name = ? IF EXISTS";
            return TPCUtils.toFuture(executeInternalAsync(String.format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, indexName))
                           .thenCompose(resultSet -> forceFlush(BUILT_INDEXES));
        });
    }

    /**
     * Return all indexes built for this specific keyspace.
     *
     * @param keyspaceName - the keyspace
     * @return a list of indexes built for the keyspace specified
     */
    public static CompletableFuture<List<String>> getBuiltIndexes(String keyspaceName)
    {
        String req = "SELECT table_name, index_name from %s.\"%s\" WHERE table_name=?";
        return TPCUtils.toFuture(executeInternalAsync(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName))
                       .thenApply(results ->
                           StreamSupport.stream(results.spliterator(), false)
                                        .map(r -> r.getString("index_name"))
                                        .collect(Collectors.toList())
                       );
    }

    /**
     * Return all indexes built for this specific keyspace and whose name is in the
     * set specified by {@code indexNames}.
     *
     * @param keyspaceName - the keyspace
     * @param indexNames  - the indexes to include
     * @return a list of indexes built for the keyspace specified and matching the names specified
     */
    public static CompletableFuture<List<String>> getBuiltIndexes(String keyspaceName, Set<String> indexNames)
    {
        List<String> names = new ArrayList<>(indexNames);
        String req = "SELECT index_name from %s.\"%s\" WHERE table_name=? AND index_name IN ?";
        return TPCUtils.toFuture(executeInternalAsync(format(req, SchemaConstants.SYSTEM_KEYSPACE_NAME, BUILT_INDEXES), keyspaceName, names))
               .thenApply(results ->
                          StreamSupport.stream(results.spliterator(), false)
                                       .map(r -> r.getString("index_name"))
                                       .collect(Collectors.toList())
               );
    }

    /**
     * Retrieve the local host id from a cached value that was set when calling {@link #setLocalHostId()}
     * during start-up.
     */
    public static UUID getLocalHostId()
    {
        verify(localHostId != null, "startup methods not yet called");
        return localHostId;
    }

    /**
     * Read the host ID from the system keyspace, creating (and storing) one if
     * none exists.
     * <p>
     * It is safe to call this method multiple times.
     * <p>
     * This is currently called during startup to ensure the host id is available without blocking when
     * calling {@link #getLocalHostId()}.
     *
     * @return the local host id
     */
    public static CompletableFuture<UUID> setLocalHostId()
    {
        return localHostId == null
               ? initialLocalHostId().thenCompose(hostId -> setLocalHostId(hostId))
               : TPCUtils.completedFuture(localHostId);
    }

    private static CompletableFuture<UUID> initialLocalHostId()
    {
        String req = "SELECT host_id FROM system.%s WHERE key='%s'";
        return TPCUtils.toFuture(executeInternalAsync(format(req, LOCAL, LOCAL)))
                       .thenApply(result -> {
                           // Look up the Host UUID (return it if found)
                           if (!result.isEmpty() && result.one().has("host_id"))
                               return result.one().getUUID("host_id");

                           // ID not found, generate a new one, persist, and then return it.
                           UUID hostId = UUID.randomUUID();
                           logger.warn("No host ID found, created {} (Note: This should happen exactly once per node).", hostId);
                           return hostId;
                       });

    }

    /**
     * Sets the local host ID explicitly.  Should only be called outside of SystemKeyspace when replacing a node.
     *
     * @return a future that will complete with the host id that was set, or the same host id if it could not be changed
     */
    public static CompletableFuture<UUID> setLocalHostId(UUID hostId)
    {
        return TPCUtils.withLock(GLOBAL_LOCK, () -> {
            UUID old = localHostId;
            localHostId = hostId;

            String req = "INSERT INTO system.%s (key, host_id) VALUES ('%s', ?)";
            return TPCUtils.toFuture(executeInternalAsync(format(req, LOCAL, LOCAL), hostId))
                           .handle((result, error) -> {
                               if (error != null)
                               {
                                   logger.error("Failed to change local host id from {} to {}", localHostId, hostId, error);
                                   localHostId = old;
                               }

                               return localHostId;
                           });
        });
    }

    /**
     * Gets the stored rack for the local node, or null if none have been set yet.
     */
    public static CompletableFuture<String> getRack()
    {
        String req = "SELECT rack FROM system.%s WHERE key='%s'";
        return TPCUtils.toFuture(executeInternalAsync(format(req, LOCAL, LOCAL)))
                       .thenApply(result -> {
                           // Look up the Rack (return it if found)
                           if (!result.isEmpty() && result.one().has("rack"))
                               return result.one().getString("rack");

                           return null;
                       });
    }

    /**
     * Gets the stored data center for the local node, or null if none have been set yet.
     */
    public static CompletableFuture<String> getDatacenter()
    {
        String req = "SELECT data_center FROM system.%s WHERE key='%s'";
        return TPCUtils.toFuture(executeInternalAsync(format(req, LOCAL, LOCAL)))
                       .thenApply(result -> {
                           // Look up the Data center (return it if found)
                           if (!result.isEmpty() && result.one().has("data_center"))
                               return result.one().getString("data_center");

                           return null;
                       });
    }

    public static CompletableFuture<PaxosState> loadPaxosState(DecoratedKey key, TableMetadata metadata, int nowInSec)
    {
        String req = "SELECT * FROM system.%s WHERE row_key = ? AND cf_id = ?";
        return TPCUtils.toFuture(QueryProcessor.executeInternalWithNow(nowInSec, System.nanoTime(), format(req, PAXOS), key.getKey(), metadata.id.asUUID()))
                       .thenApply(results -> {
                           if (results.isEmpty())
                               return new PaxosState(key, metadata);
                           UntypedResultSet.Row row = results.one();

                           Commit promised = row.has("in_progress_ballot")
                                             ? new Commit(row.getUUID("in_progress_ballot"), new PartitionUpdate(metadata, key, metadata.regularAndStaticColumns(), 1))
                                             : Commit.emptyCommit(key, metadata);
                           // either we have both a recently accepted ballot and update or we have neither
                           Commit accepted = row.has("proposal_version") && row.has("proposal")
                                             ? new Commit(row.getUUID("proposal_ballot"),
                                                          PartitionUpdate.fromBytes(row.getBytes("proposal"), getVersion(row, "proposal_version")))
                                             : Commit.emptyCommit(key, metadata);
                           // either most_recent_commit and most_recent_commit_at will both be set, or neither
                           Commit mostRecent = row.has("most_recent_commit_version") && row.has("most_recent_commit")
                                               ? new Commit(row.getUUID("most_recent_commit_at"),
                                                            PartitionUpdate.fromBytes(row.getBytes("most_recent_commit"), getVersion(row, "most_recent_commit_version")))
                                               : Commit.emptyCommit(key, metadata);
                           return new PaxosState(promised, accepted, mostRecent);
                       });
    }

    private static EncodingVersion getVersion(UntypedResultSet.Row row, String name)
    {
        int messagingVersion = row.getInt(name);
        MessagingVersion version = MessagingVersion.fromHandshakeVersion(messagingVersion);
        return version.<WriteVersion>groupVersion(Verbs.Group.WRITES).encodingVersion;
    }

    public static CompletableFuture<Void> savePaxosPromise(Commit promise)
    {
        String req = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET in_progress_ballot = ? WHERE row_key = ? AND cf_id = ?";
        return TPCUtils.toFutureVoid(executeInternalAsync(format(req, PAXOS),
                                                          UUIDGen.microsTimestamp(promise.ballot),
                                                          paxosTtlSec(promise.update.metadata()),
                                                          promise.ballot,
                                                          promise.update.partitionKey().getKey(),
                                                          promise.update.metadata().id.asUUID()));
    }

    public static CompletableFuture<Void> savePaxosProposal(Commit proposal)
    {
        String req = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = ?, proposal = ?, proposal_version = ? WHERE row_key = ? AND cf_id = ?";
        return TPCUtils.toFutureVoid(executeInternalAsync(format(req, PAXOS),
                                                          UUIDGen.microsTimestamp(proposal.ballot),
                                                          paxosTtlSec(proposal.update.metadata()),
                                                          proposal.ballot,
                                                          PartitionUpdate.toBytes(proposal.update, MessagingService.current_version.<WriteVersion>groupVersion(Verbs.Group.WRITES).encodingVersion),
                                                          MessagingService.current_version.protocolVersion().handshakeVersion,
                                                          proposal.update.partitionKey().getKey(),
                                                          proposal.update.metadata().id.asUUID()));
    }

    public static int paxosTtlSec(TableMetadata metadata)
    {
        // keep paxos state around for at least 3h
        return Math.max(3 * 3600, metadata.params.gcGraceSeconds);
    }

    public static CompletableFuture<Void> savePaxosCommit(Commit commit)
    {
        // We always erase the last proposal (with the commit timestamp to no erase more recent proposal in case the commit is old)
        // even though that's really just an optimization  since SP.beginAndRepairPaxos will exclude accepted proposal older than the mrc.
        String cql = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = null, proposal = null, most_recent_commit_at = ?, most_recent_commit = ?, most_recent_commit_version = ? WHERE row_key = ? AND cf_id = ?";
        return TPCUtils.toFutureVoid(executeInternalAsync(format(cql, PAXOS),
                                                          UUIDGen.microsTimestamp(commit.ballot),
                                                          paxosTtlSec(commit.update.metadata()),
                                                          commit.ballot,
                                                          PartitionUpdate.toBytes(commit.update, MessagingService.current_version.<WriteVersion>groupVersion(Verbs.Group.WRITES).encodingVersion),
                                                          MessagingService.current_version.protocolVersion().handshakeVersion,
                                                          commit.update.partitionKey().getKey(),
                                                          commit.update.metadata().id.asUUID()));
    }

    /**
     * Returns a RestorableMeter tracking the average read rate of a particular SSTable, restoring the last-seen rate
     * from values in system.sstable_activity if present.
     * @param keyspace the keyspace the sstable belongs to
     * @param table the table the sstable belongs to
     * @param generation the generation number for the sstable
     */
    public static CompletableFuture<RestorableMeter> getSSTableReadMeter(String keyspace, String table, int generation)
    {
        String cql = "SELECT * FROM system.%s WHERE keyspace_name=? and columnfamily_name=? and generation=?";
        return TPCUtils.toFuture(executeInternalAsync(format(cql, SSTABLE_ACTIVITY), keyspace, table, generation))
                       .thenApply(results -> {
                           if (results.isEmpty())
                               return new RestorableMeter();

                           UntypedResultSet.Row row = results.one();
                           double m15rate = row.getDouble("rate_15m");
                           double m120rate = row.getDouble("rate_120m");
                           return new RestorableMeter(m15rate, m120rate);
                       });
    }

    /**
     * Writes the current read rates for a given SSTable to system.sstable_activity
     */
    public static CompletableFuture<Void> persistSSTableReadMeter(String keyspace, String table, int generation, RestorableMeter meter)
    {
        // Store values with a one-day TTL to handle corner cases where cleanup might not occur
        String cql = "INSERT INTO system.%s (keyspace_name, columnfamily_name, generation, rate_15m, rate_120m) VALUES (?, ?, ?, ?, ?) USING TTL 864000";
        return TPCUtils.toFutureVoid(executeInternalAsync(format(cql, SSTABLE_ACTIVITY),
                                                          keyspace,
                                                          table,
                                                          generation,
                                                          meter.fifteenMinuteRate(),
                                                          meter.twoHourRate()));
    }

    /**
     * Clears persisted read rates from system.sstable_activity for SSTables that have been deleted.
     */
    public static CompletableFuture<Void> clearSSTableReadMeter(String keyspace, String table, int generation)
    {
        String cql = "DELETE FROM system.%s WHERE keyspace_name=? AND columnfamily_name=? and generation=?";
        return TPCUtils.toFutureVoid(executeInternalAsync(format(cql, SSTABLE_ACTIVITY), keyspace, table, generation));
    }

    /**
     * Writes the current partition count and size estimates into SIZE_ESTIMATES_CF
     */
    public static CompletableFuture<Void> updateSizeEstimates(String keyspace, String table, Map<Range<Token>, Pair<Long, Long>> estimates)
    {
        long timestamp = FBUtilities.timestampMicros();
        PartitionUpdate update = new PartitionUpdate(SizeEstimates, UTF8Type.instance.decompose(keyspace), SizeEstimates.regularAndStaticColumns(), estimates.size());
        Mutation mutation = new Mutation(update);

        // delete all previous values with a single range tombstone.
        int nowInSec = FBUtilities.nowInSeconds();
        update.add(new RangeTombstone(Slice.make(SizeEstimates.comparator, table), new DeletionTime(timestamp - 1, nowInSec)));

        // add a CQL row for each primary token range.
        for (Map.Entry<Range<Token>, Pair<Long, Long>> entry : estimates.entrySet())
        {
            Range<Token> range = entry.getKey();
            Pair<Long, Long> values = entry.getValue();
            update.add(Rows.simpleBuilder(SizeEstimates, table, range.left.toString(), range.right.toString())
                           .timestamp(timestamp)
                           .add("partitions_count", values.left)
                           .add("mean_partition_size", values.right)
                           .build());
        }

        return TPCUtils.toFuture(mutation.applyAsync());
    }

    /**
     * Clears size estimates for a table (on table drop)
     */
    public static CompletableFuture<Void> clearSizeEstimates(String keyspace, String table)
    {
        String cql = format("DELETE FROM %s WHERE keyspace_name = ? AND table_name = ?", SizeEstimates.toString());
        return TPCUtils.toFutureVoid(executeInternalAsync(cql, keyspace, table));
    }

    public static CompletableFuture<Void> updateAvailableRanges(String keyspace, Collection<Range<Token>> completedRanges)
    {
        return TPCUtils.withLock(GLOBAL_LOCK, () -> {
            String cql = "UPDATE system.%s SET ranges = ranges + ? WHERE keyspace_name = ?";
            return TPCUtils.toFutureVoid(executeInternalAsync(String.format(cql, AVAILABLE_RANGES),
                                                              rangesToUpdate(completedRanges),
                                                              keyspace));
        });
    }

    public static CompletableFuture<Set<Range<Token>>> getAvailableRanges(String keyspace, IPartitioner partitioner)
    {
        return TPCUtils.withLock(GLOBAL_LOCK, () -> {
            String query = "SELECT * FROM system.%s WHERE keyspace_name=?";
            return TPCUtils.toFuture(executeInternalAsync(format(query, AVAILABLE_RANGES), keyspace))
                           .thenApply(rs -> {
                               Set<Range<Token>> result = new HashSet<>();
                               for (UntypedResultSet.Row row : rs)
                               {
                                   Set<ByteBuffer> rawRanges = row.getSet("ranges", BytesType.instance);
                                   for (ByteBuffer rawRange : rawRanges)
                                   {
                                       result.add(byteBufferToRange(rawRange, partitioner));
                                   }
                               }
                               return ImmutableSet.copyOf(result);
                           });
        });
    }

    public static CompletableFuture<Void> resetAvailableRanges(String keyspace)
    {
        String cql = "UPDATE system.%s SET ranges = null WHERE keyspace_name = ?";
        return TPCUtils.toFutureVoid(executeInternalAsync(format(cql, AVAILABLE_RANGES), keyspace));
    }

    public static CompletableFuture<Void> resetAvailableRanges(String keyspace, Collection<Range<Token>> ranges)
    {
        String cql = "UPDATE system.%s SET ranges = ranges - ? WHERE keyspace_name = ?";
        return TPCUtils.toFutureVoid(executeInternalAsync(format(cql, AVAILABLE_RANGES), rangesToUpdate(ranges), keyspace));
    }

    public static void resetAvailableRangesBlocking()
    {
        ColumnFamilyStore availableRanges = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(AVAILABLE_RANGES);
        availableRanges.truncateBlocking();
    }

    public static CompletableFuture<Void> updateTransferredRanges(StreamOperation streamOperation,
                                               InetAddress peer,
                                               String keyspace,
                                               Collection<Range<Token>> streamedRanges)
    {
        return TPCUtils.withLock(GLOBAL_LOCK, () -> {
            String cql = "UPDATE system.%s SET ranges = ranges + ? WHERE operation = ? AND peer = ? AND keyspace_name = ?";
            return TPCUtils.toFutureVoid(executeInternalAsync(format(cql, TRANSFERRED_RANGES),
                                                              rangesToUpdate(streamedRanges), streamOperation.getDescription(), peer, keyspace));
        });
    }

    private static Set<ByteBuffer> rangesToUpdate(Collection<Range<Token>> ranges)
    {
        return ranges.stream()
                     .map(SystemKeyspace::rangeToBytes)
                     .collect(Collectors.toSet());
    }

    public static CompletableFuture<Map<InetAddress, Set<Range<Token>>>> getTransferredRanges(String description,
                                                                                              String keyspace,
                                                                                              IPartitioner partitioner)
    {
        return TPCUtils.withLock(GLOBAL_LOCK, () -> {
            String query = "SELECT * FROM system.%s WHERE operation = ? AND keyspace_name = ?";
            return TPCUtils.toFuture(executeInternalAsync(format(query, TRANSFERRED_RANGES), description, keyspace))
                           .thenApply(rs -> {
                               Map<InetAddress, Set<Range<Token>>> result = new HashMap<>();
                               for (UntypedResultSet.Row row : rs)
                               {
                                   InetAddress peer = row.getInetAddress("peer");
                                   Set<ByteBuffer> rawRanges = row.getSet("ranges", BytesType.instance);
                                   Set<Range<Token>> ranges = Sets.newHashSetWithExpectedSize(rawRanges.size());
                                   for (ByteBuffer rawRange : rawRanges)
                                   {
                                       ranges.add(byteBufferToRange(rawRange, partitioner));
                                   }
                                   result.put(peer, ranges);
                               }
                               return ImmutableMap.copyOf(result);
            });
        });
    }

    /**
     * Compare the release version in the system.local table with the one included in the distro.
     * If they don't match, snapshot all tables in the system and systen_schema keyspaces.
     * This is intended to be called at startup to create a backup of the system tables
     * during an upgrade.
     */
    public static CompletableFuture<Void> snapshotOnVersionChange()
    {
        return TPCUtils.toFuture(executeInternalAsync(format("SELECT release_version FROM %s.%s WHERE key='%s'",
                                                             SchemaConstants.SYSTEM_KEYSPACE_NAME, LOCAL, LOCAL))
                                 .observeOn(Schedulers.io()))
                       .thenAccept(result -> {
                           String previous = (result != null && !result.isEmpty() && result.one().has("release_version"))
                                             ? result.one().getString("release_version")
                                             : null;
                           String current = FBUtilities.getReleaseVersionString();

                           if (previous == null)
                           {
                               logger.info("No version in {}.{}. Current version is {}",
                                           SchemaConstants.SYSTEM_KEYSPACE_NAME, LOCAL, current);
                           }
                           else if (current.equals(previous))
                           {
                               logger.info("Detected current release version {} in {}.{}",
                                           current, SchemaConstants.SYSTEM_KEYSPACE_NAME, LOCAL);
                           }
                           else
                           {
                               logger.info("Detected version upgrade from {} to {}, snapshotting {} and {} keyspaces.",
                                           previous, current, SchemaConstants.SYSTEM_KEYSPACE_NAME, SchemaConstants.SCHEMA_KEYSPACE_NAME);
                               String snapshotName = Keyspace.getTimestampedSnapshotName(format("upgrade-%s-%s",
                                                                                                previous,
                                                                                                current));
                               Keyspace ks = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME);
                               try
                               {
                                   ks.snapshot(snapshotName, null);
                                   ks = Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME);
                                   ks.snapshot(snapshotName, null);
                               }
                               catch (IOException ex)
                               {
                                   throw new CompletionException(ex);
                               }
                           }
                       });

    }

    private static ByteBuffer rangeToBytes(Range<Token> range)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            // The format with which token ranges are serialized in the system tables is the pre-3.0 serialization
            // format for ranges.
            // In the future, it might be worth switching to a stable text format for the ranges to 1) save that and 2)
            // be more user friendly (the serialization format we currently use is pretty custom).
            Range.tokenSerializer.serialize(range, out, BoundsVersion.LEGACY);
            return out.buffer();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Range<Token> byteBufferToRange(ByteBuffer rawRange, IPartitioner partitioner)
    {
        try
        {
            // See rangeToBytes above for why version is 0.
            return (Range<Token>) Range.tokenSerializer.deserialize(ByteStreams.newDataInput(ByteBufferUtil.getArray(rawRange)),
                                                                    partitioner,
                                                                    BoundsVersion.LEGACY);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public static Single<UntypedResultSet> writePreparedStatement(String loggedKeyspace, MD5Digest key, String cql)
    {
        logger.debug("stored prepared statement for logged keyspace '{}': '{}'", loggedKeyspace, cql);
        return executeInternalAsync(
                format("INSERT INTO %s (logged_keyspace, prepared_id, query_string) VALUES (?, ?, ?)",
                PreparedStatements.toString()),
                loggedKeyspace, key.byteBuffer(), cql);
    }

    public static CompletableFuture<Void> removePreparedStatement(MD5Digest key)
    {
        return TPCUtils.toFutureVoid(executeInternalAsync(format("DELETE FROM %s WHERE prepared_id = ?", PreparedStatements.toString()),
                                                          key.byteBuffer()));
    }

    public static void resetPreparedStatementsBlocking()
    {
        ColumnFamilyStore preparedStatements = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(PREPARED_STATEMENTS);
        preparedStatements.truncateBlocking();
    }

    public static CompletableFuture<List<Pair<String, String>>> loadPreparedStatements()
    {
        String query = format("SELECT logged_keyspace, query_string FROM %s", PreparedStatements.toString());
        return TPCUtils.toFuture(executeOnceInternal(query)).thenApply(resultSet -> {
            List<Pair<String, String>> r = new ArrayList<>();
            for (UntypedResultSet.Row row : resultSet)
                r.add(Pair.create(row.has("logged_keyspace") ? row.getString("logged_keyspace") : null,
                                  row.getString("query_string")));
            return r;
        });
    }

    public static DecoratedKey decorateBatchKey(UUID id)
    {
        return BATCH_PARTITIONER.decorateKey(TimeUUIDType.instance.getSerializer().serialize(id));
    }
}
