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
package org.apache.cassandra.batchlog;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import com.google.common.util.concurrent.RateLimiter;

import io.reactivex.Completable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.WriteVerbs.WriteVersion;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WrappingWriteHandler;
import org.apache.cassandra.service.WriteEndpoints;
import org.apache.cassandra.service.WriteHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.flow.Threads;
import org.apache.cassandra.utils.versioning.Version;

import static com.google.common.collect.Iterables.transform;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternalWithPaging;

public class BatchlogManager implements BatchlogManagerMBean
{
    private static final WriteVersion CURRENT_VERSION = Version.last(WriteVersion.class);

    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=BatchlogManager";
    private static final long REPLAY_INITIAL_DELAY = Long.getLong("dse.batchlog.replay_initial_delay_in_ms", StorageService.RING_DELAY); // milliseconds
    private static final long REPLAY_INTERVAL = Long.getLong("dse.batchlog.replay_interval_in_ms", 10 * 1000); // milliseconds
    static final int DEFAULT_PAGE_SIZE = Integer.getInteger("dse.batchlog.page_size", 128);

    private static final Logger logger = LoggerFactory.getLogger(BatchlogManager.class);
    public static final BatchlogManager instance = new BatchlogManager();
    public static final long BATCHLOG_REPLAY_TIMEOUT = Long.getLong("cassandra.batchlog.replay_timeout_in_ms", DatabaseDescriptor.getWriteRpcTimeout() * 2);
    // number of replicas required to store batchlog for atomicity, at most 2
    private static final int REQUIRED_BATCHLOG_REPLICA_COUNT = Math.min(2, Integer.getInteger("dse.batchlog.required_replica_count", 2));

    private volatile long totalBatchesReplayed = 0; // no concurrency protection necessary as only written by replay thread.
    private volatile UUID lastReplayedUuid = UUIDGen.minTimeUUID(0);

    // Single-thread executor service for scheduling and serializing log replay.
    private final ScheduledExecutorService batchlogTasks;

    private final RateLimiter rateLimiter = RateLimiter.create(Double.MAX_VALUE);

    public BatchlogManager()
    {
        ScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("BatchlogTasks");
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        batchlogTasks = executor;
    }

    public void start()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        if (REPLAY_INTERVAL > 0L)
        {
            logger.debug("Scheduling batchlog replay initially after {} ms, then every {} ms", REPLAY_INITIAL_DELAY, REPLAY_INTERVAL);
            batchlogTasks.scheduleWithFixedDelay(this::replayFailedBatches,
                                                 REPLAY_INITIAL_DELAY,
                                                 REPLAY_INTERVAL,
                                                 TimeUnit.MILLISECONDS);
        }
        else
            logger.warn("Scheduled batchlog replay disabled (dse.batchlog.replay_interval_in_ms)");
    }

    public void shutdown() throws InterruptedException
    {
        batchlogTasks.shutdown();
        batchlogTasks.awaitTermination(60, TimeUnit.SECONDS);
    }

    public static Completable remove(UUID id)
    {
        if (logger.isTraceEnabled())
            logger.trace("Removing batch {}", id);

        return new Mutation(PartitionUpdate.fullPartitionDelete(
                SystemKeyspace.Batches,
                UUIDType.instance.decompose(id),
                FBUtilities.timestampMicros(),
                FBUtilities.nowInSeconds()))
            .applyAsync();
    }

    public static Completable store(Batch batch)
    {
        return store(batch, true);
    }

    public static Completable store(Batch batch, boolean durableWrites)
    {
        if (logger.isTraceEnabled())
            logger.trace("Storing batch {}", batch.id);

        List<ByteBuffer> mutations = new ArrayList<>(batch.encodedMutations.size() + batch.decodedMutations.size());
        mutations.addAll(batch.encodedMutations);

        for (Mutation mutation : batch.decodedMutations)
            mutations.add(Mutation.rawSerializers.get(CURRENT_VERSION.encodingVersion).serializedBuffer(mutation));

        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(SystemKeyspace.Batches, batch.id);
        builder.row()
               .timestamp(batch.creationTime)
               .add("version", MessagingService.current_version.protocolVersion().handshakeVersion)
               .appendAll("mutations", mutations);

        return builder.buildAsMutation().applyAsync(durableWrites, true);
    }

    @VisibleForTesting
    public int countAllBatches()
    {
        String query = String.format("SELECT count(*) FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BATCHES);
        // TODO make async?
        UntypedResultSet results = executeInternal(query);
        if (results == null || results.isEmpty())
            return 0;

        return (int) results.one().getLong("count");
    }

    public long getTotalBatchesReplayed()
    {
        return totalBatchesReplayed;
    }

    public void forceBatchlogReplay() throws Exception
    {
        startBatchlogReplay().get();
    }

    public Future<?> startBatchlogReplay()
    {
        // If a replay is already in progress this request will be executed after it completes.
        return batchlogTasks.submit(this::replayFailedBatches);
    }

    void performInitialReplay() throws InterruptedException, ExecutionException
    {
        // Invokes initial replay. Used for testing only.
        batchlogTasks.submit(this::replayFailedBatches).get();
    }

    private void replayFailedBatches()
    {
        logger.trace("Started replayFailedBatches");

        // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
        // max rate is scaled by the number of nodes in the cluster (same as for HHOM - see CASSANDRA-5272).
        int endpointsCount = StorageService.instance.getTokenMetadata().getSizeOfAllEndpoints();
        if (endpointsCount <= 0)
        {
            logger.trace("Replay cancelled as there are no peers in the ring.");
            return;
        }
        setRate(DatabaseDescriptor.getBatchlogReplayThrottleInKB());

        UUID limitUuid = UUIDGen.maxTimeUUID(System.currentTimeMillis() - getBatchlogTimeout());
        ColumnFamilyStore store = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES);
        int pageSize = calculatePageSize(store);
        // There cannot be any live content where token(id) <= token(lastReplayedUuid) as every processed batch is
        // deleted, but the tombstoned content may still be present in the tables. To avoid walking over it we specify
        // token(id) > token(lastReplayedUuid) as part of the query.
        String query = String.format("SELECT id, mutations, version FROM %s.%s WHERE token(id) > token(?) AND token(id) <= token(?)",
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                     SystemKeyspace.BATCHES);
        UntypedResultSet batches = executeInternalWithPaging(query, PageSize.rowsSize(pageSize), lastReplayedUuid, limitUuid);
        processBatchlogEntries(batches, pageSize, rateLimiter);
        lastReplayedUuid = limitUuid;
        logger.trace("Finished replayFailedBatches");
    }

    /**
     * Sets the rate for the current rate limiter. When {@code throttleInKB} is 0, this sets the rate to
     * {@link Double#MAX_VALUE} bytes per second.
     *
     * @param throttleInKB throughput to set in KB per second
     */
    public void setRate(final int throttleInKB)
    {
        int endpointsCount = StorageService.instance.getTokenMetadata().getSizeOfAllEndpoints();
        if (endpointsCount > 0)
        {
            int endpointThrottleInKB = throttleInKB / endpointsCount;
            double throughput = endpointThrottleInKB == 0 ? Double.MAX_VALUE : endpointThrottleInKB * 1024.0;
            if (rateLimiter.getRate() != throughput)
            {
                logger.debug("Updating batchlog replay throttle to {} KB/s, {} KB/s per endpoint", throttleInKB, endpointThrottleInKB);
                rateLimiter.setRate(throughput);
            }
        }
    }

    // read less rows (batches) per page if they are very large
    static int calculatePageSize(ColumnFamilyStore store)
    {
        double averageRowSize = store.getMeanPartitionSize();
        if (averageRowSize <= 0)
            return DEFAULT_PAGE_SIZE;

        return (int) Math.max(1, Math.min(DEFAULT_PAGE_SIZE, 4 * 1024 * 1024 / averageRowSize));
    }

    private WriteVersion getVersion(UntypedResultSet.Row row, String name)
    {
        int messagingVersion = row.getInt(name);
        MessagingVersion version = MessagingVersion.fromHandshakeVersion(messagingVersion);
        return version.groupVersion(Verbs.Group.WRITES);
    }

    // TODO make this process everything async?
    private void processBatchlogEntries(UntypedResultSet batches, int pageSize, RateLimiter rateLimiter)
    {
        ArrayList<ReplayingBatch> unfinishedBatches = new ArrayList<>(pageSize);

        Set<InetAddress> hintedNodes = new HashSet<>();
        Set<UUID> replayedBatches = new HashSet<>();

        // Sending out batches for replay without waiting for them, so that one stuck batch doesn't affect others
        TPCUtils.blockingGet(Threads.observeOn(batches.rows(), TPC.ioScheduler(), TPCTaskType.BATCH_REPLAY)
                                    .reduceToRxSingle(0, (positionInPage, row) ->
        {
            UUID id = row.getUUID("id");
            WriteVersion version = getVersion(row, "version");
            try
            {
                ReplayingBatch batch = new ReplayingBatch(id, version, row.getList("mutations", BytesType.instance));
                if (batch.replay(rateLimiter, hintedNodes))
                {
                    unfinishedBatches.add(batch);
                }
                else
                {
                    // we get here, if there are no live endpoints for the current batch
                    StorageMetrics.hintedBatchlogReplays.mark();
                    if (logger.isTraceEnabled())
                        logger.trace("Failed to replay batchlog {} of age {} seconds with {} mutations, will write hints. Reason/failure: remote endpoints not alive",
                                     id, TimeUnit.MILLISECONDS.toSeconds(batch.ageInMillis()), batch.mutations.size());
                    remove(id).blockingAwait(); // no write mutations were sent (either expired or all CFs involved truncated).
                    ++totalBatchesReplayed;
                }
            }
            catch (IOException e)
            {
                logger.warn("Skipped batch replay of {} due to {}", id, e.getMessage());
                remove(id).blockingAwait();
            }

            if (++positionInPage == pageSize)
            {
                // We have reached the end of a batch. To avoid keeping more than a page of mutations in memory,
                // finish processing the page before requesting the next row.
                finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);
                return 0;
            }
            return positionInPage;
        }));

        finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);

        // to preserve batch guarantees, we must ensure that hints (if any) have made it to disk, before deleting the batches
        if (!hintedNodes.isEmpty())
        {
            logger.trace("Batchlog replay created hints for {} nodes", hintedNodes.size());
            HintsService.instance.flushAndFsyncBlockingly(transform(hintedNodes, StorageService.instance::getHostIdForEndpoint));
        }

        // once all generated hints are fsynced, actually delete the batches
        replayedBatches.forEach(uuid -> BatchlogManager.remove(uuid).blockingAwait());
    }

    private void finishAndClearBatches(ArrayList<ReplayingBatch> batches, Set<InetAddress> hintedNodes, Set<UUID> replayedBatches)
    {
        // schedule hints for timed out deliveries
        for (ReplayingBatch batch : batches)
        {
            batch.finish(hintedNodes);
            replayedBatches.add(batch.id);
        }

        totalBatchesReplayed += batches.size();
        batches.clear();
    }

    public static long getBatchlogTimeout()
    {
        return BATCHLOG_REPLAY_TIMEOUT; // enough time for the actual write + BM removal mutation
    }

    private static class ReplayingBatch
    {
        private final UUID id;
        private final long writtenAt;
        private final List<Mutation> mutations;
        private final int replayedBytes;

        private List<ReplayWriteHandler> replayHandlers;

        ReplayingBatch(UUID id, WriteVersion version, List<ByteBuffer> serializedMutations) throws IOException
        {
            this.id = id;
            this.writtenAt = UUIDGen.unixTimestamp(id);
            this.mutations = new ArrayList<>(serializedMutations.size());
            this.replayedBytes = addMutations(version, serializedMutations);
        }

        public long ageInMillis()
        {
            return System.currentTimeMillis() - UUIDGen.getAdjustedTimestamp(id);
        }

        public boolean replay(RateLimiter rateLimiter, Set<InetAddress> hintedNodes) throws IOException
        {
            logger.trace("Replaying batch {}", id);

            if (mutations.isEmpty())
                return false;

            int gcgs = gcgs(mutations);
            if (TimeUnit.MILLISECONDS.toSeconds(writtenAt) + gcgs <= FBUtilities.nowInSeconds())
                return false;

            replayHandlers = sendReplays(mutations, writtenAt, hintedNodes);

            rateLimiter.acquire(replayedBytes); // acquire afterwards, to not mess up ttl calculation.

            return !replayHandlers.isEmpty();
        }

        public void finish(Set<InetAddress> hintedNodes)
        {
            boolean replayFailed = false;

            for (int i = 0; i < replayHandlers.size(); i++)
            {
                ReplayWriteHandler handler = replayHandlers.get(i);
                try
                {
                    handler.get();
                }
                catch (WriteTimeoutException | WriteFailureException e)
                {
                    // only write hints for failed mutations
                    writeHintsForUndeliveredEndpoints(i, hintedNodes);
                    if (!replayFailed) {
                        replayFailed = true;
                        StorageMetrics.hintedBatchlogReplays.mark();
                        if (logger.isTraceEnabled())
                            logger.trace("Failed to replay batchlog {} of age {} seconds with {} mutations, will write hints. Reason/failure: {}",
                                         id, TimeUnit.MILLISECONDS.toSeconds(ageInMillis()), mutations.size(), e.getMessage());
                    }
                }
            }
            if (replayFailed)
                return;

            if (logger.isTraceEnabled())
                logger.trace("Finished replay of batchlog {} of age {} seconds with {} mutations",
                             id, TimeUnit.MILLISECONDS.toSeconds(ageInMillis()), mutations.size());
            StorageMetrics.batchlogReplays.mark();
        }

        private int addMutations(WriteVersion version, List<ByteBuffer> serializedMutations) throws IOException
        {
            int ret = 0;
            for (ByteBuffer serializedMutation : serializedMutations)
            {
                ret += serializedMutation.remaining();
                try (DataInputBuffer in = new DataInputBuffer(serializedMutation, true))
                {
                    addMutation(Mutation.serializers.get(version).deserialize(in));
                }
            }

            return ret;
        }

        // Remove CFs that have been truncated since. writtenAt and SystemTable#getTruncatedAt() both return millis.
        // We don't abort the replay entirely b/c this can be considered a success (truncated is same as delivered then
        // truncated.
        private void addMutation(Mutation mutation)
        {
            for (TableId tableId : mutation.getTableIds())
                if (writtenAt <= SystemKeyspace.getTruncatedAt(tableId))
                    mutation = mutation.without(tableId);

            if (!mutation.isEmpty())
                mutations.add(mutation);
        }

        private void writeHintsForUndeliveredEndpoints(int index, Set<InetAddress> hintedNodes)
        {
            int gcgs = mutations.get(index).smallestGCGS();

            // expired
            if (TimeUnit.MILLISECONDS.toSeconds(writtenAt) + gcgs <= FBUtilities.nowInSeconds())
                return;

            ReplayWriteHandler handler = replayHandlers.get(index);
            Mutation undeliveredMutation = mutations.get(index);

            if (handler != null)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Adding hints for undelivered endpoints: {}", handler.undelivered);
                hintedNodes.addAll(handler.undelivered);
                HintsService.instance.write(transform(handler.undelivered, StorageService.instance::getHostIdForEndpoint),
                                            Hint.create(undeliveredMutation, writtenAt));
            }
        }

        private static List<ReplayWriteHandler> sendReplays(List<Mutation> mutations,
                                                            long writtenAt,
                                                            Set<InetAddress> hintedNodes)
        {
            List<ReplayWriteHandler> handlers = new ArrayList<>(mutations.size());
            for (Mutation mutation : mutations)
            {
                ReplayWriteHandler handler = sendSingleReplayMutation(mutation, writtenAt, hintedNodes);
                if (handler != null)
                    handlers.add(handler);
            }
            return handlers;
        }

        private static ReplayWriteHandler sendSingleReplayMutation(final Mutation mutation,
                                                                   long writtenAt,
                                                                   Set<InetAddress> hintedNodes)
        {
            WriteEndpoints endpoints = WriteEndpoints.compute(mutation);
            for (InetAddress dead : endpoints.dead())
            {
                hintedNodes.add(dead);
                HintsService.instance.write(StorageService.instance.getHostIdForEndpoint(dead),
                                            Hint.create(mutation, writtenAt));
            }

            // We iterate over 'endpoints.live()' in order to indicate to the caller that all
            // other endpoints beside the local node are not alive and there is no need to bother
            // the timeout logic to fire for something that we already know (that the endpoints
            // are dead).
            WriteEndpoints writeEndpoints = endpoints;
            if (endpoints.live().contains(FBUtilities.getBroadcastAddress()))
            {
                mutation.apply();
                if (endpoints.live().size() == 1)
                    return null;
                writeEndpoints = endpoints.withoutLocalhost(true);
            }

            // writeEndpoints contains the _remote_ endpoints that are targeted for the write.

            if (writeEndpoints.liveCount() == 0)
                // no endpoints beside the local node are alive - no need to create a handler
                return null;

            ReplayWriteHandler handler = ReplayWriteHandler.create(writeEndpoints, System.nanoTime());
            String localDataCenter = DatabaseDescriptor.getLocalDataCenter();
            Verb.AckedRequest<Mutation> messageDefinition = Verbs.WRITES.WRITE;
            MessagingService.instance().send(messageDefinition.newForwardingDispatcher(writeEndpoints.live(), localDataCenter, mutation), handler);

            return handler;
        }

        private static int gcgs(Collection<Mutation> mutations)
        {
            int gcgs = Integer.MAX_VALUE;
            for (Mutation mutation : mutations)
                gcgs = Math.min(gcgs, mutation.smallestGCGS());
            return gcgs;
        }

        /**
         * A WriteHandler that stores the addresses of the endpoints from
         * which we did not receive a successful reply.
         */
        private static class ReplayWriteHandler extends WrappingWriteHandler
        {
            private final Set<InetAddress> undelivered = Collections.newSetFromMap(new ConcurrentHashMap<>());

            private ReplayWriteHandler(WriteHandler handler)
            {
                super(handler);
                Iterables.addAll(undelivered, handler.endpoints());
            }

            static ReplayWriteHandler create(WriteEndpoints endpoints, long queryStartNanos)
            {
                WriteHandler handler = WriteHandler.builder(endpoints,
                                                            ConsistencyLevel.ALL,
                                                            WriteType.UNLOGGED_BATCH,
                                                            queryStartNanos)
                                                   .blockFor(endpoints.liveCount())
                                                   .build();
                return new ReplayWriteHandler(handler);
            }

            @Override
            public void onResponse(Response<EmptyPayload> m)
            {
                boolean removed = undelivered.remove(m.from());
                assert removed : "did not expect a response from " + m.from() + " / expected are " + endpoints().live();
                super.onResponse(m);
            }
        }
    }

    /**
     * There are three strategies to select the "best" batchlog store candidates. See the descriptions
     * of the strategies in {@link Config.BatchlogEndpointStrategy}.
     *
     *
     * @param consistencyLevel
     * @param localRack the name of the local rack
     * @param localDcEndpoints the endpoints as a multimap of rack to endpoints.
     *                  Implementation note: this one comes directly from
     *                  {@link TokenMetadata.Topology#getDatacenterRacks() TokenMetadata.Topology.getDatacenterRacks().get(LOCAL_DC)}.
     * @return list of endpoints to store the batchlog
     */
    public static Collection<InetAddress> filterEndpoints(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddress> localDcEndpoints)
    {
        EndpointFilter filter = endpointFilter(consistencyLevel, localRack, localDcEndpoints);
        return filter.filter();
    }

    @VisibleForTesting
    static EndpointFilter endpointFilter(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddress> localDcEndpoints)
    {
        return DatabaseDescriptor.getBatchlogEndpointStrategy().dynamicSnitch
               && DatabaseDescriptor.isDynamicEndpointSnitch()
                                    ? new DynamicEndpointFilter(consistencyLevel, localRack, localDcEndpoints)
                                    : new RandomEndpointFilter(consistencyLevel, localRack, localDcEndpoints);
    }

    /**
     * Picks endpoints for batchlög storage according to {@link org.apache.cassandra.locator.DynamicEndpointSnitch}.
     *
     * Unlike the default ({@link RandomEndpointFilter}) implementation, this one allows batchlog replicas in
     * the local rack.
     *
     * The implementation picks the fastest endpoint of one rack, the 2nd fastest node from another rack,
     * until {@link #REQUIRED_BATCHLOG_REPLICA_COUNT} endpoints have been collected. Endpoints in the same
     * rack are avoided.
     *
     * It can either try to prevent the local rack {@link Config.BatchlogEndpointStrategy#dynamic_remote}
     * or not ({@link Config.BatchlogEndpointStrategy#dynamic}).
     *
     * The tradeoff here is performance over availability (local rack is eligible for batchlog storage).
     */
    public static class DynamicEndpointFilter extends EndpointFilter
    {
        DynamicEndpointFilter(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddress> endpoints)
        {
            super(consistencyLevel, localRack, endpoints);
        }

        public Collection<InetAddress> filter()
        {
            Collection<InetAddress> allEndpoints = endpoints.values();
            int endpointCount = allEndpoints.size();
            if (endpointCount <= REQUIRED_BATCHLOG_REPLICA_COUNT)
                return checkFewEndpoints(allEndpoints, endpointCount);

            // strip out dead endpoints and localhost
            ListMultimap<String, InetAddress> validated = validatedNodes(endpointCount);

            // Return all validated endpoints, if we cannot achieve REQUIRED_BATCHLOG_REPLICA_COUNT.
            // This step is mandatory _before_ we filter the local rack.
            int numValidated = validated.size();
            if (numValidated <= REQUIRED_BATCHLOG_REPLICA_COUNT)
                return notEnoughAvailableEndpoints(validated);

            if (!DatabaseDescriptor.getBatchlogEndpointStrategy().allowLocalRack)
            {
                filterLocalRack(validated);

                // Return all validated endpoints, if we cannot achieve REQUIRED_BATCHLOG_REPLICA_COUNT.
                // This is a mandatory step - otherwise the for-loop below could loop forever, if
                // REQUIRED_BATCHLOG_REPLICA_COUNT can't be achieved.
                numValidated = validated.size();
                if (numValidated <= REQUIRED_BATCHLOG_REPLICA_COUNT)
                    return notEnoughAvailableEndpoints(validated);
            }

            // sort _all_ nodes to pick the best racks
            List<InetAddress> sorted = reorder(validated.values());

            List<InetAddress> result = new ArrayList<>(REQUIRED_BATCHLOG_REPLICA_COUNT);
            Set<String> racks = new HashSet<>();

            while (result.size() < REQUIRED_BATCHLOG_REPLICA_COUNT)
            {
                for (InetAddress endpoint : sorted)
                {
                    if (result.size() == REQUIRED_BATCHLOG_REPLICA_COUNT)
                        break;

                    if (racks.isEmpty())
                        racks.addAll(validated.keySet());

                    String rack = DatabaseDescriptor.getEndpointSnitch().getRack(endpoint);
                    if (!racks.remove(rack))
                        continue;
                    if (result.contains(endpoint))
                        continue;

                    result.add(endpoint);
                }
            }

            return result;
        }

        List<InetAddress> reorder(Collection<InetAddress> endpoints)
        {
            return DatabaseDescriptor.getEndpointSnitch().getSortedListByProximity(getCoordinator(), endpoints);
        }
    }

    /**
     * This is the default endpoint-filter implementation for logged batches.
     *
     * It picks random endpoints from random racks. Endpoints from non-local racks are preferred.
     * Also prefers to pick endpoints from as many different racks as possible.
     *
     * The tradeoff here is performance over availability (local rack is eligible for batchlog storage).
     */
    public static class RandomEndpointFilter extends EndpointFilter
    {
        RandomEndpointFilter(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddress> endpoints)
        {
            super(consistencyLevel, localRack, endpoints);
        }

        public Collection<InetAddress> filter()
        {
            Collection<InetAddress> allEndpoints = endpoints.values();
            int endpointCount = allEndpoints.size();
            if (endpointCount <= REQUIRED_BATCHLOG_REPLICA_COUNT)
                return checkFewEndpoints(allEndpoints, endpointCount);

            // strip out dead endpoints and localhost
            ListMultimap<String, InetAddress> validated = validatedNodes(endpointCount);

            // Return all validated endpoints, if we cannot achieve REQUIRED_BATCHLOG_REPLICA_COUNT.
            // This step is mandatory _before_ we filter the local rack.
            int numValidated = validated.size();
            if (numValidated <= REQUIRED_BATCHLOG_REPLICA_COUNT)
                return notEnoughAvailableEndpoints(validated);

            filterLocalRack(validated);

            // Return all validated endpoints, if we cannot achieve REQUIRED_BATCHLOG_REPLICA_COUNT.
            // This is a mandatory step - otherwise the for-loop below could loop forever, if
            // REQUIRED_BATCHLOG_REPLICA_COUNT can't be achieved.
            numValidated = validated.size();
            if (numValidated <= REQUIRED_BATCHLOG_REPLICA_COUNT)
                return notEnoughAvailableEndpoints(validated);

            // Randomize the racks - all we need is the collections of the endpoints per rack.
            List<Collection<InetAddress>> rackNodes = new ArrayList<>(validated.asMap().values());
            Collections.shuffle(rackNodes, ThreadLocalRandom.current());

            // Now iterate over the racks (one after each other) and randomly pick one node
            // from each rack. Repeat that until we've reached REQUIRED_BATCHLOG_REPLICA_COUNT.
            List<InetAddress> result = new ArrayList<>(REQUIRED_BATCHLOG_REPLICA_COUNT);
            for (int i = 0; result.size() < REQUIRED_BATCHLOG_REPLICA_COUNT; )
            {
                // cast to List is safe in this case, because it's an ArrayListMultimap
                List<InetAddress> singleRack = (List) rackNodes.get(i);
                InetAddress endpoint = singleRack.get(ThreadLocalRandom.current().nextInt(singleRack.size()));
                if (result.contains(endpoint))
                    continue;

                result.add(endpoint);

                i++;
                if (i == rackNodes.size())
                    i = 0;
            }

            return result;
        }
    }

    public static abstract class EndpointFilter
    {
        final ConsistencyLevel consistencyLevel;
        final String localRack;
        final Multimap<String, InetAddress> endpoints;

        @VisibleForTesting
        EndpointFilter(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddress> endpoints)
        {
            this.consistencyLevel = batchlogConsistencyLevel(consistencyLevel);
            this.localRack = localRack;
            this.endpoints = endpoints;
        }

        static ConsistencyLevel batchlogConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            if (consistencyLevel == ConsistencyLevel.ANY)
                return ConsistencyLevel.ANY;
            return REQUIRED_BATCHLOG_REPLICA_COUNT == 2 ? ConsistencyLevel.TWO : ConsistencyLevel.ONE;
        }

        /**
         * Generate the list of endpoints for batchlog hosting. If possible these will be
         * {@code REQUIRED_BATCHLOG_REPLICA_COUNT} nodes from different racks.
         */
        public abstract Collection<InetAddress> filter();

        ListMultimap<String, InetAddress> validatedNodes(int endpointCount)
        {
            int rackCount = endpoints.keySet().size();

            ListMultimap<String, InetAddress> validated = ArrayListMultimap.create(rackCount, endpointCount / rackCount);
            for (Map.Entry<String, InetAddress> entry : endpoints.entries())
                if (isValid(entry.getValue()))
                    validated.put(entry.getKey(), entry.getValue());

            return validated;
        }

        void filterLocalRack(ListMultimap<String, InetAddress> validated)
        {
            // If there are at least REQUIRED_BATCHLOG_REPLICA_COUNT nodes in _other_ racks,
            // then exclude the local rack
            if (validated.size() - validated.get(localRack).size() >= REQUIRED_BATCHLOG_REPLICA_COUNT)
                validated.removeAll(localRack);
        }

        /**
         * Called when there are not enough endpoints <em>commissioned</em>
         * to achieve {@link #REQUIRED_BATCHLOG_REPLICA_COUNT}.
         */
        Collection<InetAddress> checkFewEndpoints(Collection<InetAddress> allEndpoints, int totalEndpointCount)
        {
            int available = 0;
            for (InetAddress ep : allEndpoints)
                if (isAlive(ep))
                    available++;

            allEndpoints = maybeThrowUnavailableException(allEndpoints, totalEndpointCount, available);

            return allEndpoints;
        }

        /**
         * Called when there are not enough commissioned endpoints <em>available</em>
         * to achieve {@link #REQUIRED_BATCHLOG_REPLICA_COUNT}.
         */
        Collection<InetAddress> notEnoughAvailableEndpoints(ListMultimap<String, InetAddress> validated)
        {
            Collection<InetAddress> validatedEndpoints = validated.values();
            int validatedCount = validatedEndpoints.size();

            validatedEndpoints = maybeThrowUnavailableException(validatedEndpoints, REQUIRED_BATCHLOG_REPLICA_COUNT, validatedCount);

            return validatedEndpoints;
        }

        Collection<InetAddress> maybeThrowUnavailableException(Collection<InetAddress> endpoints,
                                                               int totalEndpointCount,
                                                               int available)
        {
            // This is exactly what the pre DB-1367 code does (one exception):
            // - If there are no available nodes AND batchlog-consistency == ANY, then use
            //   the local node as the batchlog endpoint.
            // - If there are no available nodes AND batchlog-consistency, then ...
            //   return *NO* batchlog endpoints, causing the batchlog write to timeout
            //   (waiting for noting - nothing can trigger the condition in the write handler).
            //   Changed to immediately throw an UnavailableException.
            // - If there are less batchlog endpoint candidates available than required,
            //   just use those (despite the "usual" guarantee of two batchlog-replicas).
            // - The batchlog-consistency is not respected (beside the CL ANY check
            //   if there are no available endpoints).
            // - Batchlog endpoints are always chosen from the local DC. The exact racks
            //   and endpoints depend on the strategy. The "random_remote" strategy is what's
            //   been in since forever.

            if (available == 0)
            {
                if (consistencyLevel == ConsistencyLevel.ANY)
                    return Collections.singleton(FBUtilities.getBroadcastAddress());

                // New/changed since DB-1367: we immediately throw an UnavailableException here instead
                // of letting the batchlog write unnecessarily timeout.
                throw new UnavailableException("Cannot achieve consistency level " + consistencyLevel
                                               + " for batchlog in local DC, required:" + totalEndpointCount
                                               + ", available:" + available,
                                               consistencyLevel,
                                               totalEndpointCount,
                                               available);
            }

            return endpoints;
        }

        @VisibleForTesting
        protected boolean isValid(InetAddress input)
        {
            return !input.equals(getCoordinator()) && isAlive(input);
        }

        @VisibleForTesting
        protected boolean isAlive(InetAddress input)
        {
            return FailureDetector.instance.isAlive(input);
        }

        @VisibleForTesting
        protected InetAddress getCoordinator()
        {
            return FBUtilities.getBroadcastAddress();
        }
    }
}
