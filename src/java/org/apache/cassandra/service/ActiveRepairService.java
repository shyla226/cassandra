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
package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.LocalAntiCompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.repair.AnticompactionTask;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairSession;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.UUIDGen;

/**
 * ActiveRepairService is the starting point for manual "active" repairs.
 *
 * Each user triggered repair will correspond to one or multiple repair session,
 * one for each token range to repair. On repair session might repair multiple
 * column families. For each of those column families, the repair session will
 * request merkle trees for each replica of the range being repaired, diff those
 * trees upon receiving them, schedule the streaming ofthe parts to repair (based on
 * the tree diffs) and wait for all those operation. See RepairSession for more
 * details.
 *
 * The creation of a repair session is done through the submitRepairSession that
 * returns a future on the completion of that session.
 */
public class ActiveRepairService implements IEndpointStateChangeSubscriber, IFailureDetectionEventListener
{
    /**
     * @deprecated this statuses are from the previous JMX notification service,
     * which will be deprecated on 4.0. For statuses of the new notification
     * service, see {@link org.apache.cassandra.streaming.StreamEvent.ProgressEvent}
     */
    @Deprecated
    public enum Status
    {
        STARTED, SESSION_SUCCESS, SESSION_FAILED, FINISHED
    }
    private boolean registeredForEndpointChanges = false;

    public static CassandraVersion SUPPORTS_GLOBAL_PREPARE_FLAG_VERSION = new CassandraVersion("2.2.1");

    private static final Logger logger = LoggerFactory.getLogger(ActiveRepairService.class);
    // singleton enforcement
    public static final ActiveRepairService instance = new ActiveRepairService(FailureDetector.instance, Gossiper.instance);

    public static final long UNREPAIRED_SSTABLE = 0;

    /**
     * A map of active coordinator session.
     */
    private final ConcurrentMap<UUID, RepairSession> sessions = new ConcurrentHashMap<>();

    private final ConcurrentMap<UUID, ParentRepairSession> parentRepairSessions = new ConcurrentHashMap<>();

    private final IFailureDetector failureDetector;
    private final Gossiper gossiper;

    public ActiveRepairService(IFailureDetector failureDetector, Gossiper gossiper)
    {
        this.failureDetector = failureDetector;
        this.gossiper = gossiper;
    }

    /**
     * Requests repairs for the given keyspace and column families.
     *
     * @return Future for asynchronous call or null if there is no need to repair
     */
    public RepairSession submitRepairSession(UUID parentRepairSession,
                                             Collection<Range<Token>> range,
                                             String keyspace,
                                             RepairParallelism parallelismDegree,
                                             Set<InetAddress> endpoints,
                                             long repairedAt,
                                             ListeningExecutorService executor,
                                             String... cfnames)
    {
        if (endpoints.isEmpty())
            return null;

        if (cfnames.length == 0)
            return null;

        final RepairSession session = new RepairSession(parentRepairSession, UUIDGen.getTimeUUID(), range, keyspace, parallelismDegree, endpoints, repairedAt, cfnames);

        sessions.put(session.getId(), session);
        // register listeners
        registerOnFdAndGossip(session);

        // remove session at completion
        session.addListener(new Runnable()
        {
            /**
             * When repair finished, do clean up
             */
            public void run()
            {
                sessions.remove(session.getId());
            }
        }, MoreExecutors.sameThreadExecutor());
        session.start(executor);
        return session;
    }

    private <T extends AbstractFuture &
               IEndpointStateChangeSubscriber &
               IFailureDetectionEventListener> void registerOnFdAndGossip(final T task)
    {
        gossiper.register(task);
        failureDetector.registerFailureDetectionEventListener(task);

        // unregister listeners at completion
        task.addListener(new Runnable()
        {
            /**
             * When repair finished, do clean up
             */
            public void run()
            {
                failureDetector.unregisterFailureDetectionEventListener(task);
                gossiper.unregister(task);
            }
        }, MoreExecutors.sameThreadExecutor());
    }

    public synchronized void terminateSessions()
    {
        Throwable cause = new IOException("Terminate session is called");
        for (RepairSession session : sessions.values())
        {
            session.forceShutdown(cause);
        }
        for (ParentRepairSession prs : parentRepairSessions.values())
        {
            prs.cleanup();
        }
        parentRepairSessions.clear();
    }

    /**
     * Return all of the neighbors with whom we share the provided range.
     *
     * @param keyspaceName keyspace to repair
     * @param keyspaceLocalRanges local-range for given keyspaceName
     * @param toRepair token to repair
     * @param dataCenters the data centers to involve in the repair
     *
     * @return neighbors with whom we share the provided range
     */
    public static Set<InetAddress> getNeighbors(String keyspaceName, Collection<Range<Token>> keyspaceLocalRanges,
                                                Range<Token> toRepair, Collection<String> dataCenters,
                                                Collection<String> hosts)
    {
        StorageService ss = StorageService.instance;
        Map<Range<Token>, List<InetAddress>> replicaSets = ss.getRangeToAddressMap(keyspaceName);
        Range<Token> rangeSuperSet = null;
        for (Range<Token> range : keyspaceLocalRanges)
        {
            if (range.contains(toRepair))
            {
                rangeSuperSet = range;
                break;
            }
            else if (range.intersects(toRepair))
            {
                throw new IllegalArgumentException(String.format("Requested range %s intersects a local range (%s) " +
                                                                 "but is not fully contained in one; this would lead to " +
                                                                 "imprecise repair. keyspace: %s", toRepair.toString(),
                                                                 range.toString(), keyspaceName));
            }
        }
        if (rangeSuperSet == null || !replicaSets.containsKey(rangeSuperSet))
            return Collections.emptySet();

        Set<InetAddress> neighbors = new HashSet<>(replicaSets.get(rangeSuperSet));
        neighbors.remove(FBUtilities.getBroadcastAddress());

        if (dataCenters != null && !dataCenters.isEmpty())
        {
            TokenMetadata.Topology topology = ss.getTokenMetadata().cloneOnlyTokenMap().getTopology();
            Set<InetAddress> dcEndpoints = Sets.newHashSet();
            Multimap<String,InetAddress> dcEndpointsMap = topology.getDatacenterEndpoints();
            for (String dc : dataCenters)
            {
                Collection<InetAddress> c = dcEndpointsMap.get(dc);
                if (c != null)
                    dcEndpoints.addAll(c);
            }
            return Sets.intersection(neighbors, dcEndpoints);
        }
        else if (hosts != null && !hosts.isEmpty())
        {
            Set<InetAddress> specifiedHost = new HashSet<>();
            for (final String host : hosts)
            {
                try
                {
                    final InetAddress endpoint = InetAddress.getByName(host.trim());
                    if (endpoint.equals(FBUtilities.getBroadcastAddress()) || neighbors.contains(endpoint))
                        specifiedHost.add(endpoint);
                }
                catch (UnknownHostException e)
                {
                    throw new IllegalArgumentException("Unknown host specified " + host, e);
                }
            }

            if (!specifiedHost.contains(FBUtilities.getBroadcastAddress()))
                throw new IllegalArgumentException("The current host must be part of the repair");

            if (specifiedHost.size() <= 1)
            {
                String msg = "Repair requires at least two endpoints that are neighbours before it can continue, the endpoint used for this repair is %s, " +
                             "other available neighbours are %s but these neighbours were not part of the supplied list of hosts to use during the repair (%s).";
                throw new IllegalArgumentException(String.format(msg, specifiedHost, neighbors, hosts));
            }

            specifiedHost.remove(FBUtilities.getBroadcastAddress());
            return specifiedHost;

        }

        return neighbors;
    }

    public UUID prepareForRepair(UUID parentRepairSession, InetAddress coordinator, Set<InetAddress> endpoints, RepairOption options, List<ColumnFamilyStore> columnFamilyStores)
    {
        long timestamp = System.currentTimeMillis();
        registerParentRepairSession(parentRepairSession, coordinator, columnFamilyStores, options.getRanges(), options.isIncremental(), timestamp, options.isGlobal());
        final CountDownLatch prepareLatch = new CountDownLatch(endpoints.size());
        final AtomicBoolean status = new AtomicBoolean(true);
        final Set<String> failedNodes = Collections.synchronizedSet(new HashSet<String>());
        IAsyncCallbackWithFailure callback = new IAsyncCallbackWithFailure()
        {
            public void response(MessageIn msg)
            {
                prepareLatch.countDown();
            }

            public boolean isLatencyForSnitch()
            {
                return false;
            }

            public void onFailure(InetAddress from)
            {
                status.set(false);
                failedNodes.add(from.getHostAddress());
                prepareLatch.countDown();
            }
        };

        List<UUID> cfIds = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfs : columnFamilyStores)
            cfIds.add(cfs.metadata.cfId);

        for (InetAddress neighbour : endpoints)
        {
            if (FailureDetector.instance.isAlive(neighbour))
            {
                PrepareMessage message = new PrepareMessage(parentRepairSession, cfIds, options.getRanges(), options.isIncremental(), timestamp, options.isGlobal());
                MessageOut<RepairMessage> msg = message.createMessage();
                MessagingService.instance().sendRR(msg, neighbour, callback, TimeUnit.HOURS.toMillis(1), true);
            }
            else
            {
                // bailout early to avoid potentially waiting for a long time.
                failRepair(parentRepairSession, "Endpoint not alive: " + neighbour);
            }
        }

        try
        {
            // Failed repair is expensive so we wait for longer time.
            if (!prepareLatch.await(1, TimeUnit.HOURS)) {
                failRepair(parentRepairSession, "Did not get replies from all endpoints.");
            }
        }
        catch (InterruptedException e)
        {
            failRepair(parentRepairSession, "Interrupted while waiting for prepare repair response.");
        }

        if (!status.get())
        {
            failRepair(parentRepairSession, "Got negative replies from endpoints " + failedNodes);
        }

        return parentRepairSession;
    }

    private void failRepair(UUID parentRepairSession, String errorMsg) {
        removeParentRepairSession(parentRepairSession);
        throw new RuntimeException(errorMsg);
    }

    public synchronized void registerParentRepairSession(UUID parentRepairSession, InetAddress coordinator, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental, long timestamp, boolean isGlobal)
    {
        if (!registeredForEndpointChanges)
        {
            Gossiper.instance.register(this);
            FailureDetector.instance.registerFailureDetectionEventListener(this);
            registeredForEndpointChanges = true;
        }

        if (!parentRepairSessions.containsKey(parentRepairSession))
        {
            parentRepairSessions.put(parentRepairSession, new ParentRepairSession(coordinator,
                                                                                  columnFamilyStores,
                                                                                  ranges,
                                                                                  isIncremental,
                                                                                  timestamp,
                                                                                  isGlobal,
                                                                                  parentRepairSession));
        }
    }

    public Set<SSTableReader> currentlyRepairing(UUID cfId, UUID parentRepairSession)
    {
        Set<SSTableReader> repairing = new HashSet<>();
        for (Map.Entry<UUID, ParentRepairSession> entry : parentRepairSessions.entrySet())
        {
            Collection<SSTableReader> sstables = entry.getValue().getLiveValidatedSSTables(cfId);
            if (sstables != null && !entry.getKey().equals(parentRepairSession))
                repairing.addAll(sstables);
        }
        return repairing;
    }

    /**
     * Run final process of repair.
     * This removes all resources held by parent repair session, after performing anti compaction if necessary.
     *
     * @param parentSession Parent session ID
     * @param neighbors Repair participants (not including self)
     * @param successfulRanges Ranges that repaired successfully
     */
    public synchronized ListenableFuture finishParentSession(UUID parentSession, Set<InetAddress> neighbors, Collection<Range<Token>> successfulRanges)
    {
        List<ListenableFuture<?>> tasks = new ArrayList<>(neighbors.size() + 1);
        for (InetAddress neighbor : neighbors)
        {
            AnticompactionTask task = new AnticompactionTask(parentSession, neighbor, successfulRanges);
            registerOnFdAndGossip(task);
            tasks.add(task);
            task.run(); // 'run' is just sending message
        }
        tasks.add(doAntiCompaction(parentSession, successfulRanges));
        return Futures.successfulAsList(tasks);
    }

    public ParentRepairSession getParentRepairSession(UUID parentSessionId)
    {
        ParentRepairSession session = parentRepairSessions.get(parentSessionId);
        // this can happen if a node thinks that the coordinator was down, but that coordinator got back before noticing
        // that it was down itself.
        if (session == null)
            throw new RuntimeException("Parent repair session with id = " + parentSessionId + " has failed.");

        return session;
    }

    /**
     * called when the repair session is done - either failed or anticompaction has completed
     *
     * clears out any snapshots created by this repair
     *
     * @param parentSessionId
     * @return
     */
    public synchronized ParentRepairSession removeParentRepairSession(UUID parentSessionId)
    {
        for (ColumnFamilyStore cfs : getParentRepairSession(parentSessionId).columnFamilyStores.values())
        {
            if (cfs.snapshotExists(parentSessionId.toString()))
                cfs.clearSnapshot(parentSessionId.toString());
        }
        ParentRepairSession prs = parentRepairSessions.remove(parentSessionId);
        prs.cleanup();
        return prs;
    }

    /**
     * Submit anti-compaction jobs to CompactionManager.
     * When all jobs are done, parent repair session is removed whether those are suceeded or not.
     *
     * @param parentRepairSession parent repair session ID
     * @param successfulRanges the ranges successfully repaired
     * @return Future result of all anti-compaction jobs.
     */
    @SuppressWarnings("resource")
    public ListenableFuture<List<Object>> doAntiCompaction(final UUID parentRepairSession, Collection<Range<Token>> successfulRanges)
    {
        assert parentRepairSession != null;
        ParentRepairSession prs = getParentRepairSession(parentRepairSession);
        //A repair will be marked as not global if it is a subrange repair to avoid many small anti-compactions
        //in addition to other scenarios such as repairs not involving all DCs or hosts
        if (!prs.isGlobal)
        {
            logger.info("[repair #{}] Not a global repair, will not do anticompaction", parentRepairSession);
            removeParentRepairSession(parentRepairSession);
            return Futures.immediateFuture(Collections.emptyList());
        }
        assert prs.ranges.containsAll(successfulRanges) : "Trying to perform anticompaction on unknown ranges";

        ListenableFuture<List<Object>> allAntiCompactionResults = prs.doAntiCompaction(successfulRanges);
        allAntiCompactionResults.addListener(() -> removeParentRepairSession(parentRepairSession), MoreExecutors.directExecutor());

        return allAntiCompactionResults;
    }

    public void handleMessage(InetAddress endpoint, RepairMessage message)
    {
        RepairJobDesc desc = message.desc;
        RepairSession session = sessions.get(desc.sessionId);
        if (session == null)
            return;
        switch (message.messageType)
        {
            case VALIDATION_COMPLETE:
                ValidationComplete validation = (ValidationComplete) message;
                session.validationComplete(desc, endpoint, validation.trees);
                break;
            case SYNC_COMPLETE:
                // one of replica is synced.
                SyncComplete sync = (SyncComplete) message;
                session.syncComplete(desc, sync.nodes, sync.success);
                break;
            default:
                break;
        }
    }

    /**
     * We keep a ParentRepairSession around for the duration of the entire repair, for example, on a 256 token vnode rf=3 cluster
     * we would have 768 RepairSession but only one ParentRepairSession. We use the PRS to avoid anticompacting the sstables
     * 768 times, instead we take all repaired ranges at the end of the repair and anticompact once.
     * <p>
     * In case of incremental repairs, the anticompaction algorithm needs to take into consideration remote sstables
     * containing tombstones that might shadow compacted local sstables, or local sstables intersecting with such set;
     * in such case, some local sstables might end up as unrepaired, while some remote sstables would end up repaired,
     * causing potential data resurrection in case of tombstones purged from the repaired set. As a consequence,
     * the anticompaction algorithm in {@link ParentRepairSession#buildAntiCompactionTask(ColumnFamilyStore, Collection)} identifies the
     * local set off sstables safe to anticompact, and the remote set of sstables safe to repair, and pass those
     * sstables to the {@link LocalAntiCompactionTask} for the actual execution.
     * <p>
     * Note that validation and streaming do not care about which sstables we have marked as repairing - they operate on
     * all unrepaired sstables (if it is incremental), otherwise we would not get a correct repair.
     */
    public static class ParentRepairSession implements INotificationConsumer
    {
        private final Map<UUID, ColumnFamilyStore> columnFamilyStores = new HashMap<>();
        private final Collection<Range<Token>> ranges;
        private final Set<UUID> marked = new HashSet<>();
        @VisibleForTesting
        protected final Map<UUID, Set<SSTableInfo>> validatedSSTables = new HashMap<>();
        private final Map<UUID, Set<SSTableReader>> remoteSSTablesWithTombstones = new HashMap<>();
        @VisibleForTesting
        protected final Map<UUID, Set<LifecycleTransaction>> remoteCompactingSSTables = new HashMap<>();
        public final boolean isIncremental;
        public final boolean isGlobal;
        public final long repairedAt;
        public final InetAddress coordinator;
        @VisibleForTesting
        protected final UUID parentSessionId;

        public ParentRepairSession(InetAddress coordinator, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges,
                                   boolean isIncremental, long repairedAt, boolean isGlobal, UUID parentSessionId)
        {
            this.coordinator = coordinator;
            for (ColumnFamilyStore cfs : columnFamilyStores)
            {
                this.columnFamilyStores.put(cfs.metadata.cfId, cfs);
                validatedSSTables.put(cfs.metadata.cfId, new HashSet<>());
                // We subscribe to the tracker to be able to mark SSTables containing tombstones as compacting. See {@link this#handleNotification} for details.
                if (isGlobal)
                {
                    remoteSSTablesWithTombstones.put(cfs.metadata.cfId, new HashSet<>());
                    remoteCompactingSSTables.put(cfs.metadata.cfId, new HashSet<>());
                    cfs.getTracker().subscribe(this);
                }
            }
            this.ranges = ranges;
            this.repairedAt = repairedAt;
            this.isIncremental = isIncremental;
            this.isGlobal = isGlobal;
            this.parentSessionId = parentSessionId;
        }

        /**
         * Mark the given column family and related sstables as repairing, provided there isn't an ongoing repair over the
         * same sstables.
         *
         * @param cfId the column family to mark as repairing
         */
        public synchronized void markRepairing(UUID cfId)
        {
            if (!marked.contains(cfId))
            {
                List<SSTableReader> sstables = columnFamilyStores.get(cfId).select(View.select(SSTableSet.CANONICAL, (s) -> !isIncremental || !s.isRepaired())).sstables;
                Set<SSTableReader> currentlyRepairing = ActiveRepairService.instance.currentlyRepairing(cfId, parentSessionId);
                if (!Sets.intersection(currentlyRepairing, Sets.newHashSet(sstables)).isEmpty())
                {
                    logger.error("Cannot start multiple repair sessions over the same sstables");
                    throw new RuntimeException("Cannot start multiple repair sessions over the same sstables");
                }
                addSSTables(cfId, sstables);
                marked.add(cfId);
            }
        }

        /**
         * Get the {@link LocalAntiCompactionTask} which will run the final anti compaction on local and repaired sstables.
         */
        @SuppressWarnings("resource")
        public synchronized LocalAntiCompactionTask buildAntiCompactionTask(ColumnFamilyStore cfs, Collection<Range<Token>> successfulRanges)
        {
            UUID cfId = cfs.metadata.cfId;
            assert columnFamilyStores.containsKey(cfId) && marked.contains(cfId);

            LifecycleTransaction localToAntiCompact = null;
            try
            {
                // Mark live validated SSTables as compacting
                localToAntiCompact = markSSTablesCompacting(cfs, getLiveValidatedSSTables(cfId));

                // This is the set of all SSTables (local or remote) that will be marked as repaired in this session
                Set<SSTableReader> repairedSet = Sets.newHashSet(Sets.union(localToAntiCompact.originals(), getRemoteSSTablesWithTombstones(cfs)));

                // Any SSTable validated but not in the repair set is in the unrepaired set (ie. was compacted during repair)
                Set<SSTableInfo> unrepairedSet = Sets.newHashSet(Sets.difference(validatedSSTables.getOrDefault(cfs.metadata.cfId, Collections.emptySet()),
                                                                                 repairedSet.stream().map(s -> new SSTableInfo(s)).collect(Collectors.toSet())));

                // Any repaired SSTable shadowing unrepaired data should be moved from the repaired to unrepaired set
                // This should be done in a loop, since augmenting the unrepaired set may cause more repaired
                // SSTables to be shadowed by unrepaired
                Set<SSTableReader> shadowingUnrepairedSet = getShadowingUnrepaired(unrepairedSet, repairedSet);
                while (!shadowingUnrepairedSet.isEmpty())
                {
                    for (SSTableReader shadowsUnrepaired : shadowingUnrepairedSet)
                    {
                        unrepairedSet.add(new SSTableInfo(shadowsUnrepaired, true));
                        repairedSet.remove(shadowsUnrepaired);
                    }
                    shadowingUnrepairedSet = getShadowingUnrepaired(unrepairedSet, repairedSet);
                }

                long antiCompactedBytes = repairedSet.stream().mapToLong(s -> s.onDiskLength()).sum();
                long nonAntiCompactedBytes = unrepairedSet.stream().mapToLong(s -> s.onDiskLength).sum();

                cfs.metric.antiCompactedBytes.inc(antiCompactedBytes);
                cfs.metric.nonAntiCompactedBytes.inc(nonAntiCompactedBytes);

                if (nonAntiCompactedBytes > antiCompactedBytes)
                {
                    long totalRepaired = antiCompactedBytes + nonAntiCompactedBytes;
                    logger.warn("[repair {}] Out of {} bytes repaired for table {}.{}, {} bytes from {} SSTables ({}%) could not be marked as " +
                                "repaired because they were either compacted or shadow compacted data during repair. Please check the metrics " +
                                "org.apache.cassandra.metrics.Table.{AntiCompactedBytes|NonAntiCompactedBytes} for more details on how " +
                                "anti-compaction is performing. Ideally NonAntiCompactedBytes should be zero or much lower than AntiCompactedBytes, " +
                                "otherwise incremental repair may be causing non-negligible over-streaming. If this is the case, consider " +
                                "running incremental repairs on this table with compactions disabled or switch to full repairs.",
                                parentSessionId, totalRepaired, cfs.keyspace.getName(), cfs.getTableName(), nonAntiCompactedBytes,
                                unrepairedSet.size(), String.format("%.2f", (double)nonAntiCompactedBytes*100/totalRepaired));
                }

                return new LocalAntiCompactionTask(cfs, successfulRanges, parentSessionId, repairedAt, localToAntiCompact, remoteCompactingSSTables.getOrDefault(cfId, Collections.emptySet()),
                                                   unrepairedSet.stream().filter(i -> i.sstableRef.isPresent()).map(i -> i.sstableRef.get()).collect(Collectors.toSet()));
            }
            catch(Throwable ex)
            {
                Throwable accumulate = ex;
                if (localToAntiCompact != null)
                {
                    accumulate = localToAntiCompact.abort(accumulate);
                }
                throw new RuntimeException(accumulate);
            }
        }



        private static LifecycleTransaction markSSTablesCompacting(ColumnFamilyStore cfs, Collection<SSTableReader> sstables)
        {
            LifecycleTransaction txn = null;
            while (txn == null)
            {
                sstables = sstables.stream()
                        .filter(s -> !s.isMarkedCompacted() && !cfs.getTracker().getCompacting().contains(s))
                        .collect(Collectors.toSet());
                txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
            }
            return txn;
        }

        private static Set<SSTableReader> getShadowingUnrepaired(Set<SSTableInfo> unrepairedSet, Set<SSTableReader> repairedSet)
        {
            return repairedSet.stream()
                    .filter(r -> unrepairedSet.stream().anyMatch(u -> u.mayHaveDataShadowedBy(r)))
                    .collect(Collectors.toSet());
        }

        private Set<SSTableReader> getRemoteSSTablesWithTombstones(ColumnFamilyStore cfs)
        {
            Set<SSTableReader> remoteToRepair = remoteSSTablesWithTombstones.get(cfs.metadata.cfId);
            if (remoteToRepair == null)
                remoteToRepair = Collections.emptySet();
            return remoteToRepair;
        }

        public synchronized void maybeSnapshot(UUID cfId, UUID parentSessionId)
        {
            String snapshotName = parentSessionId.toString();
            if (!columnFamilyStores.get(cfId).snapshotExists(snapshotName))
            {
                Set<SSTableReader> snapshottedSSTables = columnFamilyStores.get(cfId).snapshot(snapshotName, new Predicate<SSTableReader>()
                {
                    public boolean apply(SSTableReader sstable)
                    {
                        return sstable != null &&
                               (!isIncremental || !sstable.isRepaired()) &&
                               !(sstable.metadata.isIndex()) && // exclude SSTables from 2i
                               new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(ranges);
                    }
                }, true, new HashSet<>());

                if (isAlreadyRepairing(cfId, parentSessionId, snapshottedSSTables))
                {
                    columnFamilyStores.get(cfId).clearSnapshot(parentSessionId.toString());
                    logger.error("Cannot start multiple repair sessions over the same sstables");
                    throw new RuntimeException("Cannot start multiple repair sessions over the same sstables");
                }
                addSSTables(cfId, snapshottedSSTables);
                marked.add(cfId);
            }
        }

        /**
         * Compares other repairing sstables *generation* to the ones we just snapshotted
         *
         * we compare generations since the sstables have different paths due to snapshot names
         *
         * @param cfId id of the column family store
         * @param parentSessionId parent repair session
         * @param sstables the newly snapshotted sstables
         * @return
         */
        private boolean isAlreadyRepairing(UUID cfId, UUID parentSessionId, Collection<SSTableReader> sstables)
        {
            Set<SSTableReader> currentlyRepairing = ActiveRepairService.instance.currentlyRepairing(cfId, parentSessionId);
            Set<Integer> currentlyRepairingGenerations = new HashSet<>();
            Set<Integer> newRepairingGenerations = new HashSet<>();
            for (SSTableReader sstable : currentlyRepairing)
                currentlyRepairingGenerations.add(sstable.descriptor.generation);
            for (SSTableReader sstable : sstables)
                newRepairingGenerations.add(sstable.descriptor.generation);

            return !Sets.intersection(currentlyRepairingGenerations, newRepairingGenerations).isEmpty();
        }

        private Set<SSTableReader> getLiveValidatedSSTables(UUID cfId)
        {
            if (!columnFamilyStores.containsKey(cfId))
                return null;

            Set<SSTableInfo> validatedSSTables = this.validatedSSTables.get(cfId);
            return StreamSupport.stream(columnFamilyStores.get(cfId).getSSTables(SSTableSet.CANONICAL).spliterator(), false)
                                .filter(s -> validatedSSTables.contains(new SSTableInfo(s)))
                                .collect(Collectors.toSet());
        }

        private void addSSTables(UUID cfId, Collection<SSTableReader> sstables)
        {
            for (SSTableReader sstable : sstables)
                validatedSSTables.get(cfId).add(new SSTableInfo(sstable));
        }

        public long getRepairedAt()
        {
            if (isGlobal)
                return repairedAt;
            return ActiveRepairService.UNREPAIRED_SSTABLE;
        }

        @Override
        public String toString()
        {
            return "ParentRepairSession{" +
                   "columnFamilyStores=" + columnFamilyStores +
                   ", ranges=" + ranges +
                   ", sstableMap=" + validatedSSTables +
                   ", repairedAt=" + repairedAt +
                   '}';
        }

        private synchronized void onReceivedSSTables(Collection<SSTableReader> received)
        {
            assert isGlobal;
            UUID cfId = received.iterator().next().metadata.cfId;
            if (!marked.contains(cfId))
                return;

            received.stream()
                    .filter(s -> s.hasTombstones())
                    .forEach(s -> markRemoteWithTombstones(s, cfId));
        }

        /**
         * Mark any SSTable with  tombstones received during repair as unrepaired - we will mutate them at the end of repair.
         * This is to avoid a node marking received data with shadowing tombstones as repaired if the node crashes before or during anti-compaction
         * See {@link ParentRepairSession#buildAntiCompactionTask(ColumnFamilyStore, Collection)} for more details
         */
        private void markRemoteWithTombstones(SSTableReader received, UUID cfId)
        {
            try
            {
                logger.trace("[repair #{}] Marking SSTable with tombstone as unrepaired: {}", parentSessionId, received);
                // we don't need to notify the change of repair status because this should be called before the SSTable is added to the compaction strategy,
                columnFamilyStores.get(cfId).mutateRepairedAt(Collections.singleton(received), UNREPAIRED_SSTABLE, false);
                remoteSSTablesWithTombstones.get(cfId).add(received);
            }
            catch (IOException e)
            {
                //this will fail the streaming session.
                throw new RuntimeException(String.format("[repair %s] Could not mark received sstables as unrepaired: %s.", parentSessionId, received, e));
            }
        }

        public ListenableFuture<List<Object>> doAntiCompaction(Collection<Range<Token>> successfulRanges)
        {
            if (successfulRanges.isEmpty())
                return Futures.immediateFuture(Collections.emptyList());

            List<ListenableFuture<?>> futures = new ArrayList<>();
            for (ColumnFamilyStore cfs : columnFamilyStores.values())
            {
                LocalAntiCompactionTask antiCompactionTask = buildAntiCompactionTask(cfs, successfulRanges);
                futures.add(CompactionManager.instance.submitAntiCompaction(antiCompactionTask));
            }

            return Futures.successfulAsList(futures);
        }

        public void handleNotification(INotification notification, Object sender)
        {
            assert isGlobal;
            if (notification instanceof SSTableAddedNotification)
            {
                SSTableAddedNotification ssTableAddedNotification = (SSTableAddedNotification) notification;
                for (SSTableReader received : ssTableAddedNotification.added)
                {
                    UUID cfId = received.metadata.cfId;
                    if (remoteSSTablesWithTombstones.getOrDefault(cfId, Collections.emptySet()).contains(received))
                    {
                        // mark as compacting so the received SSTable does not participate any compaction in the unrepaired bucket,
                        // since it will be marked as repaired at the end of repair
                        if (addToRemoteCompactingSSTables(cfId, markSSTablesCompacting(columnFamilyStores.get(cfId), Collections.singleton(received))))
                        {
                            logger.trace("[repair #{}] Marked received SSTable with tombstone as compacting: {}", parentSessionId, received);
                        }
                        else
                        {
                            // this will fail the streaming session.
                            throw new RuntimeException(String.format("[repair %s] Could not mark received sstables as compacting: %s.", parentSessionId, received));
                        }
                    }
                }
            }
        }

        private boolean addToRemoteCompactingSSTables(UUID cfId, LifecycleTransaction txn)
        {
            boolean shouldAdd = txn != null && !txn.originals().isEmpty();
            if (shouldAdd)
            {
                remoteCompactingSSTables.get(cfId).add(txn);
            }
            else if (txn != null)
            {
                txn.close();
            }

            return shouldAdd;
        }

        /**
         * This method *needs* to be called when the parent repair session is finished/removed, in order to cleanup
         * resources held during repair by the parent session.
         */
        public synchronized void cleanup()
        {
            columnFamilyStores.values().forEach(cfs ->
            {
                try
                {
                    // Remove tracked sstables with tombstones
                    remoteSSTablesWithTombstones.remove(cfs.metadata.cfId);
                    // Remove sstables set as compacting:
                    Set<LifecycleTransaction> removed = remoteCompactingSSTables.remove(cfs.metadata.cfId);
                    if (removed != null)
                    {
                        Throwable accumulate = null;
                        for (LifecycleTransaction txn : removed)
                        {
                            try
                            {
                                txn.close();
                            }
                            catch (Throwable t)
                            {
                                if (accumulate != null)
                                    accumulate.addSuppressed(t);
                                else
                                    accumulate = t;
                            }
                        }
                        Throwables.maybeFail(accumulate);
                    }
                }
                finally
                {
                    // Finally unsubscribe the session from the tracker
                    cfs.getTracker().unsubscribe(this);
                }
            });
        }
    }

    /*
    If the coordinator node dies we should remove the parent repair session from the other nodes.
    This uses the same notifications as we get in RepairSession
     */
    public void onJoin(InetAddress endpoint, EndpointState epState) {}
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {}
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {}
    public void onAlive(InetAddress endpoint, EndpointState state) {}
    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void onRestart(InetAddress endpoint, EndpointState state)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    /**
     * Something has happened to a remote node - if that node is a coordinator, we mark the parent repair session id as failed.
     *
     * The fail marker is kept in the map for 24h to make sure that if the coordinator does not agree
     * that the repair failed, we need to fail the entire repair session
     *
     * @param ep  endpoint to be convicted
     * @param phi the value of phi with with ep was convicted
     */
    public void convict(InetAddress ep, double phi)
    {
        // We want a higher confidence in the failure detection than usual because failing a repair wrongly has a high cost.
        if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold() || parentRepairSessions.isEmpty())
            return;

        Set<UUID> toRemove = new HashSet<>();

        for (Map.Entry<UUID, ParentRepairSession> repairSessionEntry : parentRepairSessions.entrySet())
        {
            if (repairSessionEntry.getValue().coordinator.equals(ep))
            {
                toRemove.add(repairSessionEntry.getKey());
            }
        }

        if (!toRemove.isEmpty())
        {
            logger.debug("Removing {} in parent repair sessions", toRemove);
            for (UUID id : toRemove)
                removeParentRepairSession(id);
        }
    }

    /**
     * Auxiliary class to retain information of an SSTable that was validated but may
     * potentially be gone via compaction by the time repair reaches the anti-compaction stage.
     *
     * Used mostly by {@link ParentRepairSession#buildAntiCompactionTask(ColumnFamilyStore, Collection)}}
     * to detect when data contained in an SSTable that was compacted during repair is potentially
     * shadowed by a tombstone received from another replica.
     */
    public static class SSTableInfo
    {
        public final String filename;
        public final Range<Token> range;
        public final long minTimestamp;
        public final long onDiskLength;
        public final boolean hasTombstones;

        public final Optional<SSTableReader> sstableRef;

        public SSTableInfo(SSTableReader reader)
        {
            this(reader, false);
        }

        public SSTableInfo(SSTableReader reader, boolean keepSSTableRef)
        {
            this(reader.getFilename(), new Range<>(reader.first.getToken(), reader.last.getToken()), reader.getMinTimestamp(),
                 reader.onDiskLength(), reader.hasTombstones(), keepSSTableRef? reader : null);
        }

        public SSTableInfo(String filename, Range<Token> range, long minTimestamp, long onDiskLength, boolean hasTombstones,
                           SSTableReader sstableRef)
        {
            this.filename = filename;
            this.range = range;
            this.minTimestamp = minTimestamp;
            this.onDiskLength = onDiskLength;
            this.hasTombstones = hasTombstones;
            this.sstableRef = Optional.ofNullable(sstableRef);
        }

        public boolean mayHaveDataShadowedBy(SSTableReader sstable)
        {
            return sstable.hasTombstones() && this.range.intersects(new Range<>(sstable.first.getToken(),
                                                                                sstable.last.getToken())) &&
                   sstable.getMinTimestamp() > minTimestamp;
        }

        public Optional<SSTableReader> getSstableRef()
        {
            return sstableRef;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SSTableInfo that = (SSTableInfo) o;

            return filename != null ? filename.equals(that.filename) : that.filename == null;
        }

        public int hashCode()
        {
            return filename != null ? filename.hashCode() : 0;
        }

        public String toString()
        {
            return "SSTableInfo{" +
                   "filename='" + filename + '\'' +
                   ", range=" + range +
                   ", minTimestamp=" + minTimestamp +
                   ", onDiskLength=" + onDiskLength +
                   ", hasTombstones=" + hasTombstones +
                   '}';
        }
    }

    public void receiveStreamedSSTables(Collection<SSTableReader> readers)
    {
        if (readers.isEmpty())
            return;

        SSTableReader sstable = readers.iterator().next();
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(sstable.metadata.cfId);

        assert readers.stream().noneMatch(s -> cfs.getLiveSSTables().contains(s)) : "This method should be called before adding SSTables to the tracker";

        long repairedAt = sstable.getSSTableMetadata().repairedAt;
        if (repairedAt == UNREPAIRED_SSTABLE)
            return;

        for (ParentRepairSession prs : parentRepairSessions.values())
        {
            if (prs.isGlobal && prs.repairedAt == repairedAt)
            {
                prs.onReceivedSSTables(readers);
            }
        }
    }
}
