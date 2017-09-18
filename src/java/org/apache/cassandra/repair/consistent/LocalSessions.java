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

package org.apache.cassandra.repair.consistent;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.dht.BoundsVersion;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.OneWayRequest;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionStrategyManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.repair.consistent.ConsistentSession.State.*;

/**
 * Manages all consistent repair sessions a node is participating in.
 * <p/>
 * Since sessions need to be loaded, and since we need to handle cases where sessions might not exist, most of the logic
 * around local sessions is implemented in this class, with the LocalSession class being treated more like a simple struct,
 * in contrast with {@link CoordinatorSession}
 */
public class LocalSessions
{
    private static final Logger logger = LoggerFactory.getLogger(LocalSessions.class);

    /**
     * Amount of time a session can go without any activity before we start checking the status of other
     * participants to see if we've missed a message
     */
    static final int CHECK_STATUS_TIMEOUT = Integer.getInteger("cassandra.repair_status_check_timeout_seconds",
                                                               Ints.checkedCast(TimeUnit.HOURS.toSeconds(1)));

    /**
     * Amount of time a session can go without any activity before being automatically set to FAILED
     */
    static final int AUTO_FAIL_TIMEOUT = Integer.getInteger("cassandra.repair_fail_timeout_seconds",
                                                            Ints.checkedCast(TimeUnit.DAYS.toSeconds(1)));

    /**
     * Amount of time a completed session is kept around after completion before being deleted
     */
    static final int AUTO_DELETE_TIMEOUT = Integer.getInteger("cassandra.repair_delete_timeout_seconds",
                                                              Ints.checkedCast(TimeUnit.DAYS.toSeconds(1)));
    /**
     * How often LocalSessions.cleanup is run
     */
    public static final int CLEANUP_INTERVAL = Integer.getInteger("cassandra.repair_cleanup_interval_seconds",
                                                                  Ints.checkedCast(TimeUnit.MINUTES.toSeconds(10)));

    private static Set<TableId> uuidToTableId(Set<UUID> src)
    {
        return ImmutableSet.copyOf(Iterables.transform(src, TableId::fromUUID));
    }

    private static Set<UUID> tableIdToUuid(Set<TableId> src)
    {
        return ImmutableSet.copyOf(Iterables.transform(src, TableId::asUUID));
    }

    private final String keyspace = SchemaConstants.SYSTEM_KEYSPACE_NAME;
    private final String table = SystemKeyspace.REPAIRS;
    private boolean started = false;
    private volatile ImmutableMap<UUID, LocalSession> sessions = ImmutableMap.of();

    @VisibleForTesting
    int getNumSessions()
    {
        return sessions.size();
    }

    @VisibleForTesting
    protected InetAddress getBroadcastAddress()
    {
        return FBUtilities.getBroadcastAddress();
    }

    @VisibleForTesting
    protected boolean isAlive(InetAddress address)
    {
        return FailureDetector.instance.isAlive(address);
    }

    @VisibleForTesting
    protected boolean isNodeInitialized()
    {
        return StorageService.instance.isInitialized();
    }

    public List<Map<String, String>> sessionInfo(boolean all)
    {
        Iterable<LocalSession> currentSessions = sessions.values();
        if (!all)
        {
            currentSessions = Iterables.filter(currentSessions, s -> !s.isCompleted());
        }
        return Lists.newArrayList(Iterables.transform(currentSessions, LocalSessionInfo::sessionToMap));
    }

    /**
     * hook for operators to cancel sessions, cancelling from a non-coordinator is an error, unless
     * force is set to true. Messages are sent out to other participants, but we don't wait for a response
     */
    public void cancelSession(UUID sessionID, boolean force)
    {
        logger.info("Cancelling local repair session {}", sessionID);
        LocalSession session = getSession(sessionID);
        Preconditions.checkArgument(session != null, "Session {} does not exist", sessionID);
        Preconditions.checkArgument(force || session.coordinator.equals(getBroadcastAddress()),
                                    "Cancel session %s from it's coordinator (%s) or use --force",
                                    sessionID, session.coordinator);

        setStateAndSave(session, FAILED);
        for (InetAddress participant : session.participants)
        {
            if (!participant.equals(getBroadcastAddress()))
                send(Verbs.REPAIR.FAILED_SESSION.newRequest(participant, new FailSession(sessionID)));
        }
    }

    /**
     * Loads sessions out of the repairs table and sets state to started
     */
    public void start()
    {
        Preconditions.checkArgument(!started, "LocalSessions.start can only be called once");
        Preconditions.checkArgument(sessions.isEmpty(), "No sessions should be added before start");
        UntypedResultSet result = QueryProcessor.executeInternalWithPaging(String.format("SELECT * FROM %s.%s", keyspace, table), PageSize.rowsSize(1000));
        Map<UUID, LocalSession> loadedSessions = new ConcurrentHashMap<>();
        TPCUtils.blockingAwait(result.rows().processToRxCompletable(row -> {
            try
            {
                LocalSession session = load(row);
                loadedSessions.put(session.sessionID, session);
            }
            catch (IllegalArgumentException | NullPointerException e)
            {
                logger.warn("Unable to load malformed repair session {}, ignoring", row.has("parent_id") ? row.getUUID("parent_id") : null);
            }
        }));

        synchronized (this)
        {
            if (sessions.isEmpty() && !started)
            {
                sessions = ImmutableMap.copyOf(loadedSessions);
                started = true;
                logger.debug("Loaded {} consistent repair sessions.", sessions.size());
            }
        }
    }

    public boolean isStarted()
    {
        return started;
    }

    private static boolean shouldCheckStatus(LocalSession session, int now)
    {
        return !session.isCompleted() && (now > session.getLastUpdate() + CHECK_STATUS_TIMEOUT);
    }

    private static boolean shouldFail(LocalSession session, int now)
    {
        return !session.isCompleted() && (now > session.getLastUpdate() + AUTO_FAIL_TIMEOUT);
    }

    private static boolean shouldDelete(LocalSession session, int now)
    {
        return session.isCompleted() && (now > session.getLastUpdate() + AUTO_DELETE_TIMEOUT);
    }

    /**
     * Auto fails and auto deletes timed out and old sessions
     * Compaction will clean up the sstables still owned by a deleted session
     */
    public void cleanup()
    {
        logger.debug("Running LocalSessions.cleanup");
        if (!isNodeInitialized())
        {
            logger.trace("node not initialized, aborting local session cleanup");
            return;
        }
        Set<LocalSession> currentSessions = new HashSet<>(sessions.values());
        for (LocalSession session : currentSessions)
        {
            synchronized (session)
            {
                int now = FBUtilities.nowInSeconds();
                logger.debug("Cleaning up consistent repair session at {}: {}", now, session);
                if (shouldFail(session, now))
                {
                    logger.warn("Auto failing timed out repair session {}", session);
                    failSession(session.sessionID);
                }
                else if (shouldDelete(session, now))
                {
                    if (!sessionHasData(session))
                    {
                        logger.debug("Auto deleting repair session {}", session);
                        deleteSession(session.sessionID);
                    }
                    else
                    {
                        logger.warn("Skipping delete of LocalSession {} because it still contains sstables", session.sessionID);
                    }
                }
                else if (shouldCheckStatus(session, now))
                {
                    sendStatusRequest(session);
                }
            }
        }
    }

    private static ByteBuffer serializeRange(Range<Token> range)
    {
        int size = Token.serializer.serializedSize(range.left, BoundsVersion.OSS_30);
        size += Token.serializer.serializedSize(range.right, BoundsVersion.OSS_30);
        try (DataOutputBuffer buffer = new DataOutputBuffer(size))
        {
            Token.serializer.serialize(range.left, buffer, BoundsVersion.OSS_30);
            Token.serializer.serialize(range.right, buffer, BoundsVersion.OSS_30);
            return buffer.buffer();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Set<ByteBuffer> serializeRanges(Set<Range<Token>> ranges)
    {
        Set<ByteBuffer> buffers = new HashSet<>(ranges.size());
        ranges.forEach(r -> buffers.add(serializeRange(r)));
        return buffers;
    }

    private static Range<Token> deserializeRange(ByteBuffer bb)
    {
        try (DataInputBuffer in = new DataInputBuffer(bb, false))
        {
            IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
            Token left = Token.serializer.deserialize(in, partitioner, BoundsVersion.OSS_30);
            Token right = Token.serializer.deserialize(in, partitioner, BoundsVersion.OSS_30);
            return new Range<>(left, right);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Set<Range<Token>> deserializeRanges(Set<ByteBuffer> buffers)
    {
        Set<Range<Token>> ranges = new HashSet<>(buffers.size());
        buffers.forEach(bb -> ranges.add(deserializeRange(bb)));
        return ranges;
    }

    /**
     * Save session state to table
     */
    @VisibleForTesting
    void save(LocalSession session)
    {
        String query = "INSERT INTO %s.%s " +
                       "(parent_id, " +
                       "started_at, " +
                       "last_update, " +
                       "repaired_at, " +
                       "state, " +
                       "coordinator, " +
                       "participants, " +
                       "ranges, " +
                       "cfids) " +
                       "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        logger.debug("Saving session: " + session);

        QueryProcessor.executeInternal(String.format(query, keyspace, table),
                                       session.sessionID,
                                       Date.from(Instant.ofEpochSecond(session.startedAt)),
                                       Date.from(Instant.ofEpochSecond(session.getLastUpdate())),
                                       Date.from(Instant.ofEpochMilli(session.repairedAt)),
                                       session.getState().ordinal(),
                                       session.coordinator,
                                       session.participants,
                                       serializeRanges(session.ranges),
                                       tableIdToUuid(session.tableIds));

        // We need to sync the CL because if the node suddenly dies after finalization, we need to read the
        // correct session state when it restarts; otherwise, we might read an older session state, and
        // wrongfully consider the session as non finalized (please note this actually applies to any session state,
        // not just finalization).
        try
        {
            CommitLog.instance.sync();
        }
        catch (IOException ex)
        {
            logger.warn("Failed to sync commit log, this could cause repair inconsistencies.", ex);
        }
    }

    private static int dateToSeconds(Date d)
    {
        return Ints.checkedCast(TimeUnit.MILLISECONDS.toSeconds(d.getTime()));
    }

    private LocalSession load(UntypedResultSet.Row row)
    {
        LocalSession.Builder builder = LocalSession.builder();
        builder.withState(ConsistentSession.State.valueOf(row.getInt("state")));
        builder.withSessionID(row.getUUID("parent_id"));
        builder.withCoordinator(row.getInetAddress("coordinator"));
        builder.withTableIds(uuidToTableId(row.getSet("cfids", UUIDType.instance)));
        builder.withRepairedAt(row.getTimestamp("repaired_at").getTime());
        builder.withRanges(deserializeRanges(row.getSet("ranges", BytesType.instance)));
        builder.withParticipants(row.getSet("participants", InetAddressType.instance));
        builder.withStartedAt(dateToSeconds(row.getTimestamp("started_at")));
        builder.withLastUpdate(dateToSeconds(row.getTimestamp("last_update")));

        LocalSession session = buildSession(builder);
        logger.debug("Loaded consistent repair session: {}", session);
        return session;
    }

    private void deleteRow(UUID sessionID)
    {
        String query = "DELETE FROM %s.%s WHERE parent_id=?";
        QueryProcessor.executeInternal(String.format(query, keyspace, table), sessionID);
    }

    private void syncTable()
    {
        TableId tid = Schema.instance.getTableMetadata(keyspace, table).id;
        ColumnFamilyStore cfm = Schema.instance.getColumnFamilyStoreInstance(tid);
        cfm.forceBlockingFlush();
    }

    /**
     * Loads a session directly from the table. Should be used for testing only
     */
    @VisibleForTesting
    public LocalSession loadUnsafe(UUID sessionId)
    {
        String query = "SELECT * FROM %s.%s WHERE parent_id=?";
        UntypedResultSet result = QueryProcessor.executeInternal(String.format(query, keyspace, table), sessionId);
        if (result.isEmpty())
            return null;

        UntypedResultSet.Row row = result.one();
        return load(row);
    }

    @VisibleForTesting
    public LocalSession buildSession(LocalSession.Builder builder)
    {
        return new LocalSession(builder);
    }

    @VisibleForTesting
    public LocalSession getSession(UUID sessionID)
    {
        return sessions.get(sessionID);
    }

    @VisibleForTesting
    public synchronized void putSessionUnsafe(LocalSession session)
    {
        putSession(session);
        save(session);
    }

    private synchronized void putSession(LocalSession session)
    {
        Preconditions.checkArgument(!sessions.containsKey(session.sessionID),
                                    "LocalSession {} already exists", session.sessionID);
        Preconditions.checkArgument(started, "sessions cannot be added before LocalSessions is started");
        sessions = ImmutableMap.<UUID, LocalSession>builder()
                               .putAll(sessions)
                               .put(session.sessionID, session)
                               .build();
    }

    private synchronized void removeSession(UUID sessionID)
    {
        Preconditions.checkArgument(sessionID != null);
        Map<UUID, LocalSession> temp = new HashMap<>(sessions);
        temp.remove(sessionID);
        sessions = ImmutableMap.copyOf(temp);
    }

    @VisibleForTesting
    LocalSession createSessionUnsafe(UUID sessionId, ActiveRepairService.ParentRepairSession prs, Set<InetAddress> peers)
    {
        LocalSession.Builder builder = LocalSession.builder();
        builder.withState(ConsistentSession.State.PREPARING);
        builder.withSessionID(sessionId);
        builder.withCoordinator(prs.coordinator);

        builder.withTableIds(prs.getTableIds());
        builder.withRepairedAt(prs.repairedAt);
        builder.withRanges(prs.getRanges());
        builder.withParticipants(peers);

        int now = FBUtilities.nowInSeconds();
        builder.withStartedAt(now);
        builder.withLastUpdate(now);

        return buildSession(builder);
    }

    protected ActiveRepairService.ParentRepairSession getParentRepairSession(UUID sessionID)
    {
        return ActiveRepairService.instance.getParentRepairSession(sessionID);
    }

    // Overridden by tests to intercept messages
    // TODO: the test could probably use the messaging service mocking instead
    protected void send(OneWayRequest<? extends RepairMessage<?>> request)
    {
        logger.trace("sending {} to {}", request.payload(), request.to());
        MessagingService.instance().send(request);
    }

    @VisibleForTesting
    protected <REQ extends RepairMessage, RES extends RepairMessage> void send(Request<REQ, RES> request, MessageCallback<RES> callback)
    {
        logger.trace("sending {} to {}", request.payload(), request.to());
        MessagingService.instance().send(request, callback);
    }

    private void setStateAndSave(LocalSession session, ConsistentSession.State state)
    {
        boolean wasCompleted;

        synchronized (session)
        {
            Preconditions.checkArgument(session.getState().canTransitionTo(state),
                                        "Invalid state transition %s -> %s",
                                        session.getState(), state);

            logger.debug("Changing LocalSession state from {} -> {} for {}",
                         session.getState(), state, session.sessionID);

            wasCompleted = session.isCompleted();
            session.setState(state);
            session.setLastUpdate();
            save(session);
        }

        if (session.isCompleted() && !wasCompleted)
        {
            sessionCompleted(session);
        }
    }

    private void failPrepare(UUID session, InetAddress coordinator, ExecutorService executor)
    {
        failSession(session);
        send(Verbs.REPAIR.CONSISTENT_RESPONSE.newRequest(coordinator, new PrepareConsistentResponse(session, getBroadcastAddress(), false)));
        executor.shutdown();
    }

    public void failSession(UUID sessionID)
    {
        LocalSession session = getSession(sessionID);
        if (session != null && session.getState() != FINALIZED)
        {
            logger.info("Failing local repair session {}", sessionID);
            setStateAndSave(session, FAILED);
        }
    }

    public synchronized void deleteSession(UUID sessionID)
    {
        logger.debug("Deleting local repair session {}", sessionID);
        LocalSession session = getSession(sessionID);
        Preconditions.checkArgument(session.isCompleted(), "Cannot delete incomplete sessions");

        deleteRow(sessionID);
        removeSession(sessionID);
    }

    @VisibleForTesting
    ListenableFuture<Boolean> resolveSessions(LocalSession newSession, ExecutorService executor)
    {
        ListenableFutureTask<Boolean> task = ListenableFutureTask.create(new LocalSessionsResolver(
            newSession,
            sessions.values(),
            (Pair<LocalSession, ConsistentSession.State> sessionAndState) -> setStateAndSave(sessionAndState.left, sessionAndState.right)));

        executor.submit(task);

        return task;
    }

    @VisibleForTesting
    ListenableFuture submitPendingAntiCompaction(LocalSession session, ExecutorService executor)
    {
        PendingAntiCompaction pac = new PendingAntiCompaction(session.sessionID, session.ranges, executor);
        return pac.run();
    }

    /**
     * The PrepareConsistentRequest effectively promotes the parent repair session to a consistent
     * incremental session, and:
     * <ol>
     * <li>Resolves any conflicts with old repair sessions by either aborting the current session or completing
     * the old one (see {@link LocalSessionsResolver}).</li>
     * <li>Begins the 'pending anti compaction' which moves all sstable data
     * that is to be repaired into it's own silo, preventing it from mixing with other data.</li>
     * </ol>
     *
     * No response is sent to the repair coordinator until the pending anti compaction has completed
     * successfully. If the pending anti compaction fails, a failure message is sent to the coordinator,
     * cancelling the session.
     */
    public void handlePrepareMessage(InetAddress from, PrepareConsistentRequest request)
    {
        logger.trace("received {} from {}", request, from);
        UUID sessionID = request.sessionID;
        InetAddress coordinator = request.coordinator;
        Set<InetAddress> peers = request.participants;

        ActiveRepairService.ParentRepairSession parentSession;
        try
        {
            parentSession = getParentRepairSession(sessionID);
        }
        catch (Throwable e)
        {
            logger.trace("Error retrieving ParentRepairSession for session {}, responding with failure", sessionID);
            send(Verbs.REPAIR.FAILED_SESSION.newRequest(coordinator, new FailSession(sessionID)));
            return;
        }

        LocalSession session = createSessionUnsafe(sessionID, parentSession, peers);
        putSessionUnsafe(session);

        ExecutorService executor = getPrepareExecutor(parentSession);

        ListenableFuture<Boolean> resolveFuture = resolveSessions(session, executor);
        Futures.addCallback(resolveFuture, new FutureCallback<Boolean>()
        {
            public void onSuccess(Boolean resolved)
            {
                if (resolved)
                {
                    logger.info("Beginning local incremental repair session {}", session);

                    ListenableFuture pendingAntiCompaction = submitPendingAntiCompaction(session, executor);
                    Futures.addCallback(pendingAntiCompaction, new FutureCallback()
                    {
                        public void onSuccess(@Nullable Object result)
                        {
                            logger.debug("Prepare phase for incremental repair session {} completed", sessionID);
                            setStateAndSave(session, PREPARED);
                            send(Verbs.REPAIR.CONSISTENT_RESPONSE.newRequest(coordinator, new PrepareConsistentResponse(sessionID, getBroadcastAddress(), true)));
                            executor.shutdown();
                        }

                        public void onFailure(Throwable t)
                        {
                            logger.error(String.format("Prepare phase for incremental repair session %s failed", sessionID), t);
                            failPrepare(session.sessionID, coordinator, executor);
                        }
                    });
                }
                else
                {
                    logger.error(String.format("Prepare phase for incremental repair session %s aborted due to session resolution failure.", sessionID));
                    failPrepare(session.sessionID, coordinator, executor);
                }
            }

            public void onFailure(Throwable t)
            {
                logger.error("Prepare phase for incremental repair session {} failed", sessionID, t);
                if (t instanceof PendingAntiCompaction.SSTableAcquisitionException)
                {
                    logger.warn("Prepare phase for incremental repair session {} was unable to " +
                                "acquire exclusive access to the neccesary sstables. " +
                                "This is usually caused by running multiple incremental repairs on nodes that share token ranges",
                                sessionID);
                }
                else
                {
                    logger.error("Prepare phase for incremental repair session {} failed", sessionID, t);
                }
                failPrepare(session.sessionID, coordinator, executor);
            }
        });
    }

    @VisibleForTesting
    protected ExecutorService getPrepareExecutor(ActiveRepairService.ParentRepairSession parentSession)
    {
        return Executors.newFixedThreadPool(parentSession.getColumnFamilyStores().size(),
                                            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Inc-Repair-prepare-executor-%d").build());
    }

    public void maybeSetRepairing(UUID sessionID)
    {
        LocalSession session = getSession(sessionID);
        if (session != null && session.getState() != REPAIRING)
        {
            logger.debug("Setting local incremental repair session {} to REPAIRING", session);
            setStateAndSave(session, REPAIRING);
        }
    }

    @VisibleForTesting
    public synchronized void sessionCompleted(LocalSession session)
    {
        logger.info("Completing local repair session {}", session.sessionID);
        for (TableId tid : session.tableIds)
        {
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tid);
            if (cfs != null)
            {
                logger.info("Running pending repair sstables resolution for local repair session {} and table {}.{}",
                            session.sessionID, cfs.keyspace, cfs.name);

                // This runs the pending repair task synchronously, and with compactions disabled to avoid conflicts;
                // a few points worth noting:
                // * Only compactions over pending repair sstables for this session are disabled (see predicates below).
                // * We are not interested in stopping validations (they are read only) not view builds
                //   (not even supported with inc repair).
                // * runWithCompactionsDisabled() disables, interrupts and waits for compactions to terminate: this,
                //   coupled with the fact this method is synchronized, ensures no other compaction or session completion
                //   can interfere.
                // * If something wrong happens here, the pending repair task will be retried during normal background
                //   compaction.
                cfs.runWithCompactionsDisabled(() ->
                {
                    CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
                    strategy.getPendingRepairTasks(session.sessionID).stream().forEach(task -> task.run());
                    return null;
                },
                OperationType.COMPACTIONS_ONLY,
                (SSTableReader s) -> session.sessionID.equals(s.getPendingRepair()), false);
            }
        }
    }

    /**
     * Finalizes the repair session, completing it as successful.
     */
    public void handleFinalizeCommitMessage(InetAddress from, FinalizeCommit commit)
    {
        logger.trace("received {} from {}", commit, from);
        UUID sessionID = commit.sessionID;
        LocalSession session = getSession(sessionID);
        if (session == null)
        {
            logger.warn("Ignoring FinalizeCommit message for unknown repair session {}", sessionID);
            return;
        }

        setStateAndSave(session, FINALIZED);
        logger.info("Finalized local repair session {}", sessionID);
    }

    public void handleFailSessionMessage(InetAddress from, FailSession msg)
    {
        logger.trace("received {} from {}", msg, from);
        failSession(msg.sessionID);
    }

    public void sendStatusRequest(LocalSession session)
    {
        logger.debug("Attempting to learn the outcome of unfinished local incremental repair session {}", session.sessionID);
        StatusRequest request = new StatusRequest(session.sessionID);
        LocalStatusResponseCallback callback = new LocalStatusResponseCallback();
        for (InetAddress participant : session.participants)
        {
            if (!getBroadcastAddress().equals(participant) && isAlive(participant))
                send(Verbs.REPAIR.STATUS_REQUEST.newRequest(participant, request), callback);
        }
    }

    public StatusResponse handleStatusRequest(InetAddress from, StatusRequest request)
    {
        logger.trace("received {} from {}", request, from);
        UUID sessionID = request.sessionID;
        LocalSession session = getSession(sessionID);
        if (session == null)
        {
            logger.warn("Received status response message for unknown session {}", sessionID);
            return new StatusResponse(sessionID, UNKNOWN);
        }
        else
        {
            boolean running = ActiveRepairService.instance.hasParentRepairSession(sessionID);
            logger.debug("Responding to status response message for incremental repair session {}{} with local state {}",
                         sessionID,
                         running ? " running locally" : " ",
                         session.getState());
            return new StatusResponse(sessionID, session.getState(), running);
        }
    }

    @VisibleForTesting
    public boolean handleStatusResponse(InetAddress from, StatusResponse response)
    {
        logger.trace("received {} from {}", response, from);
        UUID sessionID = response.sessionID;
        LocalSession session = getSession(sessionID);
        if (session == null)
        {
            logger.warn("Received StatusResponse message for unknown repair session {}", sessionID);
            return false;
        }

        // only change local state if response state is FINALIZED or FAILED, since those are
        // the only statuses that would indicate we've missed a message completing the session
        if (response.state == FINALIZED || response.state == FAILED)
        {
            setStateAndSave(session, response.state);
            logger.info("Unfinished local incremental repair session {} set to state {}", sessionID, response.state);
            return true;
        }
        else
        {
            logger.debug("Received StatusResponse for repair session {} with state {}, which is not actionable. Doing nothing.", sessionID, response.state);
            return false;
        }
    }

    /**
     * determines if a local session exists, and if it's not finalized or failed
     */
    public boolean isSessionInProgress(UUID sessionID)
    {
        LocalSession session = getSession(sessionID);
        return session != null && session.getState() != FINALIZED && session.getState() != FAILED;
    }

    @VisibleForTesting
    protected boolean sessionHasData(LocalSession session)
    {
        Predicate<TableId> predicate = tid -> {
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tid);
            return cfs != null && cfs.getCompactionStrategyManager().hasDataForPendingRepair(session.sessionID);

        };
        return Iterables.any(session.tableIds, predicate::test);
    }

    /**
     * Returns the repairedAt time for a sessions which is unknown, failed, or finalized
     * calling this for a session which is in progress throws an exception
     */
    public long getFinalSessionRepairedAt(UUID sessionID)
    {
        LocalSession session = getSession(sessionID);
        if (session == null || session.getState() == FAILED)
        {
            return ActiveRepairService.UNREPAIRED_SSTABLE;
        }
        else if (session.getState() == FINALIZED)
        {
            return session.repairedAt;
        }
        else
        {
            throw new IllegalStateException("Cannot get final repaired at value for in progress session: " + session);
        }
    }

    private class LocalStatusResponseCallback implements MessageCallback<StatusResponse>
    {
        private boolean completed;

        @Override
        public synchronized void onResponse(Response<StatusResponse> response)
        {
            if (!completed)
                completed = handleStatusResponse(response.from(), response.payload());
        }

        @Override
        public void onFailure(FailureResponse<StatusResponse> failure)
        {
        }
    }
}
