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

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.*;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Uninterruptibles;

import io.reactivex.Completable;
import io.reactivex.Single;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchRemove;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.metrics.*;
import org.apache.cassandra.net.*;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.CommitCallback;
import org.apache.cassandra.service.paxos.PrepareCallback;
import org.apache.cassandra.service.paxos.ProposeCallback;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.concurrent.AsyncLatch;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.RxThreads;

public class StorageProxy implements StorageProxyMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=StorageProxy";
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);

    public static final String UNREACHABLE = "UNREACHABLE";

    public static final StorageProxy instance = new StorageProxy();

    private static volatile int maxHintsInProgress = 128 * FBUtilities.getAvailableProcessors();
    private static final CacheLoader<InetAddress, AtomicInteger> hintsInProgress = new CacheLoader<InetAddress, AtomicInteger>()
    {
        public AtomicInteger load(InetAddress inetAddress)
        {
            return new AtomicInteger(0);
        }
    };
    private static final ClientRequestMetrics readMetrics = new ClientRequestMetrics("Read");
    private static final ClientRequestMetrics rangeMetrics = new ClientRequestMetrics("RangeSlice");
    private static final ClientWriteRequestMetrics writeMetrics = new ClientWriteRequestMetrics("Write");
    private static final CASClientWriteRequestMetrics casWriteMetrics = new CASClientWriteRequestMetrics("CASWrite");
    private static final CASClientRequestMetrics casReadMetrics = new CASClientRequestMetrics("CASRead");
    private static final ViewWriteMetrics viewWriteMetrics = new ViewWriteMetrics("ViewWrite");
    private static final Map<ConsistencyLevel, ClientRequestMetrics> readMetricsMap = new EnumMap<>(ConsistencyLevel.class);
    private static final Map<ConsistencyLevel, ClientWriteRequestMetrics> writeMetricsMap = new EnumMap<>(ConsistencyLevel.class);

    private static final double CONCURRENT_SUBREQUESTS_MARGIN = 0.10;

    private StorageProxy()
    {
    }

    static
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        HintsService.instance.registerMBean();
        HintedHandOffManager.instance.registerMBean();

        for (ConsistencyLevel level : ConsistencyLevel.values())
        {
            readMetricsMap.put(level, new ClientRequestMetrics("Read-" + level.name()));
            writeMetricsMap.put(level, new ClientWriteRequestMetrics("Write-" + level.name()));
        }
    }

    /**
     * Apply @param updates if and only if the current values in the row for @param key
     * match the provided @param conditions.  The algorithm is "raw" Paxos: that is, Paxos
     * minus leader election -- any node in the cluster may propose changes for any row,
     * which (that is, the row) is the unit of values being proposed, not single columns.
     * <p>
     * The Paxos cohort is only the replicas for the given key, not the entire cluster.
     * So we expect performance to be reasonable, but CAS is still intended to be used
     * "when you really need it," not for all your updates.
     * <p>
     * There are three phases to Paxos:
     * 1. Prepare: the coordinator generates a ballot (timeUUID in our case) and asks replicas to (a) promise
     * not to accept updates from older ballots and (b) tell us about the most recent update it has already
     * accepted.
     * 2. Accept: if a majority of replicas reply, the coordinator asks replicas to accept the value of the
     * highest proposal ballot it heard about, or a new value if no in-progress proposals were reported.
     * 3. Commit (Learn): if a majority of replicas acknowledge the accept request, we can commit the new
     * value.
     * <p>
     * Commit procedure is not covered in "Paxos Made Simple," and only briefly mentioned in "Paxos Made Live,"
     * so here is our approach:
     * 3a. The coordinator sends a commit message to all replicas with the ballot and value.
     * 3b. Because of 1-2, this will be the highest-seen commit ballot.  The replicas will note that,
     * and send it with subsequent promise replies.  This allows us to discard acceptance records
     * for successfully committed replicas, without allowing incomplete proposals to commit erroneously
     * later on.
     * <p>
     * Note that since we are performing a CAS rather than a simple update, we perform a read (of committed
     * values) between the prepare and accept phases.  This gives us a slightly longer window for another
     * coordinator to come along and trump our own promise with a newer one but is otherwise safe.
     *
     * @param keyspaceName         the keyspace for the CAS
     * @param cfName               the column family for the CAS
     * @param key                  the row key for the row to CAS
     * @param request              the conditions for the CAS to apply as well as the update to perform if the conditions hold.
     * @param consistencyForPaxos  the consistency for the paxos prepare and propose round. This can only be either SERIAL or LOCAL_SERIAL.
     * @param consistencyForCommit the consistency for write done during the commit phase. This can be anything, except SERIAL or LOCAL_SERIAL.
     * @return null if the operation succeeds in updating the row, or the current values corresponding to conditions.
     * (since, if the CAS doesn't succeed, it means the current value do not match the conditions).
     */
    // this is blocking
    public static Optional<RowIterator> cas(String keyspaceName,
                                            String cfName,
                                            DecoratedKey key,
                                            CASRequest request,
                                            ConsistencyLevel consistencyForPaxos,
                                            ConsistencyLevel consistencyForCommit,
                                            ClientState state,
                                            long queryStartNanoTime)
    throws UnavailableException, IsBootstrappingException, RequestFailureException, RequestTimeoutException, InvalidRequestException
    {
        if (logger.isTraceEnabled())
            logger.trace("Execute cas on {}.{} for pk {}", keyspaceName, cfName, key);

        final long startTimeForMetrics = System.nanoTime();
        int contentions = 0;
        try
        {
            consistencyForPaxos.validateForCas();
            consistencyForCommit.validateForCasCommit(keyspaceName);

            TableMetadata metadata = Schema.instance.getTableMetadata(keyspaceName, cfName);

            long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());
            while (System.nanoTime() - queryStartNanoTime < timeout)
            {
                // for simplicity, we'll do a single liveliness check at the start of each attempt
                Pair<WriteEndpoints, Integer> p = getPaxosParticipants(metadata, key, consistencyForPaxos);
                List<InetAddress> liveEndpoints = p.left.live();
                int requiredParticipants = p.right;

                final Pair<UUID, Integer> pair = beginAndRepairPaxos(queryStartNanoTime, key, metadata, liveEndpoints, requiredParticipants, consistencyForPaxos, consistencyForCommit, true, state);
                final UUID ballot = pair.left;
                contentions += pair.right;

                // read the current values and check they validate the conditions
                Tracing.trace("Reading existing values for CAS precondition");
                SinglePartitionReadCommand readCommand = request.readCommand(FBUtilities.nowInSeconds());
                ConsistencyLevel readConsistency = consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL ? ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.QUORUM;

                FilteredPartition current = readOne(readCommand, readConsistency, queryStartNanoTime);
                if (!request.appliesTo(current))
                {
                    Tracing.trace("CAS precondition does not match current values {}", current);
                    casWriteMetrics.conditionNotMet.inc();
                    return Optional.of(current.rowIterator());
                }

                // finish the paxos round w/ the desired updates
                // TODO turn null updates into delete?
                PartitionUpdate updates = request.makeUpdates(current);

                long size = updates.dataSize();
                casWriteMetrics.mutationSize.update(size);
                writeMetricsMap.get(consistencyForPaxos).mutationSize.update(size);

                // Apply triggers to cas updates. A consideration here is that
                // triggers emit Mutations, and so a given trigger implementation
                // may generate mutations for partitions other than the one this
                // paxos round is scoped for. In this case, TriggerExecutor will
                // validate that the generated mutations are targetted at the same
                // partition as the initial updates and reject (via an
                // InvalidRequestException) any which aren't.
                updates = TriggerExecutor.instance.execute(updates);


                Commit proposal = Commit.newProposal(ballot, updates);
                Tracing.trace("CAS precondition is met; proposing client-requested updates for {}", ballot);
                if (proposePaxos(proposal, liveEndpoints, requiredParticipants, true, consistencyForPaxos, queryStartNanoTime))
                {
                    commitPaxos(proposal, consistencyForCommit, true, queryStartNanoTime);
                    Tracing.trace("CAS successful");
                    return Optional.empty();
                }

                Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
                contentions++;
                Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                // continue to retry
            }

            throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(keyspaceName)));
        }
        catch (WriteTimeoutException | ReadTimeoutException e)
        {
            casWriteMetrics.timeouts.mark();
            writeMetricsMap.get(consistencyForPaxos).timeouts.mark();
            throw e;
        }
        catch (WriteFailureException | ReadFailureException e)
        {
            casWriteMetrics.failures.mark();
            writeMetricsMap.get(consistencyForPaxos).failures.mark();
            throw e;
        }
        catch (UnavailableException e)
        {
            casWriteMetrics.unavailables.mark();
            writeMetricsMap.get(consistencyForPaxos).unavailables.mark();
            throw e;
        }
        finally
        {
            recordCasContention(contentions);
            final long latency = System.nanoTime() - startTimeForMetrics;
            casWriteMetrics.addNano(latency);
            writeMetricsMap.get(consistencyForPaxos).addNano(latency);
        }
    }

    private static void recordCasContention(int contentions)
    {
        if (contentions > 0)
            casWriteMetrics.contention.update(contentions);
    }

    private static Pair<WriteEndpoints, Integer> getPaxosParticipants(TableMetadata table, DecoratedKey key, ConsistencyLevel consistencyForPaxos) throws UnavailableException
    {
        WriteEndpoints endpoints = WriteEndpoints.compute(table.keyspace, key);
        if (consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL)
            endpoints = endpoints.restrictToLocalDC();

        int participants = endpoints.count();
        int requiredParticipants = participants / 2 + 1; // See CASSANDRA-8346, CASSANDRA-833
        if (endpoints.liveCount() < requiredParticipants)
            throw new UnavailableException(consistencyForPaxos, requiredParticipants, endpoints.liveCount());

        // We cannot allow CAS operations with 2 or more pending endpoints, see #8346.
        // Note that we fake an impossible number of required nodes in the unavailable exception
        // to nail home the point that it's an impossible operation no matter how many nodes are live.
        if (endpoints.pendingCount() > 1)
            throw new UnavailableException(String.format("Cannot perform LWT operation as there is more than one (%d) pending range movement", endpoints.pendingCount()),
                                           consistencyForPaxos,
                                           participants + 1,
                                           endpoints.liveCount());

        return Pair.create(endpoints, requiredParticipants);
    }

    /**
     * begin a Paxos session by sending a prepare request and completing any in-progress requests seen in the replies
     *
     * @return the Paxos ballot promised by the replicas if no in-progress requests were seen and a quorum of
     * nodes have seen the mostRecentCommit.  Otherwise, return null.
     */
    private static Pair<UUID, Integer> beginAndRepairPaxos(long queryStartNanoTime,
                                                           DecoratedKey key,
                                                           TableMetadata metadata,
                                                           List<InetAddress> liveEndpoints,
                                                           int requiredParticipants,
                                                           ConsistencyLevel consistencyForPaxos,
                                                           ConsistencyLevel consistencyForCommit,
                                                           final boolean isWrite,
                                                           ClientState state)
    throws WriteTimeoutException, WriteFailureException
    {
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());

        PrepareCallback summary = null;
        int contentions = 0;
        while (System.nanoTime() - queryStartNanoTime < timeout)
        {
            // We want a timestamp that is guaranteed to be unique for that node (so that the ballot is globally unique), but if we've got a prepare rejected
            // already we also want to make sure we pick a timestamp that has a chance to be promised, i.e. one that is greater that the most recently known
            // in progress (#5667). Lastly, we don't want to use a timestamp that is older than the last one assigned by ClientState or operations may appear
            // out-of-order (#7801).
            long minTimestampMicrosToUse = summary == null ? Long.MIN_VALUE : 1 + UUIDGen.microsTimestamp(summary.mostRecentInProgressCommit.ballot);
            long ballotMicros = state.getTimestampForPaxos(minTimestampMicrosToUse);
            // Note that ballotMicros is not guaranteed to be unique if two proposal are being handled concurrently by the same coordinator. But we still
            // need ballots to be unique for each proposal so we have to use getRandomTimeUUIDFromMicros.
            UUID ballot = UUIDGen.getRandomTimeUUIDFromMicros(ballotMicros);

            // prepare
            Tracing.trace("Preparing {}", ballot);
            Commit toPrepare = Commit.newPrepare(key, metadata, ballot);
            summary = preparePaxos(toPrepare, liveEndpoints, requiredParticipants, consistencyForPaxos, queryStartNanoTime);
            if (!summary.promised)
            {
                Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                contentions++;
                // sleep a random amount to give the other proposer a chance to finish
                Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                continue;
            }

            Commit inProgress = summary.mostRecentInProgressCommitWithUpdate;
            Commit mostRecent = summary.mostRecentCommit;

            // If we have an in-progress ballot greater than the MRC we know, then it's an in-progress round that
            // needs to be completed, so do it.
            if (!inProgress.update.isEmpty() && inProgress.isAfter(mostRecent))
            {
                Tracing.trace("Finishing incomplete paxos round {}", inProgress);
                if (isWrite)
                    casWriteMetrics.unfinishedCommit.inc();
                else
                    casReadMetrics.unfinishedCommit.inc();
                Commit refreshedInProgress = Commit.newProposal(ballot, inProgress.update);
                if (proposePaxos(refreshedInProgress, liveEndpoints, requiredParticipants, false, consistencyForPaxos, queryStartNanoTime))
                {
                    try
                    {
                        commitPaxos(refreshedInProgress, consistencyForCommit, false, queryStartNanoTime);
                    }
                    catch (WriteTimeoutException e)
                    {
                        recordCasContention(contentions);
                        // We're still doing preparation for the paxos rounds, so we want to use the CAS (see CASSANDRA-8672)
                        throw new WriteTimeoutException(WriteType.CAS, e.consistency, e.received, e.blockFor);
                    }
                }
                else
                {
                    Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                    // sleep a random amount to give the other proposer a chance to finish
                    contentions++;
                    Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                }
                continue;
            }

            // To be able to propose our value on a new round, we need a quorum of replica to have learn the previous one. Why is explained at:
            // https://issues.apache.org/jira/browse/CASSANDRA-5062?focusedCommentId=13619810&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13619810)
            // Since we waited for quorum nodes, if some of them haven't seen the last commit (which may just be a timing issue, but may also
            // mean we lost messages), we pro-actively "repair" those nodes (and wait for them to acknowledge that repair) before continuing.
            int nowInSec = Ints.checkedCast(TimeUnit.MICROSECONDS.toSeconds(ballotMicros));
            Iterable<InetAddress> missingMRC = summary.replicasMissingMostRecentCommit(metadata, nowInSec);
            if (Iterables.size(missingMRC) > 0)
            {
                Tracing.trace("Repairing replicas that missed the most recent commit");
                commitPaxosAndWaitAll(mostRecent, ImmutableList.copyOf(missingMRC));
            }

            return Pair.create(ballot, contentions);
        }

        recordCasContention(contentions);
        throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(metadata.keyspace)));
    }

    private static PrepareCallback preparePaxos(Commit toPrepare, List<InetAddress> endpoints, int requiredParticipants, ConsistencyLevel consistencyForPaxos, long queryStartNanoTime)
    throws WriteTimeoutException
    {
        PrepareCallback callback = new PrepareCallback(toPrepare.update.partitionKey(), toPrepare.update.metadata(), requiredParticipants, consistencyForPaxos, queryStartNanoTime);
        MessagingService.instance().send(Verbs.LWT.PREPARE.newDispatcher(endpoints, toPrepare), callback);
        callback.await();
        return callback;
    }

    private static boolean proposePaxos(Commit proposal, List<InetAddress> endpoints, int requiredParticipants, boolean timeoutIfPartial, ConsistencyLevel consistencyLevel, long queryStartNanoTime)
    throws WriteTimeoutException
    {
        ProposeCallback callback = new ProposeCallback(endpoints.size(), requiredParticipants, !timeoutIfPartial, consistencyLevel, queryStartNanoTime);
        MessagingService.instance().send(Verbs.LWT.PROPOSE.newDispatcher(endpoints, proposal), callback);

        callback.await();

        if (callback.isSuccessful())
            return true;

        if (timeoutIfPartial && !callback.isFullyRefused())
            throw new WriteTimeoutException(WriteType.CAS, consistencyLevel, callback.getAcceptCount(), requiredParticipants);

        return false;
    }

    private static void commitPaxosAndWaitAll(Commit proposal, List<InetAddress> endpoints) throws WriteTimeoutException
    {
        // ConsistencyLevel.ALL is a lie here. Unfortunately, any ConsistencyLevel here is a lie, since there's not
        // a correspondence with the replication factor but instead with the provided set of endpoints that we're
        // "repairing". Since we're waiting for all provided endpoints to respond, we pass ALL.
        CommitCallback callback = new CommitCallback(endpoints.size(), ConsistencyLevel.ALL, System.nanoTime());
        MessagingService.instance().send(Verbs.LWT.COMMIT.newDispatcher(endpoints, proposal), callback);
        callback.await();

        Map<InetAddress, RequestFailureReason> failureReasons = callback.getFailureReasons();

        // ConsistencyLevel.ALL is a lie here. Unfortunately, any ConsistencyLevel here is a lie, since there's not
        // a correspondence with the replication factor but instead with the provided set of endpoints that we're
        // "repairing". Since we're waiting for all provided endpoints to respond, we pass ALL.
        if (!failureReasons.isEmpty())
            throw new WriteFailureException(ConsistencyLevel.ALL, callback.getResponseCount(), endpoints.size(), WriteType.CAS, failureReasons);
    }

    private static void commitPaxos(Commit proposal, ConsistencyLevel consistencyLevel, boolean shouldHint, long queryStartNanoTime) throws WriteTimeoutException
    {
        commitPaxos(proposal, WriteEndpoints.compute(proposal), consistencyLevel, shouldHint, queryStartNanoTime);
    }

    private static void commitPaxos(Commit proposal, WriteEndpoints endpoints, ConsistencyLevel consistencyLevel, boolean shouldHint, long queryStartNanoTime) throws WriteTimeoutException
    {
        Mutation mutation = proposal.makeMutation();
        checkHintOverload(endpoints);

        if (shouldHint)
        {
            Collection<InetAddress> endpointsToHint = Collections2.filter(endpoints.dead(), e -> shouldHint(e));
            if (!endpointsToHint.isEmpty())
                submitHint(mutation, endpointsToHint, null);
        }

        WriteHandler.Builder builder = WriteHandler.builder(endpoints, consistencyLevel, WriteType.SIMPLE, queryStartNanoTime)
                                                   .withIdealConsistencyLevel(DatabaseDescriptor.getIdealConsistencyLevel());
        if (shouldHint)
            builder.hintOnTimeout(mutation);

        WriteHandler handler = builder.build();
        MessagingService.instance().send(Verbs.LWT.COMMIT.newDispatcher(endpoints.live(), proposal), handler);

        if (consistencyLevel != ConsistencyLevel.ANY)
            handler.get();
    }

    /**
     * Use this method to have these Mutations applied across all replicas. This method will take care
     * of the possibility of a replica being down and hint the data across to some other replica.
     *
     * @param mutations          the mutations to be applied across the replicas
     * @param consistencyLevel   the consistency level for the operation
     * @param queryStartNanoTime the value of System.nanoTime() when the query started to be processed
     */
    public static Single<ResultMessage.Void> mutate(Collection<? extends IMutation> mutations, ConsistencyLevel consistencyLevel, long queryStartNanoTime)
    throws UnavailableException, OverloadedException, WriteTimeoutException, WriteFailureException
    {
        Tracing.trace("Determining replicas for mutation");
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

        long startTime = System.nanoTime();
        List<WriteHandler> responseHandlers = new ArrayList<>(mutations.size());

        try
        {
            for (IMutation mutation : mutations)
            {
                if (mutation instanceof CounterMutation)
                {
                    responseHandlers.add(mutateCounter((CounterMutation) mutation, localDataCenter, queryStartNanoTime));
                }
                else
                {
                    WriteType wt = mutations.size() <= 1 ? WriteType.SIMPLE : WriteType.UNLOGGED_BATCH;
                    responseHandlers.add(mutateStandard((Mutation)mutation, consistencyLevel, localDataCenter, wt, queryStartNanoTime));
                }
            }
        }
        catch(UnavailableException | OverloadedException ex)
        {
            if (logger.isTraceEnabled())
                logger.trace("Unavailable or overloaded exception", ex);

            writeMetrics.unavailables.mark();
            writeMetricsMap.get(consistencyLevel).unavailables.mark();
            Tracing.trace("Unavailable");
            return Single.error(ex);
        }

        Completable ret = Completable.concat(Lists.transform(responseHandlers, WriteHandler::toObservable));
        return ret.onErrorResumeNext(ex -> {
            if (logger.isTraceEnabled())
                logger.trace("Failed to wait for handlers", ex);

            if (ex instanceof WriteTimeoutException || ex instanceof WriteFailureException)
            {
                if (consistencyLevel == ConsistencyLevel.ANY)
                {
                    hintMutations(mutations);
                    return Completable.complete(); // do not propagate the exception
                }
                else
                {
                    if (ex instanceof WriteFailureException)
                    {
                        writeMetrics.failures.mark();
                        writeMetricsMap.get(consistencyLevel).failures.mark();
                        WriteFailureException fe = (WriteFailureException) ex;
                        Tracing.trace("Write failure; received {} of {} required replies, failed {} requests",
                                      fe.received,
                                      fe.blockFor,
                                      fe.failureReasonByEndpoint.size());
                    }
                    else
                    {
                        writeMetrics.timeouts.mark();
                        writeMetricsMap.get(consistencyLevel).timeouts.mark();
                        WriteTimeoutException te = (WriteTimeoutException) ex;
                        Tracing.trace("Write timeout; received {} of {} required replies", te.received, te.blockFor);
                    }
                }
            }
            else if (ex instanceof UnavailableException)
            {
                writeMetrics.unavailables.mark();
                writeMetricsMap.get(consistencyLevel).unavailables.mark();
                Tracing.trace("Unavailable");
            }
            else if (ex instanceof OverloadedException)
            {
                writeMetrics.unavailables.mark();
                writeMetricsMap.get(consistencyLevel).unavailables.mark();
                Tracing.trace("Overloaded");
            }
            return Completable.error(ex);
        }).doFinally(() -> recordLatency(consistencyLevel, startTime)).toSingleDefault(new ResultMessage.Void());
    }

    /**
     * Hint all the mutations (except counters, which can't be safely retried).  This means
     * we'll re-hint any successful ones; doesn't seem worth it to track individual success
     * just for this unusual case.
     * <p>
     * Only used for CL.ANY
     *
     * @param mutations the mutations that require hints
     */
    private static void hintMutations(Collection<? extends IMutation> mutations)
    {
        for (IMutation mutation : mutations)
            if (!(mutation instanceof CounterMutation))
                hintMutation((Mutation) mutation);

        Tracing.trace("Wrote hints to satisfy CL.ANY after no replicas acknowledged the write");
    }

    private static void hintMutation(Mutation mutation)
    {
        String keyspaceName = mutation.getKeyspaceName();
        Token token = mutation.key().getToken();

        Iterable<InetAddress> endpoints = StorageService.instance.getNaturalAndPendingEndpoints(keyspaceName, token);
        ArrayList<InetAddress> endpointsToHint = new ArrayList<>(Iterables.size(endpoints));

        // local writes can timeout, but cannot be dropped (see LocalMutationRunnable and CASSANDRA-6510),
        // so there is no need to hint or retry.
        for (InetAddress target : endpoints)
            if (!target.equals(FBUtilities.getBroadcastAddress()) && shouldHint(target))
                endpointsToHint.add(target);

        submitHint(mutation, endpointsToHint, null);
    }

    public boolean appliesLocally(Mutation mutation)
    {
        String keyspaceName = mutation.getKeyspaceName();
        Token token = mutation.key().getToken();
        InetAddress local = FBUtilities.getBroadcastAddress();

        return StorageService.instance.getNaturalEndpoints(keyspaceName, token).contains(local)
               || StorageService.instance.getTokenMetadata().pendingEndpointsFor(token, keyspaceName).contains(local);
    }

    /**
     * Use this method to have these Mutations applied
     * across all replicas.
     *
     * @param mutations          the mutations to be applied across the replicas
     * @param writeCommitLog     if commitlog should be written
     * @param baseComplete       time from epoch in ms that the local base mutation was(or will be) completed
     * @param queryStartNanoTime the value of System.nanoTime() when the query started to be processed
     */
    public static Completable mutateMV(ByteBuffer dataKey, Collection<Mutation> mutations, boolean writeCommitLog, AtomicLong baseComplete, long queryStartNanoTime)
    throws UnavailableException, OverloadedException, WriteTimeoutException
    {
        Tracing.trace("Determining replicas for mutation");
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

        long startTime = System.nanoTime();
        final UUID batchUUID = UUIDGen.getTimeUUID();

        // if we haven't joined the ring, write everything to batchlog because paired replicas may be stale
        if (StorageService.instance.isStarting() || StorageService.instance.isJoining() || StorageService.instance.isMoving())
        {
            return BatchlogManager.store(Batch.createLocal(batchUUID, FBUtilities.timestampMicros(),
                                                           mutations), writeCommitLog);
        }

        List<MutationAndEndpoints> mutationsAndEndpoints = new ArrayList<>(mutations.size());
        Set<Mutation> nonLocalMutations = new HashSet<>(mutations);
        Token baseToken = StorageService.instance.getTokenMetadata().partitioner.getToken(dataKey);

        //Since the base -> view replication is 1:1 we only need to store the BL locally
        final List<InetAddress> batchlogEndpoints = Collections.singletonList(FBUtilities.getBroadcastAddress());
        AsyncLatch cleanupLatch = new AsyncLatch(mutations.size(), () -> asyncRemoveFromBatchlog(batchlogEndpoints, batchUUID));

        ArrayList<Completable> completables = new ArrayList<>(mutations.size() + 1);

        // add a handler for each mutation - includes checking availability, but doesn't initiate any writes, yet
        for (Mutation mutation : mutations)
        {
            WriteEndpoints endpoints = WriteEndpoints.computeForView(baseToken, mutation);

            if (endpoints.naturalCount() == 0)
            {
                if (endpoints.pendingCount() == 0)
                    logger.warn("Received base materialized view mutation for key {} that does not belong " +
                                "to this node. There is probably a range movement happening (move or decommission)," +
                                "but this node hasn't updated its ring metadata yet. Adding mutation to " +
                                "local batchlog to be replayed later.",
                                mutation.key());
                continue;
            }

            InetAddress endpoint = endpoints.natural().get(0);
            // When local node is the paired endpoint just apply the mutation locally.
            if (endpoint.equals(FBUtilities.getBroadcastAddress()) && endpoints.pendingCount() == 0 && StorageService.instance.isJoined())
            {
                completables.add(mutation.applyAsync(writeCommitLog, true)
                                         .doFinally(() -> cleanupLatch.countDown())
                                         .doOnError(exc -> logger.error("Error applying local view update to keyspace {}: {}", mutation.getKeyspaceName(), mutation)));

                nonLocalMutations.remove(mutation);

            }
            else
            {
                mutationsAndEndpoints.add(new MutationAndEndpoints(mutation, endpoints));
            }
        }

        // Apply to local batchlog memtable in this thread
        if (!nonLocalMutations.isEmpty())
            completables.add(BatchlogManager.store(Batch.createLocal(batchUUID, FBUtilities.timestampMicros(), nonLocalMutations), writeCommitLog));

        if (mutationsAndEndpoints.isEmpty())
            return Completable.concat(completables);

        // Creates write handlers: each mutation must be acknowledged to clean the batchlog.
        // We also need to update view metrics.
        Consumer<Response<EmptyPayload>> onReplicaResponse = r ->
        {
            viewWriteMetrics.viewReplicasSuccess.inc();
            long delay = Math.max(0, System.currentTimeMillis() - baseComplete.get());
            viewWriteMetrics.viewWriteLatency.update(delay, TimeUnit.MILLISECONDS);
        };

        // The copy into an immutable list is not just an optimization, it is essential for correctness
        // since it would be very wrong if we subscribed to the observable of a different handler
        List<WriteHandler> handlers = ImmutableList.copyOf(Lists.transform(mutationsAndEndpoints, mae ->
        {
            viewWriteMetrics.viewReplicasAttempted.inc(mae.endpoints.liveCount());
            WriteHandler handler = WriteHandler.builder(mae.endpoints, ConsistencyLevel.ONE, WriteType.BATCH, queryStartNanoTime)
                                               .onResponse(onReplicaResponse)
                                               .build();


            handler.thenRun(cleanupLatch::countDown);
            return handler;
        }));

        completables.addAll(handlers.stream().map(WriteHandler::toObservable).collect(Collectors.toList()));

        // now actually perform the writes
        writeBatchedMutations(mutationsAndEndpoints, handlers, localDataCenter, Verbs.WRITES.VIEW_WRITE);

        return Completable.concat(completables)
                          .doFinally(() -> viewWriteMetrics.addNano(System.nanoTime() - startTime));
    }

    @SuppressWarnings("unchecked")
    public static Single<ResultMessage.Void> mutateWithTriggers(Collection<? extends IMutation> mutations,
                                                                ConsistencyLevel consistencyLevel,
                                                                boolean mutateAtomically,
                                                                long queryStartNanoTime)
    throws WriteTimeoutException, WriteFailureException, UnavailableException, OverloadedException, InvalidRequestException
    {
        Collection<Mutation> augmented = TriggerExecutor.instance.execute(mutations);

        boolean updatesView = Keyspace.open(mutations.iterator().next().getKeyspaceName())
                              .viewManager
                              .updatesAffectView(mutations, true);

        long size = IMutation.dataSize(mutations);
        writeMetrics.mutationSize.update(size);
        writeMetricsMap.get(consistencyLevel).mutationSize.update(size);

        if (augmented != null)
            return mutateAtomically(augmented, consistencyLevel, updatesView, queryStartNanoTime)
                   .toSingleDefault(new ResultMessage.Void());
        else
        {
            if (mutateAtomically || updatesView)
                return mutateAtomically((Collection<Mutation>) mutations, consistencyLevel, updatesView, queryStartNanoTime)
                       .toSingleDefault(new ResultMessage.Void());
            else
                return mutate(mutations, consistencyLevel, queryStartNanoTime);
        }
    }

    /**
     * See mutate. Adds additional steps before and after writing a batch.
     * Before writing the batch (but after doing availability check against the FD for the row replicas):
     * write the entire batch to a batchlog elsewhere in the cluster.
     * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
     *
     * @param mutations              the Mutations to be applied across the replicas
     * @param consistencyLevel       the consistency level for the operation
     * @param requireQuorumForRemove at least a quorum of nodes will see update before deleting batchlog
     * @param queryStartNanoTime     the value of System.nanoTime() when the query started to be processed
     */
    private static Completable mutateAtomically(Collection<Mutation> mutations,
                                         ConsistencyLevel consistencyLevel,
                                         boolean requireQuorumForRemove,
                                         long queryStartNanoTime)
    throws UnavailableException, OverloadedException, WriteTimeoutException
    {
        Tracing.trace("Determining replicas for atomic batch");
        long startTime = System.nanoTime();

        String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

        // Computes the final endpoints right away to check availability before writing the batchlog
        List<MutationAndEndpoints> mutationsAndEndpoints = new ArrayList<>(mutations.size());

        try
        {
            for (Mutation mutation : mutations)
            {
                WriteEndpoints endpoints = WriteEndpoints.compute(mutation);
                endpoints.checkAvailability(consistencyLevel);
                mutationsAndEndpoints.add(new MutationAndEndpoints(mutation, endpoints));
            }
        }
        catch (UnavailableException e)
        {
            writeMetrics.unavailables.mark();
            writeMetricsMap.get(consistencyLevel).unavailables.mark();
            Tracing.trace("Unavailable");
            return Completable.error(e);
        }

        // If we are requiring quorum nodes for removal, we upgrade consistency level to QUORUM unless we already
        // require ALL, or EACH_QUORUM. This is so that *at least* QUORUM nodes see the update.
        ConsistencyLevel batchConsistencyLevel = requireQuorumForRemove && !consistencyLevel.isAtLeastQuorum()
                                                 ? ConsistencyLevel.QUORUM
                                                 : consistencyLevel;

        WriteEndpoints batchlogEndpoints = getBatchlogEndpoints(localDataCenter, batchConsistencyLevel);
        UUID batchUUID = UUIDGen.getTimeUUID();

        // write to the batchlog
        Completable batchlogCompletable = writeToBatchlog(mutations, batchlogEndpoints, batchUUID, queryStartNanoTime).toObservable();

        AsyncLatch cleanupLatch = new AsyncLatch(mutations.size(), () -> asyncRemoveFromBatchlog(batchlogEndpoints.live(), batchUUID));

        // Creates write handlers: each mutation must be acknowledged to clean the batchlog, and each mutation is
        // acknowledged once we head back from 'batchConsistencyLevel' nodes.
        // The copy into an immutable list is not just an optimization, it is essential for correctness
        // since it would be very wrong if we subscribed to the observable of a different handler
        List<WriteHandler> handlers = ImmutableList.copyOf(Lists.transform(mutationsAndEndpoints, mae ->
        {
            Keyspace keyspace = mae.endpoints.keyspace();
            AsyncLatch toCleanupLatch = new AsyncLatch(batchConsistencyLevel.blockFor(keyspace), cleanupLatch::countDown);
            return WriteHandler.builder(mae.endpoints, consistencyLevel, WriteType.BATCH, queryStartNanoTime)
                               .withIdealConsistencyLevel(DatabaseDescriptor.getIdealConsistencyLevel())
                               .onResponse(r -> toCleanupLatch.countDown())
                               .build();
        }));

        // now actually perform the writes
        Completable ret = batchlogCompletable.andThen(Completable.defer(() -> {
            writeBatchedMutations(mutationsAndEndpoints, handlers, localDataCenter, Verbs.WRITES.WRITE);
            return Completable.concat(handlers.stream().map(WriteHandler::toObservable).collect(Collectors.toList()));
        }));

        return ret.onErrorResumeNext(e -> {
            if (e instanceof UnavailableException)
            {
                writeMetrics.unavailables.mark();
                writeMetricsMap.get(consistencyLevel).unavailables.mark();
                Tracing.trace("Unavailable");
            }
            else if (e instanceof WriteTimeoutException)
            {
                writeMetrics.timeouts.mark();
                writeMetricsMap.get(consistencyLevel).timeouts.mark();
                WriteTimeoutException wte = (WriteTimeoutException)e;
                Tracing.trace("Write timeout; received {} of {} required replies", wte.received, wte.blockFor);
            }
            else if (e instanceof WriteFailureException)
            {
                writeMetrics.failures.mark();
                writeMetricsMap.get(consistencyLevel).failures.mark();
                WriteFailureException wfe = (WriteFailureException)e;
                Tracing.trace("Write failure; received {} of {} required replies", wfe.received, wfe.blockFor);
            }
            return Completable.error(e);
        }).doFinally(() -> recordLatency(consistencyLevel, startTime));
    }

    /**
     * @return true if the range is entirely local.
     */
    public static boolean isLocalRange(String keyspaceName, AbstractBounds<PartitionPosition> range)
    {
        assert !AbstractBounds.strictlyWrapsAround(range.left, range.right);

        Collection<Range<Token>> localRanges = StorageService.instance.getNormalizedLocalRanges(keyspaceName);

        // We need a range of Tokens for contains() below
        AbstractBounds<Token> queriedRange = AbstractBounds.bounds(range.left.getToken(),
                                                                   range.inclusiveLeft(),
                                                                   range.right.getToken(),
                                                                   range.inclusiveRight());

        // localRanges is normalized and therefore contains non-overlapping, non-wrapping ranges in token order.
        // Further, the queried range is a non wrapping one. So it's enough to check that the queried range
        // is contained by a local one.
        for (Range<Token> localRange : localRanges)
        {
            if (localRange.contains(queriedRange))
                return true;
        }

        return false;
    }

    /**
     * @return true if the token is local.
     */
    public static boolean isLocalToken(String keyspaceName, Token token)
    {
        Collection<Range<Token>> localRanges = StorageService.instance.getNormalizedLocalRanges(keyspaceName);
        for (Range<Token> localRange : localRanges)
        {
            if (localRange.contains(token))
                return true;
        }

        return false;
    }

    private static WriteHandler writeToBatchlog(Collection<Mutation> mutations, WriteEndpoints endpoints, UUID uuid, long queryStartNanoTime)
    {
        WriteHandler handler = WriteHandler.create(endpoints,
                                                   endpoints.liveCount() == 1 ? ConsistencyLevel.ONE : ConsistencyLevel.TWO,
                                                   WriteType.BATCH_LOG,
                                                   queryStartNanoTime);

        Batch batch = Batch.createLocal(uuid, FBUtilities.timestampMicros(), mutations);
        MessagingService.instance().send(Verbs.WRITES.BATCH_STORE.newDispatcher(endpoints.live(), batch), handler);
        return handler;
    }

    private static void asyncRemoveFromBatchlog(List<InetAddress> endpoints, UUID uuid)
    {
        MessagingService.instance().send(Verbs.WRITES.BATCH_REMOVE.newDispatcher(endpoints, new BatchRemove(uuid)));
    }

    private static void writeBatchedMutations(List<MutationAndEndpoints> mutationsAndEndpoints,
                                              List<WriteHandler> handlers,
                                              String localDataCenter,
                                              Verb.AckedRequest<Mutation> verb)
    throws OverloadedException
    {
        for (int i = 0; i < mutationsAndEndpoints.size(); i++)
        {
            Mutation mutation = mutationsAndEndpoints.get(i).mutation;
            WriteHandler handler = handlers.get(i);
            sendToHintedEndpoints(mutation, handler.endpoints(), handler, localDataCenter, verb);
        }
    }


    private static WriteHandler mutateStandard(Mutation mutation,
                                               ConsistencyLevel consistencyLevel,
                                               String localDataCenter,
                                               WriteType writeType,
                                               long queryStartNanoTime)
    {
        WriteEndpoints endpoints = WriteEndpoints.compute(mutation);
        // exit early if we can't fulfill the CL at this time
        endpoints.checkAvailability(consistencyLevel);

        WriteHandler handler = WriteHandler.builder(endpoints, consistencyLevel, writeType, queryStartNanoTime)
                                           .withIdealConsistencyLevel(DatabaseDescriptor.getIdealConsistencyLevel())
                                           .hintOnTimeout(mutation)
                                           .build();
        sendToHintedEndpoints(mutation, handler.endpoints(), handler, localDataCenter, Verbs.WRITES.WRITE);
        return handler;
    }

    // A simple pair of a mutation and its write endpoints for use by the batchlog code
    private static class MutationAndEndpoints
    {
        final Mutation mutation;
        final WriteEndpoints endpoints;

        MutationAndEndpoints(Mutation mutation, WriteEndpoints endpoints)
        {
            this.mutation = mutation;
            this.endpoints = endpoints;
        }
    }

    /*
     * Replicas are picked manually:
     * - replicas should be alive according to the failure detector
     * - replicas should be in the local datacenter
     * - choose min(2, number of qualifying candidates above)
     * - allow the local node to be the only replica only if it's a single-node DC
     */
    private static WriteEndpoints getBatchlogEndpoints(String localDataCenter, ConsistencyLevel consistencyLevel)
    throws UnavailableException
    {
        TokenMetadata.Topology topology = StorageService.instance.getTokenMetadata().cachedOnlyTokenMap().getTopology();
        Multimap<String, InetAddress> localEndpoints = HashMultimap.create(topology.getDatacenterRacks().get(localDataCenter));
        String localRack = DatabaseDescriptor.getEndpointSnitch().getRack(FBUtilities.getBroadcastAddress());

        Keyspace keyspace = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        Collection<InetAddress> chosenEndpoints = new BatchlogManager.EndpointFilter(localRack, localEndpoints).filter();
        if (chosenEndpoints.isEmpty())
        {
            if (consistencyLevel == ConsistencyLevel.ANY)
                return WriteEndpoints.withLive(keyspace, Collections.singleton(FBUtilities.getBroadcastAddress()));

            throw new UnavailableException(ConsistencyLevel.ONE, 1, 0);
        }

        return WriteEndpoints.withLive(keyspace, chosenEndpoints);
    }

    /**
     * Send the mutations to the right targets, write it locally if it corresponds or writes a hint when the node
     * is not available.
     *
     * Note about hints:
     * <pre>
     * {@code
     * | Hinted Handoff | Consist. Level |
     * | on             |       >=1      | --> wait for hints. We DO NOT notify the handler with handler.response() for hints;
     * | on             |       ANY      | --> wait for hints. Responses count towards consistency.
     * | off            |       >=1      | --> DO NOT fire hints. And DO NOT wait for them to complete.
     * | off            |       ANY      | --> DO NOT fire hints. And DO NOT wait for them to complete.
     * }
     * </pre>
     *
     * @throws OverloadedException if the hints cannot be written/enqueued
     */
    private static void sendToHintedEndpoints(final Mutation mutation,
                                              WriteEndpoints targets,
                                              WriteHandler handler,
                                              String localDataCenter,
                                              Verb.AckedRequest<Mutation> messageDefinition)
    throws OverloadedException
    {
        checkHintOverload(targets);
        MessagingService.instance().applyBackPressure(targets.live(), handler.currentTimeout()).thenAccept(v -> {

            // For performance, Mutation caches serialized buffers that are computed lazily in serializedBuffer(). That
            // computation is not synchronized however and we will potentially call that method concurrently for each
            // dispatched message (not that concurrent calls to serializedBuffer() are "unsafe" per se, just that they
            // may result in multiple computations, making the caching optimization moot). So forcing the serialization
            // here to make sure it's already cached/computed when it's concurrently used later.
            // Side note: we have one cached buffers for each used EncodingVersion and this only pre-compute the one for
            // the current version, but it's just an optimization and we're ok not optimizing for mixed-version clusters.
            Mutation.rawSerializers.get(EncodingVersion.last()).serializedBuffer(mutation);

            Collection<InetAddress> endpointsToHint = Collections2.filter(targets.dead(), StorageProxy::shouldHint);
            if (!endpointsToHint.isEmpty())
                submitHint(mutation, endpointsToHint, handler);

            MessagingService.instance().send(messageDefinition.newForwardingDispatcher(targets.live(), localDataCenter, mutation),
                                             handler);
        });
    }

    private static void checkHintOverload(WriteEndpoints endpoints)
    {
        // avoid OOMing due to excess hints.  we need to do this check even for "live" nodes, since we can
        // still generate hints for those if it's overloaded or simply dead but not yet known-to-be-dead.
        // The idea is that if we have over maxHintsInProgress hints in flight, this is probably due to
        // a small number of nodes causing problems, so we should avoid shutting down writes completely to
        // healthy nodes.  Any node with no hintsInProgress is considered healthy.
        long totalHintsInProgress = StorageMetrics.totalHintsInProgress.getCount();
        if (totalHintsInProgress <= maxHintsInProgress)
            return;

        for (InetAddress destination : endpoints)
        {
            int hintsForDestination = getHintsInProgressFor(destination).get();
            if (hintsForDestination > 0 && shouldHint(destination))
            {
                throw new OverloadedException(String.format("Too many in flight hints: %d " +
                                                            " destination: %s destination hints %d",
                                                            totalHintsInProgress, destination, hintsForDestination));
            }
        }
    }

    /**
     * Handle counter mutation on the coordinator host.
     *
     * A counter mutation needs to first be applied to a replica (that we'll call the leader for the mutation) before being
     * replicated to the other endpoint. To achieve so, there is two case:
     *   1) the coordinator host is a replica: we proceed to applying the update locally and replicate throug
     *   applyCounterMutationOnCoordinator
     *   2) the coordinator is not a replica: we forward the (counter)mutation to a chosen replica (that will proceed through
     *   applyCounterMutationOnLeader upon receive) and wait for its acknowledgment.
     *
     * Implementation note: We check if we can fulfill the CL on the coordinator host even if he is not a replica to allow
     * quicker response and because the WriteResponseHandlers don't make it easy to send back an error. We also always gather
     * the write latencies at the coordinator node to make gathering point similar to the case of standard writes.
     */
    private static WriteHandler mutateCounter(CounterMutation cm, String localDataCenter, long queryStartNanoTime) throws UnavailableException, OverloadedException
    {
        Keyspace keyspace = Keyspace.open(cm.getKeyspaceName());
        InetAddress endpoint = findSuitableEndpoint(keyspace, cm.key(), localDataCenter, cm.consistency());

        // Check availability for the whole query before forwarding to the leader
        WriteEndpoints.compute(cm).checkAvailability(cm.consistency());

        // Forward the actual update to the chosen leader replica
        WriteHandler handler = WriteHandler.create(WriteEndpoints.withLive(keyspace, Collections.singletonList(endpoint)),
                                                   ConsistencyLevel.ONE,
                                                   WriteType.COUNTER,
                                                   queryStartNanoTime);

        // While we mostly use the same path whether the leader is the local host or not (we rely on MS doing a simple
        // local delivery in the former case), only indicate forwarding if we're truly using a remote forwarding
        if (!endpoint.equals(FBUtilities.getBroadcastAddress()))
            Tracing.trace("Forwarding counter update to write leader {}", endpoint);

        MessagingService.instance().send(Verbs.WRITES.COUNTER_FORWARDING.newRequest(endpoint, cm), handler);
        return handler;
    }

    /**
     * Find a suitable replica as leader for counter update.
     * For now, we pick a random replica in the local DC (or ask the snitch if
     * there is no replica alive in the local DC).
     * TODO: if we track the latency of the counter writes (which makes sense
     * contrarily to standard writes since there is a read involved), we could
     * trust the dynamic snitch entirely, which may be a better solution. It
     * is unclear we want to mix those latencies with read latencies, so this
     * may be a bit involved.
     */
    private static InetAddress findSuitableEndpoint(Keyspace keyspace, DecoratedKey key, String localDataCenter, ConsistencyLevel cl) throws UnavailableException
    {
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        List<InetAddress> endpoints = new ArrayList<>();
        StorageService.instance.getLiveNaturalEndpoints(keyspace, key, endpoints);

        // CASSANDRA-13043: filter out those endpoints not accepting clients yet, maybe because still bootstrapping
        endpoints.removeIf(endpoint -> !StorageService.instance.isRpcReady(endpoint));

        // TODO have a way to compute the consistency level
        if (endpoints.isEmpty())
            throw new UnavailableException(cl, cl.blockFor(keyspace), 0);

        List<InetAddress> localEndpoints = new ArrayList<>(endpoints.size());

        for (InetAddress endpoint : endpoints)
            if (snitch.getDatacenter(endpoint).equals(localDataCenter))
                localEndpoints.add(endpoint);

        if (localEndpoints.isEmpty())
        {
            // If the consistency required is local then we should not involve other DCs
            if (cl.isDatacenterLocal())
                throw new UnavailableException(cl, cl.blockFor(keyspace), 0);

            // No endpoint in local DC, pick the closest endpoint according to the snitch
            snitch.sortByProximity(FBUtilities.getBroadcastAddress(), endpoints);
            return endpoints.get(0);
        }

        return localEndpoints.get(ThreadLocalRandom.current().nextInt(localEndpoints.size()));
    }

    // Must be called on a replica of the mutation. This replica becomes the leader of this mutation.
    public static CompletableFuture<Void> applyCounterMutationOnLeader(CounterMutation cm, String localDataCenter, long queryStartNanoTime)
    throws UnavailableException, OverloadedException
    {
       CompletableFuture<Void> ret = new CompletableFuture<>();

        cm.applyCounterMutation().subscribe(
            result -> {

                WriteEndpoints endpoints = WriteEndpoints.compute(cm.getKeyspaceName(), cm.key());
                WriteHandler handler = WriteHandler.builder(endpoints, cm.consistency(), WriteType.COUNTER, queryStartNanoTime)
                                                   .withIdealConsistencyLevel(DatabaseDescriptor.getIdealConsistencyLevel())
                                                   .hintOnTimeout(result)
                                                   .build();

                // We already wrote locally
                handler.onLocalResponse();

                WriteEndpoints remainingEndpoints = handler.endpoints().withoutLocalhost();
                if (!remainingEndpoints.isEmpty())
                    sendToHintedEndpoints(result, remainingEndpoints, handler, localDataCenter, Verbs.WRITES.WRITE);

                handler.whenComplete((r, t) -> {
                    if (t != null)
                        ret.completeExceptionally(t);
                    else
                        ret.complete(r);
                });
            },
            ex -> {
                ret.completeExceptionally(ex);
            });


        return ret;
    }

    private static boolean systemKeyspaceQuery(List<? extends ReadCommand> cmds)
    {
        for (ReadCommand cmd : cmds)
            if (!SchemaConstants.isSystemKeyspace(cmd.metadata().keyspace))
                return false;
        return true;
    }

    // this is blocking
    private static FilteredPartition readOne(SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel, long queryStartNanoTime)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        ReadContext ctx = ReadContext.builder(command, consistencyLevel).build(queryStartNanoTime);
        return readOne(command, ctx);
    }

    // this is blocking
    private static FilteredPartition readOne(SinglePartitionReadCommand command, ReadContext ctx)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        return read(SinglePartitionReadCommand.Group.one(command), ctx)
               .flatMap(partition -> FilteredPartition.create(partition))
               .take(1)
               .ifEmpty(FilteredPartition.empty(command))
               .blockingSingle();
    }

    public static Flow<FlowablePartition> read(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel, long queryStartNanoTime)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        // When using serial CL, the ClientState should be provided
        assert !consistencyLevel.isSerialConsistency();
        return read(group, ReadContext.builder(group, consistencyLevel).build(queryStartNanoTime));
    }

    private static void checkNotBootstrappingOrSystemQuery(List<? extends ReadCommand> commands, ClientRequestMetrics ... metrics)
    throws IsBootstrappingException
    {
        if (StorageService.instance.isBootstrapMode() && !systemKeyspaceQuery(commands))
        {
            for (ClientRequestMetrics metric : metrics)
                metric.unavailables.mark();

            throw new IsBootstrappingException();
        }
    }

    /**
     * Performs the actual reading of a row out of the StorageService, fetching
     * a specific set of column names from a given column family.
     */
    public static Flow<FlowablePartition> read(SinglePartitionReadCommand.Group group, ReadContext ctx)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        checkNotBootstrappingOrSystemQuery(group.commands, readMetrics, readMetricsMap.get(ctx.consistencyLevel));

        if (ctx.consistencyLevel.isSerialConsistency())
        {
            assert !ctx.forContinuousPaging; // this is not supported
            return readWithPaxos(group, ctx);
        }

        return ctx.forContinuousPaging && ctx.consistencyLevel.isSingleNode() && group.queriesOnlyLocalData()
             ? readLocalContinuous(group, ctx)
             : readRegular(group, ctx);
    }

    /**
     * Read data locally, but for an external request. This implements an optimized local read path for data that
     * is available locally and that has been requested at a consistency level of ONE/LOCAL_ONE. We
     * wrap the functionality of {@link ReadCommand#executeInternal()}  with additional
     * functionality that is required for client requests, such as metrics recording and local query monitoring,
     * to ensure {@link ReadExecutionController} is not kept for too long. If local queries are aborted, they
     * are not reported as failed, rather the caller will take care of restarting them.
     * <p>
     * <b>Warning:</b> because this return a direct iterator, the returned iterator keeps an {@code ExecutionController}
     * open and so callers should make sure the iterator is closed on every
     * path, preferably through the use of a try-with-resources.
     *
     * @param group the group of single partition read commands
     * @param ctx the read context for the query.
     * @return the filtered partition iterator
     */
    @SuppressWarnings("resource")
    private static Flow<FlowablePartition> readLocalContinuous(SinglePartitionReadCommand.Group group, ReadContext ctx)
    throws IsBootstrappingException, UnavailableException, ReadFailureException, ReadTimeoutException
    {
        assert ctx.consistencyLevel.isSingleNode();

        if (logger.isTraceEnabled())
            logger.trace("Querying single partition commands {} for continuous paging", group);

        return group.executeInternal()
                    .doOnError(error -> {
                        readMetrics.failures.mark();
                        readMetricsMap.get(ctx.consistencyLevel).failures.mark();
                    });
    }

    private static Flow<FlowablePartition> readWithPaxos(SinglePartitionReadCommand.Group group, ReadContext ctx)
    throws InvalidRequestException, UnavailableException, ReadFailureException, ReadTimeoutException
    {
       assert ctx.clientState != null;
       if (group.commands.size() > 1)
           throw new InvalidRequestException("SERIAL/LOCAL_SERIAL consistency may only be requested for one partition at a time");

       long start = System.nanoTime();
       SinglePartitionReadCommand command = group.commands.get(0);
       TableMetadata metadata = command.metadata();
       DecoratedKey key = command.partitionKey();
        ConsistencyLevel consistencyLevel = ctx.consistencyLevel;

       // make sure any in-progress paxos writes are done (i.e., committed to a majority of replicas), before performing a quorum read
       Pair<WriteEndpoints, Integer> p = getPaxosParticipants(metadata, key, consistencyLevel);
       List<InetAddress> liveEndpoints = p.left.live();
       int requiredParticipants = p.right;

       // does the work of applying in-progress writes; throws UAE or timeout if it can't
       final ConsistencyLevel consistencyForCommitOrFetch = consistencyLevel == ConsistencyLevel.LOCAL_SERIAL
                                                            ? ConsistencyLevel.LOCAL_QUORUM
                                                            : ConsistencyLevel.QUORUM;

       try
       {
           final Pair<UUID, Integer> pair = beginAndRepairPaxos(start,
                                                                key,
                                                                metadata,
                                                                liveEndpoints,
                                                                requiredParticipants,
                                                                consistencyLevel,
                                                                consistencyForCommitOrFetch,
                                                                false,
                                                                ctx.clientState);
           if (pair.right > 0)
               casReadMetrics.contention.update(pair.right);
       }
       catch (WriteTimeoutException e)
       {
           return Flow.error(new ReadTimeoutException(consistencyLevel, 0, consistencyLevel.blockFor(Keyspace.open(metadata.keyspace)), false));
       }
       catch (WriteFailureException e)
       {
           return Flow.error(new ReadFailureException(consistencyLevel, e.received, e.blockFor, false, e.failureReasonByEndpoint));
       }

        return fetchRows(group.commands, ctx.withConsistency(consistencyForCommitOrFetch))
               .doOnError(e -> {
                   if (e instanceof UnavailableException)
                   {
                       readMetrics.unavailables.mark();
                       casReadMetrics.unavailables.mark();
                       readMetricsMap.get(consistencyLevel).unavailables.mark();
                   }
                   else if (e instanceof ReadTimeoutException)
                   {
                       readMetrics.timeouts.mark();
                       casReadMetrics.timeouts.mark();
                       readMetricsMap.get(consistencyLevel).timeouts.mark();
                   }
                   else if (e instanceof ReadFailureException)
                   {
                       readMetrics.failures.mark();
                       casReadMetrics.failures.mark();
                       readMetricsMap.get(consistencyLevel).failures.mark();
                   }
               })
               .doOnClose(() -> {
                   long latency = recordLatency(group, ctx);
                   casReadMetrics.addNano(latency);
               });
    }

    @SuppressWarnings("resource")
    private static Flow<FlowablePartition> readRegular(SinglePartitionReadCommand.Group group, ReadContext ctx)
    throws UnavailableException, ReadFailureException, ReadTimeoutException
    {
        Flow<FlowablePartition> result = fetchRows(group.commands, ctx);

        // Note that the only difference between the command in a group must be the partition key on which
        // they applied.
        boolean enforceStrictLiveness = group.commands.get(0).metadata().enforceStrictLiveness();
        // For continuous paging, we know we enforce global limits when we have more than one command
        // later (by always wrapping in a pager) and we also don't need to update the metrics or latency
        // because it has its own dedicated metrics, so just return the result here.
        if (ctx.forContinuousPaging)
            return result;

        // If we have more than one command, then despite each read command honoring the limit, the total result
        // might not honor it and so we should enforce it;
        if (group.commands.size() > 1)
            result = result.map(r -> group.limits().truncateFiltered(r, group.nowInSec(), group.selectsFullPartition(), enforceStrictLiveness));

        return result.doOnError(e ->
                                {
                                    if (e instanceof UnavailableException)
                                    {
                                        readMetrics.unavailables.mark();
                                        readMetricsMap.get(ctx.consistencyLevel).unavailables.mark();
                                    }
                                    else if (e instanceof ReadTimeoutException)
                                    {
                                        readMetrics.timeouts.mark();
                                        readMetricsMap.get(ctx.consistencyLevel).timeouts.mark();
                                    }
                                    else if (e instanceof ReadFailureException)
                                    {
                                        readMetrics.failures.mark();
                                        readMetricsMap.get(ctx.consistencyLevel).failures.mark();
                                    }
                                })
                     .doOnClose(() -> recordLatency(group, ctx));
    }

    /**
     * Calculate the latency in nano seconds from the start time and update the latency metrics for the single
     * partition read commands in the group. Return the latency that was recorded in case it needs to be applied
     * to more metrics, e.g. CAS metrics.
     *
     * @param group the group of read commands
     * @param ctx the command context
     *
     * @return the latency that was recorded
     */
    private static long recordLatency(SinglePartitionReadCommand.Group group, ReadContext ctx)
    {
        long latency = System.nanoTime() - ctx.queryStartNanos;

        readMetrics.addNano(latency);
        readMetricsMap.get(ctx.consistencyLevel).addNano(latency);

        // TODO avoid giving every command the same latency number.  Can fix this in CASSANDRA-5329
        for (ReadCommand command : group.commands)
            Keyspace.openAndGetStore(command.metadata()).metric.coordinatorReadLatency.update(latency, TimeUnit.NANOSECONDS);

        return latency;
    }

    /**
     * This function executes local and remote reads, and blocks for the results:
     *
     * 1. Get the replica locations, sorted by response time according to the snitch
     * 2. Send a data request to the closest replica, and digest requests to either
     *    a) all the replicas, if read repair is enabled
     *    b) the closest R-1 replicas, where R is the number required to satisfy the ConsistencyLevel
     * 3. Wait for a response from R replicas
     * 4. If the digests (if any) match the data return the data
     * 5. else carry out read repair by getting data from all the nodes.
     */
    private static Flow<FlowablePartition> fetchRows(List<SinglePartitionReadCommand> commands, ReadContext ctx)
    throws UnavailableException, ReadFailureException, ReadTimeoutException
    {
        if (commands.size() == 1)
            return new SinglePartitionReadLifecycle(commands.get(0), ctx).result();

        return Flow.fromIterable(commands)
                   .flatMap(command -> new SinglePartitionReadLifecycle(command, ctx).result());
    }

    private static class SinglePartitionReadLifecycle
    {
        private final SinglePartitionReadCommand command;
        private final AbstractReadExecutor executor;
        private final ReadContext ctx;

        private ReadCallback<FlowablePartition> repairHandler;

        SinglePartitionReadLifecycle(SinglePartitionReadCommand command, ReadContext ctx)
        {
            this.command = command;
            this.executor = AbstractReadExecutor.getReadExecutor(command, ctx);
            this.ctx = ctx;
        }

        Completable doInitialQueries()
        {
            return executor.executeAsync();
        }

        Completable maybeTryAdditionalReplicas()
        {
            return executor.maybeTryAdditionalReplicas();
        }

        Flow<FlowablePartition> result()
        {
            return Flow.concat(Completable.concatArray(doInitialQueries(), maybeTryAdditionalReplicas()),
                               executor.handler.result())
                       .onErrorResumeNext(e -> {
                                if (logger.isTraceEnabled())
                                    logger.trace("Got error {}/{}", e.getClass().getName(), e.getMessage());

                                if (e instanceof RuntimeException && e.getCause() != null)
                                    e = e.getCause();

                                if (e instanceof DigestMismatchException)
                                    return retryOnDigestMismatch((DigestMismatchException)e);

                                return Flow.error(e);
                            });
        }

        Flow<FlowablePartition> retryOnDigestMismatch(DigestMismatchException ex) throws ReadFailureException, ReadTimeoutException
        {
            Tracing.trace("Digest mismatch: {}", ex.getMessage());

            ReadRepairMetrics.repairedBlocking.mark();

            // Do a full data read to resolve the correct response (and repair node that need be)
            Pair<ReadCallback<FlowablePartition>, Collection<InetAddress>> p = executor.handler.forDigestMismatchRepair(executor.getContactedReplicas());
            repairHandler = p.left;
            Collection<InetAddress> contactedReplicas = p.right;

            Tracing.trace("Enqueuing full data reads to {}", contactedReplicas);
            MessagingService.instance().send(command.dispatcherTo(contactedReplicas), repairHandler);

            return repairHandler.result().mapError(e -> {
                if (logger.isTraceEnabled())
                    logger.trace("Got error {}/{}", e.getClass().getName(), e.getMessage());

                if (e instanceof RuntimeException && e.getCause() != null)
                    e = e.getCause();

                if (e instanceof DigestMismatchException)
                    return new AssertionError(e); // full data requested from each node here, no digests should be sent

                else if(e instanceof ReadTimeoutException)
                {
                    if (Tracing.isTracing())
                        Tracing.trace("Timed out waiting on digest mismatch repair requests");
                    else
                        logger.trace("Timed out waiting on digest mismatch repair requests");

                    // the caught exception here will have CL.ALL from the repair command,
                    // not whatever CL the initial command was at (CASSANDRA-7947)
                    int blockFor = ctx.consistencyLevel.blockFor(Keyspace.open(command.metadata().keyspace));
                    return new ReadTimeoutException(ctx.consistencyLevel, blockFor-1, blockFor, true);
                }

                return e;
            });
        }
    }

    public static List<InetAddress> getLiveSortedEndpoints(Keyspace keyspace, RingPosition pos)
    {
        List<InetAddress> liveEndpoints = StorageService.instance.getLiveNaturalEndpoints(keyspace, pos);
        DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getBroadcastAddress(), liveEndpoints);
        return liveEndpoints;
    }

    private static List<InetAddress> intersection(List<InetAddress> l1, List<InetAddress> l2)
    {
        // Note: we don't use Guava Sets.intersection() for 3 reasons:
        //   1) retainAll would be inefficient if l1 and l2 are large but in practice both are the replicas for a range and
        //   so will be very small (< RF). In that case, retainAll is in fact more efficient.
        //   2) we do ultimately need a list so converting everything to sets don't make sense
        //   3) l1 and l2 are sorted by proximity. The use of retainAll  maintain that sorting in the result, while using sets wouldn't.
        List<InetAddress> inter = new ArrayList<InetAddress>(l1);
        inter.retainAll(l2);
        return inter;
    }

    /**
     * Estimate the number of result rows per range in the ring based on our local data.
     * <p>
     * This assumes that ranges are uniformly distributed across the cluster and
     * that the queried data is also uniformly distributed.
     */
    private static float estimateResultsPerRange(PartitionRangeReadCommand command, Keyspace keyspace)
    {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
        Index index = command.getIndex(cfs);
        float maxExpectedResults = index == null
                                 ? command.limits().estimateTotalResults(cfs)
                                 : index.getEstimatedResultRows();

        // adjust maxExpectedResults by the number of tokens this node has and the replication factor for this ks
        return (maxExpectedResults / DatabaseDescriptor.getNumTokens()) / keyspace.getReplicationStrategy().getReplicationFactor();
    }

    private static class RangeForQuery
    {
        public final AbstractBounds<PartitionPosition> range;
        public final List<InetAddress> liveEndpoints;
        public final List<InetAddress> filteredEndpoints;

        public RangeForQuery(AbstractBounds<PartitionPosition> range, List<InetAddress> liveEndpoints, List<InetAddress> filteredEndpoints)
        {
            this.range = range;
            this.liveEndpoints = liveEndpoints;
            this.filteredEndpoints = filteredEndpoints;
        }

        @Override
        public String toString()
        {
            return String.format("[%s -> %s", range, filteredEndpoints);
        }
    }

    private static class RangeIterator extends AbstractIterator<RangeForQuery>
    {
        private final Keyspace keyspace;
        private final ReadContext params;
        private final Iterator<? extends AbstractBounds<PartitionPosition>> ranges;
        private final int rangeCount;

        public RangeIterator(PartitionRangeReadCommand command, Keyspace keyspace, ReadContext params)
        {
            this.keyspace = keyspace;
            this.params = params;

            List<? extends AbstractBounds<PartitionPosition>> l = keyspace.getReplicationStrategy() instanceof LocalStrategy
                                                          ? command.dataRange().keyRange().unwrap()
                                                          : getRestrictedRanges(command.dataRange().keyRange());
            this.ranges = l.iterator();
            this.rangeCount = l.size();
        }

        public int rangeCount()
        {
            return rangeCount;
        }

        protected RangeForQuery computeNext()
        {
            if (!ranges.hasNext())
                return endOfData();

            AbstractBounds<PartitionPosition> range = ranges.next();
            List<InetAddress> liveEndpoints = getLiveSortedEndpoints(keyspace, range.right);
            return new RangeForQuery(range, liveEndpoints, params.filterForQuery(liveEndpoints));
        }
    }

    private static class RangeMerger extends AbstractIterator<RangeForQuery>
    {
        private final PeekingIterator<RangeForQuery> ranges;
        private final ReadContext params;

        private RangeMerger(Iterator<RangeForQuery> iterator, ReadContext params)
        {
            this.params = params;
            this.ranges = Iterators.peekingIterator(iterator);
        }

        protected RangeForQuery computeNext()
        {
            if (!ranges.hasNext())
                return endOfData();

            RangeForQuery current = ranges.next();
            Keyspace keyspace = params.keyspace;
            ConsistencyLevel consistency = params.consistencyLevel;

            // getRestrictedRange has broken the queried range into per-[vnode] token ranges, but this doesn't take
            // the replication factor into account. If the intersection of live endpoints for 2 consecutive ranges
            // still meets the CL requirements, then we can merge both ranges into the same RangeSliceCommand.
            while (ranges.hasNext())
            {
                // If the current range right is the min token, we should stop merging because CFS.getRangeSlice
                // don't know how to deal with a wrapping range.
                // Note: it would be slightly more efficient to have CFS.getRangeSlice on the destination nodes unwraps
                // the range if necessary and deal with it. However, we can't start sending wrapped range without breaking
                // wire compatibility, so It's likely easier not to bother;
                if (current.range.right.isMinimum())
                    break;

                RangeForQuery next = ranges.peek();

                List<InetAddress> merged = intersection(current.liveEndpoints, next.liveEndpoints);

                // Check if there is enough endpoint for the merge to be possible.
                if (!consistency.isSufficientLiveNodes(keyspace, merged))
                    break;

                List<InetAddress> filteredMerged = params.filterForQuery(merged);

                // Estimate whether merging will be a win or not
                if (!DatabaseDescriptor.getEndpointSnitch().isWorthMergingForRangeQuery(filteredMerged, current.filteredEndpoints, next.filteredEndpoints))
                    break;

                // If we get there, merge this range and the next one
                current = new RangeForQuery(current.range.withNewRight(next.range.right), merged, filteredMerged);
                ranges.next(); // consume the range we just merged since we've only peeked so far
            }
            return current;
        }
    }

    private static class RangeCommandPartitions
    {
        private final Iterator<RangeForQuery> ranges;
        private final int totalRangeCount;
        private final PartitionRangeReadCommand command;
        private final ReadContext ctx;

        private final long startTime;
        private DataLimits.Counter counter;

        private int concurrencyFactor;
        // The two following "metric" are maintained to improve the concurrencyFactor
        // when it was not good enough initially.
        private int liveReturned;
        private int rangesQueried;

        public RangeCommandPartitions(RangeIterator ranges,
                                      PartitionRangeReadCommand command,
                                      int concurrencyFactor,
                                      ReadContext ctx)
        {
            this.command = command;
            this.concurrencyFactor = concurrencyFactor;
            this.startTime = System.nanoTime();
            this.ranges = new RangeMerger(ranges, ctx);
            this.totalRangeCount = ranges.rangeCount();
            this.ctx = ctx;
        }

        Flow<FlowablePartition> partitions()
        {
            Flow<FlowablePartition> partitions = nextBatch().concatWith(this::moreContents);

            /** continuous paging requests use different metrics, see {@link ContinuousPagingMetrics}. */
            if (!ctx.forContinuousPaging)
                partitions = partitions.doOnClose(this::close);

            return partitions;
        }

        private Flow<FlowablePartition> nextBatch()
        {
            Flow<FlowablePartition> batch = sendNextRequests();

            /** continuous paging requests use different metrics, see {@link ContinuousPagingMetrics}. */
            if (!ctx.forContinuousPaging)
                batch = batch.doOnError(this::handleError);

            return batch.doOnComplete(this::handleBatchCompleted);
        }

        private Flow<FlowablePartition> moreContents()
        {
            // If we don't have more range to handle, we're done
            if (!ranges.hasNext())
                return null;

            return nextBatch();
        }

        private void handleError(Throwable e)
        {
            if (e instanceof UnavailableException)
            {
                rangeMetrics.unavailables.mark();
            }
            else if (e instanceof ReadTimeoutException)
            {
                rangeMetrics.timeouts.mark();
            }
            else if (e instanceof ReadFailureException)
            {
                rangeMetrics.failures.mark();
            }
        }

        private void handleBatchCompleted()
        {
            liveReturned += counter.counted();

            // It's not the first batch of queries and we're not done, so we we can use what has been
            // returned so far to improve our rows-per-range estimate and update the concurrency accordingly
            updateConcurrencyFactor();
        }

        private void updateConcurrencyFactor()
        {
            if (liveReturned == 0)
            {
                // we haven't actually gotten any results, so query all remaining ranges at once
                concurrencyFactor = totalRangeCount - rangesQueried;
                return;
            }

            // Otherwise, compute how many rows per range we got on average and pick a concurrency factor
            // that should allow us to fetch all remaining rows with the next batch of (concurrent) queries.
            int remainingRows = command.limits().count() - liveReturned;
            float rowsPerRange = (float)liveReturned / (float)rangesQueried;
            concurrencyFactor = Math.max(1, Math.min(totalRangeCount - rangesQueried, Math.round(remainingRows / rowsPerRange)));
            logger.trace("Didn't get enough response rows; actual rows per range: {}; remaining rows: {}, new concurrent requests: {}",
                         rowsPerRange, remainingRows, concurrencyFactor);
        }

        /**
         * Queries the provided sub-range.
         *
         * @param toQuery the subRange to query.
         * @param isFirst in the case where multiple queries are sent in parallel, whether that's the first query on
         * that batch or not. The reason it matters is that whe paging queries, the command (more specifically the
         * {@code DataLimits}) may have "state" information and that state may only be valid for the first query (in
         * that it's the query that "continues" whatever we're previously queried).
         */
        private Flow<FlowablePartition> query(RangeForQuery toQuery, boolean isFirst)
        {
            PartitionRangeReadCommand rangeCommand = command.forSubRange(toQuery.range, isFirst);

            ReadCallback<FlowablePartition> handler = ReadCallback.forInitialRead(rangeCommand, toQuery.filteredEndpoints, ctx);
            handler.assureSufficientLiveNodes();

            List<InetAddress> replicas = toQuery.filteredEndpoints;

            if (ctx.withDigests)
            {
                // Send the data request to the first node, digests to the rest
                MessagingService.instance().send(rangeCommand.requestTo(replicas.get(0)), handler);

                if (replicas.size() > 1)
                {
                    ReadCommand digestCommand = rangeCommand.createDigestCommand(DigestVersion.forReplicas(replicas));
                    MessagingService.instance().send(digestCommand.dispatcherTo(replicas.subList(1, replicas.size())), handler);
                }
            }
            else
            {
                MessagingService.instance().send(rangeCommand.dispatcherTo(replicas), handler);
            }
            return handler.result()
                          .onErrorResumeNext(e -> {
                              if (e instanceof RuntimeException && e.getCause() != null)
                                  e = e.getCause();

                              if (e instanceof DigestMismatchException)
                                  return retryOnDigestMismatch(handler, (DigestMismatchException)e);

                              return Flow.error(e);
                          });
        }

        Flow<FlowablePartition> retryOnDigestMismatch(ReadCallback<FlowablePartition> handler, DigestMismatchException ex) throws ReadFailureException, ReadTimeoutException
        {
            Tracing.trace("Digest mismatch: {}", ex);

            ReadCommand command = handler.command();
            Pair<ReadCallback<FlowablePartition>, Collection<InetAddress>> p = handler.forDigestMismatchRepair(handler.endpoints);
            ReadCallback<FlowablePartition> repairHandler = p.left;
            Collection<InetAddress> endpoints = p.right;

            Tracing.trace("Enqueuing full data reads to {}", endpoints);
            MessagingService.instance().send(command.dispatcherTo(endpoints), repairHandler);

            return repairHandler.result();
        }

        private Flow<FlowablePartition> sendNextRequests()
        {
            if (logger.isTraceEnabled())
                logger.trace("Sending requests with concurrencyFactor {}", concurrencyFactor);

            List<Flow<FlowablePartition>> concurrentQueries = new ArrayList<>(concurrencyFactor);
            for (int i = 0; i < concurrencyFactor && ranges.hasNext(); i++)
            {
                concurrentQueries.add(query(ranges.next(), i == 0));
                ++rangesQueried;
            }

            Tracing.trace("Submitted {} concurrent range requests", concurrentQueries.size());
            // We want to count the results for the sake of updating the concurrency factor (see updateConcurrencyFactor) but we don't want to
            // enforce any particular limit at this point (this could break code than rely on postReconciliationProcessing), hence the DataLimits.NONE.
            counter = DataLimits.NONE.newCounter(command.nowInSec(), true, command.selectsFullPartition(), command.metadata().enforceStrictLiveness());
            return DataLimits.truncateFiltered(Flow.concat(concurrentQueries), counter);
        }

        public void close()
        {
            recordLatency(command, startTime);
        }
    }

    /**
     * Calculate the latency in nano seconds from the start time and update the latency metrics for the
     * partition range read command.
     *
     * @param command - the read command
     * @param start - the read start time in nanoSeconds
     */
    private static void recordLatency(PartitionRangeReadCommand command, long start)
    {
        long latency = System.nanoTime() - start;
        rangeMetrics.addNano(latency);
        Keyspace.openAndGetStore(command.metadata()).metric.coordinatorScanLatency.update(latency, TimeUnit.NANOSECONDS);
    }

    /**
     * Record the write latency
     *
     * @param cl - consistency level used for write
     * @param start - start time of the write
     */
    private static void recordLatency(ConsistencyLevel cl, long start)
    {
        long latency = System.nanoTime() - start;
        writeMetrics.addNano(latency);
        writeMetricsMap.get(cl).addNano(latency);
    }

    public static Flow<FlowablePartition> getRangeSlice(PartitionRangeReadCommand command, ReadContext ctx)
    {
        return ctx.forContinuousPaging && ctx.consistencyLevel.isSingleNode() && command.queriesOnlyLocalData()
             ? getRangeSliceLocalContinuous(command, ctx)
             : getRangeSliceRemote(command, ctx);
    }

    @SuppressWarnings("resource")
    private static Flow<FlowablePartition> getRangeSliceRemote(PartitionRangeReadCommand command, ReadContext ctx)
    {
        checkNotBootstrappingOrSystemQuery(Collections.singletonList(command), rangeMetrics);

        Tracing.trace("Computing ranges to query");


        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        RangeIterator ranges = new RangeIterator(command, keyspace, ctx);

        // our estimate of how many result rows there will be per-range
        float resultsPerRange = estimateResultsPerRange(command, keyspace);
        // underestimate how many rows we will get per-range in order to increase the likelihood that we'll
        // fetch enough rows in the first round
        resultsPerRange -= resultsPerRange * CONCURRENT_SUBREQUESTS_MARGIN;
        int concurrencyFactor = resultsPerRange == 0.0
                                ? 1
                                : Math.max(1, Math.min(ranges.rangeCount(), (int) Math.ceil(command.limits().count() / resultsPerRange)));
        logger.trace("Estimated result rows per range: {}; requested rows: {}, ranges.size(): {}; concurrent range requests: {}",
                     resultsPerRange, command.limits().count(), ranges.rangeCount(), concurrencyFactor);
        Tracing.trace("Submitting range requests on {} ranges with a concurrency of {} ({} rows per range expected)", ranges.rangeCount(), concurrencyFactor, resultsPerRange);

        // Note that in general, RangeCommandPartitions will honor the command limit for each range, but will not enforce it globally.

        return command.withLimitsAndPostReconciliation(new RangeCommandPartitions(ranges, command, concurrencyFactor, ctx).partitions());
    }

    /**
     * Read a range slice locally, but for an external request. This implements an optimized local read path for data that
     * is available locally and that has been requested at a consistency level of ONE or LOCAL_ONE. We
     * wrap the functionality of {@link ReadCommand#executeInternal()}  with additional
     * functionality that is required for client requests, such as metrics recording and local query monitoring,
     * to ensure {@link ReadExecutionController} is not kept for too long. If local queries are aborted, they
     * are not reported as failed, rather the caller will take care of restarting them.
     * <p>
     * <b>Warning:</b> because this return a direct iterator, the returned iterator keeps an {@code ExecutionController}
     * open and so callers should make sure the iterator is closed on every
     * path, preferably through the use of a try-with-resources.
     *
     * @param command the read command
     * @param ctx the query context
     * @return the filtered partition iterator
     */
    @SuppressWarnings("resource")
    public static Flow<FlowablePartition> getRangeSliceLocalContinuous(PartitionRangeReadCommand command, ReadContext ctx)
    {
        assert ctx.consistencyLevel.isSingleNode();

        checkNotBootstrappingOrSystemQuery(Collections.singletonList(command), rangeMetrics);

        if (logger.isTraceEnabled())
            logger.trace("Querying local ranges {} for continuous paging", command);

        // Same reasoning as in readLocalContinuous, see there for details.
        return command.withLimitsAndPostReconciliation(command.executeInternal())
                      .doOnError(e -> rangeMetrics.failures.mark());
    }

    public Map<String, List<String>> getSchemaVersions()
    {
        return describeSchemaVersions();
    }

    /**
     * initiate a request/response session with each live node to check whether or not everybody is using the same
     * migration id. This is useful for determining if a schema change has propagated through the cluster. Disagreement
     * is assumed if any node fails to respond.
     */
    public static Map<String, List<String>> describeSchemaVersions()
    {
        final String myVersion = Schema.instance.getVersion().toString();
        final Map<InetAddress, UUID> versions = new ConcurrentHashMap<InetAddress, UUID>();
        final Set<InetAddress> liveHosts = Gossiper.instance.getLiveMembers();
        final CountDownLatch latch = new CountDownLatch(liveHosts.size());

        MessageCallback<UUID> cb = new MessageCallback<UUID>()
        {
            public void onResponse(Response<UUID> message)
            {
                // record the response from the remote node.
                versions.put(message.from(), message.payload());
                latch.countDown();
            }

            public void onFailure(FailureResponse<UUID> message)
            {
                // Ignore failure, we'll just timeout
            }
        };
        // an empty message acts as a request to the Schema Version message type.
        for (InetAddress endpoint : liveHosts)
            MessagingService.instance().send(Verbs.SCHEMA.VERSION.newRequest(endpoint, EmptyPayload.instance), cb);

        try
        {
            // wait for as long as possible. timeout-1s if possible.
            latch.await(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError("This latch shouldn't have been interrupted.");
        }

        // maps versions to hosts that are on that version.
        Map<String, List<String>> results = new HashMap<String, List<String>>();
        Iterable<InetAddress> allHosts = Iterables.concat(Gossiper.instance.getLiveMembers(), Gossiper.instance.getUnreachableMembers());
        for (InetAddress host : allHosts)
        {
            UUID version = versions.get(host);
            String stringVersion = version == null ? UNREACHABLE : version.toString();
            List<String> hosts = results.get(stringVersion);
            if (hosts == null)
            {
                hosts = new ArrayList<String>();
                results.put(stringVersion, hosts);
            }
            hosts.add(host.getHostAddress());
        }

        // we're done: the results map is ready to return to the client.  the rest is just debug logging:
        if (results.get(UNREACHABLE) != null)
            logger.debug("Hosts not in agreement. Didn't get a response from everybody: {}", StringUtils.join(results.get(UNREACHABLE), ","));
        for (Map.Entry<String, List<String>> entry : results.entrySet())
        {
            // check for version disagreement. log the hosts that don't agree.
            if (entry.getKey().equals(UNREACHABLE) || entry.getKey().equals(myVersion))
                continue;
            for (String host : entry.getValue())
                logger.debug("{} disagrees ({})", host, entry.getKey());
        }
        if (results.size() == 1)
            logger.debug("Schemas are in agreement.");

        return results;
    }

    /**
     * Compute all ranges we're going to query, in sorted order. Nodes can be replica destinations for many ranges,
     * so we need to restrict each scan to the specific range we want, or else we'd get duplicate results.
     */
    static <T extends RingPosition<T>> List<AbstractBounds<T>> getRestrictedRanges(final AbstractBounds<T> queryRange)
    {
        // special case for bounds containing exactly 1 (non-minimum) token
        if (queryRange instanceof Bounds && queryRange.left.equals(queryRange.right) && !queryRange.left.isMinimum())
        {
            return Collections.singletonList(queryRange);
        }

        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();

        List<AbstractBounds<T>> ranges = new ArrayList<>();
        // divide the queryRange into pieces delimited by the ring and minimum tokens
        Iterator<Token> ringIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), queryRange.left.getToken(), true);
        AbstractBounds<T> remainder = queryRange;
        while (ringIter.hasNext())
        {
            /*
             * remainder can be a range/bounds of token _or_ keys and we want to split it with a token:
             *   - if remainder is tokens, then we'll just split using the provided token.
             *   - if remainder is keys, we want to split using token.upperBoundKey. For instance, if remainder
             *     is [DK(10, 'foo'), DK(20, 'bar')], and we have 3 nodes with tokens 0, 15, 30. We want to
             *     split remainder to A=[DK(10, 'foo'), 15] and B=(15, DK(20, 'bar')]. But since we can't mix
             *     tokens and keys at the same time in a range, we uses 15.upperBoundKey() to have A include all
             *     keys having 15 as token and B include none of those (since that is what our node owns).
             * asSplitValue() abstracts that choice.
             */
            Token upperBoundToken = ringIter.next();
            T upperBound = (T)upperBoundToken.upperBound(queryRange.left.getClass());
            if (!remainder.left.equals(upperBound) && !remainder.contains(upperBound))
                // no more splits
                break;
            Pair<AbstractBounds<T>,AbstractBounds<T>> splits = remainder.split(upperBound);
            if (splits == null)
                continue;

            ranges.add(splits.left);
            remainder = splits.right;
        }
        ranges.add(remainder);

        return ranges;
    }

    public boolean getHintedHandoffEnabled()
    {
        return DatabaseDescriptor.hintedHandoffEnabled();
    }

    public void setHintedHandoffEnabled(boolean b)
    {
        synchronized (StorageService.instance)
        {
            if (b)
                StorageService.instance.checkServiceAllowedToStart("hinted handoff");

            DatabaseDescriptor.setHintedHandoffEnabled(b);
        }
    }

    public void enableHintsForDC(String dc)
    {
        DatabaseDescriptor.enableHintsForDC(dc);
    }

    public void disableHintsForDC(String dc)
    {
        DatabaseDescriptor.disableHintsForDC(dc);
    }

    public Set<String> getHintedHandoffDisabledDCs()
    {
        return DatabaseDescriptor.hintedHandoffDisabledDCs();
    }

    public int getMaxHintWindow()
    {
        return DatabaseDescriptor.getMaxHintWindow();
    }

    public void setMaxHintWindow(int ms)
    {
        DatabaseDescriptor.setMaxHintWindow(ms);
    }

    public static boolean shouldHint(InetAddress ep)
    {
        if (DatabaseDescriptor.hintedHandoffEnabled())
        {
            Set<String> disabledDCs = DatabaseDescriptor.hintedHandoffDisabledDCs();
            if (!disabledDCs.isEmpty())
            {
                final String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(ep);
                if (disabledDCs.contains(dc))
                {
                    Tracing.trace("Not hinting {} since its data center {} has been disabled {}", ep, dc, disabledDCs);
                    return false;
                }
            }
            boolean hintWindowExpired = Gossiper.instance.getEndpointDowntime(ep) > DatabaseDescriptor.getMaxHintWindow();
            if (hintWindowExpired)
            {
                HintsService.instance.metrics.incrPastWindow(ep);
                Tracing.trace("Not hinting {} which has been down {} ms", ep, Gossiper.instance.getEndpointDowntime(ep));
            }
            return !hintWindowExpired;
        }
        else
        {
            return false;
        }
    }

    /**
     * Performs the truncate operatoin, which effectively deletes all data from
     * the column family cfname
     * @param keyspace
     * @param cfname
     * @throws UnavailableException If some of the hosts in the ring are down.
     * @throws TimeoutException
     */
    public static void truncateBlocking(String keyspace, String cfname) throws UnavailableException, TimeoutException
    {
        logger.debug("Starting a blocking truncate operation on keyspace {}, CF {}", keyspace, cfname);
        if (isAnyStorageHostDown())
        {
            logger.info("Cannot perform truncate, some hosts are down");
            // Since the truncate operation is so aggressive and is typically only
            // invoked by an admin, for simplicity we require that all nodes are up
            // to perform the operation.
            int liveMembers = Gossiper.instance.getLiveMembers().size();
            throw new UnavailableException(ConsistencyLevel.ALL, liveMembers + Gossiper.instance.getUnreachableMembers().size(), liveMembers);
        }

        Set<InetAddress> allEndpoints = StorageService.instance.getLiveRingMembers(true);

        int blockFor = allEndpoints.size();
        final TruncateResponseHandler responseHandler = new TruncateResponseHandler(blockFor);

        // Send out the truncate calls and track the responses with the callbacks.
        Tracing.trace("Enqueuing truncate messages to hosts {}", allEndpoints);
        Truncation truncation = new Truncation(keyspace, cfname);
        MessagingService.instance().send(Verbs.OPERATIONS.TRUNCATE.newDispatcher(allEndpoints, truncation), responseHandler);

        // Wait for all
        try
        {
            responseHandler.get();
        }
        catch (TimeoutException e)
        {
            Tracing.trace("Timed out");
            throw e;
        }
    }

    /**
     * Asks the gossiper if there are any nodes that are currently down.
     * @return true if the gossiper thinks all nodes are up.
     */
    private static boolean isAnyStorageHostDown()
    {
        return !Gossiper.instance.getUnreachableTokenOwners().isEmpty();
    }

    public long getTotalHints()
    {
        return StorageMetrics.totalHints.getCount();
    }

    public int getMaxHintsInProgress()
    {
        return maxHintsInProgress;
    }

    public void setMaxHintsInProgress(int qs)
    {
        maxHintsInProgress = qs;
    }

    public int getHintsInProgress()
    {
        return (int) StorageMetrics.totalHintsInProgress.getCount();
    }

    public void verifyNoHintsInProgress()
    {
        if (getHintsInProgress() > 0)
            logger.warn("Some hints were not written before shutdown.  This is not supposed to happen.  You should (a) run repair, and (b) file a bug report");
    }

    private static AtomicInteger getHintsInProgressFor(InetAddress destination)
    {
        try
        {
            return hintsInProgress.load(destination);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    public static Future<Void> submitHint(Mutation mutation, InetAddress target, WriteHandler handler)
    {
        return submitHint(mutation, Collections.singleton(target), handler);
    }

    public static Future<Void> submitHint(Mutation mutation,
                                          Collection<InetAddress> targets,
                                          WriteHandler handler)
    {
        return submitHint(targets, Completable.defer(() ->
        {
            Set<InetAddress> validTargets = new HashSet<>(targets.size());
            Set<UUID> hostIds = new HashSet<>(targets.size());
            for (InetAddress target : targets)
            {
                UUID hostId = StorageService.instance.getHostIdForEndpoint(target);
                if (hostId != null)
                {
                    hostIds.add(hostId);
                    validTargets.add(target);
                }
                else
                    logger.debug("Discarding hint for endpoint not part of ring: {}", target);
            }
            logger.trace("Adding hints for {}", validTargets);
            HintsService.instance.write(hostIds, Hint.create(mutation, System.currentTimeMillis()));
            validTargets.forEach(HintsService.instance.metrics::incrCreatedHints);
            // Notify the handler only for CL == ANY
            if (handler != null && handler.consistencyLevel() == ConsistencyLevel.ANY)
                handler.onLocalResponse();
            return Completable.complete();
        }));
    }

    static Future<Void> submitHint(Collection<InetAddress> targets, Completable hintsCompletable)
    {
        StorageMetrics.totalHintsInProgress.inc(targets.size());
        for (InetAddress target : targets)
            getHintsInProgressFor(target).incrementAndGet();
        hintsCompletable.doOnTerminate(() ->
                                       {
                                           StorageMetrics.totalHintsInProgress.dec(targets.size());
                                           for (InetAddress target : targets)
                                               getHintsInProgressFor(target).decrementAndGet();
                                       });
        RxThreads.subscribeOn(hintsCompletable, StageManager.getScheduler(Stage.HINTS), TPCTaskType.HINT_SUBMIT);
        return TPCUtils.toFuture(hintsCompletable);
    }

    public Long getRpcTimeout() { return DatabaseDescriptor.getRpcTimeout(); }
    public void setRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRpcTimeout(timeoutInMillis); }

    public Long getReadRpcTimeout() { return DatabaseDescriptor.getReadRpcTimeout(); }
    public void setReadRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setReadRpcTimeout(timeoutInMillis); }

    public Long getWriteRpcTimeout() { return DatabaseDescriptor.getWriteRpcTimeout(); }
    public void setWriteRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setWriteRpcTimeout(timeoutInMillis); }

    public Long getCounterWriteRpcTimeout() { return DatabaseDescriptor.getCounterWriteRpcTimeout(); }
    public void setCounterWriteRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setCounterWriteRpcTimeout(timeoutInMillis); }

    public Long getCasContentionTimeout() { return DatabaseDescriptor.getCasContentionTimeout(); }
    public void setCasContentionTimeout(Long timeoutInMillis) { DatabaseDescriptor.setCasContentionTimeout(timeoutInMillis); }

    public Long getRangeRpcTimeout() { return DatabaseDescriptor.getRangeRpcTimeout(); }
    public void setRangeRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRangeRpcTimeout(timeoutInMillis); }

    public Long getTruncateRpcTimeout() { return DatabaseDescriptor.getTruncateRpcTimeout(); }
    public void setTruncateRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setTruncateRpcTimeout(timeoutInMillis); }

    public Long getNativeTransportMaxConcurrentConnections() { return DatabaseDescriptor.getNativeTransportMaxConcurrentConnections(); }
    public void setNativeTransportMaxConcurrentConnections(Long nativeTransportMaxConcurrentConnections) { DatabaseDescriptor.setNativeTransportMaxConcurrentConnections(nativeTransportMaxConcurrentConnections); }

    public Long getNativeTransportMaxConcurrentConnectionsPerIp() { return DatabaseDescriptor.getNativeTransportMaxConcurrentConnectionsPerIp(); }
    public void setNativeTransportMaxConcurrentConnectionsPerIp(Long nativeTransportMaxConcurrentConnections) { DatabaseDescriptor.setNativeTransportMaxConcurrentConnectionsPerIp(nativeTransportMaxConcurrentConnections); }

    public void reloadTriggerClasses() { TriggerExecutor.instance.reloadClasses(); }

    public long getReadRepairAttempted()
    {
        return ReadRepairMetrics.attempted.getCount();
    }

    public long getReadRepairRepairedBlocking()
    {
        return ReadRepairMetrics.repairedBlocking.getCount();
    }

    public long getReadRepairRepairedBackground()
    {
        return ReadRepairMetrics.repairedBackground.getCount();
    }

    public int getNumberOfTables()
    {
        return Schema.instance.getNumberOfTables();
    }

    public String getIdealConsistencyLevel()
    {
        return DatabaseDescriptor.getIdealConsistencyLevel().toString();
    }

    public String setIdealConsistencyLevel(String cl)
    {
        ConsistencyLevel original = DatabaseDescriptor.getIdealConsistencyLevel();
        ConsistencyLevel newCL = ConsistencyLevel.valueOf(cl.trim().toUpperCase());
        DatabaseDescriptor.setIdealConsistencyLevel(newCL);
        return String.format("Updating ideal consistency level new value: %s old value %s", newCL, original.toString());
    }

    public int getOtcBacklogExpirationInterval() {
        return DatabaseDescriptor.getOtcBacklogExpirationInterval();
    }

    public void setOtcBacklogExpirationInterval(int intervalInMillis) {
        DatabaseDescriptor.setOtcBacklogExpirationInterval(intervalInMillis);
    }
}
