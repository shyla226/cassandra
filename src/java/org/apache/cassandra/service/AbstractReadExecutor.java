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

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DigestVersion;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.metrics.ReadCoordinationMetrics;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SpeculativeRetryParam;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.flow.Flow;

/**
 * Sends a read request to the replicas needed to satisfy a given ConsistencyLevel.
 *
 * Optionally, may perform additional requests to provide redundancy against replica failure:
 * AlwaysSpeculatingReadExecutor will always send a request to one extra replica, while
 * SpeculatingReadExecutor will wait until it looks like the original request is in danger
 * of timing out before performing extra reads.
 */
public abstract class AbstractReadExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadExecutor.class);

    protected final ReadCommand command;
    protected final List<InetAddress> targetReplicas;
    protected final ReadCallback<FlowablePartition> handler;
    protected final DigestVersion digestVersion;
    protected final ColumnFamilyStore cfs;

    static
    {
        MessagingService.instance().register(ReadCoordinationMetrics::updateReplicaLatency);
    }

    AbstractReadExecutor(ColumnFamilyStore cfs, ReadCommand command, List<InetAddress> targetReplicas, ReadContext ctx)
    {
        this.cfs = cfs;
        this.command = command;
        this.targetReplicas = targetReplicas;
        this.handler = ReadCallback.forInitialRead(command, targetReplicas, ctx);
        this.digestVersion = DigestVersion.forReplicas(targetReplicas);
    }

    /**
     * Create and send actual requests.
     *
     * @param endpoints the endpoints to which to send the request.
     * @param minDataRead the minimal number of data read to include if digests are enabled. That is, if digests are
     *                    enabled, the first {@code minDataRead} nodes from {@code endpoints} will be sent a data read,
     *                    while the rest will be sent a digest one. If digest is not enabled, all nodes will obviously
     *                    get a data read. For convenience, a value lower than {@code endpoints.size()} is allowed, in
     *                    which case this is equivalent to passing {@code endpoints.size()}. This must be strictly
     *                    positive.
     */
    protected Completable makeRequests(List<InetAddress> endpoints, int minDataRead)
    {
        assert minDataRead > 0 : "Asked for only digest reads, which makes no sense";
        if (handler.readContext().withDigests && minDataRead < endpoints.size())
        {
            return makeDataRequests(endpoints.subList(0, minDataRead))
                   .concatWith(makeDigestRequests(endpoints.subList(minDataRead, endpoints.size())));
        }
        else
        {
            return makeDataRequests(endpoints);
        }
    }

    private Completable makeDataRequests(List<InetAddress> endpoints)
    {
        assert !endpoints.isEmpty();
        Tracing.trace("Reading data from {}", endpoints);
        logger.trace("Reading data from {}", endpoints);
        return makeRequests(command, endpoints);
    }

    private Completable makeDigestRequests(List<InetAddress> endpoints)
    {
        assert !endpoints.isEmpty();
        Tracing.trace("Reading digests from {}", endpoints);
        logger.trace("Reading digests from {}", endpoints);
        return makeRequests(command.createDigestCommand(digestVersion), endpoints);
    }

    private Completable makeRequests(ReadCommand readCommand, List<InetAddress> endpoints)
    {
        MessagingService.instance().send(readCommand.dispatcherTo(endpoints), handler);
        return Completable.complete();
    }

    /**
     * Perform additional requests if it looks like the original will time out.  May block while it waits
     * to see if the original requests are answered first.
     */
    public abstract Completable maybeTryAdditionalReplicas();

    /**
     * Get the replicas involved in the [finished] request.
     *
     * @return target replicas + the extra replica, *IF* we speculated.
     */
    public abstract List<InetAddress> getContactedReplicas();

    /**
     * send the initial set of requests
     */
    public abstract Completable executeAsync();

    /**
     * wait for an answer.  Blocks until success or timeout, so it is caller's
     * responsibility to call maybeTryAdditionalReplicas first.
     */
    public Flow<FlowablePartition> result()
    {
            return handler.result().doOnError(e ->
                                              {
                                                  if (e instanceof ReadTimeoutException)
                                                      this.onReadTimeout();
                                              });

    }

    /**
     * @return an executor appropriate for the configured speculative read policy
     */
    public static AbstractReadExecutor getReadExecutor(SinglePartitionReadCommand command, ReadContext ctx) throws UnavailableException
    {
        Keyspace keyspace = ctx.keyspace;
        ConsistencyLevel consistencyLevel = ctx.consistencyLevel;
        List<InetAddress> allReplicas = StorageProxy.getLiveSortedEndpoints(keyspace, command.partitionKey());
        List<InetAddress> targetReplicas = ctx.filterForQuery(allReplicas);

        // See APOLLO-637
        if (!allReplicas.contains(FBUtilities.getBroadcastAddress()))
            ReadCoordinationMetrics.nonreplicaRequests.inc();
        else if (!targetReplicas.contains(FBUtilities.getBroadcastAddress()))
            ReadCoordinationMetrics.preferredOtherReplicas.inc();

        // Throw UAE early if we don't have enough replicas.
        consistencyLevel.assureSufficientLiveNodes(keyspace, targetReplicas);

        if (ctx.readRepairDecision != ReadRepairDecision.NONE)
        {
            Tracing.trace("Read-repair {}", ctx.readRepairDecision);
            ReadRepairMetrics.attempted.mark();
        }

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
        SpeculativeRetryParam retry = cfs.metadata().params.speculativeRetry;

        // Speculative retry is disabled
        // 11980: Disable speculative retry if using EACH_QUORUM in order to prevent miscounting DC responses
        if (retry.equals(SpeculativeRetryParam.NONE)
            | consistencyLevel == ConsistencyLevel.EACH_QUORUM)
            return new NeverSpeculatingReadExecutor(cfs, command, targetReplicas, ctx, false);

        // There are simply no extra replicas to speculate.
        // Handle this separately so it can log failed attempts to speculate due to lack of replicas
        if (consistencyLevel.blockFor(keyspace) == allReplicas.size())
            return new NeverSpeculatingReadExecutor(cfs, command, targetReplicas, ctx, true);

        if (targetReplicas.size() == allReplicas.size())
        {
            // CL.ALL, RRD.GLOBAL or RRD.DC_LOCAL and a single-DC.
            // We are going to contact every node anyway, so ask for 2 full data requests instead of 1, for redundancy
            // (same amount of requests in total, but we turn 1 digest request into a full blown data request).
            return new AlwaysSpeculatingReadExecutor(cfs, command, targetReplicas, ctx);
        }

        // RRD.NONE or RRD.DC_LOCAL w/ multiple DCs.
        InetAddress extraReplica = allReplicas.get(targetReplicas.size());
        // With repair decision DC_LOCAL all replicas/target replicas may be in different order, so
        // we might have to find a replacement that's not already in targetReplicas.
        if (ctx.readRepairDecision == ReadRepairDecision.DC_LOCAL && targetReplicas.contains(extraReplica))
        {
            for (InetAddress address : allReplicas)
            {
                if (!targetReplicas.contains(address))
                {
                    extraReplica = address;
                    break;
                }
            }
        }
        targetReplicas.add(extraReplica);

        if (retry.equals(SpeculativeRetryParam.ALWAYS))
            return new AlwaysSpeculatingReadExecutor(cfs, command, targetReplicas, ctx);
        else // PERCENTILE or CUSTOM.
            return new SpeculatingReadExecutor(cfs, command, targetReplicas, ctx);
    }

    /**
     *  Returns true if speculation should occur.
     */
    boolean shouldSpeculate()
    {
        // no latency information, or we're overloaded
        if (cfs.keyspace.getReplicationStrategy().getReplicationFactor() == 1 ||
            cfs.sampleLatencyNanos > TimeUnit.MILLISECONDS.toNanos(command.getTimeout()))
            return false;

        return true;
    }

    void onReadTimeout() {}

    public static class NeverSpeculatingReadExecutor extends AbstractReadExecutor
    {
        /**
         * If never speculating due to lack of replicas
         * log it is as a failure if it should have happened
         * but couldn't due to lack of replicas
         */
        private final boolean logFailedSpeculation;

        public NeverSpeculatingReadExecutor(ColumnFamilyStore cfs, ReadCommand command, List<InetAddress> targetReplicas, ReadContext ctx, boolean logFailedSpeculation)
        {
            super(cfs, command, targetReplicas, ctx);
            this.logFailedSpeculation = logFailedSpeculation;
        }

        public Completable executeAsync()
        {
            return makeRequests(targetReplicas, 1);
        }

        public Completable maybeTryAdditionalReplicas()
        {
            return Completable.defer(() -> {

                if (!shouldSpeculate() || !logFailedSpeculation)
                    return CompletableObserver::onComplete;

                command.getScheduler().schedule(() ->
                                                           {
                                                               if (!handler.hasValue())

                                                                   cfs.metric.speculativeInsufficientReplicas.inc();

                                                           },
                                                TPCTaskType.TIMED_SPECULATE,
                                                cfs.sampleLatencyNanos,
                                                TimeUnit.NANOSECONDS);

                return CompletableObserver::onComplete;
            });
        }

        public List<InetAddress> getContactedReplicas()
        {
            return targetReplicas;
        }
    }

    static class SpeculatingReadExecutor extends AbstractReadExecutor
    {
        private volatile boolean speculated = false;

        public SpeculatingReadExecutor(ColumnFamilyStore cfs,
                                       ReadCommand command,
                                       List<InetAddress> targetReplicas,
                                       ReadContext ctx)
        {
            super(cfs, command, targetReplicas, ctx);
        }

        public Completable executeAsync()
        {
            // if CL + RR result in covering all replicas, getReadExecutor forces AlwaysSpeculating.  So we know
            // that the last replica in our list is "extra."
            List<InetAddress> initialReplicas = targetReplicas.subList(0, targetReplicas.size() - 1);

            if (handler.blockFor() < initialReplicas.size())
            {
                // We're hitting additional targets for read repair.  Since our "extra" replica is the least-
                // preferred by the snitch, we do an extra data read to start with against a replica more
                // likely to reply; better to let RR fail than the entire query.
                return makeRequests(initialReplicas, 2);
            }
            else
            {
                // not doing read repair; all replies are important, so it doesn't matter which nodes we
                // perform data reads against vs digest.
                return makeRequests(initialReplicas, 1);
            }
        }

        public Completable maybeTryAdditionalReplicas()
        {
            return Completable.defer(
            () -> {

                if (!shouldSpeculate())
                    return CompletableObserver::onComplete;

                command.getScheduler().schedule(() -> {
                       if (!handler.hasValue())
                       {
                           //Handle speculation stats first in case the callback fires immediately
                           speculated = true;
                           cfs.metric.speculativeRetries.inc();
                           // Could be waiting on the data, or on enough digests.
                           ReadCommand retryCommand = command;
                           if (handler.resolver.isDataPresent() && (handler.resolver instanceof DigestResolver))
                               retryCommand = command.createDigestCommand(digestVersion);

                           InetAddress extraReplica = Iterables.getLast(targetReplicas);
                           Tracing.trace("Speculating read retry on {}", extraReplica);
                           logger.trace("Speculating read retry on {}", extraReplica);
                           MessagingService.instance().send(retryCommand.requestTo(extraReplica), handler);
                       }
                   }, TPCTaskType.TIMED_SPECULATE, cfs.sampleLatencyNanos, TimeUnit.NANOSECONDS);

                return CompletableObserver::onComplete;
            });
        }

        public List<InetAddress> getContactedReplicas()
        {
            return speculated
                 ? targetReplicas
                 : targetReplicas.subList(0, targetReplicas.size() - 1);
        }

        @Override
        void onReadTimeout()
        {
            //Shouldn't be possible to get here without first attempting to speculate even if the
            //timing is bad
            assert speculated;
            cfs.metric.speculativeFailedRetries.inc();
        }
    }

    private static class AlwaysSpeculatingReadExecutor extends AbstractReadExecutor
    {
        public AlwaysSpeculatingReadExecutor(ColumnFamilyStore cfs,
                                             ReadCommand command,
                                             List<InetAddress> targetReplicas,
                                             ReadContext ctx)
        {
            super(cfs, command, targetReplicas, ctx);
        }

        public Completable maybeTryAdditionalReplicas()
        {
            // no-op
            return Completable.complete();
        }

        public List<InetAddress> getContactedReplicas()
        {
            return targetReplicas;
        }

        @Override
        public Completable executeAsync()
        {
            cfs.metric.speculativeRetries.inc();
            return makeRequests(targetReplicas, 2);
        }

        @Override
        void onReadTimeout()
        {
            cfs.metric.speculativeFailedRetries.inc();
        }
    }
}
