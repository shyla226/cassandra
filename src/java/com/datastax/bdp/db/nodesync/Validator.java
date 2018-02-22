/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.NodeSyncConfig;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.NodeSyncReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.ReadReconciliationObserver;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.UnknownKeyspaceException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ReadRepairDecision;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.Threads;
import org.apache.cassandra.utils.units.SizeUnit;

/**
 * Performs a NodeSync validation on a particular table segment.
 * <p>
 * A NodeSync validation on a segment {@code s} does the following steps:
 * <ul>
 *     <li>it advertises the intent to validate {@code s} to other replica by setting the lock in the
 *     {@link SystemDistributedKeyspace#NODESYNC_STATUS} table.</li>
 *     <li>it incrementally (by pages) reads the range covered by {@code s} from all (alive) replicas to compare data
 *     on every replica and detect any inconsistency</li>
 *     <li>it repairs any found inconsistencies which, in practice, is done automatically by the read-repair mechanism</li>
 *     <li>on completion of that process, it records the result of that validation (through {@link ValidationInfo}) in the
 *     {@link SystemDistributedKeyspace#NODESYNC_STATUS} table.</li>
 * </ul>
 * <p>
 * In practice, a {@link Validator} execution is started and ran on a {@link ValidationExecutor} through the call to
 * {@link #executeOn}.
 * <p>>
 * A {@link Validator} is thread-safe, but note that his {@link #executeOn} can really only be called once (it throws
 * otherwise) so this mostly only mean that checking completion (through the {@link #completionFuture()} or cancelling
 * the validator is thead-safe.
 */
class Validator
{
    private static final Logger logger = LoggerFactory.getLogger(Validator.class);

    /**
     * The possible states of a validator.
     */
    private enum State
    {
        /** The validator has been created but {@link #executeOn} hasn't yet been called. */
        CREATED,
        /** The validator is running. */
        RUNNING,
        /** The validator has finished running without interruption. This says nothing of the outcome of the validation. */
        FINISHED,
        /** The validator has been cancelled before proper completion. */
        CANCELLED;

        boolean isDone()
        {
            return this == FINISHED || this == CANCELLED;
        }
    }

    private final ValidationLifecycle lifecycle;

    private final ValidationMetrics metrics = new ValidationMetrics();
    private final RateLimiter limiter;
    private final PageSize pageSize;

    protected final Observer observer;
    protected final int replicationFactor;
    private final Set<InetAddress> segmentReplicas;

    private final Future completionFuture = new Future();
    private CompletableFuture<Void> flowFuture;

    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);

    // The total outcome for the validation. Note that this is build by "composing" the outcome of each individual pages,
    // and so it's enough for a single page to come up partial (for instance) for the whole outcome to be deemed partial.
    private volatile ValidationOutcome validationOutcome = ValidationOutcome.FULL_IN_SYNC;
    // A set containing any node for which we didn't got the response on _any_ page (null otherwise). If this is not null
    // this imply validationOutcome will be at least partial (or worst, failed or uncompleted). Note that this could
    // theoretically contain all replicas if each node don't respond to a different page.
    @Nullable
    private volatile Set<InetAddress> missingNodes;

    protected Validator(ValidationLifecycle lifecycle)
    {
        this.lifecycle = lifecycle;

        NodeSyncConfig config = lifecycle.service().config();
        this.limiter = config.rateLimiter;
        this.pageSize = new PageSize(Ints.checkedCast(config.getPageSize(SizeUnit.BYTES)), PageSize.PageUnit.BYTES);

        this.observer = new Observer();
        AbstractReplicationStrategy replicationStrategy = Keyspace.open(table().keyspace).getReplicationStrategy();
        // Note that we can't entirely use segmentReplicas.size() as a shortcut for replicationFactor because it's
        // actually possible for the replication factor to be greater than the total number of nodes (and thus replica)
        // and we want to make sure we detect this (mark pages as PARTIAL below even though the missingNodes recorded
        // will be empty).
        this.replicationFactor = replicationStrategy.getReplicationFactor();
        this.segmentReplicas = new HashSet<>(replicationStrategy.getNaturalEndpoints(range().left));
    }

    /**
     * Creates a new validator on the provided segment, which must have been locked prior to this call.
     * <p>
     * Note that because the lock has been set, caller should ensure that the validator is dealt with relatively
     * promptly (failure to release the lock isn't critical in the sense that the lock has a timeout in the first place
     * but that timeout is meant to protect against node failures, not programmers error).
     *
     * @param lifecycle the lifecycle manager for the segment to validate/repair.
     * @return the newly created {@link Validator}.
     */
    static Validator create(ValidationLifecycle lifecycle)
    {
        return new Validator(lifecycle);
    }

    /**
     * Future on the completion of this validator.
     */
    Future completionFuture()
    {
        return completionFuture;
    }

    Segment segment()
    {
        return lifecycle.segment();
    }

    ValidationMetrics metrics()
    {
        return metrics;
    }

    private TableMetadata table()
    {
        return segment().table;
    }

    private Range<Token> range()
    {
        return segment().range;
    }

    private NodeSyncTracing.SegmentTracing tracing()
    {
        return lifecycle.tracing();
    }

    Future executeOn(ValidationExecutor executor)
    {
        // We can only call this once.
        if (!state.compareAndSet(State.CREATED, State.RUNNING))
        {
            // It's actually not a programming error to cancel the validator before it had a chance to start execution,
            // so simply return the completion future (which will be marked cancelled) if that happens
            if (state.get() == State.CANCELLED)
                return completionFuture;
            throw new IllegalStateException("Cannot call executeOn multiple times");
        }

        ReadCommand command = new NodeSyncReadCommand(segment(),
                                                      NodeSyncHelpers.time().currentTimeSeconds(),
                                                      executor.asScheduler());
        QueryPager pager = createPager(command);
        // We don't want to use CL.ALL, because that would throw Unavailable unless all nodes are up. Instead, we want
        // read to proceed as soon as at least 2 nodes are up (hence CL.TWO), but we still want to block in the resolver
        // until all queried (live) replicas have replied (and ack eventual repairs), hence the
        // 'blockForAllTargets()' option. Of course, we also want to read from all live replicas, so we force GLOBAL
        // read-repairs. Lastly, we force digests since it's a range query and those don't use digests by default.
        ReadContext context = ReadContext.builder(command, ConsistencyLevel.TWO)
                                         .useDigests()
                                         .blockForAllTargets()
                                         .observer(observer)
                                         .readRepairDecision(ReadRepairDecision.GLOBAL)
                                         .readRepairTimeoutInMs(2 * DatabaseDescriptor.getWriteRpcTimeout())
                                         .build(System.nanoTime());

        logger.trace("Starting execution of validation on {}", segment());
        try
        {
            flowFuture = fetchAll(executor, pager, context).lift(Threads.requestOn(executor.asScheduler(), TPCTaskType.NODESYNC_VALIDATION))
                                                           .flatProcess(FlowablePartition::content)
                                                           .processToFuture()
                                                           .handleAsync((v, t) -> {
                                                               if (t == null)
                                                                   markFinished();
                                                               else
                                                                   handleError(t, executor);
                                                               return null;
                                                           }, executor.asExecutor());
        }
        catch (Throwable t)
        {
            // We can typically get UnavailableException here. In any case, delegate to handleError(), which will
            // always make sure to complete the validator so the returned completed future will finish.
            handleError(t, executor);
        }

        return completionFuture;
    }

    @VisibleForTesting
    protected QueryPager createPager(ReadCommand command)
    {
        return command.getPager(null, ProtocolVersion.CURRENT);
    }

    private Flow<FlowablePartition> fetchAll(ValidationExecutor executor, QueryPager pager, ReadContext context)
    {
        return fetchPage(executor, pager, context).concatWith(() -> isDone(pager)
                                                                    ? null
                                                                    : fetchPage(executor, pager, context.forNewQuery(System.nanoTime())));
    }

    private Flow<FlowablePartition> fetchPage(ValidationExecutor executor, QueryPager pager, ReadContext context)
    {
        assert !pager.isExhausted();
        // Maybe schedule a refresh of the lock.
        lifecycle.onNewPage(pageSize);
        observer.onNewPage();
        executor.onNewPage();
        return Flow.concat(pager.fetchPage(pageSize, context),
                           Flow.defer(() -> { onPageComplete(executor); return Flow.empty(); }));
    }

    private boolean isDone(QueryPager pager)
    {
        return pager.isExhausted() || state.get() == State.CANCELLED;
    }

    private void onPageComplete(ValidationExecutor executor)
    {
        ValidationOutcome outcome = ValidationOutcome.completed(!observer.isComplete, observer.hasMismatch);
        recordPage(outcome, executor);
        lifecycle.onCompletedPage(outcome, observer.pageMetrics);
    }

    private void handleError(Throwable t, PageProcessingListener listener)
    {
        t = Throwables.unwrapped(t);

        // Cancellation is the one we ignore as we already cancel the completion future when that happens and there is
        // nothing more to do here.
        if (t instanceof CancellationException)
            return;

        if (t instanceof InvalidatedNodeSyncStateException)
        {
            cancel("Validation invalidated by either a topology change or a depth decrease. " +
                   "The segment will be retried.");
        }
        else if (isException(t, UnknownKeyspaceException.class, RequestFailureReason.UNKNOWN_KEYSPACE))
        {
            // This means the keyspace has been dropped concurrently from us.
            cancel(String.format("Keyspace %s was dropped while validating", table().keyspace));
        }
        else if (isException(t, UnknownTableException.class, RequestFailureReason.UNKNOWN_TABLE))
        {
            // This means the table has been dropped concurrently from us.
            cancel(String.format("Table %s was dropped while validating", table()));
        }
        else if ((t instanceof UnavailableException) || (t instanceof RequestTimeoutException))
        {
            tracing().trace("Unable to complete page (and validation): {}", t.getMessage());

            // As we ultimately use CL.TWO, it means this is either the only live replica (unavailable) or the only
            // one that answered (timeout). In that case, we can't do more for this segment so record the outcome but
            // otherwise mark the segment finished.
            recordPage(ValidationOutcome.UNCOMPLETED, listener);
            markFinished();
        }
        else
        {
            tracing().trace("Unexpected error: {}", t.getMessage());
            logger.error("Unexpected error during synchronization of table {} on range {}", table(), range(), t);
            // If we don't know what happens, we can't assume anything so marking the whole segment failed.
            recordPage(ValidationOutcome.FAILED, listener);
            markFinished();
        }
    }

    /**
     * Some exception can either happen locally directly, or be send by a replica, without us particularly wanting to
     * act differently. This makes this easier.
     */
    private static boolean isException(Throwable t, Class<?> localExceptionClass, RequestFailureReason remoteFailureReason)
    {
        if (localExceptionClass.isInstance(t))
            return true;

        if (!(t instanceof RequestFailureException))
            return false;

        RequestFailureException rfe = (RequestFailureException) t;
        return rfe.failureReasonByEndpoint.values().stream().anyMatch(r -> r == remoteFailureReason);
    }

    /**
     * Cancels this validator if it's not already done.
     * <p>
     * Cancelling a validator will make it stop doing additional work, potentially stopping in the middle of a page.
     * <p>
     * This <b>must</b>  be called if the validator is thrown away before completion so the "lock" hold by the validator
     * is properly released.
     *
     * @return {@code true} if the validator is now cancelled (meaning that it was either already cancelled, or this
     * call cancelled it). {@code false} if the validator has already finished (without having being cancelled).
     */
    boolean cancel(String reason)
    {
        // If we were already done, do nothing. Mark cancelled otherwise and proceed.
        State previous = state.getAndUpdate(s -> s.isDone() ? s : State.CANCELLED);
        if (previous.isDone())
            return previous == State.CANCELLED;

        lifecycle.cancel(reason);

        if (flowFuture != null)
            flowFuture.cancel(true);

        completionFuture.actuallyCancel();
        return true;
    }

    private void markFinished()
    {
        // If we were already done (typically cancelled), do nothing. Mark finished otherwise and proceed.
        if (state.getAndUpdate(s -> s.isDone() ? s : State.FINISHED).isDone())
            return;

        lifecycle.service().updateMetrics(table(), metrics);
        ValidationInfo info = new ValidationInfo(lifecycle.startTime(), validationOutcome, missingNodes);
        lifecycle.onCompletion(info, metrics);
        completionFuture.complete(info);
    }

    private void recordPage(ValidationOutcome outcome, PageProcessingListener listener)
    {
        metrics.addPageOutcome(outcome);
        validationOutcome = validationOutcome.composeWith(outcome);

        listener.onPageComplete(observer.dataValidatedBytesForPage,
                                  observer.limiterWaitTimeMicrosForPage,
                                  observer.timeIdleBeforeProcessingPage);
    }

    static interface PageProcessingListener
    {
        void onNewPage();
        void onPageComplete(long processedBytes, long waitedOnLimiterMicros, long waitedForProcessingNanos);
    }

    /**
     * A future on the completion of a validator.
     * <p>
     * This is essentially a completable future that provides information on the validation on completion but with a
     * convenience method to access the validator this is the completion future of.
     * <p>
     * This future will be completed exceptionally with a {@link CancellationException} if the underlying validator
     * is cancelled (which can be done either through this future {@link #cancel(boolean)} method, or directly with
     * {@link Validator#cancel}).
     * <p>
     * Implementation note: the real reason we extend CompletableFuture is so {@link #cancel(boolean)} can actually
     * cancel the underlying validator. Costs us nothing to add a few conveniences while we're at it.
     */
    class Future extends CompletableFuture<ValidationInfo>
    {
        private Future()
        {
        }

        /**
         * The validator this is the future of.
         * <p>
         * Note that, by construction, you will have {@code this == this.validator().completionFuture()}.
         */
        Validator validator()
        {
            return Validator.this;
        }

        /**
         * Cancel the validator (that is, call {@link Validator#cancel}) this is a future of if that validator isn't yet
         * completed (or do nothing otherwise).
         * <p>
         * As with {@link CompletableFuture}, if the validator is indeed canceled (meaning it isn't already completed),
         * the future itself is completed with a {@link CancellationException} (on top of the
         * validator being cancelled that is).
         *
         * @param mayInterruptIfRunning this value is ignored:
         * @return {@code true} if the underlying validator is now cancelled (meaning that it was either already
         * cancelled, or this call cancelled it). {@code false} if the validator has already finished (without having
         * being cancelled).
         */
        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            // We want cancellation through the future to actually cancel the validator if necessary, hence the override
            // here. Validator#cancel will cancel this future itself properly through 'actuallyCancel' below.
            return Validator.this.cancel("cancelled through validation future");
        }

        private void actuallyCancel()
        {
            super.cancel(true);
        }
    }

    /**
     * A {@code ReadReconciliationObserver} that maintains the metrics exposed by NodeSync.
     * <p>
     * This is also where we rate limit.
     */
    @VisibleForTesting
    protected class Observer implements ReadReconciliationObserver
    {
        private volatile boolean isComplete;
        private volatile boolean hasMismatch;

        private volatile long limiterWaitTimeMicrosForPage;
        private volatile long dataValidatedBytesForPage;
        private volatile long responseReceivedTimeNanos;
        private volatile long timeIdleBeforeProcessingPage;

        // Only collected when tracing is enabled.
        private volatile ValidationMetrics pageMetrics;

        @VisibleForTesting
        protected void onNewPage()
        {
            hasMismatch = false;
            limiterWaitTimeMicrosForPage = 0;
            dataValidatedBytesForPage = 0;
            timeIdleBeforeProcessingPage = 0;

            if (tracing().isEnabled())
                pageMetrics = new ValidationMetrics();
        }

        public void queried(Collection<InetAddress> queried)
        {
            tracing().trace("Querying {} replica: {}", queried.size(), queried);
        }

        public void responsesReceived(Collection<InetAddress> responded)
        {
            responseReceivedTimeNanos = System.nanoTime();
            isComplete = responded.size() == replicationFactor;
            if (isComplete)
            {
                tracing().trace("All replica responded ({})", responded);
            }
            else
            {
                if (missingNodes == null)
                    missingNodes = Sets.newConcurrentHashSet();
                Set<InetAddress> missingThisTime = Sets.difference(segmentReplicas, setOf(responded));
                missingNodes.addAll(missingThisTime);

                if (tracing().isEnabled())
                {
                    // If the cluster has less node than the RF, missingThisTime will be empty, so make sure we don't write
                    // a weird message. Note that while this is a bit of an edge case, tests cluster (typically 2 nodes-ones)
                    // will get there by virtue of some of our distributed system keyspace being RF=3.
                    if (missingThisTime.isEmpty())
                        tracing().trace("Partial responses: {} responded ({}) but RF={}",
                                      responded.size(), responded, replicationFactor);
                    else
                        tracing().trace("Partial responses: {} responded ({}) but {} did not ({})",
                                      responded.size(), responded, missingThisTime.size(), missingThisTime);
                }
            }
        }

        private Set<InetAddress> setOf(Collection<InetAddress> c)
        {
            return c instanceof Set ? (Set<InetAddress>)c : new HashSet<>(c);
        }

        public void onDigestMatch()
        {
            hasMismatch = false;
        }

        public void onDigestMismatch()
        {
            hasMismatch = true;
            tracing().trace("Digest mismatch, issuing full data request");
        }

        public void onPartition(DecoratedKey partitionKey)
        {
            onData(partitionKey.getKey().remaining(), true);
        }

        public void onPartitionDeletion(DeletionTime deletion, boolean isConsistent)
        {
            // Internally, we distinguish partition deletions partly for historical reasons and partly for
            // efficiency sake, but logically it can be seen as a range tombstone covering the full partition
            // so counting it like so as far as reporting goes. It's simpler and not really less accurate.
            onRangeTombstoneMarker(null, isConsistent);
        }

        public void onRow(Row row, boolean isConsistent)
        {
            metrics.incrementRowsRead(isConsistent);
            if (pageMetrics != null)
                pageMetrics.incrementRowsRead(isConsistent);
            onData(row.dataSize(), isConsistent);
        }

        public void onRangeTombstoneMarker(RangeTombstoneMarker marker, boolean isConsistent)
        {
            metrics.incrementRangeTombstoneMarkersRead(isConsistent);
            if (pageMetrics != null)
                pageMetrics.incrementRangeTombstoneMarkersRead(isConsistent);
            onData(dataSize(marker), isConsistent);
        }

        @VisibleForTesting
        protected void onData(int size, boolean isConsistent)
        {
            // We're running on a specific executor (ValidationExecutor) explicitly because of the limiter, because
            // it is blocking. So make double sure we haven't introduced a bug by mistake by running on a TPC thread.
            assert !TPC.isTPCThread();
            // We want to track how much work received for a page/validator sits idle due to a lack of processing
            // resources (threads): this is used in ValidationExecutor to understand if we may be bottle-necking
            // on threads.
            // TODO(Sylvain): this feels a bit hacky. It would be more direct to compute this in ValidationExecutor
            // by recording how long tasks spend on the queue. Requires to wrap every task though and extend the
            // executor, so maybe it's not worth the trouble.
            if (timeIdleBeforeProcessingPage == 0)
                timeIdleBeforeProcessingPage = System.nanoTime() - responseReceivedTimeNanos;

            metrics.addDataValidated(size, isConsistent);
            if (pageMetrics != null)
                pageMetrics.addDataValidated(size, isConsistent);
            dataValidatedBytesForPage += size;

            double waitedSeconds = limiter.acquire(size);
            limiterWaitTimeMicrosForPage +=  waitedSeconds * TimeUnit.SECONDS.toMicros(1L);
        }

        private int dataSize(RangeTombstoneMarker marker)
        {
            // We want to account the marker for rate limiting, though we sometimes gets null here. In practice, we know
            // a marker is a clustering prefix + a DeletionTime. The DeletionTime is a long and an int so 12 bytes worth
            // of data. For the clustering prefix, we use its size if we have it but don't bother otherwise: we get null
            // here when some markers was on some replica but isn't needed because shadowed by something else and we
            // could almost justify not counting those at all for rate limiting.
            return 12 + (marker == null ? 0 : marker.clustering().dataSize());
        }

        public void onRepair(InetAddress endpoint, PartitionUpdate repair)
        {
            metrics.addRepair(repair);
            if (pageMetrics != null)
                pageMetrics.addRepair(repair);
        }
    }
}
