/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.nodesync;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
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
import org.apache.cassandra.utils.FBUtilities;
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
     * The TTL used on the {@link SystemDistributedKeyspace#NODESYNC_STATUS} "lock". Note that this doesn't put a limit
     * on the maximum time for validating a segment as the lock is refreshed if necessary, it's only here to have the
     * lock automatically release on failure.
     */
    private static final long LOCK_TIMEOUT_MS = Long.getLong("dse.nodesync.segment_lock_timeout_ms", TimeUnit.MINUTES.toMillis(10));

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

    private final NodeSyncService service;
    private final Segment segment;
    private final long startTime;

    private final ValidationMetrics metrics = new ValidationMetrics();
    private final RateLimiter limiter;
    private final long pageSize;

    private final Observer observer;
    private final int replicationFactor;
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

    private volatile long nextLockRefreshTimeMs;

    private Validator(NodeSyncService service, Segment segment, long startTime)
    {
        this.service = service;
        this.segment = segment;
        this.startTime = startTime;
        this.limiter = service.config.rateLimiter;
        this.pageSize = service.config.getPageSize(SizeUnit.BYTES);
        // Integer.MAX_VALUE is the max page size in bytes we support:
        assert pageSize <= Integer.MAX_VALUE : "page size cannot be bigger than Integer.MAX_VALUE";

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
     * Creates a new validator and "lock" it in the {@link SystemDistributedKeyspace#NODESYNC_STATUS} table.
     * <p>
     * Note that because the lock has been set, caller should ensure that the validator is dealt with relatively
     * promptly (failure to release the lock isn't critical in the sense that the lock has a timeout in the first place
     * but that timeout is meant to protect against node failures, not programmers error).
     * <p>
     * See {@link NodeSyncRecord#lockedBy} for details on how we use the segment "lock" in practice.
     *
     * @param service the NodeSync service for which this is a validator.
     * @param segment the segment to validate/repair.
     * @return the newly created and "locked" {@link Validator}.
     */
    static Validator createAndLock(NodeSyncService service, Segment segment)
    {
        long now = System.currentTimeMillis();
        Validator validator = new Validator(service, segment, now);
        // Setup the lock initially.
        validator.refreshLock(now);
        return validator;
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
        return segment;
    }

    ValidationMetrics metrics()
    {
        return metrics;
    }

    private TableMetadata table()
    {
        return segment.table;
    }

    private Range<Token> range()
    {
        return segment.range;
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

        ReadCommand command = new NodeSyncReadCommand(segment, FBUtilities.nowInSeconds(), executor.asExecutor());
        QueryPager pager = command.getPager(null, ProtocolVersion.CURRENT);
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
                                         .build(System.nanoTime());


        logger.trace("Starting execution of validation on {}", segment);
        Flow<FlowablePartition> flow = moreContents(executor, pager, context);
        // Can be null on an exception
        if (flow != null)
            flowFuture = flow.lift(Threads.requestOn(executor.asScheduler(), TPCTaskType.VALIDATION))
                             .flatProcess(p -> p.content)
                             .processToFuture()
                             .handleAsync((v, t) -> {
                                 if (t == null)
                                     markFinished();
                                 else
                                     handleError(t, executor);
                                 return null;
                             }, executor.asExecutor());

        return completionFuture;
    }

    private Flow<FlowablePartition> moreContents(ValidationExecutor executor, QueryPager pager, ReadContext context)
    {
        assert !pager.isExhausted();
        try
        {
            // Maybe schedule a refresh of the lock.
            maybeRefreshLock();
            observer.onNewPage();
            return pager.fetchPage(new PageSize((int) pageSize, PageSize.PageUnit.BYTES), context)
                        .doOnComplete(() -> recordPage(ValidationOutcome.completed(!observer.isComplete, observer.hasMismatch), executor))
                        .concatWith(() -> isDone(pager)
                                          ? null :
                                          moreContents(executor, pager, context.forNewQuery(System.nanoTime())));

        }
        catch (Throwable t)
        {
            // We can typically get UnavailableException here. In any case, delegate to handleError(), which will
            // always make sure to complete the validator so we can just return null (to stop the flow otherwise).
            // Side-note: handleError() shouldn't be called on a core thread as it blocks (to write system tables) but
            // moreContents() can be called on a core-thread, hence we make sure to call it on the executor. Note that
            // it's possible we're already on the executor, but performance isn't a big concern here, so keep it simple.
            executor.asExecutor().execute(() -> handleError(t, executor));
            return null;
        }
    }

    private boolean isDone(QueryPager pager)
    {
        return pager.isExhausted() || state.get() == State.CANCELLED;
    }

    private void handleError(Throwable t, PageProcessingStatsListener listener)
    {
        if (t instanceof CompletionException)
            t = t.getCause();

        // Cancellation is the one we ignore as we already cancel the completion future when that happens and there is
        // nothing more to do here.
        if (t instanceof CancellationException)
            return;

        if (t instanceof UnknownKeyspaceException)
        {
            // This means the keyspace has been dropped concurrently from us.
            logger.trace("Keyspace {} has been dropped while validating segment for table {}", table().keyspace, table());
            cancel();
        }
        else if (t instanceof UnknownTableException)
        {
            // This means the table has been dropped concurrently from us.
            logger.trace("Table {} has been dropped while validating one of its segment", table());
            cancel();
        }
        else if ((t instanceof UnavailableException) || (t instanceof RequestTimeoutException))
        {
            logger.trace("Failed validation on page of range {} of {}, got {}", range(), table(), t.getMessage());

            // As we ultimately use CL.TWO, it means this is either the only live replica (unavailable) or the only
            // one that answered (timeout). In that case, we can't do more for this segment so record the outcome but
            // otherwise mark the segment finished.
            recordPage(ValidationOutcome.UNCOMPLETED, listener);
            markFinished();
        }
        else
        {
            logger.error(String.format("Unexpected error during synchronization of table %s on range %s", table(), range()), t);
            // If we don't know what happens, we can't assume anything so marking the whole segment failed.
            recordPage(ValidationOutcome.FAILED, listener);
            markFinished();
        }
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
    boolean cancel()
    {
        // If we were already done, do nothing. Mark cancelled otherwise and proceed.
        State previous = state.getAndUpdate(s -> s.isDone() ? s : State.CANCELLED);
        if (previous.isDone())
            return previous == State.CANCELLED;

        logger.trace("Cancelling validation of segment {}", segment);

        if (flowFuture != null)
            flowFuture.cancel(true);

        // Release the lock
        SystemDistributedKeyspace.forceReleaseNodeSyncSegmentLock(segment);

        return completionFuture.cancel(true);
    }

    private void markFinished()
    {
        // If we were already done (typically cancelled), do nothing. Mark finished otherwise and proceed.
        if (state.getAndUpdate(s -> s.isDone() ? s : State.FINISHED).isDone())
            return;

        if (logger.isTraceEnabled())
            logger.trace("Finished execution of validation on {}: metrics={}", segment, metrics.toDebugString());

        service.updateMetrics(table(), metrics);
        ValidationInfo info = new ValidationInfo(startTime, validationOutcome, missingNodes);
        SystemDistributedKeyspace.recordNodeSyncValidation(segment, info);
        completionFuture.complete(info);
    }

    private void recordPage(ValidationOutcome outcome, PageProcessingStatsListener listener)
    {
        metrics.addPageOutcome(outcome);
        validationOutcome = validationOutcome.composeWith(outcome);

        listener.onPageProcessing(observer.dataValidatedBytesForPage,
                                  observer.limiterWaitTimeNanosForPage,
                                  observer.timeIdleBeforeProcessingPage);
    }

    private void refreshLock(long currentTimeMs)
    {
        SystemDistributedKeyspace.lockNodeSyncSegment(segment, LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        // Refresh a bit (1/4 of the time) before the previous record timeout
        nextLockRefreshTimeMs = currentTimeMs + (3 * LOCK_TIMEOUT_MS / 4);
    }

    private void maybeRefreshLock()
    {
        long currentTimeMs = System.currentTimeMillis();
        if (currentTimeMs > nextLockRefreshTimeMs)
            refreshLock(currentTimeMs);
    }

    static interface PageProcessingStatsListener
    {
        void onPageProcessing(long processedBytes, long waitedOnLimiterNanos, long waitedForProcessingNanos);
    }

    /**
     * A future on the completion of a validator.
     * <p>
     * This is essentially a completable future that provides information on the validation on completion but with a
     * convenience method to access the validator this is the completion future of.
     * <p>
     * This future will be completed exceptionally with a {@link CancellationException} if the underlying validator
     * is cancelled (which can be done either through this future {@link #cancel(boolean)} method, or directly with
     * {@link Validator#cancel()}).
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
            // Note and cancelling the validator actually cancels the future, so we have nothing to do for the future
            // itself here.
            return Validator.this.cancel();
        }
    }

    /**
     * A {@code ReadReconciliationObserver} that maintains the metrics exposed by NodeSync.
     * <p>
     * This is also where we rate limit.
     */
    private class Observer implements ReadReconciliationObserver
    {
        private volatile boolean isComplete;
        private volatile boolean hasMismatch;

        private volatile long limiterWaitTimeNanosForPage;
        private volatile long dataValidatedBytesForPage;
        private volatile long responseReceivedTimeNanos;
        private volatile long timeIdleBeforeProcessingPage;

        private void onNewPage()
        {
            hasMismatch = false;
            limiterWaitTimeNanosForPage = 0;
            dataValidatedBytesForPage = 0;
            timeIdleBeforeProcessingPage = 0;
        }

        public void responsesReceived(Collection<InetAddress> responded)
        {
            responseReceivedTimeNanos = System.nanoTime();
            isComplete = responded.size() == replicationFactor;
            if (!isComplete)
            {
                if (missingNodes == null)
                    missingNodes = new HashSet<>();
                missingNodes.addAll(Sets.difference(segmentReplicas, setOf(responded)));
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
            onData(row.dataSize(), isConsistent);
        }

        public void onRangeTombstoneMarker(RangeTombstoneMarker marker, boolean isConsistent)
        {
            metrics.incrementRangeTombstoneMarkersRead(isConsistent);
            onData(dataSize(marker), isConsistent);
        }

        private void onData(int size, boolean isConsistent)
        {
            // We want to track how much work received for a page/validator sits idle due to a lack of processing
            // resources (threads): this is used in ValidationExecutor to understand if we may be bottle-necking
            // on threads.
            // TODO(Sylvain): this feels a bit hacky. It would be more direct to compute this in ValidationExecutor
            // by recording how long tasks spend on the queue. Requires to wrap every task though and extend the
            // executor, so maybe it's not worth the trouble.
            if (timeIdleBeforeProcessingPage == 0)
                timeIdleBeforeProcessingPage = System.nanoTime() - responseReceivedTimeNanos;

            metrics.addDataValidated(size, isConsistent);
            dataValidatedBytesForPage += size;

            // Save some calls to System.nanoTime if we're not blocking at all
            if (!limiter.tryAcquire(size))
            {
                // We're running on a specific executor (ValidationExecutor) explicitly because of the limiter, because
                // it is blocking. So make double sure we haven't introduced a bug by mistake by running on a TPC thread.
                assert !TPC.isTPCThread();
                long start = System.nanoTime();
                limiter.acquire(size);
                limiterWaitTimeNanosForPage += System.nanoTime() - start;
            }
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
        }
    }
}
