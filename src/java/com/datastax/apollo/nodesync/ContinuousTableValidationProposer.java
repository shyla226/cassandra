/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.nodesync;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.NodeSyncConfig;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.units.Units;

/**
 * A {@link ValidationProposer} that continuously/indefinitely creates validation proposals over a given table so as to
 * ensure this table data is constantly validated.
 * <p>
 * This is the main {@link ValidationProposer} used by NodeSync in that this is the only kind of proposer that is
 * automatically setup by NodeSync. More precisely, NodeSync ensures that any table eligible to NodeSync has a
 * corresponding {@link ContinuousTableValidationProposer} in the {@link ValidationScheduler}.
 * <p>
 * The general idea being this proposer is that it will prioritize the validation of whatever segment has been without
 * validation the longest (according to {@link SystemDistributedKeyspace#NodeSyncStatus}).
 * <p>
 * Note that it doesn't make sense to have more than one instance of this class per table and we make sure this is not
 * the case in {@link ValidationScheduler} (see {@link #equals(Object)} method javadoc for details).
 */
class ContinuousTableValidationProposer extends AbstractValidationProposer
{
    private static final Logger logger = LoggerFactory.getLogger(ContinuousTableValidationProposer.class);

    /**
     * The minimum delay we enforce between doing 2 validation on the same segment.
     * This exists because on very small clusters (typically brand new empty ones) we might end up validating everything
     * in a very very short time, and it doesn't feel very meaningful to re-validate the empty system distributed tables
     * every 50ms (which is what happens without this on an empty cluster). Note that the amount of resources devoted to
     * NodeSync is globally guarded by {@link NodeSyncConfig#rateLimiter} so the importance of this shouldn't be
     * over-stated but 1) this feels reasonable and 2) we don't account for validations over empty data with the limiter
     * (since it rate limit validated bytes) but there is costs associated to creating the validation in the first place
     * (reading the {@link SystemDistributedKeyspace#NodeSyncStatus} table mostly) and this prevents this to get out of
     * hand.
     */
    // TODO(Sylvain): should make this configurable for testing purposes (less meaningful for end user though)
    private static final long MIN_VALIDATION_INTERVAL_MS = TimeUnit.MINUTES.toMillis(10);
    // TODO(Sylvain): should (probably) make this configurable, at least for testing (not that important for end-user).
    private static final ThreadPoolExecutor reloadExecutor = DebuggableThreadPoolExecutor.createWithMaximumPoolSize("NodeSyncValidationProposers", 4, 30, TimeUnit.SECONDS);

    // Used to make initialization of ContinuousTableValidationProposer easier. It's mutable because I'm not aware of an
    // immutable empty PriorityQueue and it doesn't feel worth creating one here but _do not_ ever add to this obviously!
    private static final PriorityQueue<Proposal> EMPTY_QUEUE = new PriorityQueue<>();
    /**
     * Currently loaded/prepared proposals. Once this is empty, new proposals are automatically re-loaded and replace
     * this, see {@link ReloadableProposals} below.
     */
    private volatile ReloadableProposals proposals = new ReloadableProposals(EMPTY_QUEUE);

    private ContinuousTableValidationProposer(NodeSyncService service,
                                              TableMetadata table,
                                              int depth,
                                              Function<String, Collection<Range<Token>>> localRangesProvider,
                                              ToLongFunction<ColumnFamilyStore> tableSizeProvider)
    {
        super(service, table, depth, localRangesProvider, tableSizeProvider);
    }

    /**
     * Creates a dummy continuous table proposer that is only suitable for use of its {@link #equals(Object)} and
     * {@link #hashCode()} methods. Calling any other method on the returned object has undefinted behavior (most likely,
     * will throw an exception). This exists because we use proposers in a set in {@link ValidationScheduler} and we use
     * this to remove continuous table proposers without iterating over the whole set. * * @param table the table for the proposer.
     * @return a dummy proposer that is only usable in rare situations, see above.
     */
    static ContinuousTableValidationProposer dummyProposerFor(TableMetadata table)
    {
        return new ContinuousTableValidationProposer(null, table, 0, DEFAULT_LOCAL_RANGES_PROVIDER, DEFAULT_TABLE_SIZE_PROVIDER);
    }

    /**
     * Creates continuous table proposers for all existing tables (at the time of the call) that have NodeSync enabled
     * (and are in a keyspace with RF > 1).
     *
     * @param service the NodeSync service for which we create the proposers.
     * @return a list of the proposers for all table eligible to NodeSync (have NodeSync enabled and have RF>1).
     */
    static List<ContinuousTableValidationProposer> createAll(NodeSyncService service)
    {
        List<ContinuousTableValidationProposer> proposers = new ArrayList<>();
        for (Keyspace keyspace : Iterables.transform(StorageService.instance.getNonSystemKeyspaces(), Keyspace::open))
            proposers.addAll(createForKeyspace(service, keyspace));
        return proposers;
    }

    /**
     * Creates continuous table proposers for all existing tables (at the time of the call) of the provided
     * keyspace that are eligible to NodeSync.
     *
     * @param service the NodeSync service for which we create the proposers.
     * @param keyspace the keyspace for which to create the proposers.
     * @return a list containing a continuous table proposers for every table eligible to NodeSync. Note that ff the
     * keyspace has RF <= 1, then this will return an empty list.
     */
    static List<ContinuousTableValidationProposer> createForKeyspace(NodeSyncService service, Keyspace keyspace)
    {
        return createForKeyspace(service,
                                 keyspace,
                                 NodeSyncService.MAX_SEGMENT_SIZE,
                                 DEFAULT_TABLE_SIZE_PROVIDER,
                                 DEFAULT_LOCAL_RANGES_PROVIDER);
    }

    /**
     * Only used for testing to fake the size on disk of tables. Use {@link #createForKeyspace(NodeSyncService, Keyspace)}
     * in any non-test code.
     */
    @VisibleForTesting
    static List<ContinuousTableValidationProposer> createForKeyspace(NodeSyncService service,
                                                                     Keyspace keyspace,
                                                                     long maxSegmentSize,
                                                                     ToLongFunction<ColumnFamilyStore> sizeProvider,
                                                                     Function<String, Collection<Range<Token>>> rangeProvider)
    {
        if (keyspace.getReplicationStrategy().getReplicationFactor() <= 1)
            return Collections.emptyList();

        int localRangeCount = rangeProvider.apply(keyspace.getName()).size();

        List<ContinuousTableValidationProposer> proposers = new ArrayList<>();
        for (ColumnFamilyStore store : keyspace.getColumnFamilyStores())
            create(service, store, localRangeCount, maxSegmentSize, sizeProvider, rangeProvider).ifPresent(proposers::add);
        return proposers;
    }

    /**
     * Creates a continuous table proposer for the provided table, assuming it has NodeSync enabled (contrarily to
     * the previous ones, this method does <b>not</b> check the replication factor for the table, so this should be
     * checked externally).
     *
     * @param service the NodeSync service for which we create the proposers.
     * @param store   the {@link ColumnFamilyStore} of the table for which to create the proposer.
     * @return the newly created {@link ContinuousTableValidationProposer} or {@link Optional#empty()} if NodeSync is not enabled on {@code table}.
     */
    static Optional<ContinuousTableValidationProposer> create(NodeSyncService service, ColumnFamilyStore store)
    {
        return create(service,
                      store,
                      DEFAULT_LOCAL_RANGES_PROVIDER.apply(store.keyspace.getName()).size(),
                      NodeSyncService.MAX_SEGMENT_SIZE,
                      DEFAULT_TABLE_SIZE_PROVIDER,
                      DEFAULT_LOCAL_RANGES_PROVIDER);
    }

    private static Optional<ContinuousTableValidationProposer> create(NodeSyncService service,
                                                                      ColumnFamilyStore store,
                                                                      int localRangeCount,
                                                                      long maxSegmentSize,
                                                                      ToLongFunction<ColumnFamilyStore> sizeProvider,
                                                                      Function<String, Collection<Range<Token>>> rangeProvider)
    {
        TableMetadata table = store.metadata();
        if (!table.params.nodeSync.isEnabled(table))
            return Optional.empty();

        int depth = computeDepth(store, localRangeCount, sizeProvider, maxSegmentSize);
        return Optional.of(new ContinuousTableValidationProposer(service, table, depth, rangeProvider, sizeProvider));
    }

    public void init()
    {
        proposals.maybeScheduleReload();
    }

    public boolean supplyNextProposal(Consumer<ValidationProposal> proposalConsumer)
    {
        proposals.supplyNextTo(proposalConsumer);
        return true;
    }

    public boolean isDone()
    {
        return false;
    }

    public ValidationProposer onTableUpdate(TableMetadata table)
    {
        // Drop the proposer if it's the table it is a proposer for and NodeSync just got disabled on it.
        if (table.equals(this.table) && !table.params.nodeSync.isEnabled(table))
        {
            logger.info("Stopping NodeSync validations on table {} following user deactivation", table);
            return null;
        }
        return this;
    }

    public ValidationProposer onTableRemoval(String keyspace, String table)
    {
        if (this.table.keyspace.equals(keyspace) && this.table.name.equals(table))
        {
            // Logging at debug because when you explicitly dropped a table, it doesn't feel like you'd care too much
            // about that confirmation. Further, when a keyspace is dropped, this is called for every table it has
            // and this would feel like log spamming if the keyspace has very many tables.
            logger.debug("Stopping NodeSync validations on table {} as the table has been dropped", table);
            return null;
        }
        return this;
    }

    /**
     * There is absolutely no reason to have more than one {@link ContinuousTableValidationProposer} for a given table (in
     * a given {@link ValidationScheduler}), so basing equality solely on the table makes our life a lot easier in
     * {@link ValidationScheduler} (we can do blind addition (resp. removal) for a table and know that it won't created
     * duplicate (resp. remove what we want).
     */
    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ContinuousTableValidationProposer))
            return false;

        ContinuousTableValidationProposer that = (ContinuousTableValidationProposer) o;
        return this.table.equals(that.table);
    }

    @Override
    public int hashCode()
    {
        return table.hashCode();
    }

    @Override
    public String toString()
    {
        return String.format("NodeSync of %s at depth %d", table, depth);
    }

    /**
     * We want to generate proposals indefinitely but we also need to consult the {@link SystemDistributedKeyspace#NodeSyncStatus}
     * table to establish the proper priority of each proposals. Reading the records for all the ranges we're a replica
     * of for each new proposal (to find the next proposal) and throwing the rest of the information we read away is
     * however a bit wasteful. So instead, we read all said records once and generate proposals for it all. Once all
     * those those proposals have been used, we automatically reload a new batch of proposals and use those.
     * <p>
     * This is what this class implement. A given {@link ReloadableProposals} instance contains a loaded batch of
     * proposals. But when it's ask to supply a new proposal and it has none anymore, it schedule the asynchronous
     * loading of the next batch and have that be the new active batch.
     * <p>
     * Note that this does mean that while we go through a batch of proposals, we don't update proposals based on what
     * other replica do. But that's were activation plays a role: when activating, we double-check the system table to
     * see if the segment has been validated since we created the proposal. If it has, we consider the proposal out-dated
     * and simply skip it.
     *
     * TODO(Sylvain): this actually isn't ideal and we can save lots of querying to the system table by almost exclusively
     * relying on the queries done during activation. More precisely, we'd read the system tables only once at startup to
     * generate proposals for all segments. Then, when activating a proposal, we'd read the system table as we do now,
     * but if the proposal is outdated, we'd simply use the information we just read to re-queue the segment with updated
     * information. If we do end up doing the validation, we'd simply requeue the segment at the end of said validation.
     * This would also provide slightly better behavior overall and it's on my todo-list to implement this, but the
     * current version is likely good-enough in practice so this is not a top priority. We'd also have to be a bit careful
     * to always requeue segments (even on errors), as well as be careful with topology changes (we don't want to
     * generate proposals for ranges for which we're not the replica anymore, which is currently kind of deal with
     * automatically on each reload).
     */
    private class ReloadableProposals
    {
        private final PriorityQueue<Proposal> loadedProposals;
        private final CompletableFuture<ReloadableProposals> reloadFuture;
        private final AtomicBoolean reloadTriggered = new AtomicBoolean();

        private ReloadableProposals(PriorityQueue<Proposal> loadedProposals)
        {
            this.loadedProposals = loadedProposals;
            this.reloadFuture = new CompletableFuture<>();
        }

        /**
         * Supply the next proposal to the provided consumer. If we still have loaded proposals, we use the next one
         * in the queue. Otherwise, we asynchronously load a new batch and pass the first of this new batch to the
         * consumer when ready. This does mean callers of this method shouldn't expect the consumer to be called
         * synchronously as it may not be.
         */
        private void supplyNextTo(Consumer<ValidationProposal> consumer)
        {
            Proposal nextLoaded = loadedProposals.poll();
            if (nextLoaded == null)
            {
                maybeScheduleReload();
                reloadFuture.thenAccept(p -> p.supplyNextTo(consumer));
            }
            else
            {
                if (nextLoaded.minTimeForNextValidation < 0)
                    supplyTo(nextLoaded, consumer);

                // The next proposal is the one having the smallest minTimeForNextValidation of what's in the queue.
                // So if that's in the future, schedule the actual supply then.
                long delayToValidation = nextLoaded.minTimeForNextValidation - System.currentTimeMillis();
                if (delayToValidation > 0)
                    ScheduledExecutors.nonPeriodicTasks.schedule(() -> supplyTo(nextLoaded, consumer), delayToValidation, TimeUnit.MILLISECONDS);
                else
                    supplyTo(nextLoaded, consumer);
            }
        }

        private void supplyTo(Proposal proposal, Consumer<ValidationProposal> consumer)
        {
            // Schedule reload in background, no need to wait for this to be called next.
            if (loadedProposals.isEmpty())
                maybeScheduleReload();

            consumer.accept(proposal);
        }

        private void maybeScheduleReload()
        {
            if (!reloadTriggered.compareAndSet(false, true))
                return;

            doReload();
        }

        // Should only be called if reloadTriggered has been set to true by us!!
        private void doReload()
        {
            reloadExecutor.submit(() -> {
                try
                {
                    PriorityQueue<Proposal> nextProposals = loadProposals();
                    ReloadableProposals newReloadableProposals = new ReloadableProposals(nextProposals);
                    reloadFuture.complete(newReloadableProposals);
                    ContinuousTableValidationProposer.this.proposals = newReloadableProposals;
                }
                catch (Exception e)
                {
                    // This shouldn't happen because since we already have exception catch-all when reading the system
                    // table and that's the only part that could legitimately throw. But simply failing here would mean
                    // that we basically stop validating the table and that's bad. So if that unfortunately happen, log
                    // a scary error message and retry despite not really knowing if it'll work (that's why we add a
                    // delay). Note that we may end up spamming the log every minute if this fails continuously due to
                    // a programming error, but it's serious enough that this doesn't feel crazy (and user can stop
                    // the spam by disabling NodeSync on the table, which in turn make it crystal clear that the table
                    // is not validated anymore).
                    logger.error("Unexpected error reloading NodeSync validation proposals for {};"
                                 + " this is a bug and should be reported to DataStax support as such;"
                                 + " this _will_ prevent proper validation and repair of table {} by NodeSync;"
                                 + " this will be retried in 1 minute but if this continue failing and you want to stop"
                                 + " this message from being logged, you should disable NodeSync on {}",
                                 table, table, table, e);
                    ScheduledExecutors.optionalTasks.schedule(this::doReload, 1, TimeUnit.MINUTES);
                }
            });
        }

        private PriorityQueue<Proposal> loadProposals()
        {
            Collection<Range<Token>> localRanges = localRanges();
            PriorityQueue<Proposal> nextProposals = new PriorityQueue<>(Segments.estimateSegments(localRanges, depth));
            Iterator<Segment> segments = Segments.generateSegments(table, localRanges, depth);
            while (segments.hasNext())
            {
                Segment segment = segments.next();
                // TODO: fetching the records for every individual segments is a bit inefficient, we could get all the records
                // for what we cover in one go instead, but we'd need to re-split the list afterwards so we can consolidate
                // the record individually for each segment. We may also have to be a bit careful with fetching too much
                // data at once (though if that's a problem, we should probably rather change the way we generate the
                // proposals to be more incremental in the first place). Anyway, keeping it simple for now as that's not
                // critical.
                List<NodeSyncRecord> records = SystemDistributedKeyspace.nodeSyncRecords(segment);
                NodeSyncRecord record = NodeSyncRecord.consolidate(segment, records);
                nextProposals.add(new Proposal(ContinuousTableValidationProposer.this, segment, record));
            }
            assert !nextProposals.isEmpty() : "We shouldn't be generating empty proposals";
            return nextProposals;
        }

    }

    /**
     * A Proposal object that prioritize segments whose last validation is the oldest.
     */
    private static class Proposal extends ValidationProposal
    {
        /**
         * The last known time at which the segment for which this is a proposal was validated.
         * <p>
         * Can be negative is we have not record of said last validation.
         */
        private final long lastValidationTime;
        /**
         * The time at which we should have re-validated the segment to meet the table validation deadline target.
         * This is the value on which prioritisation is based: validating next the segment whose deadline is the closest
         * is the best way to make sure we will meet all deadlines.
         * <p>
         * Can be negative if we have no record of having previously validated this segment and thus want to validate it
         * ASAP.
         */
        private final long nextValidationDeadlineTarget;
        /**
         * For reasons explained on {@link #MIN_VALIDATION_INTERVAL_MS}, we want to ensure a minimum time interval
         * between 2 validations on the same segment. To enforce that, this variable represents the earlier time
         * at which we're willing to validate the segment again. The proposer will make sure it doesn't "publish"
         * this proposal until that time (see {@link ReloadableProposals#supplyNextProposal(Consumer)}). Of course,
         * there is no guarantee that once the proposal is published to the scheduler, it will be picked up for
         * execution right away, so this is really the minimum time at which the next validation may happen, with
         * no real guarantee on when it will do so.
         * <p>
         * Can be negative if we have no record of having previously validated.
         */
        private final long minTimeForNextValidation;

        private Proposal(ContinuousTableValidationProposer proposer,
                         Segment segment,
                         NodeSyncRecord record)
        {
            super(proposer, segment);
            this.lastValidationTime = record == null || record.lastValidation == null
                                      ? Long.MIN_VALUE
                                      : record.lastValidation.startedAt;

            long tableDeadlineTargetMs = segment.table.params.nodeSync.deadlineTarget(segment.table, TimeUnit.MILLISECONDS);
            this.nextValidationDeadlineTarget = lastValidationTime < 0
                                                ? Long.MIN_VALUE
                                                : lastValidationTime + tableDeadlineTargetMs;
            this.minTimeForNextValidation = computeMinTimeForNextValidation(lastValidationTime, record, tableDeadlineTargetMs);
        }

        private long computeMinTimeForNextValidation(long lastValidationTime, NodeSyncRecord record, long tableDeadlineTargetMs)
        {
            if (lastValidationTime < 0)
                return lastValidationTime;

            assert record != null && record.lastValidation != null; // we wouldn't have lastValidationTime >= 0 otherwise
            // In general, we don't want to validate the same segment more than once every MIN_VALIDATION_INTERVAL_MS. A
            // slight exception is for partial validation where the missing nodes are known to be now UP: it make sense
            // to try to validate those without delay to get a full validation (they still won't prioritize before a
            // segment whose validation is older than this segment last partial validation).
            // Side-note: when missingNodes is empty on a partial validation, this means the RF for the table was greater than
            // the number of nodes. In this case, we could check if we have enough nodes now, but it's enough of an edge case
            // that we simply don't bother.
            if (record.lastValidation.outcome.wasPartial()
                && !record.lastValidation.missingNodes.isEmpty()
                && Iterables.all(record.lastValidation.missingNodes, FailureDetector.instance::isAlive))
            {
                return lastValidationTime;
            }

            // Having MIN_VALIDATION_INTERVAL_MS >= tableDeadlineTargetMs is kind of a misconfiguration by design, but
            // this is not the place to validate that (and it's actually kind of hard to enforce it doesn't happen: we
            // can offer recommendation on table deadline not being set crazy low, nor changing MIN_VALIDATION_INTERVAL_MS
            // unless you know what you're doing but that's about it). So if that ever happen, just ignore
            // MIN_VALIDATION_INTERVAL_MS as, within that realm of misconfiguration, is what makes the most sense.
            return MIN_VALIDATION_INTERVAL_MS >= tableDeadlineTargetMs
                   ? lastValidationTime
                   : lastValidationTime + MIN_VALIDATION_INTERVAL_MS;
        }

        Validator activate()
        {
            // Things may have changed since we created the proposal and the segment may be or have been validated by
            // another node, so we need to re-check the system table.
            List<NodeSyncRecord> records = SystemDistributedKeyspace.nodeSyncRecords(segment);
            NodeSyncRecord record = NodeSyncRecord.consolidate(segment, records);

            // We want to do the validation unless one of the following is true:
            // 1) the segment is locked by another node (someone else is already handling this as we speak).
            // 2) the record indicates a validation that is newer than the one we used to create the proposal. This
            //    means another node validated that segment since we created the proposal and the proposal is out-of-date.
            if (record != null && (record.lockedBy != null || (record.lastValidation != null && record.lastValidation.startedAt > lastValidationTime)))
            {
                if (logger.isTraceEnabled())
                {
                    if (record.lockedBy == null)
                        logger.trace("Skipping validation on {}: has been validated since the proposal was created "
                                     + "(know last validation: now={}, at proposal={})",
                                     segment, timeSinceStr(record.lastValidation.startedAt), timeSinceStr(lastValidationTime));
                    else
                        logger.trace("Skipping validation on {}: locked by {}", segment, record.lockedBy);
                }
                return null;
            }

            if (logger.isTraceEnabled())
                logger.trace("Submitting validation of {} for execution: last known validation={}", segment, timeSinceStr(lastValidationTime));
            return Validator.createAndLock(proposer().service(), segment);
        }

        @Override
        public int compareTo(ValidationProposal other)
        {
            if (!(other instanceof Proposal))
                return super.compareTo(other);

            Proposal that = (Proposal) other;
            assert this.priorityLevel == that.priorityLevel : this.priorityLevel + " != " + that.priorityLevel;

            // As explained above, nextDeadlineTarget is the value on which we want to do prioritization. Note that any
            // negative value will win (have higher priority than any positive value), which is exactly what we want.
            // TODO: we may want to get a lot more fancy in how we prioritize. In particular, we don't really distinguish
            // between fully successful and partial validation but maybe it could make sense to some extent.
            int cmp = Longs.compare(nextValidationDeadlineTarget, that.nextValidationDeadlineTarget);
            if (cmp != 0)
                return cmp;

            // If we happen to have the same deadline, simply break the tie by whichever range comes first in ring order
            return segment.compareTo(that.segment);
        }

        @Override
        public String toString()
        {
            long now = System.currentTimeMillis();
            String lastStr = lastValidationTime < 0 ? "<none>" : Units.toString(now - lastValidationTime, TimeUnit.MILLISECONDS) + " ago";
            String nextStr = nextValidationDeadlineTarget < 0 ? "<asap>" : "in " + Units.toString(nextValidationDeadlineTarget - now, TimeUnit.MILLISECONDS);
            String minStr = minTimeForNextValidation < 0 ? "<asap>" : "in " + Units.toString(minTimeForNextValidation - now, TimeUnit.MILLISECONDS);
            return String.format("%s(last validation=%s, next deadline=%s, min validation=%s)", segment, lastStr, nextStr, minStr);
        }
    }
}
