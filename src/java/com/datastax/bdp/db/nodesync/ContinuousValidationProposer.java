/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.schema.NodeSyncParams;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.units.Units;

/**
 * A {@link ValidationProposer} that continuously/indefinitely creates validation proposals over a given table so as to
 * ensure this table data is constantly validated.
 * <p>
 * This is the main {@link ValidationProposer} used by NodeSync and NodeSync ensures that any NodeSync-enabled table
 * has a corresponding {@link ContinuousValidationProposer} set-up in {@link ValidationScheduler}.
 * <p>
 * The general idea being this proposer will continuously retrieve from the correspond {@link TableState} whichever
 * segment needs to be validated next (according to {@link SegmentState#priority()}) and submit validations for those.
 * <p>
 * Note that it doesn't make sense to have more than one instance of this class per table and we make sure this is not
 * the case in {@link ValidationScheduler}.
 */
class ContinuousValidationProposer extends ValidationProposer
{
    private static final long RETRY_DELAY_MS = TimeUnit.SECONDS.toMillis(5);

    private static final Logger logger = LoggerFactory.getLogger(ContinuousValidationProposer.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 10, TimeUnit.MINUTES);

    private final Consumer<Proposal> proposalConsumer;
    private volatile boolean cancelled;

    /**
     * Creates a new continuous proposer on the provided table state, and that passes proposal to the provided consumer.
     *
     * @param state the state for the table on which this should be a proposer.
     * @param proposalConsumer the consumer to which proposal are passed. The first of those proposal will be passed to
     *                         that consumer upon the call to {@link #start}, and then a new proposal will be automatically
     *                         passed every time the previous proposal is activated (with {@link Proposal#activate()}).
     */
    ContinuousValidationProposer(TableState state,
                                 Consumer<Proposal> proposalConsumer)
    {
        super(state);
        this.proposalConsumer = proposalConsumer;
    }

    /**
     * Starts the proposer, passing the first proposer to the consumer passed in the ctor.
     *
     * @return this proposer.
     */
    ContinuousValidationProposer start()
    {
        generateNextProposal();
        return this;
    }

    @VisibleForTesting
    void generateNextProposal()
    {
        if (cancelled)
            return;

        TableState.Ref ref = state.nextSegmentToValidate();
        if (ref.segmentStateAtCreation().isLocallyLocked())
        {
            // We don't want to validate a segment that is already being validated by ourselves. Getting such segment
            // here means however that despite the priority penalty on locked segments, this is still the one with
            // the most pressing priority. In that case, we simply wait a little bit for the validation of that segment
            // finishes, at which point his priority will be updated according to the success of that validation. Note
            // that if we have a bug and the validation somehow never finishes, the local lock will eventually expire
            // and we'll still make progress.
            ScheduledExecutors.nonPeriodicTasks.schedule(this::generateNextProposal, RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
            return;
        }

        Proposal next = new Proposal(ref);
        long now = NodeSyncHelpers.time().currentTimeMillis();
        if (next.minTimeForSubmission() > now)
            ScheduledExecutors.nonPeriodicTasks.schedule(() -> proposalConsumer.accept(next),
                                                         next.minTimeForSubmission() - now, TimeUnit.MILLISECONDS);
        else
            proposalConsumer.accept(next);
    }

    public boolean cancel()
    {
        cancelled = true;
        return true;
    }

    public boolean isCancelled()
    {
        return cancelled;
    }

    @Override
    public String toString()
    {
        return String.format("Continuous validations of %s", table());
    }

    /**
     * A {@link ValidationProposal} for continuous validations.
     */
    class Proposal extends ValidationProposal implements Comparable<Proposal>
    {
        private final long priority;

        /**
         * For reasons explained on {@link NodeSyncService#MIN_VALIDATION_INTERVAL_MS}, we want to ensure a minimum time
         * interval between 2 validations on the same segment. To enforce that, this represents the earlier time
         * at which we're willing to validate the segment. The proposer will make sure it doesn't "publish"
         * this proposal until that time (see {@link ContinuousValidationProposer#generateNextProposal}). Of course,
         * there is no guarantee that once the proposal is published to the scheduler, it will be picked up for execution
         * right away, so this is really the minimum time at which the next validation may happen, with no real guarantee
         * on when it will do so.
         */
        private final long minTimeForSubmission;

        private Proposal(TableState.Ref segmentRef)
        {
            super(segmentRef);
            this.priority = stateAtProposal().priority();
            this.minTimeForSubmission = minTimeForNextValidation(stateAtProposal());
        }

        private SegmentState stateAtProposal()
        {
            return segmentRef.segmentStateAtCreation();
        }

        private long minTimeForNextValidation(SegmentState state)
        {
            if (NodeSyncService.MIN_VALIDATION_INTERVAL_MS < 0)
                return Long.MIN_VALUE;

            // As explained in NodeSyncParams, having tableDeadlineTargetMs <= MIN_VALIDATION_INTERVAL_MS is wrong and this
            // may be the only place where we can detect this, even if that's not ideal in theory (we will get there for every
            // proposal created while the misconfiguration persists). So still warn, but with care of not spamming the log.
            if (state.deadlineTargetMs() <= NodeSyncService.MIN_VALIDATION_INTERVAL_MS)
            {
                // Somewhat random estimation of when user have toyed with the min validation interval in an un-reasonable
                // way. Only there to provide slightly more helpful message so guess-estimate is fine.
                boolean minValidationIsHigh = NodeSyncService.MIN_VALIDATION_INTERVAL_MS > TimeUnit.HOURS.toMillis(10);
                noSpamLogger.warn("NodeSync '{}' setting on {} is {} which is lower than the {} value ({}): "
                                  + "this mean that deadline cannot be achieved, at least on this node, and indicate a misconfiguration. {}",
                                  NodeSyncParams.Option.DEADLINE_TARGET_SEC,
                                  state.segment().table,
                                  Units.toString(state.deadlineTargetMs(), TimeUnit.MILLISECONDS),
                                  NodeSyncService.MIN_VALIDATION_INTERVAL_PROP_NAME,
                                  Units.toString(NodeSyncService.MIN_VALIDATION_INTERVAL_MS, TimeUnit.MILLISECONDS),
                                  minValidationIsHigh ? "The custom value set for " + NodeSyncService.MIN_VALIDATION_INTERVAL_PROP_NAME + " seems unwisely high"
                                                      : "That '" + NodeSyncParams.Option.DEADLINE_TARGET_SEC + "' value seems unwisely low value");
            }
            return state.lastValidationTimeMs() + NodeSyncService.MIN_VALIDATION_INTERVAL_MS;
        }

        private long minTimeForSubmission()
        {
            return minTimeForSubmission;
        }

        /**
         * Activate the proposal. Because there can be delay between when the proposal was created and its activation
         * (other table may have proposal that have higher priority and go first), activation basically ensure that
         * the proposal is still 'up to date' (it typically won't be up-to-date if another replica has validated that
         * segment since proposal). If it isn't, then this method simply return {@link null} and a new proposal is
         * generated (based on the updated information). If it is up to date, a corresponding {@link Validator} is
         * created (and a proposal for whatever segment comes next is generated).
         */
        Validator activate()
        {
            // Things may have changed since we created the proposal and the segment may be or have been validated by
            // another node, so refresh the state for the system table.
            TableState.Ref.Status status = segmentRef.checkStatus();
            if (status.isUpToDate())
            {
                NodeSyncTracing.SegmentTracing tracing  = service().tracing().startContinuous(segment(), stateAtProposal().toTraceString());
                ValidationLifecycle lifecycle = ValidationLifecycle.createAndStart(segmentRef, tracing);
                // Note: it's important generating the next proposal is after the lifecycle has been created, as that
                // is what locks the segment locally and make sure we don't re-generate that same segment.
                generateNextProposal();
                return Validator.create(lifecycle);
            }

            service().tracing().skipContinuous(segment(), status.toString());
            // Note: if the segment was already (remotely) locked at proposal, this would mean that despite the
            // lock penalty to the priority, that segment was still the highest priority, but it is still
            // locked remotely. In that case, it's probably worth waiting a small delay before retrying
            // generation to avoid spinning on that segment.
            if (status.isRemotelyLocked() && stateAtProposal().isRemotelyLocked())
                ScheduledExecutors.nonPeriodicTasks.schedule(ContinuousValidationProposer.this::generateNextProposal, RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
            else
                generateNextProposal();
            return null;
        }

        /**
         * Compares proposals priority, where the smallest proposal is the one having the lowest value for {@link #priority}
         * and is the the one that should be validated first.
         * <p>
         * Note that because comparison implements priority, callers should not expect this comparison to be consistent with
         * equal. Meaning that 2 proposals can be indistinguishable in terms of priority (we don't care which one executes
         * first) without being equal.
         */
        public int compareTo(Proposal other)
        {
            if (equals(other))
                return 0;

            return Longs.compare(this.priority, other.priority);
        }

        @Override
        public String toString()
        {
            long now = NodeSyncHelpers.time().currentTimeMillis();
            if (minTimeForSubmission <= now)
                return stateAtProposal().toString();

            String delayToSubmission = Units.toString(minTimeForSubmission - now, TimeUnit.MILLISECONDS);
            return String.format("%s (to be submitted in %s)", stateAtProposal(), delayToSubmission);
        }
    }
}
