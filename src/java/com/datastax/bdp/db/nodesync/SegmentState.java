/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.units.Units;

/**
 * Represents the state of a particular segment at a particular point in time, and the resulting priority of this
 * segment.
 *
 * <h2>Prioritization/h2>
 *
 * One of the more important part this class implement is the {@link #priority()} function which is used to prioritize
 * segments.
 * <p>
 * The general rational is that we want to prioritize whichever segment is the closest of missing its deadline.
 * Two factor complicates that simple heuristic however:
 *   1) a validation can be partial or fail, and we cannot consider such partial/failed validation as ok in term of
 *      meeting the deadline. But we can't entirely ignore those validation either, maintaining the segment priority
 *      as if that validation didn't happen, or we'll concretely loop on such partial segment until it can be successfully
 *      validated.
 *   2) a segment can in the process of being validated (possibly by another node), and while we don't want to duplicate
 *     such validation if unnecessary, that validation can fail (either not complete at all because the node performing
 *     it dies mid-validation, or the validation can be partial), and we want to be careful not to de-prioritize a
 *     locked segment so much that if that happen it won't picked up again fairly quickly.
 * For point 1), the general idea is that we always prioritize a segment from his last validation (successful or not),
 * but add to that a higher value if it has been successful than if it failed/was partial. In other words, we'll retry
 * comparatively more quickly a failed/partial segment than a successful one. And in practice, the "how much quickly"
 * depend of how far the segment is from failing his deadline. Meaning that if a segment is close to miss his deadline
 * (or already missed it) and we fail a validation, we'll retry it fairly aggressively, but if it still far from his
 * deadline, we retry only slightly more aggressively than if it was successful (basically, it's ok to dedicate
 * resources to other validation if we still have time for this segment).
 * <p>
 * For point 2), we handle it by penalizing the priority of a locked segment, so that anything that is not too far behind
 * in term of priority will get in front. Basically, we try a locked segment a bit later, and by then, either the
 * validation will hopefully have either succeeded or failed, and we'll prioritize the segment accordingly.
 */
abstract class SegmentState
{
    /**
     * By how much we "penalize" a segment locked by a remote node in term of priority (see above). As the lock timeout
     * is meant as an upper time on segment validation with some margin of error, it doesn't feel like too bad a penalty.
     */
    private static final long REMOTE_LOCK_PRIORITY_PENALTY_MS = Long.getLong("dse.nodesync.remote_lock_penalty_ms",
                                                                             TimeUnit.SECONDS.toMillis(ValidationLifecycle.LOCK_TIMEOUT_SEC));

    /**
     * We're a lot more aggressive in penalizing local locks. The reason is that while a remote node having locked a
     * segment can fail, and thus we want to detect that reasonably promptly, that problem doesn't really exist for
     * local locks outside of bugs that makes us drop a local validation on the floor. And as much as we want to protect
     * against such bug, it also make sense to not bother spending much time on locally locked segment as much as possible.
     * (note that by default the penalty will amount to 1000 minutes, so ~16 hours).
     */
    private static final long LOCAL_LOCK_PRIORITY_PENALTY_MS = Long.getLong("dse.nodesync.local_lock_penalty_ms", 100 * REMOTE_LOCK_PRIORITY_PENALTY_MS);

    /**
     * The segment this is the state of.
     */
    abstract Segment segment();

    /**
     * The last time the segment was validated (successfully or not), as of the state this represents.
     */
    abstract long lastValidationTimeMs();

    /**
     * The last time the segment was validated successfully, as of the state this represents.
     */
    abstract long lastSuccessfulValidationTimeMs();

    /**
     * The table deadline target at the time corresponding to the state this represents.
     */
    abstract long deadlineTargetMs();

    /**
     * Whether the segment is locked locally.
     */
    abstract boolean isLocallyLocked();

    /**
     * Whether the segment is remotely locally.
     */
    abstract boolean isRemotelyLocked();

    boolean lastValidationWasSuccessful()
    {
        return lastValidationTimeMs() == lastSuccessfulValidationTimeMs();
    }

    /**
     * The priority for the validation of the segment this is the state of, as of this state.
     * <p>
     * The intuition being this priority is that it loosely represents the deadline for this segment, i.e. the latest
     * time at which the segment must have been ideally validated. Note however that this is only an intuition and the
     * actual number shouldn't be relied upon in any other way that for prioritizing validations (lowest priority value
     * wins).
     * <p>
     * See the class javadoc for details on the reasoning behind the heuristic implemented.
     */
    long priority()
    {
        return priority(lastValidationTimeMs(),
                        lastSuccessfulValidationTimeMs(),
                        deadlineTargetMs(),
                        isLocallyLocked(),
                        isRemotelyLocked());
    }

    static long priority(long lastValidationTimeMs,
                         long lastSuccessfulValidationTimeMs,
                         long deadlineTargetMs,
                         boolean locallyLocked,
                         boolean remotelyLocked)
    {
        long priority = rawPriority(lastValidationTimeMs, lastSuccessfulValidationTimeMs, deadlineTargetMs);
        if (locallyLocked)
            priority += LOCAL_LOCK_PRIORITY_PENALTY_MS;
        if (remotelyLocked)
            priority += REMOTE_LOCK_PRIORITY_PENALTY_MS;
        return priority;
    }

    private static long rawPriority(long lastValidationTimeMs, long lastSuccessfulValidationTimeMs, long deadlineTargetMs)
    {
        long priority = lastValidationTimeMs + deadlineTargetMs;
        if (lastValidationTimeMs == lastSuccessfulValidationTimeMs)
            return priority;

        // If we denote last validations with l, last successful ones with s and the deadline as d, the idea here is
        // that we want to subtract from l + d a number such that, for any s and d and using p(s, l) for
        // priority(s, l, d), we have:
        //   1) for any l1, l2 such that s <= l1 < l2, we have p(l1, s) < p(l2, s); that's our liveness property:
        //      even a failed validation bump the priority at least some amount so we don't loop on the same failing
        //      validation.
        //   2) for any l1, l2 such that s < l1 < l2, p(l1, l1) - p(l1, s) >= p(l2, l2) - p(l2, s); the farther the
        //      last failed attempt is from the last successful one, the less the priority is bumped following that
        //      failed attempt. In other words, we retry more aggressively on a a failed attempt if the segment is
        //      closed to missing (or over) its deadline.
        // The idea of our exact number is that we remove (from l + d) half of the distance between l and s: so if the
        // last attempt is close from the last successful, that number will be small, meaning that we prioritize only
        // slightly less than if the attempt was successful in the first place. Of course, this doesn't work once the
        // distance between l and s become too big, so we cap this number to be both greater than d/10 and lower than
        // d-(d/10). In other words, a failed attempt always at least bump the priority by d/10 and never does so
        // more than 9d/10 (bumping by d would be considering the attempt successful).
        long diff = (lastValidationTimeMs - lastSuccessfulValidationTimeMs) / 2;
        return priority - Math.max(5000, Math.min((9 * deadlineTargetMs) / 10, diff));
    }

    /**
     * A String representation more suitable for tracing (where the segment itself is part of the context in particular).
     */
    String toTraceString()
    {
        long last = lastValidationTimeMs();
        long lastSucc = lastSuccessfulValidationTimeMs();
        if (last < 0 && lastSucc < 0)
            return "never validated";

        if (last == lastSucc)
            return String.format("validated %s", NodeSyncHelpers.sinceStr(last));

        if (lastSucc < 0)
            return String.format("validated unsuccessfully %s (no known success)", NodeSyncHelpers.sinceStr(last));

        return String.format("validated unsuccessfully %s (last success: %s)",
                             NodeSyncHelpers.sinceStr(last),
                             NodeSyncHelpers.sinceStr(lastSucc));
    }

    @Override
    public String toString()
    {
        String deadlineStr = Units.toString(deadlineTargetMs(), TimeUnit.MILLISECONDS);
        long last = lastValidationTimeMs();
        long lastSucc = lastSuccessfulValidationTimeMs();
        String lockString = isLocallyLocked() ? "[L. locked]" : (isRemotelyLocked() ? "[R. locked]" : "");
        if (last < 0 && lastSucc < 0)
            return String.format("%s(<never validated>)%s", segment(), lockString);

        if (lastValidationWasSuccessful())
            return String.format("%s(last validation=%s (successful), deadline=%s)%s",
                                 segment(), NodeSyncHelpers.sinceStr(last), deadlineStr, lockString);
        else
            return String.format("%s(last validation=%s, last successful one=%s, deadline=%s)%s",
                                 segment(), NodeSyncHelpers.sinceStr(last), NodeSyncHelpers.sinceStr(lastSucc), deadlineStr, lockString);
    }
}
