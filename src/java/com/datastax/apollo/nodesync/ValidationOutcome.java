/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.nodesync;

import org.apache.cassandra.repair.SystemDistributedKeyspace;

/**
 * Represents the outcome of the validation of a piece of data (a page, a full segment, ...) by NodeSync.
 * <p>
 * When we validate (and repair), we can have the following outcomes:
 *   1) all replica are up and respond ("full")
 *   2) some replica are dead but we have at least 2 that are up and respond ("partial")
 *   3) only one (or zero, but the coordinator of the validation is at least always alive and will generally respond) replica
 *     are alive or respond, there is no validation we can do ("uncompleted").
 *   4) some unexpected error occurs which prevents the validation ("failed").
 * In the 2 first cases, where we do perform the validation/repair, we can distinguish the following variants:
 *   a) every replica (that responded) were in sync, nothing needed repair.
 *   b) some replica were not in sync, we had to send some repair and did so successfully.
 *
 * The following enum represents those possible outcomes.
 */
public enum ValidationOutcome
{
    // The order of the enum shouldn't be relied upon externally but matter internally. More precisely, composeWith
    // rely on the fact that if you combine 2 outcome, you have to keep the biggest one as it's the "worst" option.
    FULL_IN_SYNC    (0),
    FULL_REPAIRED   (1),
    PARTIAL_IN_SYNC (2),
    PARTIAL_REPAIRED(3),
    UNCOMPLETED     (4),
    FAILED          (5);

    private final byte code;

    // Map from code to their respective value. Currently assumes we don't wast any code, which we have no reason to.
    private static final ValidationOutcome[] codeToValue = new ValidationOutcome[ValidationOutcome.values().length];
    static
    {
        for (ValidationOutcome outcome : ValidationOutcome.values())
        {
            byte c = outcome.code;
            if (codeToValue[c] != null)
                throw new IllegalStateException(String.format("Code %d is use twice, for both %s and %s", c, codeToValue[c],  outcome));

            codeToValue[c] = outcome;
        }
    }

    ValidationOutcome(int code)
    {
        this.code = (byte)code;
    }

    /**
     * Unique code identifying (really the ordinal, but relying on enum ordering too much is kind of bad) used
     * when recording the outcome in system tables (more precisely in the {@link SystemDistributedKeyspace#NODESYNC_VALIDATION})
     * type) where we can't afford to use too much bytes for this.
     */
    byte code()
    {
        return code;
    }

    /**
     * Return the outcome associated to the provided code.
     *
     * @param code the code for the outcome.
     * @return the outcome corresponding to {@code code}.
     *
     * @throws IllegalArgumentException if {@code code} doesn't correspond to a valid outcome code.
     */
    static ValidationOutcome fromCode(byte code)
    {
        if (code < 0 || code >= codeToValue.length)
            throw new IllegalArgumentException("Invalid (out-of-bound) code " + code);

        ValidationOutcome outcome = codeToValue[code];
        if (outcome == null)
            throw new IllegalArgumentException("Code " + code + " has no defined corresponding outcome");

        return outcome;
    }

    /**
     * Creates a new {@link ValidationOutcome} for a completed validation.
     *
     * @param isPartial whether the validation was only partial.
     * @param hadMismatch whether the segment had any mismatch between replica responses, i.e. whether we had to repair
     *                    any node at all (or if everything was already in sync).
     * @return the corresponding {@link ValidationOutcome}.
     */
    static ValidationOutcome completed(boolean isPartial, boolean hadMismatch)
    {
        if (isPartial)
            return hadMismatch ? PARTIAL_REPAIRED : PARTIAL_IN_SYNC;
        else
            return hadMismatch ? FULL_REPAIRED : FULL_IN_SYNC;
    }

    /**
     * Whether the validation with this outcome was successful, meaning that is was "full". A successful validation
     * means that we can guarantee the segment is in sync on on all replica up to the start the of the validation this
     * is an outcome of (barring data loss).
     */
    public boolean wasSuccessful()
    {
        switch (this)
        {
            case FULL_IN_SYNC:
            case FULL_REPAIRED:
                return true;
            default:
                return false;
        }
    }

    /**
     * Whether the validation with this outcome was complete but "partial".
     */
    public boolean wasPartial()
    {
        switch (this)
        {
            case PARTIAL_IN_SYNC:
            case PARTIAL_REPAIRED:
                return true;
            default:
                return false;
        }
    }

    /**
     * Composes two validation outcomes.
     * <p>
     * This is used when we have the outcomes for validation on 2 consecutive sub-parts of a segment/range and want to
     * compose them into a single outcome for the whole part covered by those 2 sub-parts. It is, in practice, the
     * "worst" of the 2 outcomes since that's the best we can say (meaning, if (0, 100] was fully successful but
     * (100, 200] was uncompleted, the best we can say of (0, 200] is that it was uncompleted).
     *
     * @param other the other outcome to compose this outcome with.
     * @return the outcome corresponding to composing this outcome to {@code other}, which is, in practice, the "worst"
     * of the 2 outcomes.
     */
    ValidationOutcome composeWith(ValidationOutcome other)
    {
        // As mention in the enum declaration above, outcomes are declared in such a way that the max of two is the
        // "worst" outcome and so the one we have to keep when composing them.
        return this.compareTo(other) > 0 ? this : other;
    }

    @Override
    public String toString()
    {
        return super.toString().toLowerCase();
    }
}
