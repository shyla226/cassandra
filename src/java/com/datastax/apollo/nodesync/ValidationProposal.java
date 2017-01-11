/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.nodesync;

import com.google.common.primitives.Longs;

/**
 * A proposal for the validation of a particular segment.
 * <p>
 * The main purpose of this class is to implement priorities amongst competing possible NodeSync validations. Namely,
 * multiple {@link ValidationProposer} submit proposals to the {@link ValidationScheduler}, which sorts those proposals
 * and schedule them in ascending order. When a proposal is finally scheduled for execution, it is activated
 * ({@link #activate()}) to create the actual {@link Validator} that is executed.
 * <p>
 * This class is abstract and each {@link ValidationProposer} sub-class will generally implement its own variant. However, for
 * the reason above, all proposal sub-class should be comparable with any other proposal. To make that meaningful, this
 * class define a basic priority scheme based on 2 elements: 1) each proposal has a numeric priority level where the
 * lowest priority level proposal should always have priority and 2) on similar priority level, proposals are ordered
 * by their creation time.
 */
abstract class ValidationProposal implements Comparable<ValidationProposal>
{
    static final int DEFAULT_PRIORITY_LEVEL = 10;
    static final int HIGH_PRIORITY_LEVEL = 0;

    private final ValidationProposer proposer;
    protected final Segment segment;

    /** The priority level for that proposal. Priority level can be any integer and the expectation is that all proposal
     * implementations will respect that priority level _first_, meaning that proposals with lower priority level
     * will always have priority over other ones. Proposal Subclasses that don't want to impose any special priority
     * of their proposals should simply use the {@link #DEFAULT_PRIORITY_LEVEL}. */
    protected final int priorityLevel;
    /** Creation time of the proposal. This is used by default to disambiguate the priority of proposals on the same
     * priority level: the oldest proposals have higher priority. */
    protected final long creationTime;

    ValidationProposal(ValidationProposer proposer, Segment segment, int priorityLevel)
    {
        assert proposer != null && segment != null;
        this.proposer = proposer;
        this.segment = segment;
        this.priorityLevel = priorityLevel;
        this.creationTime = System.currentTimeMillis();
    }

    ValidationProposal(ValidationProposer proposer, Segment segment)
    {
        this(proposer, segment, DEFAULT_PRIORITY_LEVEL);
    }

    /** The proposer that created this proposal. */
    ValidationProposer proposer()
    {
        return proposer;
    }

    /** The segment on which this is a validation proposer. */
    Segment segment()
    {
        return segment;
    }

    /**
     * Activate the proposal, potentially starting a new {@link Validator}.
     *
     * @return a new {@link Validator} corresponding to this proposal or {@code null} if, upon activation, it is
     * decided that this proposal is out of date (generally because another node has validated the same segment in the
     * interval between the proposal creation and now) and shouldn't be executed.
     */
    abstract Validator activate();

    /**
     * Compares proposals priority, where the smallest proposal is the one having the highest priority.
     * <p>
     * Note that because comparison implements priority, callers should not expect this comparison to be consistent with
     * equal. Meaning that 2 proposals can be indistinguishable in terms of priority (we don't care which one executes
     * first) without being equal.
     * <p>
     * This method provides the default comparison described in the class javadoc, but can be overridden to provide
     * different priorities when comparing 2 proposals of a particular sub-class (see
     * {@link ContinuousTableValidationProposer.Proposal#compareTo(ValidationProposal)} for an example). Do keep in mind however
     * that comparison <b>must</b> stay commutative, so any override should probably stick to the behavior of this
     * method when comparing proposal of different sub-classes.
     */
    public int compareTo(ValidationProposal other)
    {
        if (equals(other))
            return 0;

        // Smallest priority level win and otherwise older wins.
        int cmp = Integer.compare(priorityLevel, other.priorityLevel);
        if (cmp != 0)
            return cmp;

        return Longs.compare(creationTime, other.creationTime);
    }
}
