/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

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
abstract class ValidationProposal
{
    protected final TableState.Ref segmentRef;

    ValidationProposal(TableState.Ref segmentRef)
    {
        this.segmentRef = segmentRef;
    }

    /** The segment on which this is a validation proposer. */
    Segment segment()
    {
        return segmentRef.segment();
    }

    /**
     * Activate the proposal, potentially creating a new {@link Validator}.
     *
     * @return a new {@link Validator} corresponding to this proposal or {@code null} if, upon activation, it is
     * decided that this proposal is out of date (generally because another node has validated the same segment in the
     * interval between the proposal creation and now) and shouldn't be executed. Note that if a {@link Validator} is
     * returned by this method, it should be either executed or cancelled as it the underlying segment will have been
     * locked.
     */
    abstract Validator activate();
}
