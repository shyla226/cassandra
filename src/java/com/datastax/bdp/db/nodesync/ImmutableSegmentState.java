/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

class ImmutableSegmentState extends SegmentState
{
    private final Segment segment;
    private final long lastValidationTimeMs;
    private final long lastSuccessfulValidationTimeMs;
    private final long deadlineTargetMs;
    private final boolean isLocallyLocked;
    private final boolean isRemotelyLocked;

    ImmutableSegmentState(Segment segment,
                          long lastValidationTimeMs,
                          long lastSuccessfulValidationTimeMs,
                          long deadlineTargetMs,
                          boolean isLocallyLocked,
                          boolean isRemotelyLocked)
    {
        this.segment = segment;
        this.lastValidationTimeMs = lastValidationTimeMs;
        this.lastSuccessfulValidationTimeMs = lastSuccessfulValidationTimeMs;
        this.deadlineTargetMs = deadlineTargetMs;
        this.isLocallyLocked = isLocallyLocked;
        this.isRemotelyLocked = isRemotelyLocked;
    }

    Segment segment()
    {
        return segment;
    }

    long lastValidationTimeMs()
    {
        return lastValidationTimeMs;
    }

    long lastSuccessfulValidationTimeMs()
    {
        return lastSuccessfulValidationTimeMs;
    }

    long deadlineTargetMs()
    {
        return deadlineTargetMs;
    }

    boolean isLocallyLocked()
    {
        return isLocallyLocked;
    }

    boolean isRemotelyLocked()
    {
        return isRemotelyLocked;
    }
}
