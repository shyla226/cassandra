/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.concurrent.TimeUnit;

/**
 * An object that allows acting on the main events of the lifecycle of a segment validation.
 * <p>
 * Such lifecycle object is created when the validation is started, after which the {@link #onNewPage} method will be
 * called at the start of every new page of data, and either {@link #onCompletion} is called on the completion of the
 * segment validation, or that validation is cancelled and {@link #cancel()} is called.
 */
class ValidationLifecycle
{
    /**
     * Timeout on segment locking to avoid holding a lock forever in case a problem happen.
     * This is not a limit on the time to validate a segment because if validating a segment come close to that time,
     * we will refresh the lock. The point being that if we don't refresh the lock for that much, the lock is
     * automatically released.
     */
    static final int LOCK_TIMEOUT_SEC = Integer.getInteger("dse.nodesync.segment_lock_timeout_sec", (int)TimeUnit.MINUTES.toSeconds(10));

    private final TableState.Ref segmentRef;

    private final long startTime;

    private volatile int nextLockRefreshTimeSec;

    private ValidationLifecycle(TableState.Ref segmentRef, long startTime)
    {
        this.segmentRef = segmentRef;
        this.startTime = startTime;
        this.nextLockRefreshTimeSec = computeNextLockRefresh((int)(startTime / 1000));
    }

    /**
     * Creates a new {@link ValidationLifecycle} object for the provided segment and "lock" it.
     * <p>
     * Note that any lifecycle returned by this method is "started" and thus much be either completed or cancelled, or
     * the lock set won't be released (we timeout locks, so this is not critical, but not releasing the lock would still
     * be a bug, just one the implementation protects against).
     *
     * @param segmentRef a reference to the state of the segment on which this is a lifecycle. This reference allow to
     *                   update said state based on the progress of the validation lifecycle.
     * @return the newly created and started {@link ValidationLifecycle}.
     */
    static ValidationLifecycle createAndStart(TableState.Ref segmentRef)
    {
        ValidationLifecycle lifecycle = new ValidationLifecycle(segmentRef, NodeSyncHelpers.time().currentTimeMillis());
        lifecycle.onStart();
        return lifecycle;
    }

    /**
     * The segment for which this is a validation lifecycle.
     */
    Segment segment()
    {
        return segmentRef.segment();
    }

    NodeSyncService service()
    {
        return segmentRef.service();
    }

    /**
     * The starting time of the validation.
     */
    long startTime()
    {
        return startTime;
    }

    private NodeSyncStatusTableProxy statusTable()
    {
        return service().statusTableProxy;
    }

    /**
     * Called no the start of the validation to lock the segment.
     */
    private void onStart()
    {
        // Lock in the system table and locally
        statusTable().lockNodeSyncSegment(segment(), LOCK_TIMEOUT_SEC, TimeUnit.SECONDS);
        segmentRef.lock();
    }

    private void checkForInvalidation()
    {
        if (segmentRef.isInvalidated())
            throw new InvalidatedNodeSyncStateException();
    }

    /**
     *  Called by {@link Validator} at the start of every new page of the data correspond to this segment validation.
     */
    void onNewPage()
    {
        checkForInvalidation();

        int nowInSec = NodeSyncHelpers.time().currentTimeSeconds();
        if (nowInSec > nextLockRefreshTimeSec)
        {
            statusTable().lockNodeSyncSegment(segment(), LOCK_TIMEOUT_SEC, TimeUnit.SECONDS);
            segmentRef.refreshLock();
            nextLockRefreshTimeSec = computeNextLockRefresh(nowInSec);
        }
    }

    /**
     *  Called by {@link Validator} on the completion of the validation with the information regarding said validation.
     */
    void onCompletion(ValidationInfo info)
    {
        checkForInvalidation();

        // This will release the lock.
        statusTable().recordNodeSyncValidation(segment(), info, segmentRef.segmentStateAtCreation().lastValidationWasSuccessful());
        segmentRef.onCompletedValidation(info.startedAt, info.wasSuccessful());
    }

    /**
     *  Called by {@link Validator} if the validation is cancelled before it is completed.
     */
    void cancel()
    {
        statusTable().forceReleaseNodeSyncSegmentLock(segment());
        segmentRef.forceUnlock();
    }

    private static int computeNextLockRefresh(int nowInSec)
    {
        // Refresh a bit (1/4 of the time) before the previous record timeout
        return nowInSec + (3 * LOCK_TIMEOUT_SEC / 4);
    }
}
