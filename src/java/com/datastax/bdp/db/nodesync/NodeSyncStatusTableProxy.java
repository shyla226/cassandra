/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Interface that abstract our use of the NodeSync {@link SystemDistributedKeyspace#NODESYNC_STATUS} table.
 * <p>
 * This exists mainly to make testing of the components that depend on it ({@link TableState}) easier.
 */
public interface NodeSyncStatusTableProxy
{
    public static NodeSyncStatusTableProxy DEFAULT = new NodeSyncStatusTableProxy()
    {
        public CompletableFuture<List<NodeSyncRecord>> nodeSyncRecords(TableMetadata table, Range<Token> range)
        {
            return SystemDistributedKeyspace.nodeSyncRecords(table, range);
        }

        public void lockNodeSyncSegment(Segment segment, long timeout, TimeUnit timeoutUnit)
        {
            SystemDistributedKeyspace.lockNodeSyncSegment(segment, timeout, timeoutUnit);
        }

        public void forceReleaseNodeSyncSegmentLock(Segment segment)
        {
            SystemDistributedKeyspace.forceReleaseNodeSyncSegmentLock(segment);
        }

        public void recordNodeSyncValidation(Segment segment, ValidationInfo info, boolean wasPreviousSuccessful)
        {
            SystemDistributedKeyspace.recordNodeSyncValidation(segment, info, wasPreviousSuccessful);
        }
    };

    /**
     * Retrieves the recorded NodeSync validations that cover a specific {@code range} of a specific table.
     *
     * @param table the table for which to retrieve the records.
     * @param range the range for which to retrieve the records. This <b>must</b> be a non wrapping range.
     * @return all the NodeSync records that cover {@code range}.
     */
    public CompletableFuture<List<NodeSyncRecord>> nodeSyncRecords(TableMetadata table, Range<Token> range);

    /**
     * Retrieves the recorded NodeSync validations that cover a specific {@code segment}.
     * <p>
     * Note that we're interested in the status for a single segment, but the ranges stored in the table may be of
     * different granularity than the one we're interested of, so in practice we return a list of all records that
     * intersect with the segment of interest. In most case though, we consolidate those records into one to make it
     * easier to work with using {@link NodeSyncRecord#consolidate}.
     *
     * @param segment the segment for which to retrieve the records.
     * @return a future on the NodeSync records that cover {@code segment} fully.
     */
    default public CompletableFuture<List<NodeSyncRecord>> nodeSyncRecords(Segment segment)
    {
        return nodeSyncRecords(segment.table, segment.range);
    }

    /**
     * Record that a {@code Segment} is being currently validated by NodeSync on this node (locking it temporarily).
     * <p>
     * See {@link NodeSyncRecord#lockedBy} for details on how we use the segment "lock".
     *
     * @param segment the segment that is currently being validated.
     * @param timeout the timeout to set on the record (so as to not "lock" the range indefinitely if the node dies
     *                while validating the range).
     * @param timeoutUnit the unit for timeout.
     */
    public void lockNodeSyncSegment(Segment segment, long timeout, TimeUnit timeoutUnit);

    /**
     * Removes the lock set on a {@code Segment} by {@link #lockNodeSyncSegment}.
     * <p>
     * Note that 1) this is mainly used to release the lock on failure, as on normal completion {@link #recordNodeSyncValidation}
     * releases the lock directly and we don't have to call this method, and 2) this doesn't perform any check that we
     * do hold the lock, so this shouldn't be called unless we know we do (but reminder that our locking is an
     * optimization in the first place so we don't have to work too hard around races either).
     *
     * @param segment the segment on which to remove the lock.
     */
    public void forceReleaseNodeSyncSegmentLock(Segment segment);

    /**
     * Records the completion (successful or not) of the validation by NodeSync of the provided table {@code segment} on
     * this node.
     *
     * @param segment the segment that has been validated.
     * @param info the information regarding the NodeSync validation to record.
     * @param wasPreviousSuccessful whether the previous validation was successful or not. This is an optimization and
     *                              doesn't have to be exact (it's ok to pass {@code false} if this is unknown for
     *                              instance), but avoid generating too much tombstones unnecessarily.
     */
    public void recordNodeSyncValidation(Segment segment, ValidationInfo info, boolean wasPreviousSuccessful);
}
