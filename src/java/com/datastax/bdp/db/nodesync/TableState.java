/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.units.Units;

/**
 * Holds the NodeSync state for a specific (NodeSync-enabled) table.
 * <p>
 * See {@link NodeSyncState} for a general description of how the NodeSync states work. This class implements the
 * subpart of that global state dedicated to a specific table.
 *
 * <h2>External usage<h2/>
 *
 * The main methods of this class are {@link #nextSegmentToValidate()}, which retrieve the next segment that should be
 * validated by continuous NodeSync validations for the table this represents, and {@link #intersectingSegments(List)}
 * which return all segments intersecting with the provided ranges (used for user validations).
 * <p>
 * Both of those method return {@link Ref} objects, which represents a particular segment, but maintain a link to the
 * state of that segment so that we can easily update the state based on any work on that segment.
 * <p>
 * Outside of those, this class exposes methods to be called when the parameters that affect the state (which are
 * the local ranges of the replica and the depth to use, which defines which segments we consider, as well as the table
 * deadline target, which impacts priority) are updated.
 *
 * <h2>Implementation details</h2>
 *
 * The state for a table (at least the part we keep in memory) is comprised of the list of segments for the table (that
 * this node is responsible for) along with the time of the last validation, time of last successful one (which may be
 * the same) and whether the segment is locked (both by this node or another one). That state is stored by the
 * {@link StateHolder} inner class below.
 * <p>
 * An instance of the {@link StateHolder} class is defined by the particular list of segments it contains (which in turn
 * are defined by the local ranges and depth that were used to generate them). In other words, any change that modifies
 * the list of segments require a new instance of {@link StateHolder} to be created and to replace the old one. This
 * should be a rare event however as:
 *  - Local ranges will only change following topology changes. This is the most costly case, because the the new ranges
 *    will be unrelated to the old ones (partly in practice, but we don't assume anything at all in this class) and so
 *    the new state needs to be reloaded from the NodeSync status table.
 *  - Depth will change when the data for the table increase or decrease significantly. The former may happen
 *    semi-regularly on a new cluster, but as an increase in depth implies data doubled, this should overall really rare
 *    in production cluster. This is also much less costly as the range covered don't change, segment simply get more
 *    precise, and we can migrate the old state without re-reading the status table. The latter (significant table size
 *    decrease) will likely never happen in most workload, and be very rare at best. We do reload from the status table
 *    in that case because that is simpler and not worth optimizing.
 * <p>
 * The state is in practice maintained in the following way:
 *  - At creation time, it is populated with the information from the status table.
 *  - Validation proposal are created from that original state. When a proposal is to be activated
 *    ({@link ValidationProposal#activate}), we read the status table for that segment to refresh the in-memory
 *    information. Upon such refresh, the segment might be validated.
 *  - Local validation update the underlying in-memory state directly through the user of {@link Ref} objects.
 * <p>
 * In order to track state changes, a {@link Version} object is used: at every state update, the version value changes
 * (as described in the {@link Version} javadoc), and each segment {@link Ref} is created with a link to the version at
 * the time they are creation, so that we can easily find out state changes since the ref creation.
 * <p>
 * The main subtleties of this class are how concurrency is handled, and the segment locking handling, which we'll
 * describe next.
 *
 * <h2>Concurrency</h2>
 *
 * This class is externally thread-safe, and rely for that on a read-write lock. The write lock is taken for any update,
 * whether it is one that changes the underlying state object, or simply one that update said state.
 * <p>
 * Note that in few cases (namely when the local ranges change or the depth is lowered), the write lock might be hold
 * for a relatively long time as the state is reloaded from the status table while holding the lock. This is both
 * intended (there would be no point in letting validations continue going on while we reload the state since in those
 * case the validation will be invalidated anyway) and unlikely a performance concern given how rare those events are.
 *
 * <h2>Segment Locking</h2>
 *
 * When a replica starts to validate a segment, it "locks" it to avoid having multiple replica validating the same
 * segment. The state differentiates between local locks (those set because the local node is starting a validation)
 * and remote locks (those set by remote replica and found in the status table). In both case, locking a segment is
 * a way to indicate it is undergoing validation and so when the state is asked for the next segment to validate, it
 * will exclude locked ones (in practice, we don't fully exclude them, see {@link SegmentState} for details on how
 * priority is handled with respect to locking).
 * <p>
 * The reason we distinguish local and remote locks is that they are not updated the same way: local locks are set by
 * this node during activation and released at the end of the validation. Remote locks however are only find out when
 * the state is refreshed during activation, and we will have to re-consider the segment and refresh again to notice
 * the lock is released (or expires in practice).
 * <p>
 * Note that both type of lock have a timeout: for remote locks, it avoids that a node dying mid-validation keep the
 * lock on that validation forever. For local locks, this is mostly to protect against bugs, if we ever have a path where
 * we don't properly release the local locks.
 */
public class TableState
{
    private static final Logger logger = LoggerFactory.getLogger(TableState.class);

    private final NodeSyncService service;
    private volatile TableMetadata table;

    private volatile StateHolder stateHolder;
    private final Version version = new Version();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private TableState(NodeSyncService service, TableMetadata table)
    {
        this.service = service;
        this.table = table;

        // Safety measure: we timestamp acquisition of local lock and force release expired locks here. This should
        // really never do anything or we have a bug, but avoid keeping a segment locked forever because a bug leaked
        // a Ref object.
        ScheduledExecutors.optionalTasks.scheduleAtFixedRate(this::unlockExpiredLocalLocks,
                                                             ValidationLifecycle.LOCK_TIMEOUT_SEC,
                                                             ValidationLifecycle.LOCK_TIMEOUT_SEC,
                                                             TimeUnit.SECONDS);
    }

    NodeSyncService service()
    {
        return service;
    }

    /**
     * Load the state (from the NodeSync status table) for the provided table using the provided local ranges and depth.
     *
     * @param service the service for which this state is loaded.
     * @param table the table to load the state for.
     * @param localRanges the local ranges to use to generate segments.
     * @param depth the depth to use to generate segments.
     * @return the state for {@code table} that holds the segments corresponding to {@code localRanges} and {@code depth}
     * the current information stored in {@link SystemDistributedKeyspace#NODESYNC_STATUS} for those segments.
     */
    static TableState load(NodeSyncService service, TableMetadata table, Collection<Range<Token>> localRanges, int depth)
    {
        TableState tableState = new TableState(service, table);
        tableState.stateHolder = tableState.emptyState(localRanges, depth).populateFromStatusTable();
        return tableState;
    }

    private NodeSyncStatusTableProxy statusTable()
    {
        return service.statusTableProxy;
    }

    private StateHolder emptyState(Collection<Range<Token>> localRanges, int depth)
    {
        return new StateHolder(deadline(), Segments.generate(table, localRanges, depth));
    }

    /**
     * The current NodeSync deadline target for the table this is the state of.
     */
    private long deadline()
    {
        return table.params.nodeSync.deadlineTarget(table, TimeUnit.MILLISECONDS);
    }

    /**
     * The table this is the state of.
     */
    TableMetadata table()
    {
        return table;
    }

    /**
     * The depth currently used for segments of this state.
     */
    int depth()
    {
        return stateHolder.segments.depth();
    }

    /**
     * The local ranges currently used for segments of this state.
     */
    Collection<Range<Token>> localRanges()
    {
        return stateHolder.segments.localRanges();
    }

    private NodeSyncTracing tracing()
    {
        return service.tracing();
    }

    /**
     * Updates the state so that it uses the provided altered metadata for future segments and proposals.
     */
    void onTableUpdate(TableMetadata alteredTable)
    {
        // Cheap check outside the lock; we'll check again inside it.
        if (alteredTable == this.table)
            return;

        lock.writeLock().lock();
        try
        {
            if (alteredTable == this.table)
                return;

            long newDeadline = deadline();

            this.table = alteredTable;
            stateHolder.updateTable(table);

            if (stateHolder.deadlineTargetMs != newDeadline)
            {
                logger.debug("Updating NodeSync state and deadline target for {} following table update", table);
                tracing().trace("Updating deadline from {} to {} for {}",
                                Units.toString(stateHolder.deadlineTargetMs, TimeUnit.MILLISECONDS),
                                Units.toString(newDeadline, TimeUnit.MILLISECONDS),
                                table);
                stateHolder.updateDeadline(newDeadline);
            }
            else
            {
                logger.debug("Updating NodeSync state for {} following table update", table);
            }
            // invalidated not activated proposals
            version.priority++;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Updates the state so that it uses the provided depth for its segments. If the state already uses this depth, this
     * is a no-op.
     */
    void update(int depth)
    {
        // Cheap check outside the lock; we'll check again inside it.
        if (depth == stateHolder.segments.depth())
            return;

        lock.writeLock().lock();
        try
        {
            if (depth == stateHolder.segments.depth())
                return;

            logger.debug("Updating NodeSync state for {} to {} as depth have been updated", table, depth);
            StateHolder newStateHolder = emptyState(stateHolder.segments.localRanges(), depth);
            // If we increase the depth, we don't really need to reload from disk, the copyTo() call below will fully
            // populate the new state properly. If the depth is decreased, we could theoretically also populate the new
            // state entirely from the old one, but copyTo() will not work for that (we'd need slightly more complex
            // logic) so we don't bother, we just reload from disk and racing validation will get invalidated. Decreasing
            // the depth should be so rare than re-validating a handful of segments more quickly than we should is fine.
            if (depth < stateHolder.segments.depth())
            {
                newStateHolder.populateFromStatusTable();
                version.major++;
            }
            else
            {
                newStateHolder.updateFrom(stateHolder);
                version.minor++;
            }
            tracing().trace("Updating depth from {} to {} for {} (segments count: {} -> {})",
                            stateHolder.segments.depth(), depth, table, stateHolder.size, newStateHolder.size);
            stateHolder = newStateHolder;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Updates the state so that it uses the provided local ranges as base for its segments. If the state already uses
     * those local ranges, this is a no-op.
     */
    void update(Collection<Range<Token>> localRanges)
    {
        // Cheap check outside the lock; we'll check again inside it.
        if (localRanges.equals(stateHolder.segments.localRanges()))
            return;

        lock.writeLock().lock();
        try
        {
            if (localRanges.equals(stateHolder.segments.localRanges()))
                return;

            logger.debug("Updating NodeSync state for {} as local ranges have been updated", table);
            StateHolder newStateHolder = emptyState(localRanges, stateHolder.segments.depth()).populateFromStatusTable();
            tracing().trace("Updating local ranges from {} to {} for {} (segments count: {} -> {})",
                            stateHolder.segments.localRanges(), localRanges, table, stateHolder.size, newStateHolder.size);
            stateHolder = newStateHolder;
            version.major++;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Find the next segment that should be validated according to the validation priority defined by
     * {@link SegmentState#priority()}.
     *
     * @return a reference to the segment having the smallest priority value. Note that it is possible that this
     * segment is currently locally (or remotely) locked, so this should usually be taken into account by callers.
     */
    Ref nextSegmentToValidate()
    {
        lock.readLock().lock();
        try
        {
            return stateHolder.nextSegmentToValidate();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Return a list of the {@link SegmentState} corresponding to all segments intersecting with the range provided
     * as argument, which must all be local sub-ranges (at the time of this call at least).
     *
     * @param localSubRanges the local sub-ranges for which to return intersecting segment state.
     * @return a list with all the state of the segment that intersect any of {@code localSubRanges}. The returned list
     * shouldn't be assumed to be in any particular order.
     */
    ImmutableList<Ref> intersectingSegments(List<Range<Token>> localSubRanges)
    {
        lock.readLock().lock();
        try
        {
            return stateHolder.intersectingSegments(localSubRanges);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * The time of the oldest successful validation amongst all segments of this state.
     *
     * @return the smallest successful validation time for all segment in this state. Note that if any segment has never
     * been successfully validated, this will return {@link NodeSyncHelpers#NO_VALIDATION_TIME}.
     */
    long oldestSuccessfulValidation()
    {
        lock.readLock().lock();
        try
        {
            return stateHolder.oldestSuccessfulValidation();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private void unlockExpiredLocalLocks()
    {
        if (stateHolder == null)
            return;

        int nowInSec = NodeSyncHelpers.time().currentTimeSeconds();
        if (stateHolder.hasExpiredLocalLocks(nowInSec))
        {
            // A local lock should get expired unless we've dropped a Ref object on the floor without cancelling it,
            // which is a bug. Of course, slowness and bad timing could _theoretically_ make this happen without it
            // being a bug, but given the lock default timeout, that is more than unlikely. Anyway, at least warning
            // when that happen make sense.
            logger.warn("Detected expired (and thus non released) local NodeSync locks. Will force-release them  to " +
                        "fix, but this should normally not happen and if you see this message with any kind of regularity, " +
                        "please report it");
            lock.writeLock().lock();
            try
            {
                stateHolder.expireLocalLocks(nowInSec);
            }
            finally
            {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * A testing method, not meant to be used anywhere else.
     */
    @VisibleForTesting
    List<SegmentState> dumpSegmentStates()
    {
        StateHolder current = stateHolder;
        List<SegmentState> segments = new ArrayList<>(current.size);
        for (int i = 0; i < current.size; i++)
            segments.add(current.immutableSegmentState(i));
        return segments;
    }

    private static boolean isRemotelyLocked(NodeSyncRecord record)
    {
        // We check if the node that locked is alive. If it isn't, we ignore the lock. Because the locks in the status
        // table are timed out, this is not essential, but it makes the system more reactive and thus easier to reason
        // about and test.
        return record.lockedBy != null && FailureDetector.instance.isAlive(record.lockedBy);
    }

    @Override
    public String toString()
    {
        return stateHolder.toString();
    }

    /**
     * The actual holder of the state, corresponding to a particular fixed state of the table.
     * <p>
     * Note that all the methods of this expect that the write lock will be held for any operation that update the
     * state and the read one held for any read operation.
     */
    private class StateHolder
    {
        private static final int UNLOCKED = Integer.MIN_VALUE;

        // The deadline of the table. We keep a version here instead of reaching into the table parameters when we
        // need it so as to ensure it stay stable in calls like nextSegmentToValidate(). Besides, we access it a lot
        // (since it's part of the priority computation) so having it isn't a bad idea performance-wise as well.
        private long deadlineTargetMs;
        private Segments segments;

        private final int size;
        private final long[] lastValidations;
        private final long[] lastSuccessfulValidations;

        // Each segment can be either locked locally (by this node) or remotely (by another replica). For local ones,
        // the state will be directly updated when the lock is released (the segment is validated) and so we won't
        // rely on reading the status table, but we use an in-memory expiration schema to prevent any issue if something
        // bad happens. For the remote locks, we will have to check the status table to know if the lock is still on,
        // but that does mean we can rely the status table timeout for expiration.
        private final int[] localLocks;
        private final BitSet remoteLocks;

        // The index to the next segment that should be validated. Update to this is what triggers a "priority" version
        // change.
        private int nextIdx;

        private StateHolder(long deadlineTargetMs,
                            Segments segments,
                            long[] lastValidations,
                            long[] lastSuccessfulValidations,
                            int[] localLocks,
                            BitSet remoteLocks)
        {
            this.deadlineTargetMs = deadlineTargetMs;

            this.segments = segments;
            this.size = segments.size();

            this.lastValidations = lastValidations;
            this.lastSuccessfulValidations = lastSuccessfulValidations;
            this.localLocks = localLocks;
            this.remoteLocks = remoteLocks;

            assert segments.size() == size;
            assert lastValidations.length == size;
            assert lastSuccessfulValidations.length == size;
            assert localLocks.length == size;
            assert remoteLocks.size() >= size; // BitSet size is always a multiple of 64
        }

        private StateHolder(long deadlineTargetMs, Segments segments)
        {
            this(deadlineTargetMs,
                 segments,
                 new long[segments.size()],
                 new long[segments.size()],
                 new int[segments.size()],
                 new BitSet(segments.size()));
            Arrays.fill(lastValidations, NodeSyncHelpers.NO_VALIDATION_TIME);
            Arrays.fill(lastSuccessfulValidations, NodeSyncHelpers.NO_VALIDATION_TIME);
            Arrays.fill(localLocks, UNLOCKED);
        }

        private long priority(int i)
        {
            return SegmentState.priority(lastValidations[i],
                                         lastSuccessfulValidations[i],
                                         deadlineTargetMs,
                                         localLocks[i] != UNLOCKED,
                                         remoteLocks.get(i));
        }

        private void setNextIdx(int i)
        {
            nextIdx = i;
            version.priority++;
        }

        private boolean isLocalLockExpired(int i, int nowInSec)
        {
            // It's expired if it was locked in the first place but the lock expiration is in the past
            long lock = localLocks[i];
            return lock != UNLOCKED && lock < nowInSec;
        }

        private boolean hasExpiredLocalLocks(int nowInSec)
        {
            for (int i = 0; i < size; i++)
            {
                if (isLocalLockExpired(i, nowInSec))
                    return true;
            }
            return false;
        }

        private void expireLocalLocks(int nowInSec)
        {
            for (int i = 0; i < size; i++)
            {
                if (isLocalLockExpired(i, nowInSec))
                    localLocks[i] = UNLOCKED;
            }
            // Force-releasing some expired locks might have change priorities so just recompute. Note that don't
            // call this unless we know there is expired lock so it's ok to call unconditionally here.
            updateNextToValidate();
        }

        private void updateNextToValidate()
        {
            // Find the min value (in term of priority value)
            int minIdx = 0;
            long minPriority = priority(minIdx);
            for (int i = 1; i < size; i++)
            {
                long p = priority(i);
                if (p < minPriority)
                {
                    minIdx = i;
                    minPriority = p;
                }
            }

            // If that's not the same as the current one, update and bump the minor version
            if (minIdx != nextIdx)
                setNextIdx(minIdx);
        }

        private StateHolder populateFromStatusTable()
        {
            // We load record separately for each local ranges for 2 reasons:
            // 1) as local ranges may not be contiguous, we can't really query that in a single CQL query (the internal code
            //    could actually do that since we can request multiple slices, but that's not exposed to CQL and it would
            //    annoying to fall back on internal querying just for this.
            // 2) this give us a poor-man limitation on how many records are loaded at once. Of course, that's an ugly
            //    (and not that reliable) way to do so.
            // 3) makes it a bit easier to deal with wrapping at this stage.
            // TODO: in a perfect world, we'd query everything with multiple slices and simply page it. But we'd have to
            // rework consolidate() a bit for this to work well, and that's probably not urgent in practice.
            segments.localRanges().forEach(range -> {
                if (range.isTrulyWrapAround())
                    range.unwrap().forEach(this::populateRangeFromStatusTable);
                else
                    populateRangeFromStatusTable(range);
            });
            updateNextToValidate();
            return this;
        }

        private void populateRangeFromStatusTable(Range<Token> toLoad)
        {
            // TODO: related to the TODO from the parent load method, we could improve this: recalling consolidate on all
            // records for every segments is inefficient and we can do that much cleanly. This is probably not urgent as
            // the impact in practice is likely low (this is not performance critical).
            List<NodeSyncRecord> records = statusTable().nodeSyncRecords(table, toLoad);
            for (int i = 0; i < size; i++)
                update(i, NodeSyncRecord.consolidate(segments.get(i), records));
        }

        private void updateTable(TableMetadata table)
        {
            segments = Segments.updateTable(segments, table);
        }

        private void updateDeadline(long newDeadlineTargetMs)
        {
            deadlineTargetMs = newDeadlineTargetMs;
            updateNextToValidate();
        }

        private Ref newRef(int i)
        {
            return new Ref(TableState.this, i);
        }

        private SegmentState immutableSegmentState(int i)
        {
            return new ImmutableSegmentState(segments.get(i),
                                             lastValidations[i],
                                             lastSuccessfulValidations[i],
                                             deadlineTargetMs,
                                             localLocks[i] != UNLOCKED,
                                             remoteLocks.get(i));
        }

        /**
         * Return the segment with the smallest priority value, unless said segment is locally locked.
         */
        private Ref nextSegmentToValidate()
        {
            return newRef(nextIdx);
        }

        private ImmutableList<Ref> intersectingSegments(List<Range<Token>> localSubRanges)
        {
            checkAllLocalRanges(localSubRanges);

            ImmutableList.Builder<Ref> builder = ImmutableList.builder();
            for (int i = 0; i < size; i++)
            {
                if (localSubRanges.stream().anyMatch(segments.get(i).range::intersects))
                    builder.add(newRef(i));
            }
            return builder.build();
        }

        private void checkAllLocalRanges(List<Range<Token>> toCheck)
        {
            // Because ranges are normalized, either every requested is strictly contained in one local range, or it
            // has some part that is not local.
            List<Range<Token>> localRanges = segments.normalizedLocalRanges();
            Set<Range<Token>> nonLocal = toCheck.stream()
                                                .filter(r -> localRanges.stream().noneMatch(l -> l.contains(r)))
                                                .collect(Collectors.toSet());
            if (!nonLocal.isEmpty())
                throw new IllegalArgumentException(String.format("Can only validate local ranges: ranges %s are not (entirely) local to node %s with ranges %s",
                                                                 nonLocal, FBUtilities.getBroadcastAddress(), localRanges));
        }

        private void updateFrom(StateHolder o)
        {
            for (int i = 0; i < o.size; i++)
            {
                int[] r = segments.findFullyIncludedIn(o.segments.get(i));
                for (int j = r[0]; j < r[1]; j++)
                    updateInternal(j, o.lastValidations[i], o.lastSuccessfulValidations[i], o.localLocks[i], o.remoteLocks.get(i));
            }
            updateNextToValidate();
        }

        private void updateInternal(int i, long newLast, long newLastSuccess, int newLocalLock, boolean newRemoteLock)
        {
            long previousPriority = priority(i);

            if (newLast > lastValidations[i])
                lastValidations[i] = newLast;
            if (newLastSuccess > lastSuccessfulValidations[i])
                lastSuccessfulValidations[i] = newLastSuccess;

            localLocks[i] = newLocalLock;
            remoteLocks.set(i, newRemoteLock);

            long newPriority = priority(i);

            // If the priority has changed, it might have impacted the next to validate
            if (newPriority != previousPriority)
            {
                // If it was the next to validate, it may or may not be anymore so we have to recompute to know.
                // Otherwise, it becomes the next to validate if the new priority is lower than said current next.
                if (i == nextIdx)
                {
                    updateNextToValidate();
                }
                else
                {
                    if (newPriority < priority(nextIdx))
                        setNextIdx(i);
                }
            }
        }

        private void update(int i, NodeSyncRecord record)
        {
            updateInternal(i,
                           record.lastValidationTimeMs(),
                           record.lastSuccessfulValidationTimeMs(),
                           localLocks[i], // keep whatever local lock we had
                           isRemotelyLocked(record));
        }

        private void lockLocally(int i)
        {
            localLocks[i] = newLockExpiration();
            // By design we'll lock the next in line, but doesn't cost us much to check if that changes: as locking
            // increase the priority, if it wasn't the min, it won't be post-locking.
            if (nextIdx == i)
                updateNextToValidate();
        }

        private void refreshLocalLock(int i)
        {
            if (localLocks[i] != UNLOCKED)
                localLocks[i] = newLockExpiration();
        }

        private void updateCompletedValidation(int i, long last, long lastSuccess)
        {
            updateInternal(i, last, lastSuccess, UNLOCKED, false);
        }

        private void forceLocalUnlock(int i)
        {
            localLocks[i] = UNLOCKED;
            // This will lower the priority of segment i, so it can now be the smaller one if it wasn't.
            if (nextIdx != i)
                updateNextToValidate();
        }

        private int newLockExpiration()
        {
            return NodeSyncHelpers.time().currentTimeSeconds() + ValidationLifecycle.LOCK_TIMEOUT_SEC;
        }

        private long oldestSuccessfulValidation()
        {
            long min = lastSuccessfulValidations[0];
            for (int i = 1; i < size; i++)
                min = Math.min(min, lastSuccessfulValidations[i]);
            return min;
        }

        @Override
        public String toString()
        {
            // Mostly only meant for test debugging, shouldn't be logged in a production cluster as that can get rather
            // unreadable with real-life segments.
            int LINES = 6;
            List<List<String>> lines = new ArrayList<>(LINES);
            for (int i = 0; i < LINES; i++)
                lines.add(new ArrayList<>(size));

            for (int i = 0; i < size; i++)
            {
                lines.get(0).add(segments.get(i).range.toString());
                lines.get(1).add(Long.toString(lastValidations[i]));
                lines.get(2).add(Long.toString(lastSuccessfulValidations[i]));
                lines.get(3).add(Integer.toString(localLocks[i]));
                lines.get(4).add(Boolean.toString(remoteLocks.get(i)));
                lines.get(5).add(Long.toString(priority(i)));
            }
            int[] widths = new int[size];
            for (int i = 0; i < size; i++)
            {
                for (int l = 0; l < LINES; l++)
                    widths[i] = Math.max(widths[i], lines.get(l).get(i).length());
            }

            StringBuilder sb = new StringBuilder();
            sb.append("Version = ").append(version).append(", next to validate = ").append(nextIdx).append('\n');
            sb.append("        ");
            for (int i = 0; i < size; i++)
                sb.append(" | ").append(pad(lines.get(0).get(i), widths[i]));
            sb.append("\nlast    ");
            for (int i = 0; i < size; i++)
                sb.append(" | ").append(pad(lines.get(1).get(i), widths[i]));
            sb.append("\nlastSucc");
            for (int i = 0; i < size; i++)
                sb.append(" | ").append(pad(lines.get(2).get(i), widths[i]));
            sb.append("\nL. locks");
            for (int i = 0; i < size; i++)
                sb.append(" | ").append(pad(lines.get(3).get(i), widths[i]));
            sb.append("\nR. lock ");
            for (int i = 0; i < size; i++)
                sb.append(" | ").append(pad(lines.get(4).get(i), widths[i]));
            sb.append("\npriority");
            for (int i = 0; i < size; i++)
                sb.append(" | ").append(pad(lines.get(5).get(i), widths[i]));
            return sb.toString();
        }

        private String pad(String val, int width)
        {
            int spaces = width - val.length();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < spaces; i++)
                sb.append(' ');
            sb.append(val);
            return sb.toString();
        }
    }

    /**
     * A reference to a particular segment and it's in-memory state, allowing to update such state (or get updates to
     * such state) during the lifecycle of a validation.
     * <p>
     * Note that such ref can get "invalidated" if either local ranges changes, or if the depth used is lowered. The
     * reason being that in both cases, the segment validated likely won't be part of the in-memory state anymore, or
     * at least not in such a way that we can update that state, and we'd rather drop concurrently running validations
     * when those very rare event happen. The whole point of having relatively small segments is that cancelling
     * a segment validation is not a big deal if it is rare.
     */
    static class Ref
    {
        /**
         * Status as returned by the {@link #checkStatus()} method. The state is either up-to-date or it is not, but for
         * logging purposes, we include a few other information when it is not up to date.
         */
        static class Status
        {
            private final boolean upToDate;
            private final Ref ref;

            /** Only present if not up to date because locked remotely. */
            @Nullable
            private final InetAddress lockedBy;

            private Status(boolean upToDate, Ref ref, InetAddress lockedBy)
            {
                this.upToDate = upToDate;
                this.ref = ref;
                this.lockedBy = lockedBy;
            }

            boolean isUpToDate()
            {
                return upToDate;
            }

            boolean isRemotelyLocked()
            {
                return lockedBy != null;
            }

            private static Status upToDate(Ref ref)
            {
                return new Status(true, ref, null);
            }

            private static Status updated(Ref ref)
            {
                return new Status(false, ref, null);
            }

            private static Status locked(Ref ref, InetAddress lockedBy)
            {
                return new Status(false, ref, lockedBy);
            }

            public String toString()
            {
                if (upToDate)
                    return "up to date";

                if (lockedBy != null)
                    return "segment locked by " + lockedBy;

                String reason;
                SegmentState currentState = ref.currentState();
                if (currentState == null)
                {
                    reason = "the topology or depth has changed";
                }
                else
                {
                    if (ref.segmentAtCreation.lastValidationTimeMs() != currentState.lastValidationTimeMs())
                    {
                        reason = String.format("was recently validated by another node (%s, previously know: %s)",
                                               NodeSyncHelpers.sinceStr(currentState.lastValidationTimeMs()),
                                               NodeSyncHelpers.sinceStr(ref.segmentAtCreation.lastValidationTimeMs()));
                    }
                    else
                    {
                        // The segment wasn't validated since the proposal was created but it's not the one with the
                        // most priority anymore: some concurrent validation came up (probably unsuccessfully) and
                        // took the lead.
                        reason = String.format("segment %s has now higher priority",
                                               ref.tableState.nextSegmentToValidate().segment());
                    }
                }
                return String.format("state updated: %s", reason);
            }
        }

        private final TableState tableState;
        private final SegmentState segmentAtCreation;

        private final Version versionAtCreation;
        private final int indexAtCreation;

        private Ref(TableState tableState, int indexAtCreation)
        {
            this.tableState = tableState;
            this.segmentAtCreation = tableState.stateHolder.immutableSegmentState(indexAtCreation);
            this.versionAtCreation = tableState.version.copy();
            this.indexAtCreation = indexAtCreation;
        }

        NodeSyncService service()
        {
            return tableState.service;
        }

        Segment segment()
        {
            return segmentAtCreation.segment();
        }

        SegmentState segmentStateAtCreation()
        {
            return segmentAtCreation;
        }

        /**
         * Whether this reference is still valid.
         */
        boolean isInvalidated()
        {
            return versionAtCreation.major != tableState.version.major;
        }

        /**
         * The current (at the time of this call) state of the segment this is a reference to, or {@code null} if the
         * ref has been invalidated.
         */
        SegmentState currentState()
        {
            tableState.lock.readLock().lock();
            try
            {
                if (isInvalidated())
                    return null;

                return tableState.stateHolder.immutableSegmentState(indexAtCreation);
            }
            finally
            {
                tableState.lock.readLock().unlock();
            }
        }

        /**
         * Lock (locally) the segment in the in-memory state.
         */
        void lock()
        {
            doUpdate(StateHolder::lockLocally);
        }

        /**
         * Refresh the (local) lock on this segment in the in-memory state.
         */
        void refreshLock()
        {
            doUpdate(StateHolder::refreshLocalLock);
        }

        /**
         * Indicates that a validation on this segment has been completed, updating the in-memory state accordingly.
         *
         * @param validationTime the time at which the validation has started.
         * @param wasSuccessful whether the validation was fully successful.
         */
        void onCompletedValidation(long validationTime, boolean wasSuccessful)
        {
            long lastSuccess = wasSuccessful ? validationTime : NodeSyncHelpers.NO_VALIDATION_TIME;
            doUpdate((s, i) -> s.updateCompletedValidation(i, validationTime, lastSuccess));
        }

        /**
         * Release the local lock on this segment from the in-memory state.
         */
        void forceUnlock()
        {
            doUpdate(StateHolder::forceLocalUnlock);
        }

        /**
         * Check whether the state of this segment has changed since the ref was created.
         * This *will* read the status table and thus ensure that we notice potential validations of this segment by
         * remote replicas. It also mean this method is blocking (and not exactly free).
         */
        Status checkStatus()
        {
            // Do a quick check to see if the state has changed in memory before bothering with reading the status
            // table.
            if (!versionAtCreation.equals(tableState.version))
                return Status.updated(this);

            List<NodeSyncRecord> records = tableState.statusTable().nodeSyncRecords(segment());
            NodeSyncRecord consolidated = NodeSyncRecord.consolidate(segment(), records);
            tableState.lock.writeLock().lock();
            try
            {
                if (!isInvalidated())
                    tableState.stateHolder.update(indexAtCreation, consolidated);

                // Note that it's possible that a remotely locked segment is still the one
                // with the highest priority and so that the version hasn't changed. And we
                // could decide to double-validate the segment in that case, it wouldn't exactly
                // be wrong or break things, but it's waste of resources and likely unexpected,
                // so we don't.
                if (isRemotelyLocked(consolidated))
                    return Status.locked(this, consolidated.lockedBy);

                return versionAtCreation.equals(tableState.version)
                       ? Status.upToDate(this)
                       : Status.updated(this);
            }
            finally
            {
                tableState.lock.writeLock().unlock();
            }
        }

        private void doUpdate(SegmentUpdater updater)
        {
            tableState.lock.writeLock().lock();
            try
            {
                if (isInvalidated())
                    return;

                StateHolder holder = tableState.stateHolder;
                if (tableState.version.minor == versionAtCreation.minor)
                {
                    // If the state is still the one on which we created the original proposal, we have the index of that
                    // segment directly and update is constant time.
                    updater.update(holder, indexAtCreation);
                }
                else
                {
                    // If the state has changed however, we have to find all segment in the new state that correspond
                    // to the segment that got validated. Note that the only case where the state change without
                    // invalidating the ref currently is when the depth increase. In that case, we'll the segment we
                    // just validated just happen to cover 2 segments (or 4, 8, ... if the depth augmented by more than
                    // 1 but that's very very unlikely) and we'll simply update those 2 segments.
                    int[] r = holder.segments.findFullyIncludedIn(segment());
                    for (int i = r[0]; i < r[1]; i++)
                        updater.update(holder, i);
                }
            }
            finally
            {
                tableState.lock.writeLock().unlock();
            }
        }

        /** Interface that only exists for the sake of {@link #doUpdate}. **/
        interface SegmentUpdater
        {
            void update(StateHolder stateHolder, int i);
        }
    }

    /**
     * Track "meaningful" updates made to the state.
     * <p>
     * This concretely track 3 numbers, major, minor and priority that are updated in the following way and with the
     * following meaning:
     * <ul>
     * <li>major: incremented on updates to the state that invalidate any in-flight validations (any {@link Ref} object
     * created before the update). In practice, corresponds to change of local range or depth decrease.</li>
     * <li>minor: incremented when the underlying state-holder changes (the segments considered have changed), but
     * without invalidating in-flight validations. This happen on depth increase, where we can relatively easily
     * avoid the invalidation.</li>
     * <li>priority: updated every time the state changes in such a way that this modify which segment has the lowest
     * priority value (the one that should be validated next).</li>
     * </ul>
     * <p>
     * Note that this class is mutable and so updates to minor/major should be done only while holding the
     * {@link TableState} write lock.
     */
    private static class Version
    {
        volatile long major;
        volatile long minor;
        volatile long priority;

        private Version()
        {
            this(0, 0, 0);
        }

        private Version(long major, long minor, long priority)
        {
            this.major = major;
            this.minor = minor;
            this.priority = priority;
        }

        Version copy()
        {
            return new Version(major, minor, priority);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof Version)
            {
                Version other = (Version) obj;
                return this.major == other.major
                       && this.minor == other.minor
                       && this.priority == other.priority;
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(major, minor, priority);
        }

        @Override
        public String toString()
        {
            return String.format("[%d,%d,%d]", major, minor, priority);
        }
    }
}
