/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Information on the status of NodeSync for a particular segment (table + range).
 * <p>
 * This is the information that we store in the {@link SystemDistributedKeyspace#NODESYNC_STATUS} table and is
 * mainly used to know which segments needs to be prioritize based on their NodeSync history.
 */
public class NodeSyncRecord
{
    private static final Comparator<NodeSyncRecord> byRangeLeft = Comparator.comparing(r -> r.segment.range.left);

    /** The segment of which this is a record of. */
    final Segment segment;

    /**
     * Information on the last NodeSync validation done on this segment, or {@code null} if no such information is recorded.
     */
    @Nullable
    final ValidationInfo lastValidation;

    /**
     * Information on the last successful validation. Note that if the last validation was a successful one, this will be the
     * same object than {@link #lastValidation}. This will be {@code null} if no successful validation is recorded.
     */
    @Nullable
    final ValidationInfo lastSuccessfulValidation;

    /**
     * If a node is currently synchronizing that segment, the address of that node ({@code null} otherwise).
     * <p>
     * This information acts as a form of lock to avoid having 2 nodes validate/repair the same segment at the same time.
     * It is imperfect however as 1) we don't use Paxos to synchronize nodes on this so it's possible for 2 nodes to race
     * and start synchronizing the same range and 2) while a node will clean this up when it finish synchronizing a range,
     * it may die without doing so, in which case this (poor-man) "lock" simply expires after a few minutes.
     * <p>
     * That imperfection is fine in practice though: it's not incorrect to have 2 nodes synchronize the same range at the
     * same time (it's just a bit inefficient), so we're just striving for a simple and cheap way to avoid it "most of
     * the time".
     */
    @Nullable
    final InetAddress lockedBy;

    public NodeSyncRecord(Segment segment,
                          ValidationInfo lastValidation,
                          ValidationInfo lastSuccessfulValidation,
                          InetAddress lockedBy)
    {
        // Make sure we don't build something that don't conform to what we're documenting above.
        assert segment != null;
        assert lastSuccessfulValidation == null ||
               (lastValidation != null                                              // A successful validation _is_ a validation
                && lastSuccessfulValidation.outcome.wasSuccessful()                 // It's name successful for a reason
                && lastSuccessfulValidation.startedAt <= lastValidation.startedAt); // The last can't be older than the last successful

        this.segment = segment;
        this.lastValidation = lastValidation;
        this.lastSuccessfulValidation = lastSuccessfulValidation;
        this.lockedBy = lockedBy;
    }

    private NodeSyncRecord(Segment segment, ValidationInfo lastValidation)
    {
        this(segment, lastValidation, lastValidation.wasSuccessful() ? lastValidation : null, null);
    }

    /**
     * Empty record for the provided segment.
     */
    @VisibleForTesting
    static NodeSyncRecord empty(Segment segment)
    {
        return new NodeSyncRecord(segment, null, null, null);
    }

    long lastValidationTimeMs()
    {
        return lastValidation == null ? NodeSyncHelpers.NO_VALIDATION_TIME : lastValidation.startedAt;
    }

    long lastSuccessfulValidationTimeMs()
    {
        return lastSuccessfulValidation == null ? NodeSyncHelpers.NO_VALIDATION_TIME : lastSuccessfulValidation.startedAt;
    }

    boolean isLocked()
    {
        return lockedBy != null;
    }

    /**
     * Consolidate a list of records that cover a given segment.
     * <p>
     * For reasons explained in {@link Segment} and {@link SystemDistributedKeyspace#nodeSyncRecords}, while
     * most of the time for every segment we'll have a single record directly corresponding to that segment, there is
     * time where that won't be the case and we'll have multiple records covering that segment. That method makes it
     * easier to work with such a case by consolidating such a list of covering records into a single record that is
     * still correct (meaning, which don't pretend something that hasn't been synchronized has). This does mean that in
     * some case, this method loses some precision. However, this is best we can do without making things a lot more
     * complicated, and as looseness will only happen a fraction of the time, it's likely an acceptable trade-off.
     *
     * @param segment the segment for which to consolidate records.
     * @param coveringRecords the records covering {@code segment}, which <b>must</b> be in order of their segment range
     *                        start token (range can overlap however) and must be records for the table of {@code segment}.
     *                        Note that in most case this will either be empty (we have no record for that segment yet)
     *                        or contain a single record corresponding to {@code segment} exactly and the method optimize
     *                        for this. The method is resilient to the list containing records that don't intersect with
     *                        {@code segment.range} (and that is currently relied on during startup, see
     *                        {@link TableState#load}) but those are simply ignored.
     * @return a record for {@code segment} that consolidate/combine the information of {@code coveringRecords}. If there
     * is no record to consolidate, the returned record will simply have all its fields to {@code null}.
     */
    static NodeSyncRecord consolidate(Segment segment, List<NodeSyncRecord> coveringRecords)
    {
        if (coveringRecords.isEmpty())
            return empty(segment);

        Range<Token> range = segment.range;
        if (coveringRecords.size() == 1)
        {
            // A single record. Either it cover segment entirely (or fully cover it) and we return the record directly,
            // or it doesn't and we can't use it.
            NodeSyncRecord record = coveringRecords.get(0);
            assert record.segment.table.equals(segment.table);
            if (record.segment.equals(segment))
                return record;
            if (record.segment.range.contains(range))
                return new NodeSyncRecord(segment, record.lastValidation, record.lastSuccessfulValidation, record.lockedBy);
            else
                return empty(segment);
        }

        // We have more than one record so we need to truly consolidate.
        ValidationInfo lastSuccessfulValidation = consolidateValidations(range,
                                                                         Iterables.filter(coveringRecords, r -> r != null && r.lastSuccessfulValidation != null),
                                                                         coveringRecords.size(),
                                                                r -> r.lastSuccessfulValidation);
        ValidationInfo lastValidation = consolidateValidations(range,
                                                               Iterables.filter(coveringRecords, r -> r != null && r.lastValidation != null),
                                                               coveringRecords.size(),
                                                      r -> r.lastValidation);
        InetAddress lockedBy = consolidateLockedBy(range, coveringRecords);

        return new NodeSyncRecord(segment, lastValidation, lastSuccessfulValidation, lockedBy);
    }

    /**
     * Consolidate the validations (either the last ones or the last successful ones depending on {@code getter}) for
     * a given segment ({@code range}) given the records covering that segment.
     * <p>
     * The rules we use are that:
     *   1. if any sub-part of {@code range} is not covered by a record, we return null as we can't assume anything.
     *   2. if any sub-part of {@code range} is covered by multiple recorded validation, we use the most recent of
     *      those validation.
     *   3. for different (non-intersecting) sub-parts of {@code range}, we combine the different validations using
     *      {@link ValidationInfo#composeWith}, which picks the oldest start time and worst outcome we have (it's the
     *      best we can do).
     */
    @VisibleForTesting
    static ValidationInfo consolidateValidations(Range<Token> range,
                                                 Iterable<NodeSyncRecord> coveringRecords,
                                                 int maxRecords,
                                                 Function<NodeSyncRecord, ValidationInfo> getter)
    {
        // To make this a bit simpler, we proceed in 2 phases. In the first one, we basically deal with rule 1. and 2.
        // above, building list of ValidationInfo that correspond to strictly successive sub-ranges covering 'range'.
        // In other words, if the records we have are for {(0, 10], (5, 20], (20, 28], (25, 30]}, then we're build the
        // list of validation info that correspond to the ranges: {(0, 5](5, 10](10, 20](20, 25](25, 30]}, and in doing
        // so we'll use rule 2. to "merge" the info of intersecting parts (and if there is any "hole", we'll short-cut
        // the method to return null, enforcing rule 1.).
        // Once we have this list, we simply merge all resulting info using rule 3. above to get our final output.

        // Step 1:

        // We'll want to iterate over the record in order, but to make things simpler we'll needs to re-add some records
        // during iteration, so a priority queue is a simple option.
        PriorityQueue<NodeSyncRecord> records = new PriorityQueue<>(maxRecords, byRangeLeft);
        Iterables.addAll(records, coveringRecords);

        // Skips any segment that ends before our interested range
        while (!records.isEmpty() && compareLeftRight(range.left, records.peek().segment.range.right) >= 0)
            records.poll();

        if (records.isEmpty())
            return null;

        List<ValidationInfo> step1Output = new ArrayList<>();
        NodeSyncRecord first = records.poll();
        // The first record is not entirely on the left of our segment. But if it starts after our segment start, we're
        // done as it means some part of the range is not covered by any record.
        if (compareLeftLeft(first.segment.range.left, range.left) > 0)
            return null;

        TableMetadata table = first.segment.table;
        Token currLeft = first.segment.range.left;
        Token currRight = first.segment.range.right;
        ValidationInfo currInfo = getter.apply(first);

        while (!records.isEmpty() && compareLeftRight(currLeft, range.right) < 0)
        {
            NodeSyncRecord nextRecord = records.poll();
            Token nextLeft = nextRecord.segment.range.left;
            Token nextRight = nextRecord.segment.range.right;
            ValidationInfo nextInfo = getter.apply(nextRecord);

            // Enforces rule 1. of the javadoc: if the next record starts strictly after what we've seen so far, we have
            // a "hole"
            int leftRightCmp = compareLeftRight(nextLeft, currRight);
            if (leftRightCmp > 0)
                return null;

            if (leftRightCmp == 0)
            {
                // The next range starts exactly where the current one ends. We can add the current one to the output
                // and move on with the next one.
                step1Output.add(currInfo);
                currInfo = nextInfo;
                currLeft = nextLeft;
                currRight = nextRight;
            }
            else
            {
                // The next record intersects with our current one for some part. We can add to the output the info
                // corresponding to _before_ the intersection (if there is any).
                if (compareLeftLeft(currLeft, nextLeft) < 0)
                    step1Output.add(currInfo);

                // Then we're going to update the current info, left and right to reflect the intersection of the current
                // and next record. But said intersection only goes up to whichever record stops first, so we should make
                // sure to push the handling of what's remain to later.
                int rightRightCmp = compareRightRight(nextRight, currRight);
                if (rightRightCmp < 0)
                {
                    // The next record ends before the current one. Push the rest of the current one afterwards.
                    records.add(new NodeSyncRecord(new Segment(table, new Range<>(nextRight, currRight)), currInfo));
                }
                else if (rightRightCmp > 0)
                {
                    // The next record ends after the current one. Push the rest of the next one afterwards.
                    records.add(new NodeSyncRecord(new Segment(table, new Range<>(currRight, nextRight)), nextInfo));
                }

                // now do update for the new intersection using rule 2. above
                if (nextInfo.startedAt > currInfo.startedAt)
                    currInfo = nextInfo;
                currLeft = nextLeft;
                currRight = rightRightCmp < 0 ? nextRight : currRight;
            }
        }
        // Unless we exited the loop because we where past our range of interest, we should deal with what's outstanding
        if (compareRightRight(currRight, range.right) < 0)
            return null;
        if (compareLeftRight(currLeft, range.right) < 0)
            step1Output.add(currInfo);

        // Step 2:
        Optional<ValidationInfo> reduced = step1Output.stream().reduce(ValidationInfo::composeWith);
        assert reduced.isPresent() : "The output of step1 shouldn't have been empty, got " + step1Output;
        return reduced.get();
    }

    /**
     * Consolidate locked by information on a segment ({@code range}) given the records covering that segment.
     * <p>
     * The rule we use is that we can considered a segment locked only if all the segment is covered by records that
     * are locked, as locking really mean "someone else is currently repairing that segment". Even when that's the case,
     * all record will likely not be locked by the same node, in which case we pick any of the IP for the resulting
     * lockedBy. This is slightly imprecise but is good enough in term of knowing if any other node is currently
     * validating the range (in other words, the result of this method is for internal usage and we don't internally
     * use the exact IP for anything, we only care about 'blockedBy' being set or not, the details is for external
     * monitoring).
     */
    @VisibleForTesting
    static InetAddress consolidateLockedBy(Range<Token> range, List<NodeSyncRecord> coveringRecords)
    {
        int i = 0;

        // Skip records that are fully before our range of interest
        while (compareLeftRight(range.left, coveringRecords.get(i).segment.range.right) >= 0)
            if (++i == coveringRecords.size()) return null; //all ranges before range of interest

        // If the first record that ends after our range start is strictly after said start, it means at least some part
        // of the range is not covered by a record and is thus not locked.
        if (compareLeftLeft(coveringRecords.get(i).segment.range.left, range.left) > 0)
            return null;

        InetAddress lockedBy = coveringRecords.get(i).lockedBy;
        Token lockedUpTo = coveringRecords.get(i).segment.range.right;
        while (++i < coveringRecords.size() && compareRightRight(lockedUpTo, range.right) < 0)
        {
            NodeSyncRecord record = coveringRecords.get(i);
            // If we have a gap with no records, we can lock the whole range
            if (compareLeftRight(record.segment.range.left, lockedUpTo) > 0)
                return null;

            // If the range is strictly comprised within range that we know are locked, we can ignore it.
            if (compareRightRight(record.segment.range.right, lockedUpTo) <= 0)
                continue;

            // Otherwise, extend lockedUpTo if the record is locked itself. If the record is not locked, we do nothing
            // because a latter record may cover those parts following lockedUpTo and if none does, we'll notice that
            // properly in a following iteration or at the end of the loop.
            if (record.lockedBy != null)
            {
                lockedBy = record.lockedBy;
                lockedUpTo = record.segment.range.right;
            }
        }

        return compareRightRight(lockedUpTo, range.right) < 0 ? null : lockedBy;
    }
    
    private static int compareLeftLeft(Token l1, Token l2)
    {
        return l1.compareTo(l2);
    }
    
    private static int compareLeftRight(Token l, Token r)
    {
        if (r.isMinimum())
            return -1;
        
        return l.compareTo(r);
    }
    
    private static int compareRightRight(Token r1, Token r2)
    {
        if (r1.isMinimum() && r2.isMinimum())
            return 0;
        if (r1.isMinimum())
            return 1;
        if (r2.isMinimum())
            return -1;
        
        return r1.compareTo(r2);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(segment, lastValidation, lastSuccessfulValidation, lockedBy);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof NodeSyncRecord))
            return false;

        NodeSyncRecord that = (NodeSyncRecord) o;
        return this.segment.equals(that.segment)
               && Objects.equals(this.lastValidation, that.lastValidation)
               && Objects.equals(this.lastSuccessfulValidation, that.lastSuccessfulValidation)
               && Objects.equals(this.lockedBy, that.lockedBy);
    }

    @Override
    public String toString()
    {
        // If the last validation wasn't successful and we have a last successful one, the string of that
        String lastSuccString = lastSuccessfulValidation == null || (lastValidation != null && lastValidation.outcome.wasSuccessful())
                                ? ""
                                : ", last success=" + lastSuccessfulValidation;
        String lockStr = lockedBy == null ? "" : ", locked by " + lockedBy;
        return String.format("%s (last validation=%s%s%s)",
                             segment,
                             lastValidation == null ? "<none>" : lastValidation.toString(),
                             lastSuccString,
                             lockStr);
    }
}
