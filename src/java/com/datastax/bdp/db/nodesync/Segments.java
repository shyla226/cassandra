/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Represents a list of generated segments for a given table (sorted by left token).
 * <p>
 * By and large this class behaves like a {@code List<Segment>} where all segments are for the same table, with methods
 * to generate such list. However, we keep such segment list in memory for all NodeSync-enabled table but really only
 * work on a handful segments for each table at any given time, so this class optimizes for memory usage on that
 * assumption.
 * <p>
 * Segments are generated for a given table {@code t} and on the local node in the following way:
 * 1) We compute the "depth" {@code d} of {@code t} (see {@link #depth): that depth is directly related to the size of the data
 *    contained in {@code t} and is computed in such a way that the segments generated in the next step don't cover
 *    more than {@link NodeSyncService#SEGMENT_SIZE_TARGET } of data.
 * 2) We then take each local ranges for {@code t} and recursively split said range in half {@code d} times.
 * 3) If one of the range generated in the previous step is wrapping, we split it in two at the minimum token to get 2
 *    non-wrapping ranges.
 * 4) The resulting ranges are the segments for {@code t} as computed by the local node.
 * <p>
 * Some of the direct consequences of this generation are that:
 * - A given node only validates his local ranges, the one for which he is a replica. This is mostly done this way
 *   because this is the simplest way to distribute NodeSync work amongst all nodes[1] while having a natural amount
 *   of replication for failure tolerance. It happens to also be a small optimization as the local node is always a
 *   replica for any read performed by NodeSync, saving us cross-node traffic (and we use digests read, so the initial
 *   data-read ends up being local 99% of the time).
 * - Segments generation is done locally without any synchronization. However, the only information truly local used is
 *   the table depth which is based on the table local data size. And given how the depth is computed and used, we can
 *   expect that in most cases every node will compute the same depth for the same table[2]. In other words, in the
 *   majority of cases, replicas of a given range will use the exact same segment, and the code optimizes on that
 *   assumption (see {@link NodeSyncRecord#consolidate} for instance, but the use of the
 *   {@link SystemDistributedKeyspace#NodeSyncStatus} table is implicitly simplied when segments are the same on all
 *   nodes in general). The code is however always tolerant to this assumption being violated.
 * - Related to the previous point, as segment generation depends on local ranges, it means that the segments a node
 *   consider will change on topology changes (that is, in cases where the node wins or loses local ranges).
 * <p>
 * In general, the tl;dr is that segments are fixed neither over time (for the same node) nor across nodes and the
 * code needs to cope with that fact. But the code is still optimized for segment not changing as this the case the
 * vast majority of time.
 *
 * [1]: little in the actual validation code ({@link Validator}) actually assumes that the range validated are local,
 * and it would be possible to not base segment generation on local ranges, but rather to split the whole ring in a
 * predefinite number of segment, with no regard for actual tokens, on have nodes consider those. In fact, this would
 * have the advantage over the current method that this means segments wouldn't change across topology changes. This
 * would however open the question of if and how we distribute segments amongst nodes. If we don't, all nodes would
 * handles the whole ring for all tables (exclusively relying on the system distributed table to avoid duplicated work),
 * but that wouldn't scale well: classes like {@link ContinuousValidationProposer } would either use increasing
 * amount of memories when the cluster grows, or would become a lot more complex. If we do try to distribute segments,
 * it's not trivial to do so in a way that guarantees failure tolerance, is fair and never forget any range even with
 * topology changes. Using local ranges solves all those problem "out of the box" (though, again, with the downside that
 * segments are impacted by topology changes).
 * [2]: assuming decently well distributed data.
 */
class Segments
{
    // As said in the javadoc, we don't keep this as a List<Segment> because as that's memory heavy. Instead, we keep
    // a single pointer to the table (since all segments belong to it) and encode the list of ranges represented as a
    // list of tokens, where the ith range is stored by (ranges[i*2], ranges[i*2 + 1]].
    // This does mean we have to re-create a Segment object every time we access one, but as we only access a few of
    // them at a time (and not that rapidly overall), this feel like a good trade-off (though, to be fair, that intuition
    // would need to ultimately be validated properly (and we could get even more aggressive memory wise as well with
    // more efforts)).

    private final TableMetadata table;
    // The parameters (local ranges and depth) from which those segments have been created.
    private final Collection<Range<Token>> localRanges;
    private final int depth;

    private final int size; // Always ranges.size() / 2, but it's convenient to have it separate.
    private final List<Token> ranges; // The list of range, sorted by left token

    // This class represents non-wrapped, non-overlapping, sorted ranges, and does so using a list of these ranges
    // boundaries. So this is basically a sorted list of tokens. Except that when the original local ranges we split
    // to generate this had a wrapping one, the "last" range here (in sorted order) will have the minimum token as
    // right bound, and that would make the list not totally sorted, and that makes it a pain to work with. To avoid
    // that, we use a trick within this class: we replace the min token when basically used as max token by a special
    // token instance that is made maximal by the comparison method used (see the generate() method for how those are
    // created). We do replace that "fake maximum token" when returning ranges from this class so this stay private to
    // this class.
    // Of course, all this is a hack and could be ideally much cleaner if we were to rework how we deal with
    // wrapping/non-wrapping token ranges in general (this is not the only place where this is annoying, the code is
    // riddled with case where having the min token on the right side of a range requires special casing), but such
    // change is out of scope here.
    private final Token maxToken;
    private final Comparator<Token> comparator;

    private final List<Range<Token>> normalizedLocalRanges;

    private Segments(TableMetadata table,
                     Collection<Range<Token>> localRanges,
                     int depth,
                     List<Token> ranges,
                     Token maxToken,
                     Comparator<Token> comparator)
    {
        assert ranges.size() % 2 == 0 : ranges;
        this.table = table;
        this.localRanges = localRanges;
        this.depth = depth;
        this.size = ranges.size() / 2;
        this.ranges = ranges;

        this.maxToken = maxToken;
        this.comparator = comparator;

        this.normalizedLocalRanges = Range.normalize(localRanges);
    }

    /**
     * The local ranges that were used to generate those segments.
     */
    Collection<Range<Token>> localRanges()
    {
        return localRanges;
    }

    List<Range<Token>> normalizedLocalRanges()
    {
        return normalizedLocalRanges;
    }

    /**
     * The depth that was used to generate those segments.
     */
    int depth()
    {
        return depth;
    }

    int size()
    {
        return size;
    }

    Segment get(int i)
    {
        int rangeIdx = 2 * i;
        Token left = ranges.get(rangeIdx);
        Token right = ranges.get(rangeIdx + 1);
        if (right == maxToken)
            right = table.partitioner.getMinimumToken();
        return new Segment(table, new Range<>(left, right));
    }

    /**
     * Returns the index range covering the segments of this object that are fully included in the provided one.
     *
     * @param segment the segment.
     * @return a 2 element array {@code a} such that all segments of this object between {@code a[0]} (included) and
     * {@code a[1]} (excluded) are fully included in {@code segment}. If there is no such segment, the returned array
     * will simply be such that {@code a[0] >= a[1]}.
     */
    int[] findFullyIncludedIn(Segment segment)
    {
        // Compute startIdx (resp. endIdx) to be the start of the first (resp. last) range of which we should return the
        // index.

        int startIdx = Collections.binarySearch(ranges, segment.range.left, comparator);
        if (startIdx < 0)
            startIdx = -startIdx-1;

        // if the found idx is a start bound, then it already point to the first range to include (whether it was an
        // exact match or not, whatever range comes before wasn't fully included). If it's an end bound, it means
        // that's the end of the first range not fully covered, and we move it to the start of the first one that is.
        if (!isStartBound(startIdx))
            ++startIdx;

        if (startIdx >= ranges.size())
            return new int[]{0, 0};

        int endIdx = ranges.size();
        if (!segment.range.right.isMinimum())
        {
            endIdx = Collections.binarySearch(ranges.subList(startIdx, ranges.size()), segment.range.right, comparator);
            boolean isExactMatch = endIdx >= 0;
            if (!isExactMatch)
                endIdx = -endIdx - 1;
            // We started our search from startIdx, so make it a true index
            endIdx = startIdx + endIdx;

            // if the idx found is a start bound, then it already point to the first range to exclude (if it wasn't an exact
            // match, it still means that any previous range end before that insertion point and thus is fully included).
            // If its a end bound, then it depends on whether it's an exact match or not. If it is, then the range it points
            // to is fully included (and so we want to point to the start of the next one). Otherwise, it isn't and it is the
            // end of the first range not to include (and so we move the point ot its start for consistency).
            if (!isStartBound(endIdx))
                endIdx = isExactMatch ? endIdx + 1 : endIdx - 1;
        }
        return new int[]{ startIdx / 2, endIdx / 2};
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(table).append('{');
        for (int i = 0; i < size; i++)
            sb.append(i == 0 ? "" : ", ").append(get(i).range);
        return sb.append('}').toString();
    }

    private boolean isStartBound(int idx)
    {
        return idx % 2 == 0;
    }

    /**
     * Computes the depth at which each local ranges should be split for a given table so that each segment covers data
     * that is (assuming perfect distribution) no bigger than {@link NodeSyncHelpers#segmentSizeTarget()} (of course, in
     * practice, distribution is never perfect so there is no guarantee that no segment will ever cover more data than
     * {@link NodeSyncHelpers#segmentSizeTarget()}, but this is good enough for NodeSync).
     *
     * @param table the table for which to compute the depth.
     * @param localRangesCount the number of local ranges to consider for the computation.
     * @return the depth for splitting {@code localRangesCount} local ranges for {@code table} to respect our segment
     * size target.
     */
    static int depth(ColumnFamilyStore table, int localRangesCount)
    {
        return depth(NodeSyncHelpers.estimatedSizeOf(table), localRangesCount, NodeSyncHelpers.segmentSizeTarget());
    }

    /**
     * Computes the depth at which each local ranges should be split for a given table so that each segment covers data
     * that is (assuming perfect distribution) no bigger than the provided size (of course, in practice, distribution
     * is never perfect so there is no guarantee that no segment will ever cover more data than
     * {@code maxSegmentsSizeInBytes}, but this is good enough for NodeSync).
     *
     * @param estimatedTableSizeInBytes the (estimated) size of the table for which to compute the depth.
     * @param localRangesCount the number of local ranges to consider for the computation.
     * @param maxSegmentSizeInBytes the maximum size we want to a segment.
     * @return the depth at which to compute each local range for {@code table} to respect our {@code maxSegmentSizeInBytes}
     * limit, assuming there is {@code localRangesCount} local ranges.
     */
    @VisibleForTesting
    static int depth(long estimatedTableSizeInBytes, int localRangesCount, long maxSegmentSizeInBytes)
    {
        long rangeSize = estimatedTableSizeInBytes / localRangesCount;
        int depth = 0;
        while (rangeSize > maxSegmentSizeInBytes)
        {
            ++depth;
            rangeSize /= 2;
        }
        return depth;
    }

    /**
     * Returns the segments for a table given the local ranges and the depth to use.
     * <p>
     * This method basically applies {@link #splitRangeAndUnwrap} to each local range so also see the javadoc of that
     * method for more details.
     *
     * @param table the table for which to generate the segments.
     * @param localRanges the local ranges for {@code table}.
     * @param depth the depth to use to generate the segments (generally computed by {@link #depth}, see class javadoc).
     * @return the list of ranges to consider as segments for {@code table}.
     */
    static Segments generate(TableMetadata table, Collection<Range<Token>> localRanges, int depth)
    {
        // See the comment on the maxToken field of Segments above for what this is about. Note that the reason
        // we don't use IPartitioner.getMaximumToken(), which would seem much more appropriate, is that not all
        // partitioner implement that method (the default implementation throws). The comparator ensure that token
        // is maximal for the comparison though, no matter what it is in practice. Of course, this hack rely on
        // tokens instance not be "interned", but that's not the case and it would kind of crazy to start interning
        // them so it's a reasonably safe assumption (also, we should fix that mess at a higher level, so this is
        // meant to be a temporary hack).
        // Side-note: the actual value of maxToken matter since we only rely on reference equality on it, but we pick
        // a deterministic value (for the partitioner) to make things easier to debug/test if necessary.
        Token maxToken = table.partitioner.midpoint(table.partitioner.getMinimumToken(), table.partitioner.getMinimumToken());
        Comparator<Token> comparator = (t1, t2) -> {
            if (t1 == maxToken)
                return t2 == maxToken ? 0 : 1;
            if (t2 == maxToken)
                return -1;
            return t1.compareTo(t2);
        };

        List<Token> ranges = new ArrayList<>(estimateSegments(localRanges, depth) * 2);

        for (Range<Token> localRange : localRanges)
            splitRangeAndUnwrap(localRange, depth, table.partitioner, ranges, maxToken);

        // Note: we want the range sorted and they currently are not (because local ranges are not; they could have one
        // wrapping in particular). The generated ranges are however non-overlapping and non wrapped, so sorting the
        // tokens will basically preserve the ranges.
        ranges.sort(comparator);
        return new Segments(table, localRanges, depth, ranges, maxToken, comparator);
    }

    /**
     * Generate segments with updated table metadata
     *
     * @param segments existing segments
     * @param table new table metadata
     * @return
     */
    static Segments updateTable(Segments segments, TableMetadata table )
    {
        return new Segments(table, segments.localRanges, segments.depth, segments.ranges, segments.maxToken, segments.comparator);
    }

    /**
     * The (estimated) number of segments the iterator from {@link #generate} will generate.
     * <p>
     * It's called an estimate because there is theoretically rare cases where ranges may not be splittable beyond
     * a certain depth and so we keep the contract of the method conservative to avoid surprises (we don't use this
     * method in places where we couldn't suffer imprecision). In practice, it will likely always be exactly the number
     * of segments generated.
     *
     * @param depth the depth we'll use to split ranges.
     * @param localRanges the ranges to validate. It must not be empty.
     * @return an estimate of the number of segments obtained when splitting {@code localRanges} at depth {@code depth}.
     */
    @VisibleForTesting
    static int estimateSegments(Collection<Range<Token>> localRanges, int depth)
    {
        assert !localRanges.isEmpty();
        // In general, for every depth, we split each range in half...
        int segments = localRanges.size();
        for (int i = 0; i < depth; i++)
            segments *= 2;

        // ... but we'll also unwrap a ranged wrap
        for (Range<Token> range : localRanges)
        {
            if (range.isTrulyWrapAround())
                segments++;
        }
        return segments;
    }

    /**
     * Returns an iterator producing the result of 1) recursively splitting the provided range in half the provided
     * number of times and 2) unwrapping the resulting range that would wrap around.
     *
     * @param toSplit the range to split.
     * @param depth the number of time to split recursively. So if {@code depth == 1}, the method will return 2 ranges
     *              corresponding to the 2 halves of {@code toSplit}, if {@code depth == 2}, it will return 4 ranges, etc.
     *              This cannot be a negative number.
     * @param partitioner the partitioner for the range to split (necessary to do the actual splitting).
     * @param output the list of to which to add the generate ranges to. For each range generate, the start and end
     *               token are added to the list in order.
     * @param maxToken the token to use if we split a wrapping range to represent the maximum token. See the comment
     *                 in {@link #generate} for details.
     */
    private static void splitRangeAndUnwrap(Range<Token> toSplit, int depth, IPartitioner partitioner, List<Token> output, Token maxToken)
    {
        assert depth >= 0 : "Invalid depth " + depth;
        if (depth == 0)
        {
            // We have nothing to do for depth 0, except for unwrapping the range if it's wrapped.
            maybeUnwrap(toSplit, output, maxToken);
            return;
        }

        // We special case depth 1 because it's likely somewhat common (along with depth=0). For instance, with 256
        // vnodes, each node stores 256 * RF ranges for every table, so typically 768 (RF=3). Depth == 1 will handle
        // ranges up to 400MB (with the default SEGMENT_SIZE_TARGET of 200MB), which handles 768*400 ~= 300GB.
        // And while node with > 300GB may not be that rare, this is for a single table, so this likely cover many cases.
        // We course, we still handle higher depth properly below, but worth a simple optimization.
        if (depth == 1)
        {
            Token midpoint = midpoint(toSplit.left, toSplit.right, partitioner);
            if (midpoint == null)
            {
                maybeUnwrap(toSplit, output, maxToken);
            }
            else
            {
                maybeUnwrap(toSplit.left, midpoint, output, maxToken);
                maybeUnwrap(midpoint, toSplit.right, output, maxToken);
            }
            return;
        }

        // At worth, we'll have 1 range waiting to be processed for each depth + 1 that we put temporarily to make the
        // loop easier.
        final Deque<RangeWithDepth> queue = new ArrayDeque<>(depth + 1);
        queue.offerFirst(withDepth(toSplit.left, toSplit.right, 0));
        while (!queue.isEmpty())
        {
            RangeWithDepth range = queue.poll();
            assert range.depth < depth;

            Token midpoint = midpoint(range.left, range.right, partitioner);
            // Can't split further, so be it
            if (midpoint == null)
            {
                maybeUnwrap(range.left, range.right, output, maxToken);
                continue;
            }

            int nextDepth = range.depth + 1;
            if (nextDepth == depth)
            {
                maybeUnwrap(range.left, midpoint, output, maxToken);
                maybeUnwrap(midpoint, range.right, output, maxToken);
            }
            else
            {
                // otherwise, let left be split some more
                queue.offerFirst(withDepth(midpoint, range.right, nextDepth));
                queue.offerFirst(withDepth(range.left, midpoint, nextDepth));
            }
        }
    }

    private static void maybeUnwrap(Range<Token> range, List<Token> output, Token maxToken)
    {
        maybeUnwrap(range.left, range.right, output, maxToken);
    }

    private static void maybeUnwrap(Token left, Token right, List<Token> output, Token maxToken)
    {
        if (Range.isTrulyWrapAround(left, right))
        {
            add(left, maxToken, output);
            add(left.minValue(), right, output);
        }
        else
        {
            // We basically don't want to add the min token to our output but instead replace it by maxToken. But even
            // if the input range doesn't wrap, it may be using the min token as end bound, so replace it now.
            add(left, right.isMinimum() ? maxToken : right, output);
        }
    }

    private static void add(Token left, Token right, List<Token> output)
    {
        output.add(left);
        output.add(right);
    }

    private static Token midpoint(Token left, Token right, IPartitioner partitioner)
    {
        Token midpoint = partitioner.midpoint(left, right);
        // It's theoretically possible the range is small enough to not be splittable. This shouldn't really happen as
        // local ranges shouldn't be *that* small and we don't split unless a range covers more than SEGMENT_SIZE_TARGET,
        // but let's not have a hard to reproduce bug just-in-case.
        return midpoint.equals(left) || midpoint.equals(right) ? null : midpoint;
    }

    private static RangeWithDepth withDepth(Token left, Token right, int depth)
    {
        return new RangeWithDepth(left, right, depth);
    }

    // Minor auxiliary class for use by splitRangeAndUnwrap
    private static class RangeWithDepth
    {
        private final Token left;
        private final Token right;
        private final int depth;

        private RangeWithDepth(Token left, Token right, int depth)
        {
            this.left = left;
            this.right = right;
            this.depth = depth;
        }
    }
}
