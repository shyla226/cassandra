/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.nodesync;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Static helpers to generate and manipulate segments.
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
 *
 * [1]: little in the actual validation code ({@link Validator}) actually assumes that the range validated are local,
 * and it would be possible to not base segment generation on local ranges, but rather to split the whole ring in a
 * predefinite number of segment, with no regard for actual tokens, on have nodes consider those. In fact, this would
 * have the advantage over the current method that this means segments wouldn't change across topology changes. This
 * would however open the question of if and how we distribute segments amongst nodes. If we don't, all nodes would
 * handles the whole ring for all tables (exclusively relying on the system distributed table to avoid duplicated work),
 * but that wouldn't scale well: classes like {@link ContinuousTableValidationProposer} would either use increasing
 * amount of memories when the cluster grows, or would become a lot more complex. If we do try to distribute segments,
 * it's not trivial to do so in a way that guarantees failure tolerance, is fair and never forget any range even with
 * topology changes. Using local ranges solves all those problem "out of the box" (though, again, with the downside that
 * segments are impacted by topology changes).
 * [2]: assuming decently well distributed data.
 */
abstract class Segments
{
    private Segments()
    {}

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
     * Returns an iterator that produces all the segments for a table given the local ranges and the depth to use.
     * <p>
     * This method basically applies {@link #splitRangeAndUnwrap} to each local range so also see the javadoc of that
     * method for more details.
     *
     * @param table the table for which to generate the segments.
     * @param localRanges the local ranges for {@code table}.
     * @param depth the depth to use to generate the segments (generally computed by {@link #depth}, see class javadoc).
     * @return an iterator over the local segments to consider for {@code table}.
     */
    static Iterator<Segment> generateSegments(TableMetadata table, Collection<Range<Token>> localRanges, int depth)
    {
        return new AbstractIterator<Segment>()
        {
            private final Iterator<Range<Token>> rangeIterator = localRanges.iterator();
            private Iterator<Range<Token>> currentRangeIterator = Collections.emptyIterator();

            protected Segment computeNext()
            {
                if (currentRangeIterator.hasNext())
                    return segment(currentRangeIterator.next());

                if (!rangeIterator.hasNext())
                    return endOfData();

                Range<Token> range = rangeIterator.next();
                currentRangeIterator = splitRangeAndUnwrap(range, depth, table.partitioner);
                assert currentRangeIterator.hasNext() : "splitRangeAndUnwrap shouldn't be able to return an empty iterator";
                return segment(currentRangeIterator.next());
            }

            private Segment segment(Range<Token> range)
            {
                return new Segment(table, range);
            }
        };
    }

    /**
     * The (estimated) number of segments the iterator from {@link #generateSegments} will generate.
     * <p>
     * It's called an estimate because there is theoretically rare cases where ranges may not be splittable beyond
     * a certain depth and so we keep the contract of the method conservative to avoid surprises (we don't use this
     * method in places where we couldn't suffer imprecision; if that changes, the simplest solution would be to make
     * {@link #generateSegments} directly return a list). In practice, it will likely always be exactly the number of
     * segments generated.
     *
     * @param depth the depth we'll use to split ranges.
     * @param localRanges the ranges to validate. It must not be empty.
     * @return an estimate of the number of segments obtained when splitting {@code localRanges} at depth {@code depth}.
     */
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
     * @return an iterator over the range produced by splitting {@code toSplit} in half recursively {@code depth} times,
     * and with no wrapped ranges.
     */
    @VisibleForTesting
    static Iterator<Range<Token>> splitRangeAndUnwrap(Range<Token> toSplit, int depth, IPartitioner partitioner)
    {
        assert depth >= 0 : "Invalid depth " + depth;
        if (depth == 0)
        {
            // We have nothing to do for depth 0, except for unwrapping the range if it's wrapped.
            return maybeUnwrap(toSplit);
        }

        // We special case depth 1 because it's likely somewhat common (along with depth=0). For instance, with 256
        // vnodes, each node stores 256 * RF ranges for every table, so typically 768 (RF=3). Depth == 1 will handle
        // ranges up to 400MB (with the default SEGMENT_SIZE_TARGET of 200MB), which handles 768*400 ~= 300GB.
        // And while node with > 300GB may not be that rare, this is for a single table, so this likely cover many cases.
        // We course, we still handle higher depth properly below, but worth a simple optimization.
        if (depth == 1)
        {
            Token midpoint = midpoint(toSplit, partitioner);
            if (midpoint == null)
                return maybeUnwrap(toSplit);

            Range<Token> left = new Range<>(toSplit.left, midpoint);
            Range<Token> right = new Range<>(midpoint, toSplit.right);
            return Iterators.concat(maybeUnwrap(left), maybeUnwrap(right));
        }

        // At worth, we'll have 1 range waiting to be processed for each depth, plus 1 potential range due to unwrapping.
        final Deque<RangeWithDepth> queue = new ArrayDeque<>(depth + 1);
        queue.offerFirst(withDepth(toSplit, 0));
        return new AbstractIterator<Range<Token>>()
        {
            protected Range<Token> computeNext()
            {
                while (!queue.isEmpty())
                {
                    RangeWithDepth range = queue.poll();
                    if (range.depth == depth)
                        return maybeUnwrap(range.range);

                    Token midpoint = midpoint(range.range, partitioner);
                    // Can't split further, so be it
                    if (midpoint == null)
                        return maybeUnwrap(range.range);

                    Range<Token> left = new Range<>(range.range.left, midpoint);
                    Range<Token> right = new Range<>(midpoint, range.range.right);
                    int nextDepth = range.depth + 1;

                    queue.offerFirst(withDepth(right, nextDepth));

                    // Save allocating the RangeWithDepth if we've reach our depth
                    if (nextDepth == depth)
                        return maybeUnwrap(left);

                    // otherwise, let left be split some more
                    queue.offerFirst(withDepth(left, nextDepth));
                }
                return endOfData();
            }

            // Should only be call for a range we're about to return
            private Range<Token> maybeUnwrap(Range<Token> range)
            {
                if (!range.isTrulyWrapAround())
                    return range;

                List<Range<Token>> ranges = range.unwrap();
                assert ranges.size() == 2;
                Range<Token> left = ranges.get(0);
                Range<Token> right = ranges.get(1);
                queue.offerFirst(withDepth(right, depth));
                return left;
            }
        };
    }

    private static Iterator<Range<Token>> maybeUnwrap(Range<Token> range)
    {
        return range.isTrulyWrapAround() ? range.unwrap().iterator() : Iterators.singletonIterator(range);
    }

    private static Token midpoint(Range<Token> range, IPartitioner partitioner)
    {
        Token midpoint = partitioner.midpoint(range.left, range.right);
        // It's theoretically possible the range is small enough to not be splittable. This shouldn't really happen as
        // local ranges shouldn't be *that* small and we don't split unless a range covers more than SEGMENT_SIZE_TARGET,
        // but let's not have a hard to reproduce bug just-in-case.
        return midpoint.equals(range.left) || midpoint.equals(range.right) ? null : midpoint;
    }

    private static RangeWithDepth withDepth(Range<Token> range, int depth)
    {
        return new RangeWithDepth(range, depth);
    }

    // Minor auxiliary class for use by splitRangeAndUnwrap
    private static class RangeWithDepth
    {
        final Range<Token> range;
        final int depth;

        private RangeWithDepth(Range<Token> range, int depth)
        {
            this.range = range;
            this.depth = depth;
        }
    }
}
