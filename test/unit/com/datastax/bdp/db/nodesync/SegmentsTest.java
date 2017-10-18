/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;

import static com.datastax.bdp.db.nodesync.NodeSyncTestTools.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class SegmentsTest
{
    @Test
    public void testDepth() throws Exception
    {
        // 100MB in a single range. With a 200MB max segment, we should have depth 0.
        assertEquals(0, Segments.depth(100, 1, 200));

        // 400MB, divided in 2 ranges (200MB per range). With a 200MB max segment, we should have depth 0.
        assertEquals(0, Segments.depth(400, 2, 200));

        // 3 GB, divided in 3 ranges (1GB per range). With a 200MB max segment, we should have depth 3, which gives us
        // 125MB segments.
        assertEquals(3, Segments.depth(3000, 3, 200));
    }

    /**
     * Tests segment generation in the simple case of single non-wrapping range.
     */
    @Test
    public void testGenerateSingleNonWrappingRange() throws Exception
    {
        TableMetadata table = metadataBuilder("ks", "table")
                              .addPartitionKeyColumn("k", Int32Type.instance)
                              .build();

        List<Range<Token>> toSplit = Collections.singletonList(range(0, 100));

        assertSegments(segs(table).add(0, 100).asList(),
                     Segments.generate(table, toSplit, 0));

        assertSegments(segs(table).add(0, 50).add(50, 100).asList(),
                       Segments.generate(table, toSplit, 1));

        assertSegments(segs(table).add(0, 25).add(25, 50)
                                  .add(50, 75).add(75, 100).asList(),
                     Segments.generate(table, toSplit, 2));

        assertSegments(segs(table).add(0, 12).add(12, 25)
                                  .add(25, 37).add(37, 50)
                                  .add(50, 62).add(62, 75)
                                  .add(75, 87).add(87, 100).asList(),
                     Segments.generate(table, toSplit, 3));
    }

    /**
     * Tests segment generation in the simple case of single wrapping range.
     */
    @Test
    public void testGenerateSingleWrappingRange() throws Exception
    {
        TableMetadata table = metadataBuilder("ks", "table")
                              .addPartitionKeyColumn("k", Int32Type.instance)
                              .build();

        // We use the range (0, 0] to split (which cover the whole ring due to wrapping) because 0 is basically in the
        // middle of the partitioner range, which makes it a bit cleaner to express split points.
        List<Range<Token>> toSplit = Collections.singletonList(range(0, 0));

        assertSegments(segs(table).add(min(), 0).add(0, min()).asList(),
                       Segments.generate(table, toSplit, 0));

        assertSegments(segs(table).add(min(), 0).add(0, max()).add(max() , min()).asList(),
                       Segments.generate(table, toSplit, 1));

        assertSegments(segs(table).add(min(), min()/2).add(min()/2, 0)
                                  .add(0, max()/2).add(max()/2, max())
                                  .add(max() , min())
                                  .asList(),
                       Segments.generate(table, toSplit, 2));
    }

    /**
     * Tests segment generation with multiple ranges.
     */
    @Test
    public void testGenerateMultipleRanges()
    {
        TableMetadata table = metadataBuilder("ks", "table")
                              .addPartitionKeyColumn("k", Int32Type.instance)
                              .build();

        List<Range<Token>> ranges = asList(range(0, 100),
                                           range(200, 300),
                                           range(300, 400));

        // tableSize = 1000 with 3 ranges, so 333 per range. As we target a max size of 100, we'll have depth=2 which
        // split everything in 4.
        int depth = Segments.depth(1000, 3, 100);
        assertEquals(2, depth);
        List<Segment> expected = segs(table)
                                 // 1st range
                                 .add(0, 25).add(25, 50).add(50, 75).add(75, 100)
                                 // 2nd range
                                 .add(200, 225).add(225, 250).add(250, 275).add(275, 300)
                                 // 3rd range
                                 .add(300, 325).add(325, 350).add(350, 375).add(375, 400)
                                 .asList();

        assertSegments(expected, Segments.generate(table, ranges, depth));

        // estimatedSegments should always be exact in the context of those tests
        assertEquals(expected.size(), Segments.estimateSegments(ranges, depth));

    }

    @Test
    public void testFullyIncludedIn()
    {
        TableMetadata table = metadataBuilder("ks", "table")
                              .addPartitionKeyColumn("k", Int32Type.instance)
                              .build();

        // Creates segments corresponding to: (0, 50], (50, 100], (200, 250], (250, 300]
        List<Range<Token>> toSplit = Arrays.asList(range(0, 100), range(200, 300));
        Segments segments = Segments.generate(table, toSplit, 1);
        // Sanity check
        assertSegments(segs(table).add(0, 50).add(50, 100)
                                  .add(200, 250).add(250, 300).asList(),
                       segments);

        // Large segment that include it all
        assertAllIncluded(segments, seg(table, -100, 500), 0, 4);

        // Exact segment should full include itself
        assertOneIncluded(segments, seg(table, 0, 50), 0);
        assertOneIncluded(segments, seg(table, 50, 100), 1);
        assertOneIncluded(segments, seg(table, 200, 250), 2);
        assertOneIncluded(segments, seg(table, 250, 300), 3);

        // Inclusion of 2 consecutive segment (typical of "the depth have been increased")
        assertAllIncluded(segments, seg(table, 0, 100), 0, 2);
        assertAllIncluded(segments, seg(table, 200, 300), 2, 4);

        // Smaller segments  shouldn't include a bigger one
        assertNothingIncluded(segments, seg(table, 1, 50));
        assertNothingIncluded(segments, seg(table, 50, 99));
        assertNothingIncluded(segments, seg(table, 201, 249));
        assertNothingIncluded(segments, seg(table, 251, 300));

        // Intersection is not inclusion
        assertNothingIncluded(segments, seg(table, 25, 75));
        assertNothingIncluded(segments, seg(table, 75, 249));

        // Mix of some fully included segment and some not fully included
        assertOneIncluded(segments, seg(table, 20, 150), 1);
        assertOneIncluded(segments, seg(table, 150, 275), 2);
        assertAllIncluded(segments, seg(table, 20, 275), 1, 3);

        // Tests with min tokens
        assertAllIncluded(segments, seg(table, min(), min()), 0, 4);
        assertAllIncluded(segments, seg(table, 150, min()), 2, 4);
        assertAllIncluded(segments, seg(table, min(), 150), 0, 2);
    }

    @Test
    public void testFullyIncludedInMinToken()
    {
        // Test to make sure we handle the min token within the segments correctly.

        TableMetadata table = metadataBuilder("ks", "table")
                              .addPartitionKeyColumn("k", Int32Type.instance)
                              .build();

        // Creates segments corresponding to: (min, -1], (-1, min] (because -1 is the midpoint between Long.MIN_VALUE and Long.MAX_VALUE)
        List<Range<Token>> toSplit = Collections.singletonList(range(min(), min()));
        Segments segments = Segments.generate(table, toSplit, 1);
        // Sanity check
        assertSegments(segs(table).add(min(), -1).add(-1, min()).asList(), segments);


        // Only the segment covering the full ring will include everything
        assertAllIncluded(segments, seg(table, min(), min()), 0, 2);

        // Check other possible inclusions
        assertOneIncluded(segments, seg(table, min(),10), 0);
        assertOneIncluded(segments, seg(table, -10, min()), 1);

        // And non-inclusions
        assertNothingIncluded(segments, seg(table, min(), -10));
        assertNothingIncluded(segments, seg(table, 10, min()));
        assertNothingIncluded(segments, seg(table, -10, 10));
    }

    /**
     * Asserts that {@code segments.findFullyIncludedInd(seg)} returns no segments included.
     */
    private static void assertNothingIncluded(Segments segments, Segment seg)
    {
        int[] res = segments.findFullyIncludedIn(seg);
        assertTrue(String.format("%s should include nothing from %s, but got %s from findFullyIncludedIn()",
                                 seg, segments, Arrays.toString(res)),
                   res[0] >= res[1]);
    }

    /**
     * Asserts that {@code segments.findFullyIncludedInd(seg)} returns a single segment of index {@code included}.
     */
    private static void assertOneIncluded(Segments segments, Segment seg, int included)
    {
        int[] res = segments.findFullyIncludedIn(seg);
        assertTrue(String.format("%s should include exactly index %d from %s, but got %s from findFullyIncludedIn()",
                                 seg, included, segments, Arrays.toString(res)),
                   res[0] == included && res[1] == (included+1));
    }

    /**
     * Asserts that {@code segments.findFullyIncludedInd(seg)} returns all segment of index between {@code startInclusive}
     * and {@code endExclusive}.
     */
    private static void assertAllIncluded(Segments segments, Segment seg, int startInclusive, int endExclusive)
    {
        int[] res = segments.findFullyIncludedIn(seg);
        assertTrue(String.format("%s should include indexes [%d, %d) from %s, but got %s from findFullyIncludedIn()",
                                 seg, startInclusive, endExclusive, segments, Arrays.toString(res)),
                   res[0] == startInclusive && res[1] == endExclusive);
    }
}