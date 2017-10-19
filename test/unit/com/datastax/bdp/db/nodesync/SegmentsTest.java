/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.bdp.db.nodesync;

import java.util.List;

import org.junit.Test;

import com.datastax.bdp.db.nodesync.Segment;
import com.datastax.bdp.db.nodesync.Segments;

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
     * Tests {@link Segments#splitRangeAndUnwrap} in the simple cases where there is no wrapping.
     */
    @Test
    public void testSplitRangeAndUnwrapSimple() throws Exception
    {
        assertRanges(asList(range(0, 100)),
                     Segments.splitRangeAndUnwrap(range(0, 100), 0, PARTITIONER));

        assertRanges(asList(range(0, 50), range(50, 100)),
                     Segments.splitRangeAndUnwrap(range(0, 100), 1, PARTITIONER));

        assertRanges(asList(range(0, 25), range(25, 50),
                            range(50, 75), range(75, 100)),
                     Segments.splitRangeAndUnwrap(range(0, 100), 2, PARTITIONER));

        assertRanges(asList(range(0, 12), range(12, 25),
                            range(25, 37), range(37, 50),
                            range(50, 62), range(62, 75),
                            range(75, 87), range(87, 100)),
                     Segments.splitRangeAndUnwrap(range(0, 100), 3, PARTITIONER));
    }

    /**
     * Tests {@link Segments#splitRangeAndUnwrap} with unwrapping involved.
     */
    @Test
    public void testSplitRangeAndUnwrapWithUnwrapping() throws Exception
    {
        // We use the range (0, 0] to split (which cover the whole ring due to wrapping) because 0 is basically in the
        // middle of the partitioner range, which makes it a bit cleaner to express split points.

        assertRanges(asList(range(0, min()), range(min(), 0)),
                     Segments.splitRangeAndUnwrap(range(0, 0), 0, PARTITIONER));

        assertRanges(asList(range(0, max()), range(max() , min()), range(min(), 0)),
                     Segments.splitRangeAndUnwrap(range(0, 0), 1, PARTITIONER));

        assertRanges(asList(range(0, max()/2), range(max()/2, max()),
                            range(max() , min()), // unwrapping in action
                            range(min(), min()/2), range(min()/2, 0)),
                     Segments.splitRangeAndUnwrap(range(0, 0), 2, PARTITIONER));
    }

    @Test
    public void testSplitRangesAtDepth()
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

        assertSegments(expected, Segments.generateSegments(table, ranges, depth));

        // estimatedSegments should always be exact in the context of those tests
        assertEquals(expected.size(), Segments.estimateSegments(ranges, depth));

    }
}