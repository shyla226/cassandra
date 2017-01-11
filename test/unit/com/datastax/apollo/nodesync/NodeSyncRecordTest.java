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
package com.datastax.apollo.nodesync;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.*;
import static com.datastax.apollo.nodesync.NodeSyncTestTools.*;

public class NodeSyncRecordTest
{
    // Simple table reused by all test (details don't matter).
    private static final TableMetadata TABLE = metadataBuilder("ks", "table")
                                               .addPartitionKeyColumn("k", Int32Type.instance)
                                               .build();

    // Overall segment over which the test consolidate.
    private static final Segment SEGMENT = seg(TABLE, 0, 100);

    /** Trivial case where we consolidate an empty list of records */
    @Test
    public void consolidateEmpty() throws Exception
    {
        assertEquals(null, NodeSyncRecord.consolidate(SEGMENT, Collections.emptyList()));
    }

    /** Also trivial case where we consolidate a single record that happens to be the segment for which we consolidate.*/
    @Test
    public void consolidateSingleCoveringRecord() throws Exception
    {
        for (NodeSyncRecord record : Arrays.asList(record(SEGMENT),
                                                   record(SEGMENT, fullInSync(1)),
                                                   record(SEGMENT, inet(127, 0, 0, 1)),
                                                   record(SEGMENT, partialRepaired(42, inet(127, 0, 0, 10))),
                                                   record(SEGMENT, uncompleted(3), fullInSync(4)),
                                                   record(SEGMENT, failed(2), fullInSync(13), inet(127, 0, 0, 4))))
        {
            assertEquals(record, NodeSyncRecord.consolidate(SEGMENT, Collections.singletonList(record)));
        }
    }

    /** Still fairly trivial case where we consolidate on a single record, but that is not the segment for which we consolidate. */
    @Test
    public void consolidateSingleNotCoveringRecord() throws Exception
    {
        for (NodeSyncRecord record : Arrays.asList(record(seg(TABLE, 0, 99)),
                                                   record(seg(TABLE, -10, 50), fullInSync(1)),
                                                   record(seg(TABLE, 1, 101), inet(127, 0, 0, 1))))
        {
            assertEquals(null, NodeSyncRecord.consolidate(SEGMENT, Collections.singletonList(record)));
        }
    }

    /** More complex cases where we consolidate multiple (non-intersecting) records, but that fully cover the segment on which we consolidate. * */
    @Test
    public void consolidateMultipleCoveringRecords() throws Exception
    {
        RecordsBuilder toConsolidate;
        NodeSyncRecord expected;
        
        // All with same outcomes.
        toConsolidate = records(TABLE).add(0, 10, fullInSync(10))
                                      .add(10, 60, fullInSync(12))
                                      .add(60, 100, fullInSync(8));
        // should pick oldest validation time
        expected = record(SEGMENT, fullInSync(12));
        assertEquals(expected, NodeSyncRecord.consolidate(SEGMENT, toConsolidate.asList()));

        // Different outcomes, with one having no successful outcome.
        toConsolidate = records(TABLE).add(0, 10, fullInSync(10))
                                      .add(10, 60, partialRepaired(8, inet(127, 0, 0, 1)))
                                      .add(60, 100, fullInSync(12));
         // should pick the "worst" outcome _and_ oldest validation time
        expected = record(SEGMENT, partialRepaired(12, inet(127, 0, 0, 1)));
        assertEquals(expected, NodeSyncRecord.consolidate(SEGMENT, toConsolidate.asList()));

        // Different outcomes, some lastValidation != lastSuccessfulValidation.
        toConsolidate = records(TABLE).add(0, 10, failed(4), fullInSync(8))
                                      .add(10, 60, partialRepaired(6, inet(127, 0, 0, 1)), fullInSync(8))
                                      .add(60, 100, fullInSync(2));
        // consolidating on lastValidation will give us failed(6) and on lastSuccessfulValidation will give us fullInSync(8)
        expected = record(SEGMENT, failed(6), fullInSync(8));
        assertEquals(expected, NodeSyncRecord.consolidate(SEGMENT, toConsolidate.asList()));
    }

    /** More complex cases where we consolidate multiple intersecting records, but that fully cover the segment on which we consolidate. * */
    @Test
    public void consolidateMultipleIntersectingRecords() throws Exception
    {
        RecordsBuilder toConsolidate;
        NodeSyncRecord expected;

        // All with same outcomes.
        toConsolidate = records(TABLE).add(0, 30, fullInSync(10))
                                      .add(10, 60, fullInSync(12))
                                      .add(50, 100, fullInSync(8));
        // should pick oldest validation time
        expected = record(SEGMENT, fullInSync(12));
        assertEquals(expected, NodeSyncRecord.consolidate(SEGMENT, toConsolidate.asList()));

        // Similar to the one above, but where the oldest record is fully overriding by other outcomes.
        toConsolidate = records(TABLE).add(0, 40, fullInSync(10))
                                      .add(10, 60, fullInSync(12))
                                      .add(35, 100, fullInSync(8));
        // fullInSync(12) is fully covered, so should pick the 2nd oldest one
        expected = record(SEGMENT, fullInSync(10));
        assertEquals(expected, NodeSyncRecord.consolidate(SEGMENT, toConsolidate.asList()));

        // A case where 2 ranges are full covered by a bigger one (typical of what would happen if we increment the
        // depth due to a table growing).
        toConsolidate = records(TABLE).add(0, 50, partialRepaired(10))
                                      .add(0, 100, fullInSync(8))
                                      .add(50, 100, fullInSync(11));
        // The more recent record covers everything, so we should pick it up
        expected = record(SEGMENT, fullInSync(8));
        assertEquals(expected, NodeSyncRecord.consolidate(SEGMENT, toConsolidate.asList()));
    }

    /** More complex cases where we consolidate multiple records that do not cover the segment on which we consolidate. */
    @Test
    public void consolidateMultipleNotCoveringRecords() throws Exception
    {
        RecordsBuilder toConsolidate = records(TABLE).add(0, 10, fullInSync(10))
                                                     .add(10, 60, fullInSync(12))
                                                     .add(61, 100, fullInSync(8));
        assertEquals(null, NodeSyncRecord.consolidate(SEGMENT, toConsolidate.asList()));
    }

    @Test
    public void consolidateBlockedBy() throws Exception
    {
        RecordsBuilder toConsolidate;
        NodeSyncRecord expected;

        toConsolidate = records(TABLE).add(0, 10, inet(127, 0, 0, 1))
                                      .add(10, 60, inet(127, 0, 0, 2))
                                      .add(40, 100, inet(127, 0, 0, 3));
        NodeSyncRecord result = NodeSyncRecord.consolidate(SEGMENT, toConsolidate.asList());
        // Which one we get is unimportant and no point in making the test fragile so only checking it's not null
        assertNotNull(result);
        assertNotNull(result.lockedBy);

        // Make sure we don't claim something is locked if it has "hole"
        toConsolidate = records(TABLE).add(0, 10, inet(127, 0, 0, 1))
                                      .add(10, 60, fullInSync(1))
                                      .add(60, 100, inet(127, 0, 0, 3));
        assertEquals(null, NodeSyncRecord.consolidate(SEGMENT, toConsolidate.asList()));

    }
}