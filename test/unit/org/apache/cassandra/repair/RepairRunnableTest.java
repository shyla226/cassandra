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

package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.repair.RepairRunnable.CommonRange;

import static org.apache.cassandra.repair.RepairRunnable.filterRanges;

public class RepairRunnableTest extends AbstractRepairTest
{
    private static final CommonRange COMMON_RANGE1 = new CommonRange(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2), Sets.newHashSet(RANGE1, RANGE2));
    private static final CommonRange COMMON_RANGE2 = new CommonRange(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3), Sets.newHashSet(RANGE3));
    private static final CommonRange COMMON_RANGE3 = new CommonRange(Sets.newHashSet(PARTICIPANT1, PARTICIPANT4), Sets.newHashSet(RANGE4));

    private static final List<CommonRange> ALL_COMMON_RANGES = Lists.newArrayList(COMMON_RANGE1, COMMON_RANGE2, COMMON_RANGE3);

    private static final Set<InetAddress> ALL_PARTICIPANTS = Sets.newHashSet(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3, PARTICIPANT4);

    /**
     * Filter by all participants should return all ranges
     */
    @Test
    public void testFilterRangesAllParticipants()
    {
        Assert.assertEquals(ALL_COMMON_RANGES, filterRanges(ALL_COMMON_RANGES, ALL_PARTICIPANTS));
    }

    /**
     * Filter by no participants should throw exception
     */
    @Test
    public void testFilterRangesNoParticipants()
    {
        try
        {
            filterRanges(ALL_COMMON_RANGES, Collections.emptySet());
            Assert.fail("Should have thrown exception.");
        } catch (IllegalStateException e)
        {
            Assert.assertEquals("Not enough live endpoints for a repair", e.getMessage());
        }
    }

    /**
     * when filtering by 1 participant, return only ranges of that participant
     */
    @Test
    public void testFilterRangesOneParticipant() throws Exception
    {
        Assert.assertEquals(Lists.newArrayList(new CommonRange(Collections.singleton(PARTICIPANT4), Collections.singleton(RANGE4))),
                            filterRanges(ALL_COMMON_RANGES, Collections.singleton(PARTICIPANT4)));
    }

    /**
     * when filtering by 2 participants, return only ranges of those participants
     */
    @Test
    public void testFilterRangesTwoParticipants()
    {
        Set<InetAddress> liveEndpoints = Sets.newHashSet(PARTICIPANT2, PARTICIPANT3); // PARTICIPANT1 and PARTICIPANT4 is excluded
        List<CommonRange> expected = Lists.newArrayList(new CommonRange(Sets.newHashSet(PARTICIPANT2), Sets.newHashSet(RANGE1, RANGE2)),
                                                        new CommonRange(Sets.newHashSet(PARTICIPANT2, PARTICIPANT3), Sets.newHashSet(RANGE3)));
        Assert.assertEquals(expected, filterRanges(ALL_COMMON_RANGES, liveEndpoints));
    }
}
