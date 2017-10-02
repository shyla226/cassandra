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
package org.apache.cassandra.concurrent;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.junit.Assert.*;

public class TPCBoundariesTest
{
    private static IPartitioner oldPartitioner;

    @BeforeClass
    public static void setupClass()
    {
        oldPartitioner = DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        DatabaseDescriptor.daemonInitialization();
    }

    @AfterClass
    public static void afterClass()
    {
        DatabaseDescriptor.setPartitionerUnsafe(oldPartitioner);
    }

    @Test
    public void testWithSingleCore()
    {
        DatabaseDescriptor.getRawConfig().num_tokens = 1;

        Token t1 = Murmur3Partitioner.instance.midpoint(Murmur3Partitioner.instance.getMinimumToken(), Murmur3Partitioner.instance.getMaximumToken());
        Token t2 = Murmur3Partitioner.instance.midpoint(t1, t1);
        Range<Token> r1 = new Range<>(t1, t2);
        Range<Token> r2 = new Range<>(t2, t1);

        TPCBoundaries boundaries = TPCBoundaries.compute(ImmutableList.of(r1, r2), 1);
        List<Range<Token>> ranges = boundaries.asRanges();
        assertEquals(1, ranges.size());
        assertEquals(new Range<>(Murmur3Partitioner.instance.getMinimumToken(), Murmur3Partitioner.instance.getMaximumToken()), ranges.get(0));
    }

    @Test
    public void testWithMultipleCores()
    {
        DatabaseDescriptor.getRawConfig().num_tokens = 2;

        Token t1 = Murmur3Partitioner.instance.midpoint(Murmur3Partitioner.instance.getMinimumToken(), Murmur3Partitioner.instance.getMaximumToken());
        Token t2 = Murmur3Partitioner.instance.midpoint(t1, t1);
        Range<Token> r1 = new Range<>(t1, t2);
        Range<Token> r2 = new Range<>(t2, t1);

        TPCBoundaries boundaries = TPCBoundaries.compute(ImmutableList.of(r1, r2), 2);
        List<Range<Token>> ranges = boundaries.asRanges();
        assertEquals(2, ranges.size());
        assertEquals(new Range<>(Murmur3Partitioner.instance.getMinimumToken(), t2), ranges.get(0));
        assertEquals(new Range<>(t2, Murmur3Partitioner.instance.getMaximumToken()), ranges.get(1));
    }
}
