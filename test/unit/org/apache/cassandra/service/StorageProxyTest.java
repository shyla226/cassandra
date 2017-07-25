/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.Int32Type;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.locator.TokenMetadata;

import static org.junit.Assert.assertEquals;

public class StorageProxyTest
{
    private static final List<Integer> sortedKeys = CQLTester.partitionerSortedKeys(Arrays.asList(0, 1, 3, 4, 5, 6, 7, 8));
    private static final int MIN = Integer.MIN_VALUE;

    private static Range<PartitionPosition> range(PartitionPosition left, PartitionPosition right)
    {
        return new Range<>(left, right);
    }

    private static Bounds<PartitionPosition> bounds(PartitionPosition left, PartitionPosition right)
    {
        return new Bounds<>(left, right);
    }

    private static ExcludingBounds<PartitionPosition> exBounds(PartitionPosition left, PartitionPosition right)
    {
        return new ExcludingBounds<>(left, right);
    }

    private static IncludingExcludingBounds<PartitionPosition> incExBounds(PartitionPosition left, PartitionPosition right)
    {
        return new IncludingExcludingBounds<>(left, right);
    }

    private static PartitionPosition startOf(int key)
    {
        return token(key).minKeyBound();
    }

    private static PartitionPosition endOf(int key)
    {
        return token(key).maxKeyBound();
    }

    private static Range<Token> tokenRange(int left, int right)
    {
        Token leftToken = left == MIN ? Murmur3Partitioner.instance.getMinimumToken() : token(left);
        Token rightToken = right == MIN ? Murmur3Partitioner.instance.getMinimumToken() : token(right);
        return new Range<>(leftToken, rightToken);
    }

    private static Bounds<Token> tokenBounds(int left, int right)
    {
        Token leftToken = left == MIN ? Murmur3Partitioner.instance.getMinimumToken() : token(left);
        Token rightToken = right == MIN ? Murmur3Partitioner.instance.getMinimumToken() : token(right);
        return new Bounds<>(leftToken, rightToken);
    }

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.getHintsDirectory().mkdir();
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(token(1), InetAddress.getByName("127.0.0.1"));
        tmd.updateNormalToken(token(6), InetAddress.getByName("127.0.0.6"));
    }

    // test getRestrictedRanges for token
    private void testGRR(AbstractBounds<Token> queryRange, AbstractBounds<Token>... expected)
    {
        // Testing for tokens
        List<AbstractBounds<Token>> restricted = StorageProxy.getRestrictedRanges(queryRange);
        assertEquals(restricted.toString(), expected.length, restricted.size());
        for (int i = 0; i < expected.length; i++)
            assertEquals("Mismatch for index " + i + ": " + restricted, expected[i], restricted.get(i));
    }

    // test getRestrictedRanges for keys
    private void testGRRKeys(AbstractBounds<PartitionPosition> queryRange, AbstractBounds<PartitionPosition>... expected)
    {
        // Testing for keys
        List<AbstractBounds<PartitionPosition>> restrictedKeys = StorageProxy.getRestrictedRanges(queryRange);
        assertEquals(restrictedKeys.toString(), expected.length, restrictedKeys.size());
        for (int i = 0; i < expected.length; i++)
            assertEquals("Mismatch for index " + i + ": " + restrictedKeys, expected[i], restrictedKeys.get(i));

    }

    @Test
    public void testGRR() throws Throwable
    {
        // no splits
        testGRR(tokenRange(2, 5), tokenRange(2, 5));
        testGRR(tokenBounds(2, 5), tokenBounds(2, 5));
        // single split
        testGRR(tokenRange(2, 7), tokenRange(2, 6), tokenRange(6, 7));
        testGRR(tokenBounds(2, 7), tokenBounds(2, 6), tokenRange(6, 7));
        // single split starting from min
        testGRR(tokenRange(MIN, 2), tokenRange(MIN, 1), tokenRange(1, 2));
        testGRR(tokenBounds(MIN, 2), tokenBounds(MIN, 1), tokenRange(1, 2));
        // single split ending with max
        testGRR(tokenRange(5, MIN), tokenRange(5, 6), tokenRange(6, MIN));
        testGRR(tokenBounds(5, MIN), tokenBounds(5, 6), tokenRange(6, MIN));
        // two splits
        testGRR(tokenRange(0, 7), tokenRange(0, 1), tokenRange(1, 6), tokenRange(6, 7));
        testGRR(tokenBounds(0, 7), tokenBounds(0, 1), tokenRange(1, 6), tokenRange(6, 7));


        // Keys
        // no splits
        testGRRKeys(range(rp(2), rp(5)), range(rp(2), rp(5)));
        testGRRKeys(bounds(rp(2), rp(5)), bounds(rp(2), rp(5)));
        testGRRKeys(exBounds(rp(2), rp(5)), exBounds(rp(2), rp(5)));
        // single split
        testGRRKeys(bounds(rp(2), rp(7)), bounds(rp(2), endOf(6)), range(endOf(6), rp(7)));
        testGRRKeys(exBounds(rp(2), rp(7)), range(rp(2), endOf(6)), exBounds(endOf(6), rp(7)));
        testGRRKeys(incExBounds(rp(2), rp(7)), bounds(rp(2), endOf(6)), exBounds(endOf(6), rp(7)));
        // single split starting from min
        testGRRKeys(range(rp(MIN), rp(2)), range(rp(MIN), endOf(1)), range(endOf(1), rp(2)));
        testGRRKeys(bounds(rp(MIN), rp(2)), bounds(rp(MIN), endOf(1)), range(endOf(1), rp(2)));
        testGRRKeys(exBounds(rp(MIN), rp(2)), range(rp(MIN), endOf(1)), exBounds(endOf(1), rp(2)));
        testGRRKeys(incExBounds(rp(MIN), rp(2)), bounds(rp(MIN), endOf(1)), exBounds(endOf(1), rp(2)));
        // single split ending with max
        testGRRKeys(range(rp(5), rp(MIN)), range(rp(5), endOf(6)), range(endOf(6), rp(MIN)));
        testGRRKeys(bounds(rp(5), rp(MIN)), bounds(rp(5), endOf(6)), range(endOf(6), rp(MIN)));
        testGRRKeys(exBounds(rp(5), rp(MIN)), range(rp(5), endOf(6)), exBounds(endOf(6), rp(MIN)));
        testGRRKeys(incExBounds(rp(5), rp(MIN)), bounds(rp(5), endOf(6)), exBounds(endOf(6), rp(MIN)));
        // two splits
        testGRRKeys(range(rp(0), rp(7)), range(rp(0), endOf(1)), range(endOf(1), endOf(6)), range(endOf(6), rp(7)));
        testGRRKeys(bounds(rp(0), rp(7)), bounds(rp(0), endOf(1)), range(endOf(1), endOf(6)), range(endOf(6), rp(7)));
        testGRRKeys(exBounds(rp(0), rp(7)), range(rp(0), endOf(1)), range(endOf(1), endOf(6)), exBounds(endOf(6), rp(7)));
        testGRRKeys(incExBounds(rp(0), rp(7)), bounds(rp(0), endOf(1)), range(endOf(1), endOf(6)), exBounds(endOf(6), rp(7)));
    }

    @Test
    public void testGRRExact() throws Throwable
    {
        // min
        testGRR(tokenRange(1, 5), tokenRange(1, 5));
        testGRR(tokenBounds(1, 5), tokenBounds(1, 1), tokenRange(1, 5));
        // max
        testGRR(tokenRange(2, 6), tokenRange(2, 6));
        testGRR(tokenBounds(2, 6), tokenBounds(2, 6));
        // both
        testGRR(tokenRange(1, 6), tokenRange(1, 6));
        testGRR(tokenBounds(1, 6), tokenBounds(1, 1), tokenRange(1, 6));


        // Keys
        // min
        testGRRKeys(range(endOf(1), endOf(5)), range(endOf(1), endOf(5)));
        testGRRKeys(range(rp(1), endOf(5)), range(rp(1), endOf(1)), range(endOf(1), endOf(5)));
        testGRRKeys(bounds(startOf(1), endOf(5)), bounds(startOf(1), endOf(1)), range(endOf(1), endOf(5)));
        testGRRKeys(exBounds(endOf(1), rp(5)), exBounds(endOf(1), rp(5)));
        testGRRKeys(exBounds(rp(1), rp(5)), range(rp(1), endOf(1)), exBounds(endOf(1), rp(5)));
        testGRRKeys(exBounds(startOf(1), endOf(5)), range(startOf(1), endOf(1)), exBounds(endOf(1), endOf(5)));
        testGRRKeys(incExBounds(rp(1), rp(5)), bounds(rp(1), endOf(1)), exBounds(endOf(1), rp(5)));
        // max
        testGRRKeys(range(endOf(2), endOf(6)), range(endOf(2), endOf(6)));
        testGRRKeys(bounds(startOf(2), endOf(6)), bounds(startOf(2), endOf(6)));
        testGRRKeys(exBounds(rp(2), rp(6)), exBounds(rp(2), rp(6)));
        testGRRKeys(incExBounds(rp(2), rp(6)), incExBounds(rp(2), rp(6)));
        // bothKeys
        testGRRKeys(range(rp(1), rp(6)), range(rp(1), endOf(1)), range(endOf(1), rp(6)));
        testGRRKeys(bounds(rp(1), rp(6)), bounds(rp(1), endOf(1)), range(endOf(1), rp(6)));
        testGRRKeys(exBounds(rp(1), rp(6)), range(rp(1), endOf(1)), exBounds(endOf(1), rp(6)));
        testGRRKeys(incExBounds(rp(1), rp(6)), bounds(rp(1), endOf(1)), exBounds(endOf(1), rp(6)));
    }

    @Test
    public void testGRRWrapped() throws Throwable
    {
        // one token in wrapped range
        testGRR(tokenRange(7, 0), tokenRange(7, MIN), tokenRange(MIN, 0));
        // two tokens in wrapped range
        testGRR(tokenRange(5, 0), tokenRange(5, 6), tokenRange(6, MIN), tokenRange(MIN, 0));
        testGRR(tokenRange(7, 2), tokenRange(7, MIN), tokenRange(MIN, 1), tokenRange(1, 2));
        // full wraps
        testGRR(tokenRange(0, 0), tokenRange(0, 1), tokenRange(1, 6), tokenRange(6, MIN), tokenRange(MIN, 0));
        testGRR(tokenRange(MIN, MIN), tokenRange(MIN, 1), tokenRange(1, 6), tokenRange(6, MIN));
        // wrap on member tokens
        testGRR(tokenRange(6, 6), tokenRange(6, MIN), tokenRange(MIN, 1), tokenRange(1, 6));
        testGRR(tokenRange(6, 1), tokenRange(6, MIN), tokenRange(MIN, 1));
        // end wrapped
        testGRR(tokenRange(5, MIN), tokenRange(5, 6), tokenRange(6, MIN));

        // Keys
        // one token in wrapped range
        testGRRKeys(range(rp(7), rp(0)), range(rp(7), rp(MIN)), range(rp(MIN), rp(0)));
        // two tokens in wrapped range
        testGRRKeys(range(rp(5), rp(0)), range(rp(5), endOf(6)), range(endOf(6), rp(MIN)), range(rp(MIN), rp(0)));
        testGRRKeys(range(rp(7), rp(2)), range(rp(7), rp(MIN)), range(rp(MIN), endOf(1)), range(endOf(1), rp(2)));
        // full wraps
        testGRRKeys(range(rp(0), rp(0)), range(rp(0), endOf(1)), range(endOf(1), endOf(6)), range(endOf(6), rp(MIN)), range(rp(MIN), rp(0)));
        testGRRKeys(range(rp(MIN), rp(MIN)), range(rp(MIN), endOf(1)), range(endOf(1), endOf(6)), range(endOf(6), rp(MIN)));
        // wrap on member tokens
        testGRRKeys(range(rp(6), rp(6)), range(rp(6), endOf(6)), range(endOf(6), rp(MIN)), range(rp(MIN), endOf(1)), range(endOf(1), rp(6)));
        testGRRKeys(range(rp(6), rp(1)), range(rp(6), endOf(6)), range(endOf(6), rp(MIN)), range(rp(MIN), rp(1)));
        // end wrapped
        testGRRKeys(range(rp(5), rp(MIN)), range(rp(5), endOf(6)), range(endOf(6), rp(MIN)));
    }

    @Test
    public void testGRRExactBounds() throws Throwable
    {
        // equal tokens are special cased as non-wrapping for bounds
        testGRR(tokenBounds(0, 0), tokenBounds(0, 0));
        // completely empty bounds match everything
        testGRR(tokenBounds(MIN, MIN), tokenBounds(MIN, 1), tokenRange(1, 6), tokenRange(6, MIN));

        // Keys
        // equal tokens are special cased as non-wrapping for bounds
        testGRRKeys(bounds(rp(0), rp(0)), bounds(rp(0), rp(0)));
        // completely empty bounds match everything
        testGRRKeys(bounds(rp(MIN), rp(MIN)), bounds(rp(MIN), endOf(1)), range(endOf(1), endOf(6)), range(endOf(6), rp(MIN)));
        testGRRKeys(exBounds(rp(MIN), rp(MIN)), range(rp(MIN), endOf(1)), range(endOf(1), endOf(6)), exBounds(endOf(6), rp(MIN)));
        testGRRKeys(incExBounds(rp(MIN), rp(MIN)), bounds(rp(MIN), endOf(1)), range(endOf(1), endOf(6)), exBounds(endOf(6), rp(MIN)));
    }

    public static Token token(int key)
    {
        return Util.testPartitioner().getToken(Int32Type.instance.getSerializer().serialize(sortedKeys.get(key)));
    }

    public static PartitionPosition rp(int key)
    {
        return rp(key, Util.testPartitioner());
    }

    public static PartitionPosition rp(int key, IPartitioner partitioner)
    {
        if (key == MIN)
            return Murmur3Partitioner.instance.getMinimumToken().minKeyBound();

        return PartitionPosition.ForKey.get(Int32Type.instance.getSerializer().serialize(sortedKeys.get(key)), partitioner);
    }
}
