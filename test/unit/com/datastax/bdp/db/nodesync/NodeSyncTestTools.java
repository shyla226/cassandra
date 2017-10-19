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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.datastax.bdp.db.nodesync.NodeSyncRecord;
import com.datastax.bdp.db.nodesync.Segment;
import com.datastax.bdp.db.nodesync.ValidationInfo;
import com.datastax.bdp.db.nodesync.ValidationOutcome;
import com.datastax.bdp.db.nodesync.ValidationProposer;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.*;

/**
 * Small static helpers targeted at NodeSync unit tests.
 * <p>
 * NodeSync fundamentally deals with token ranges, and tokens are a bit unreadable by default. But at least with
 * Murmur3 partitioner, tokens are really just integers (longs), so those methods (and the tests that use them) attempt
 * to make things more readable by using tokens as integer directly, and using small readable values as much as
 * possible. This does mean the methods and associated tests assume Murmur3.
 *
 * Note: some methods really deal with token range, which has usefulness outside of NodeSync, could probably be move t
 */
public class NodeSyncTestTools
{
    // When building validation time, we want in test pass human-readable values, but the ValidatioInfo.toString()
    // assumes that the time is genuine timestamp for proper display, so the methods of this class ends up mixing the
    // value passed in the/ test with the current time so we get the best of both word. We do use a reference fixed for
    // a single test execution so things are decently predictable.
    private static final long NOW = System.currentTimeMillis();

    static final IPartitioner PARTITIONER = Murmur3Partitioner.instance;

    /**
     * Thin wrapping around {@link TableMetadata#builder} that sets the partitioner.
     * Not saving that much typing, but using that consistently highlight the fact this all depends on a specific
     * partitioner.
     */
    static TableMetadata.Builder metadataBuilder(String ks, String table)
    {
        return TableMetadata.builder(ks, table)
                            .partitioner(PARTITIONER);
    }

    /** Creates a token from the provided integer value. */
    static Token tk(long value)
    {
        return new Murmur3Partitioner.LongToken(value);
    }

    /** The minimum token (on which we wrap). */
    public static long min()
    {
        return Long.MIN_VALUE;
    }

    /** The maximum token. Note that we don't wrap on this, but it can be useful to express some ranges. */
    public static long max()
    {
        return Long.MAX_VALUE;
    }

    /** Create a range from {@code left} to {@code right}. */
    static Range<Token> range(long left, long right)
    {
        return new Range<>(tk(left), tk(right));
    }

    /**
     * Asserts that the provided iterator of range generates the expected ranges.
     *
     * @param expected the range that we expect {@code actual} to generates. Note that we assert that {@code actual}
     *                 generates <b>exactly</b> the range from {@code expected}, not more, not less.
     * @param actual the iterator for the range to check.
     */
    static void assertRanges(List<Range<Token>> expected, Iterator<Range<Token>> actual)
    {
        List<Range<Token>> actualAsList = new ArrayList<>();
        Iterators.addAll(actualAsList, actual);
        assertEquals(expected, actualAsList);
    }

    /**
     * Similar to {@link #assertRanges} but with segments.
     *
     * @param expected the segments that we expect {@code actual} to generates. Note that we assert that {@code actual}
     *                 generates <b>exactly</b> the segments from {@code expected}, not more, not less.
     * @param actual the iterator for the segments to check.
     */
    @SuppressWarnings("unchecked")
    static void assertSegments(List<Segment> expected,
                               Iterator<Segment> actual)
    {
        List<Segment> actualAsList = new ArrayList<>();
        Iterators.addAll(actualAsList, actual);
        assertEquals(expected, actualAsList);
    }

    /**
     * Creates a segment from the table and range it covers.
     */
    public static Segment seg(TableMetadata table, long left, long right)
    {
        return new Segment(table, range(left, right));
    }

     /**
     * Creates a simple builder to create multiple segments over the provided table.
     */
    static SegmentsBuilder segs(TableMetadata table)
    {
        return new SegmentsBuilder(table);
    }

    /**
     * Asserts that the next segments produced by {@code proposer} are the {@code expected} ones. Note that this doesn't
     * check on purpose that {@code actual} doesn't have more segments, as when testing continuous proposers they will
     * generate segment indefinitely and we only want to test the n first generated in this case (but the test can
     * manually check the proposer if necessary after this call).
     *
     * @param expected the segments that we expect {@code proposer} to generate next.
     * @param proposer the {@link ValidationProposer} to test.
     */
    static void assertSegments(List<Segment> expected,
                               ValidationProposer proposer)
    {
        for (int i = 0; i < expected.size(); i++)
        {
            Segment nextExpected = expected.get(i);
            final int idx = i;
            boolean hadProposal = proposer.supplyNextProposal(p -> assertEquals(String.format("Expected %s for %dth segment, but got %s", nextExpected, idx, p.segment()),
                                                                                nextExpected,
                                                                                p.segment));

            assertTrue(String.format("Expected at least %d segment(s) but got only %d. First missing range is %s", expected.size(), i, nextExpected),
                       hadProposal);
        }
    }

    private static ValidationInfo vInfo(long daysAgo, ValidationOutcome outcome, Set<InetAddress> missingNodes)
    {
        return new ValidationInfo(NOW - TimeUnit.DAYS.toMillis(daysAgo),
                                  outcome,
                                  missingNodes);
    }

    public static ValidationInfo fullInSync(long daysAgo)
    {
        return vInfo(daysAgo, ValidationOutcome.FULL_IN_SYNC, null);
    }

    public static ValidationInfo fullRepaired(long daysAgo)
    {
        return vInfo(daysAgo, ValidationOutcome.FULL_REPAIRED, null);
    }

    public static ValidationInfo partialInSync(long daysAgo, InetAddress... missingNodes)
    {
        return vInfo(daysAgo, ValidationOutcome.PARTIAL_IN_SYNC, Sets.newHashSet(missingNodes));
    }

    public static ValidationInfo partialRepaired(long daysAgo, InetAddress... missingNodes)
    {
        return vInfo(daysAgo, ValidationOutcome.PARTIAL_REPAIRED, Sets.newHashSet(missingNodes));
    }

    public static ValidationInfo uncompleted(long daysAgo)
    {
        return vInfo(daysAgo, ValidationOutcome.UNCOMPLETED, null);
    }

    public static ValidationInfo failed(long daysAgo)
    {
        return vInfo(daysAgo, ValidationOutcome.FAILED, null);
    }

    public static NodeSyncRecord record(Segment seg)
    {
        return record(seg, null, null);
    }

    public static NodeSyncRecord record(Segment seg, ValidationInfo lastValidation)
    {
        return record(seg, lastValidation, lastValidation.wasSuccessful() ? lastValidation : null);
    }

    public static NodeSyncRecord record(Segment seg, InetAddress lockedBy)
    {
        return record(seg, null, null, lockedBy);
    }

    public static NodeSyncRecord record(Segment seg, ValidationInfo lastValidation, ValidationInfo lastSuccessfulValidation)
    {
        assert lastSuccessfulValidation == null || lastSuccessfulValidation.wasSuccessful();
        return record(seg, lastValidation, lastSuccessfulValidation, null);
    }

    public static NodeSyncRecord record(Segment seg,
                                        ValidationInfo lastValidation,
                                        ValidationInfo lastSuccessfulValidation,
                                        InetAddress lockedBy)
    {
        return new NodeSyncRecord(seg, lastValidation, lastSuccessfulValidation, lockedBy);
    }

    public static RecordsBuilder records(TableMetadata table)
    {
        return new RecordsBuilder(table);
    }

    public static InetAddress inet(int... ipAddress)
    {
        assert ipAddress.length == 4;
        try
        {
            byte[] address = new byte[4];
            for (int i = 0; i < ipAddress.length; i++)
                address[i] = (byte)ipAddress[i];
            return InetAddress.getByAddress(address);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Very simple builder of multiple segments. Mostly saves typing the table for every segment when building a
     * big list of them.
     */
    static class SegmentsBuilder
    {
        private final TableMetadata table;
        private final List<Segment> segments = new ArrayList<>();

        private SegmentsBuilder(TableMetadata table)
        {
            this.table = table;
        }

        SegmentsBuilder add(long left, long right)
        {
            segments.add(seg(table, left, right));
            return this;
        }

        SegmentsBuilder addAll(Collection<Range<Token>> ranges)
        {
            Iterables.addAll(segments, Iterables.transform(ranges, r -> new Segment(table, r)));
            return this;
        }

        List<Segment> asList()
        {
            return new ArrayList<>(segments);
        }
    }

    /**
     * Very simple builder of multiple NodeSync records over a table.
     */
    public static class RecordsBuilder
    {
        private final TableMetadata table;
        private final List<NodeSyncRecord> records = new ArrayList<>();

        private RecordsBuilder(TableMetadata table)
        {
            this.table = table;
        }

        public RecordsBuilder add(long left, long right, ValidationInfo lastValidation)
        {
            return add(record(seg(table, left, right), lastValidation));
        }

        public RecordsBuilder add(long left, long right, InetAddress lockedBy)
        {
            return add(record(seg(table, left, right), lockedBy));
        }

        public RecordsBuilder add(long left, long right, ValidationInfo lastValidation, ValidationInfo lastSuccessfulValidation)
        {
            return add(record(seg(table, left, right), lastValidation, lastSuccessfulValidation));
        }

        public RecordsBuilder add(long left, long right, ValidationInfo lastValidation, ValidationInfo lastSuccessfulValidation, InetAddress lockedBy)
        {
            return add(record(seg(table, left, right), lastValidation, lastSuccessfulValidation, lockedBy));
        }

        private RecordsBuilder add(NodeSyncRecord record)
        {
            records.add(record);
            return this;
        }

        public List<NodeSyncRecord> asList()
        {
            return new ArrayList<>(records);
        }
    }
}
