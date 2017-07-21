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
package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy.getWindowBoundsInMillis;
import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy.newestBucket;
import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy.validateOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TimeWindowCompactionStrategyTest extends SchemaLoader
{
    public static final String KEYSPACE1 = "Keyspace1";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        // Allow testing aggressive expiration; still has to be enabled on cf level
        System.setProperty(TimeWindowCompactionStrategyOptions.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY,  "true");

        // Disable tombstone histogram rounding for tests
        System.setProperty("cassandra.streaminghistogram.roundseconds", "1");

        loadSchema(null);
    }

    @Test
    public void testOptionsValidation() throws ConfigurationException
    {
        Map<String, String> options = new HashMap<>();
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "30");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        Map<String, String> unvalidated = validateOptions(options);
        assertTrue(unvalidated.isEmpty());

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "0");
            validateOptions(options);
            fail(String.format("%s == 0 should be rejected", TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY));
        }
        catch (ConfigurationException e) {}

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "-1337");
            validateOptions(options);
            fail(String.format("Negative %s should be rejected", TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "1");
        }

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MONTHS");
            validateOptions(options);
            fail(String.format("Invalid time units should be rejected", TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        }

        options.put("bad_option", "1.0");
        unvalidated = validateOptions(options);
        assertTrue(unvalidated.containsKey("bad_option"));
    }


    @Test
    public void testTimeWindows()
    {
        Long tstamp1 = 1451001601000L; // 2015-12-25 @ 00:00:01, in milliseconds
        Long tstamp2 = 1451088001000L; // 2015-12-26 @ 00:00:01, in milliseconds
        Long lowHour = 1451001600000L; // 2015-12-25 @ 00:00:00, in milliseconds

        // A 1 hour window should round down to the beginning of the hour
        assertTrue(getWindowBoundsInMillis(TimeUnit.HOURS, 1, tstamp1).left.compareTo(lowHour) == 0);

        // A 1 minute window should round down to the beginning of the hour
        assertTrue(getWindowBoundsInMillis(TimeUnit.MINUTES, 1, tstamp1).left.compareTo(lowHour) == 0);

        // A 1 day window should round down to the beginning of the hour
        assertTrue(getWindowBoundsInMillis(TimeUnit.DAYS, 1, tstamp1).left.compareTo(lowHour) == 0 );

        // The 2 day window of 2015-12-25 + 2015-12-26 should round down to the beginning of 2015-12-25
        assertTrue(getWindowBoundsInMillis(TimeUnit.DAYS, 2, tstamp2).left.compareTo(lowHour) == 0);


        return;
    }

    @Test
    public void testPrepBucket()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        Long tstamp = System.currentTimeMillis();
        Long tstamp2 =  tstamp - (2L * 3600L * 1000L);

        // create 5 sstables
        for (int r = 0; r < 3; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            rm.add(CF_STANDARD1, Util.cellname("column"), value, tstamp+r);
            rm.apply();
            cfs.forceBlockingFlush();
        }
        // Decrement the timestamp to simulate a timestamp in the past hour
        for (int r = 3; r < 5; r++)
        {
            // And add progressively more cells into each sstable
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            rm.add(CF_STANDARD1, Util.cellname("column"), value, tstamp2 + r);
            rm.apply();
            cfs.forceBlockingFlush();
        }

        cfs.forceBlockingFlush();

        HashMultimap<Long, SSTableReader> buckets = HashMultimap.create();
        List<SSTableReader> sstrs = new ArrayList<>(cfs.getSSTables());

        Pair<Long,Long> bounds = getWindowBoundsInMillis(TimeUnit.HOURS, 1, tstamp);
        // We'll put 3 sstables into the newest bucket
        for (int i = 0 ; i < 3; i++)
        {
            buckets.put(bounds.left, sstrs.get(i));
        }



        List<SSTableReader> newBucket = newestBucket(buckets, 4, 32, new SizeTieredCompactionStrategyOptions(), bounds.left);
        assertTrue("incoming bucket should not be accepted when it has below the min threshold SSTables", newBucket.isEmpty());

        newBucket = newestBucket(buckets, 2, 32, new SizeTieredCompactionStrategyOptions(), bounds.left);
        assertTrue("incoming bucket should be accepted when it is larger than the min threshold SSTables", !newBucket.isEmpty());

        // And 2 into the second bucket (1 hour back)
        for (int i = 3 ; i < 5; i++)
        {
            bounds = getWindowBoundsInMillis(TimeUnit.HOURS, 1, tstamp2 );
            buckets.put(bounds.left, sstrs.get(i));
        }

        assertEquals("an sstable with a single value should have equal min/max timestamps", sstrs.get(0).getMinTimestamp(), sstrs.get(0).getMaxTimestamp());
        assertEquals("an sstable with a single value should have equal min/max timestamps", sstrs.get(1).getMinTimestamp(), sstrs.get(1).getMaxTimestamp());
        assertEquals("an sstable with a single value should have equal min/max timestamps", sstrs.get(2).getMinTimestamp(), sstrs.get(2).getMaxTimestamp());

        // Test trim
        int numSSTables = 40;
        for (int r = 5; r < numSSTables; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            for(int i = 0 ; i < r ; i++)
            {
                Mutation rm = new Mutation(KEYSPACE1, key.getKey());
                rm.add(CF_STANDARD1, Util.cellname("column"), value, tstamp + r);
                rm.apply();
            }
            cfs.forceBlockingFlush();
        }

        // Reset the buckets, overfill it now
        sstrs = new ArrayList<>(cfs.getSSTables());
        for (int i = 0 ; i < 40; i++)
        {
            bounds = getWindowBoundsInMillis(TimeUnit.HOURS, 1, sstrs.get(i).getMaxTimestamp());
            buckets.put(bounds.left, sstrs.get(i));
        }

        newBucket = newestBucket(buckets, 4, 32, new SizeTieredCompactionStrategyOptions(), bounds.left);
        assertEquals("new bucket should be trimmed to max threshold of 32", newBucket.size(),  32);
    }


    @Test
    public void testDropExpiredSSTables() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // create 2 sstables
        makeUpdate(cfs, System.currentTimeMillis(), 1, Util.dk("expired"), value);

        cfs.forceBlockingFlush();
        SSTableReader expiredSSTable = cfs.getSSTables().iterator().next();
        Thread.sleep(10);

        makeUpdate(cfs, System.currentTimeMillis(), -1, Util.dk("nonexpired"), value);

        cfs.forceBlockingFlush();
        assertEquals(cfs.getSSTables().size(), 2);

        TimeWindowCompactionStrategy twcs = makeTSWC(cfs, 30, TimeUnit.SECONDS, TimeUnit.MILLISECONDS, 0, false);

        twcs.startup();
        assertNull(twcs.getNextBackgroundTask((int) (System.currentTimeMillis() / 1000)));
        Thread.sleep(2000);
        AbstractCompactionTask t = twcs.getNextBackgroundTask((int) (System.currentTimeMillis()/1000));
        assertNotNull(t);
        assertEquals(1, Iterables.size(t.sstables));
        SSTableReader sstable = t.sstables.iterator().next();
        assertEquals(sstable, expiredSSTable);
        cfs.getDataTracker().unmarkCompacting(cfs.getSSTables());
    }

    @Test
    public void testOverlappingSSTables() throws Throwable
    {
        testOverlappingSSTables(0, true);
        testOverlappingSSTables(0, false);

        testOverlappingSSTables(1000, true);
        testOverlappingSSTables(1000, false);
    }

    private void testOverlappingSSTables(long timeDifference, boolean allowUnsafeExpiration) throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        long ts1 = System.currentTimeMillis();
        makeUpdate(cfs, ts1, 1, Util.dk("expired1"), value);
        cfs.forceBlockingFlush();
        Thread.sleep(1000);

        long ts2 = System.currentTimeMillis();
        makeUpdate(cfs, ts2, 1, Util.dk("expired2"), value);
        cfs.forceBlockingFlush();
        Thread.sleep(1000);

        // Create an expired yet resident row
        long ts3 = ts1 - timeDifference;
        makeUpdate(cfs, ts3, 1, Util.dk("expired3"), value);
        makeUpdate(cfs, System.currentTimeMillis(), -1, Util.dk("nonexpired"), value);
        cfs.forceBlockingFlush();
        Thread.sleep(1000);

        TimeWindowCompactionStrategy twcs = makeTSWC(cfs, 30, TimeUnit.SECONDS, TimeUnit.MILLISECONDS, 0, allowUnsafeExpiration);
        twcs.startup();

        if (allowUnsafeExpiration)
        {
            // First two sstables expire
            AbstractCompactionTask t = twcs.getNextBackgroundTask((int) (System.currentTimeMillis() / 1000));
            t.run();
            assertEquals(cfs.getLiveSSTableCount(), 1);
            assertEquals(cfs.getSSTables().iterator().next().getMinTimestamp(), ts3);
        }
        else
        {
            // None of them can expire
            AbstractCompactionTask t = twcs.getNextBackgroundTask((int) (System.currentTimeMillis() / 1000));
            assertNull(t);
        }
        cfs.getDataTracker().unmarkCompacting(cfs.getDataTracker().getCompacting());
    }


    @Test
    public void testNonDroppedOverlappingSSTables() throws Throwable
    {
        testNonDroppedOverlappingSSTables(0, true);
        testNonDroppedOverlappingSSTables(0, false);

        testNonDroppedOverlappingSSTables(1000, true);
        testNonDroppedOverlappingSSTables(1000, false);
    }

    private void testNonDroppedOverlappingSSTables(long timeDifference, boolean allowUnsafeExpiration) throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        long ts1 = System.currentTimeMillis();
        makeUpdate(cfs, ts1, 1, Util.dk("expired1"), value);
        makeUpdate(cfs, ts1, -1, Util.dk("nonexpired"), value);
        cfs.forceBlockingFlush();
        Thread.sleep(1000);

        long ts2 = System.currentTimeMillis();
        makeUpdate(cfs, ts2, 1, Util.dk("expired2"), value);
        makeUpdate(cfs, ts1, -1, Util.dk("nonexpired"), value);
        cfs.forceBlockingFlush();
        Thread.sleep(1000);

        // Create an expired yet resident row
        long ts3 = ts1 - timeDifference;
        makeUpdate(cfs, ts3, 1, Util.dk("expired3"), value);
        makeUpdate(cfs, System.currentTimeMillis(), -1, Util.dk("nonexpired"), value);
        cfs.forceBlockingFlush();
        Thread.sleep(1000);

        TimeWindowCompactionStrategy twcs = makeTSWC(cfs, 30, TimeUnit.SECONDS, TimeUnit.MILLISECONDS, 0, allowUnsafeExpiration);
        twcs.startup();

        // None of them can expire
        AbstractCompactionTask t = twcs.getNextBackgroundTask((int) (System.currentTimeMillis() / 1000));
        assertNull(t);
        cfs.getDataTracker().unmarkCompacting(cfs.getDataTracker().getCompacting());
    }

    private static void makeUpdate(ColumnFamilyStore cfs, long timestamp, int ttl, DecoratedKey key, ByteBuffer value)
    {
        Mutation mutation = new Mutation(cfs.keyspace.getName(), key.getKey());

        if (ttl > 0)
            mutation.add(cfs.name, Util.cellname("column"), value, timestamp, ttl);
        else
            mutation.add(cfs.name, Util.cellname("column"), value, timestamp);

        mutation.apply();
    }

    private TimeWindowCompactionStrategy makeTSWC(ColumnFamilyStore cfs, int windowSize, TimeUnit timeUnit, TimeUnit resolution, int expiredSSTablesCheckFrequencySeconds,
                                                  boolean allowUnsafeExpiration)
    {
        Map<String, String> options = new HashMap<>();
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, Integer.toString(windowSize));
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, timeUnit.toString());
        options.put(TimeWindowCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, resolution.toString());
        options.put(TimeWindowCompactionStrategyOptions.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, Integer.toString(expiredSSTablesCheckFrequencySeconds));
        options.put(TimeWindowCompactionStrategyOptions.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_TABLE_OPTION, Boolean.toString(allowUnsafeExpiration));

        TimeWindowCompactionStrategy twcs = new TimeWindowCompactionStrategy(cfs, options);
        for (SSTableReader sstable : cfs.getSSTables())
            twcs.addSSTable(sstable);
        return twcs;
    }
}
