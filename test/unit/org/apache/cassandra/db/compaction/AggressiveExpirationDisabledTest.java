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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.junit.Assert.assertNull;

public class AggressiveExpirationDisabledTest extends SchemaLoader
{
    public static final String KEYSPACE1 = "Keyspace1";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        // Make sure expiration is disabled
        System.setProperty(TimeWindowCompactionStrategyOptions.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY,  "false");

        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }
    @Test
    public void testOverlappingSSTables() throws Throwable
    {
        testOverlappingSSTables(0);
        testOverlappingSSTables(1000);
    }

    private void testOverlappingSSTables(long timeDifference) throws InterruptedException
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

        TimeWindowCompactionStrategy twcs = makeTSWC(cfs, 30, TimeUnit.SECONDS, TimeUnit.MILLISECONDS, 0, true);
        twcs.startup();

        // None of them can expire as the system property overrules the table property
        AbstractCompactionTask t = twcs.getNextBackgroundTask((int) (System.currentTimeMillis() / 1000));
        assertNull(t);
    }

    private static void makeUpdate(ColumnFamilyStore cfs, long timestamp, int ttl, DecoratedKey key, ByteBuffer value)
    {
        RowUpdateBuilder builder;

        if (ttl > 0)
            builder = new RowUpdateBuilder(cfs.metadata(), timestamp, ttl, key.getKey());
        else
            builder = new RowUpdateBuilder(cfs.metadata(), timestamp, key.getKey());

        builder.clustering("column")
               .add("val", value).build().applyUnsafe();
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
        for (SSTableReader sstable : cfs.getLiveSSTables())
            twcs.addSSTable(sstable);
        return twcs;
    }
}
