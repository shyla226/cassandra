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
package org.apache.cassandra.db.commitlog;

import java.io.*;
import java.util.concurrent.TimeUnit;

import org.junit.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.AdjustedTimeSource;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class CommitLogTimeSourceTest
{
    private static final String KEYSPACE1 = "CommitLogTimeSourceTest";
    private static final String STANDARD1 = "Standard1";

    @BeforeClass
    public static void beforeClass() throws ConfigurationException
    {
        // Enable the testing time source, so we can manipulate time:
        System.setProperty("dse.commitlog.timesource", AdjustedTimeSource.class.getCanonicalName());

        // Disable durable writes to avoid writing on the commit log outside the actual test:
        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;

        // Initialize stuff:
        DatabaseDescriptor.daemonInitialization();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1, 0, AsciiType.instance, BytesType.instance));

        CompactionManager.instance.disableAutoCompaction();
    }

    @Test
    public void testNanoTimeOverflow() throws IOException
    {
        // Set the time source with a nano time close to overflow, in an attempt to overflow between adding a mutation
        // to the commit log and waiting for it to be synced: this is not deterministic, due to the multithreaded
        // nature of the commit log, but it's the best we can do and has been shown to randomly fail if the nano time
        // arithmetic in the commit log is wrong.
        AdjustedTimeSource timeSource = (AdjustedTimeSource) CommitLog.instance.timeSource;
        long sleepLength = TimeUnit.MILLISECONDS.toNanos(10);
        int cycleCount = 60;
        int mutationsPerCycle = 20;
        timeSource.setNanosTo(Long.MAX_VALUE - cycleCount / 2 * sleepLength);

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);

        Assert.assertTrue("nanoTime() must be close to overflow at the start of this test", Long.MAX_VALUE - timeSource.nanoTime() < (1L << 50));
        for (int i = 0; i < cycleCount; i++)
        {
            for (int j = 0; j < mutationsPerCycle; ++j)
            {
                Mutation m = new RowUpdateBuilder(cfs.metadata(), 0, "k1")
                             .clustering("bytes")
                             .add("val", bytes("this is a string"))
                             .build();

                CommitLog.instance.add(m).blockingGet();
            }
            timeSource.sleepUninterruptibly(sleepLength, TimeUnit.NANOSECONDS);
        }
        Assert.assertTrue("nanoTime() must be overflowed at the end of this test", timeSource.nanoTime() - Long.MIN_VALUE < (1L << 50));
    }
}
