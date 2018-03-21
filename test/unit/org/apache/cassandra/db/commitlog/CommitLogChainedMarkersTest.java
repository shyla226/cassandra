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

package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.utils.BytemanUtil;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.schema.KeyspaceParams;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

/**
 * Tests the commitlog to make sure we can replay it - explicitly for the case where we use memory-mapped segments
 * and we do not explicitly flush the data to disk (e.g. because the process died before we could do that).
 *
 * Note: This test is only relevant to commit logs written in memory-mapped mode. Direct, compressed or encrypted
 * commit logs do not write any section content to disk before a sync request is sent, and in their case the
 * section marker is written first.
 */
@RunWith(BMUnitRunner.class)
public class CommitLogChainedMarkersTest
{
    static
    {
        BytemanUtil.randomizeBytemanPort();
    }
    
    private static final String KEYSPACE1 = "CommitLogTest";
    private static final String STANDARD1 = "CommitLogChainedMarkersTest";

    ColumnFamilyStore cfs1;

    @Before
    public void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCommitlogAccessMode(Config.AccessMode.mmap);
        DatabaseDescriptor.setCommitLogSegmentSize(5);
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.periodic);
        DatabaseDescriptor.setCommitLogSyncPeriod(10000 * 1000);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1, 0, AsciiType.instance, BytesType.instance));

        CompactionManager.instance.disableAutoCompaction();

        cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
    }

    @Test
    @BMRule(name = "force all calls to sync() to not flush to disk",
    targetClass = "CommitLogSegment",
    targetMethod = "sync",
    targetLocation = "ENTRY",
    action = "return")
    public void replayCommitLogWithoutFlushing() throws IOException
    {
        if (CommitLog.instance.configuration.useCompression() || CommitLog.instance.configuration.useEncryption())
        {
            System.out.println("Test does not make sense with commit log compression.");
            return;
        }

        byte[] entropy = new byte[1024];
        new Random().nextBytes(entropy);
        final Mutation m = new RowUpdateBuilder(cfs1.metadata.get(), 0, "k")
                           .clustering("bytes")
                           .add("val", ByteBuffer.wrap(entropy))
                           .build();

        int samples = 10000;
        for (int i = 0; i < samples; i++)
            CommitLog.instance.add(m).blockingGet();

        // No sync wanted; byteman should have prevented any automatically triggered syncs

        ArrayList<File> toCheck = CommitLogReaderTest.getCommitLogs();
        CommitLogReader reader = new CommitLogReader();
        CommitLogReaderTest.TestCLRHandler testHandler = new CommitLogReaderTest.TestCLRHandler(cfs1.metadata.get());
        for (File f : toCheck)
            reader.readCommitLogSegment(testHandler, f, CommitLogReader.ALL_MUTATIONS, false);

        Assert.assertEquals(samples, testHandler.seenMutationCount());
    }
}
