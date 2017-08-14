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

package org.apache.cassandra.cql3;


import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

import static org.apache.cassandra.cql3.TimeOrderedReadTest.Waiter;

public class TimeOrderedDefragTest extends CQLTester
{
    static String keyspace;
    String table;
    String writeStatement;
    ColumnFamilyStore cfs;
    static final int count = 100_000;
    static final int overwriteEach = 10_000;
    int BATCH = 1_000;

    Random rand = new Random(1);

    @Before
    public void prepare() throws Throwable
    {
        System.err.println("setupClass done.");
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, v0 int, v1 int, v2 int, v3 int, v4 int, v5 int, v6 int, v7 int, v8 int, v9 int, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}");
        execute("use "+keyspace+";");
        System.err.println("Prepared.");

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        System.err.println("Writing " + count);
        int lastPrint = 0;
        int lastFlush = 0;
        for (int i = 0; i < count; i += BATCH)
        {
            int vid = (i / overwriteEach) % 10;
            writeStatement = String.format("INSERT INTO %s(userid,picid,commentid,v%d)VALUES(?,?,?,?)", table, vid);
            int max = Math.min(BATCH, count - i);
            Waiter waiter = new Waiter(max, 0);
            for (int j = 0; j < max; ++j)
            {
                long v = i + j;
                long w = v % overwriteEach;
                executeAsync(writeStatement, w, w, v, (int) v).subscribe(waiter);
            }
            waiter.get();

            if (i - lastPrint > count / 100)
            {
                System.out.print(".");
                lastPrint = i;
            }
            if (i - lastFlush >= overwriteEach * 1.2)
            {
                cfs.forceBlockingFlush();
                lastFlush = i;
            }
        }
        cfs.forceBlockingFlush();

        System.err.println("SSTable count: " + cfs.getLiveSSTables().size());
        cfs.enableAutoCompaction();
    }

    @After
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        TimeOrderedReadTest.dumpTpcCounters();
    }

    /**
     * This tests the timestamp-ordered single partition read command path, in the case where it needs to defragment
     * the content.
     */
    @Test
    public void readDefragBatch() throws Throwable
    {
        String readStatement = "SELECT * from "+table+" where userid=? and picid=?";
        System.out.println(execute(readStatement, 0L, 0L).one());
        for (int bb = 0; bb < 10; ++bb)
        {
            Waiter waiter = new Waiter(BATCH, overwriteEach);
            for (int i = 0; i < BATCH; ++i)
            {
                long val = rand.nextInt(count) % overwriteEach;
                executeAsync(readStatement, val, val).subscribe(waiter);
            }
            waiter.get();
        }

        Assert.assertTrue(TPC.metrics(TPC.getNumCores()).completedTaskCount(TPCTaskType.WRITE_DEFRAGMENT) > BATCH);
    }
}
