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
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCMetrics;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.Throwables;

public class TimeOrderedReadTest extends CQLTester
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
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}");
        execute("use "+keyspace+";");
        writeStatement = "INSERT INTO "+table+"(userid,picid,commentid)VALUES(?,?,?)";
        System.err.println("Prepared.");

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        System.err.println("Writing " + count);
        int lastPrint = 0;
        int lastFlush = 0;
        for (int i = 0; i < count; i += BATCH)
        {
            int max = Math.min(BATCH, count - i);
            Waiter waiter = new Waiter(max, 0);
            for (int j = 0; j < max; ++j)
            {
                long v = i + j;
                long w = v % overwriteEach;
                executeAsync(writeStatement, w, w, v).subscribe(waiter);
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
    }

    @After
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        dumpTpcCounters();
    }

    public static void dumpTpcCounters()
    {
        for (TPCTaskType stage : TPCTaskType.values())
        {
            String v = "";
            for (int i = 0; i <= TPC.getNumCores(); ++i)
            {
                TPCMetrics metrics = TPC.metrics(i);
                if (metrics.completedTaskCount(stage) > 0)
                    v += String.format(" %d: %,d", i, metrics.completedTaskCount(stage));
            }
            if (!v.isEmpty())
                System.out.println(stage + ":" + v);
        }
    }

    /**
     * This tests the timestamp-ordered single partition read command path, making sure it does not defragment content
     * when it shouldn't.
     */
    @Test
    public void readNonDefragBatch() throws Throwable
    {
        String readStatement = "SELECT * from "+table+" where userid=? and picid=?";
        Waiter waiter = new Waiter(BATCH, overwriteEach);
        for (int i = 0; i < BATCH; ++i)
        {
            long val = rand.nextInt(count) % overwriteEach;
            executeAsync(readStatement, val, val).subscribe(waiter);
        }
        waiter.get();

        // Check defragmentation wasn't triggered (we just read one sstable here)
        Assert.assertTrue(TPC.metrics(TPC.getNumCores()).completedTaskCount(TPCTaskType.WRITE_DEFRAGMENT) < 10);
    }

    static class Waiter implements SingleObserver<UntypedResultSet>
    {
        UntypedResultSet.Row value = null;
        Throwable error = null;
        final AtomicInteger countNeeded;
        final boolean check;
        final long overwriteEach;

        Waiter(int countNeeded, long overwriteEach)
        {
            this.countNeeded = new AtomicInteger(countNeeded);
            this.check = overwriteEach > 0;
            this.overwriteEach = overwriteEach;
        }

        public UntypedResultSet.Row get() throws Throwable
        {
            while (countNeeded.get() > 0)
                Thread.yield();

            if (error != null)
                throw error;
            return value;
        }

        public void accept(UntypedResultSet v) throws Exception
        {
            if (check)
            {
                UntypedResultSet.Row value = v.one();
                long id = value.getLong("commentid");
                Assert.assertEquals(id % overwriteEach, value.getLong("userid"));
                Assert.assertEquals(id % overwriteEach, value.getLong("picid"));
                this.value = value;
            }
            countNeeded.decrementAndGet();
        }

        public void onSubscribe(Disposable disposable)
        {

        }

        public void onSuccess(UntypedResultSet v)
        {
            try
            {
                accept(v);
            }
            catch (Throwable t)
            {
                onError(t);
            }
        }

        public void onError(Throwable throwable)
        {
            error = Throwables.merge(error, throwable);
            countNeeded.decrementAndGet();
        }
    }

}
