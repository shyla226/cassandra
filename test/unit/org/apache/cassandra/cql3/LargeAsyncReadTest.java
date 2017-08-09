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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCMetrics;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

public class LargeAsyncReadTest extends CQLTester
{
    static String keyspace;
    String table;
    String writeStatement;
    String readStatement;
    ColumnFamilyStore cfs;
    static final int count = 500_000;
    int BATCH = 10_000;

    Random rand = new Random(1);

    @Before
    public void prepare() throws Throwable
    {
        System.err.println("setupClass done.");
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}");
        execute("use "+keyspace+";");
        writeStatement = "INSERT INTO "+table+"(userid,picid,commentid)VALUES(?,?,?)";
        readStatement = "SELECT * from "+table+" where userid=?";
        System.err.println("Prepared.");

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        System.err.println("Writing " + count);
        for (long i = 0; i < count; i++)
            execute(writeStatement, i, i, i );
        cfs.forceBlockingFlush();
        while (cfs.getLiveSSTables().size() >= 15)
        {
            cfs.enableAutoCompaction(true);
            cfs.disableAutoCompaction();
        }
    }

    @After
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        for (TPCTaskType stage : TPCTaskType.values())
        {
            String v = "";
            for (int i = 0; i < TPC.perCoreMetrics.length; ++i)
            {
                TPCMetrics metrics = TPC.perCoreMetrics[i];
                if (metrics.completedTaskCount(stage) > 0)
                    v += String.format(" %d: %,d(%,d)", i, metrics.completedTaskCount(stage), metrics.blockedTaskCount(stage));
            }
            if (!v.isEmpty())
                System.out.println(stage + ":" + v);
        }
    }

    @Test
    public void readLargeBatch() throws Throwable
    {
        Waiter<UntypedResultSet> waiter = new Waiter<>(BATCH);
        for (int i = 0; i < BATCH; ++i)
            executeAsync(readStatement, (long) rand.nextInt(count)).subscribe(waiter);
        waiter.get();
    }

    static class Waiter<T> implements SingleObserver<T>
    {
        T value;
        Throwable error = null;
        final AtomicInteger countNeeded;

        Waiter(int countNeeded)
        {
            this.countNeeded = new AtomicInteger(countNeeded);
        }

        public T get() throws Throwable
        {
            while (countNeeded.get() > 0)
                Thread.yield();

            if (error != null)
                throw error;
            return value;
        }

        public void accept(T v) throws Exception
        {
            value = v;
            countNeeded.decrementAndGet();
        }

        public void onSubscribe(Disposable disposable)
        {

        }

        public void onSuccess(T v)
        {
            value = v;
            countNeeded.decrementAndGet();
        }

        public void onError(Throwable throwable)
        {
            error = throwable;
            countNeeded.decrementAndGet();
        }
    }

}
