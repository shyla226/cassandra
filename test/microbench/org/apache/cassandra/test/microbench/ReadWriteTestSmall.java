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

package org.apache.cassandra.test.microbench;


import java.io.IOException;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import io.reactivex.Scheduler;
import io.reactivex.functions.Consumer;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.concurrent.IOScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCMetrics;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 15, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class ReadWriteTestSmall extends CQLTester
{
    static String keyspace;
    String table;
    String writeStatement;
    String readStatement;
    long numRows = 0;
    ColumnFamilyStore cfs;
    static final int count = 1_100_000;

    @Param({"10000", "1000", "100"})
    int BATCH = 1_000;

    @Param({"false", "true"})
    boolean flush = true;

    Random rand = new Random(1);

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        Scheduler ioScheduler = Schedulers.from(Executors.newFixedThreadPool(IOScheduler.MAX_POOL_SIZE));
        RxJavaPlugins.setComputationSchedulerHandler((s) -> TPC.bestTPCScheduler());
        RxJavaPlugins.initIoScheduler(() -> ioScheduler);
        RxJavaPlugins.setErrorHandler(t -> logger.error("RxJava unexpected Exception ", t));

        CQLTester.setUpClass();
//        CQLTester.requireNetwork();
        System.err.println("setupClass done.");
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}");
        execute("use "+keyspace+";");
        writeStatement = "INSERT INTO "+table+"(userid,picid,commentid)VALUES(?,?,?)";
        readStatement = "SELECT * from "+table+" where userid=?";
        System.err.println("Prepared.");

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        //Warm up
        System.err.println("Writing " + count);
        for (long i = 0; i < count; i++)
            execute(writeStatement, i, i, i );

        if (flush)
            cfs.forceBlockingFlush();

        // Needed to stabilize sstable count for off-cache sized tests (e.g. count = 100_000_000)
        while (cfs.getLiveSSTables().size() >= 15)
        {
            cfs.enableAutoCompaction(true);
            cfs.disableAutoCompaction();
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        for (TPCTaskType stage : TPCTaskType.values())
        {
            String v = "";
            for (int i = 0; i < TPC.perCoreMetrics.length; ++i)
            {
                TPCMetrics metrics = TPC.perCoreMetrics[i];
                if (metrics.completedTaskCount(stage) > 0)
                    v += String.format(" %d: %,d", i, metrics.completedTaskCount(stage));
            }
            if (!v.isEmpty())
                System.out.println(stage + ":" + v);
        }

        JVMStabilityInspector.removeShutdownHooks();
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }

    @Benchmark
    public Object write() throws Throwable
    {
        numRows++;
        return execute(writeStatement, numRows, numRows, numRows );
    }


    @Benchmark
    public Object readRandomInside() throws Throwable
    {
        Waiter<UntypedResultSet> waiter = new Waiter<>(BATCH);
        for (int i = 0; i < BATCH; ++i)
            executeAsync(readStatement, (long) rand.nextInt(count)).subscribe(waiter);
        return waiter.get();
    }

    @Benchmark
    public Object readRandomWOutside() throws Throwable
    {
        return execute(readStatement, (long) rand.nextInt(count + count / 6));
    }

    @Benchmark
    public Object readFixed() throws Throwable
    {
        return execute(readStatement, 1234567890123L % count);
    }

    @Benchmark
    public Object readOutside() throws Throwable
    {
        return execute(readStatement, count + 1234567L);
    }

    @Override
    public UntypedResultSet execute(String query, Object... values) throws Throwable
    {
        Waiter<UntypedResultSet> waiter = new Waiter<>(1);
        executeAsync(query, values).subscribe(waiter);
        return waiter.get();
    }

    static class Waiter<T> implements Consumer<T>
    {
        T value;
        final AtomicInteger countNeeded;

        Waiter(int countNeeded)
        {
            this.countNeeded = new AtomicInteger(countNeeded);
        }

        public T get()
        {
            while (countNeeded.get() > 0)
//            {}
                Thread.yield();

            return value;
        }

        public void accept(T v) throws Exception
        {
            value = v;
            countNeeded.decrementAndGet();
        }
    }

}
