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

package org.apache.cassandra.test.microbench.instance;


import java.io.IOException;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import io.reactivex.Scheduler;
import io.reactivex.functions.Consumer;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.concurrent.IOScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.openjdk.jmh.annotations.*;

import static org.apache.cassandra.test.microbench.instance.Util.printTPCStats;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 15, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class ReadTestSmallPartitions extends CQLTester
{
    static String keyspace;
    String table;
    String writeStatement;
    String readStatement;
    ColumnFamilyStore cfs;
    static final int count = 2_000_000;
    Random rand;

    int BATCH = 1_000;

    public enum Flush
    {
        NO, YES, BIG
    }

    @Param({"NO", "YES", "BIG"})
    Flush flush = Flush.YES;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        rand = new Random(1);
        Scheduler ioScheduler = Schedulers.from(Executors.newFixedThreadPool(IOScheduler.MAX_POOL_SIZE));
        RxJavaPlugins.setComputationSchedulerHandler((s) -> TPC.bestTPCScheduler());
        RxJavaPlugins.initIoScheduler(() -> ioScheduler);
        RxJavaPlugins.setErrorHandler(t -> logger.error("RxJava unexpected Exception ", t));

        CQLTester.setUpClass();
        CQLTester.requireNetwork();
        System.err.println("setupClass done.");
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}");
        execute("use "+keyspace+";");
        writeStatement = "INSERT INTO "+table+"(userid,picid,commentid)VALUES(?,?,?)";
        readStatement = "SELECT * from "+table+" where userid=?";
        System.err.println("Prepared, batch " + BATCH + " flush " + flush);
        System.err.println("Disk access mode " + DatabaseDescriptor.getDiskAccessMode() + " index " + DatabaseDescriptor.getIndexAccessMode() + " aio " + TPC.USE_AIO);
        System.err.println("Cores " + TPC.getNumCores() + " boundaries " + Keyspace.open(keyspace).getTPCBoundaries());

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        cfs.forceBlockingFlush();

        //Warm up
        System.err.println("Writing " + count);
        long i;
        for (i = 0; i <= count - BATCH; i += BATCH)
            performWrite(i, BATCH);
        if (i < count)
            performWrite(i, count - i);

        switch (flush)
        {
        case YES:
        case BIG:
            cfs.forceBlockingFlush();
            break;
        default:
            // don't flush
        }

        // Needed to stabilize sstable count for off-cache sized tests (e.g. count = 100_000_000)
        while (cfs.getLiveSSTables().size() >= 15)
        {
            cfs.enableAutoCompaction(true);
            cfs.disableAutoCompaction();
        }

        switch (flush)
        {
        case BIG:
            org.apache.cassandra.Util.rewriteToFormat(cfs, SSTableFormat.Type.BIG);
            break;
        default:
            // we are ready
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        printTPCStats();

        JVMStabilityInspector.removeShutdownHooks();
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }

    public Object performRead(LongSupplier supplier) throws Throwable
    {
        Waiter<UntypedResultSet> waiter = new Waiter<>(BATCH);
        for (int i = 0; i < BATCH; ++i)
            executeAsync(readStatement, supplier.getAsLong()).subscribe(waiter);
        return waiter.get();
    }

    public Object performWrite(long ofs, long count) throws Throwable
    {
        Waiter<UntypedResultSet> waiter = new Waiter<>((int) count);
        for (long i = ofs; i < ofs + count; ++i)
            executeAsync(writeStatement, i, i, i).subscribe(waiter);
        return waiter.get();
    }

    @Benchmark
    public Object readRandomInside() throws Throwable
    {
        return performRead(() -> rand.nextInt(count));
    }

    @Benchmark
    public Object readRandomWOutside() throws Throwable
    {
        return performRead(() -> rand.nextInt(count + count / 6));
    }

    @Benchmark
    public Object readFixed() throws Throwable
    {
        return performRead(() -> 1234567890123L % count);
    }

    @Benchmark
    public Object readOutside() throws Throwable
    {
        return performRead(() -> count + 1234567L);
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
