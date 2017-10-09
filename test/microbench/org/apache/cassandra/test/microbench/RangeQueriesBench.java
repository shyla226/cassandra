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
import java.util.concurrent.*;

import io.reactivex.Scheduler;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.concurrent.IOScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCMetrics;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.LineNumberInference;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 15, time = 2, timeUnit = TimeUnit.SECONDS)
//@Fork(value = 1, jvmArgsPrepend = {"-Xmx4G", "-Xms4G", "-Xmn2G",
//                                   "-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder",
//                                   "-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints",
//                                   "-XX:StartFlightRecording=name=auto,filename=RangeQueriesBench.jfr,delay=30s," +
//                                   "settings=/home/stefi/profiling-advanced.jfc,dumponexit=true"})
//@Fork(value = 1, jvmArgsPrepend = {"-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintAssembly", "-Xmx4G", "-Xms4G", "-Xmn2G"})
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class RangeQueriesBench extends CQLTester
{
    static String keyspace;
    String tableNarrow;
    String tableWide;
    String writeStatement;
    String readStatement;
    String countStatement;
    static final int numPartitions = 5;
    static final int numRows = 100_000;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        LineNumberInference.init();
        DatabaseDescriptor.daemonInitialization();

        Scheduler ioScheduler = Schedulers.from(Executors.newFixedThreadPool(IOScheduler.MAX_POOL_SIZE));
        RxJavaPlugins.setComputationSchedulerHandler((s) -> TPC.bestTPCScheduler());
        RxJavaPlugins.initIoScheduler(() -> ioScheduler);
        RxJavaPlugins.setErrorHandler(t -> logger.error("RxJava unexpected Exception ", t));

        CQLTester.setUpClass();
        Keyspace.setInitialized();

        System.err.println("setupClass done.");

        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        tableNarrow = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}");
        tableWide = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}");

        execute("use " + keyspace + ";");
        writeStatement = "INSERT INTO %s (userid,picid,commentid)VALUES(?,?,?)";
        readStatement = "SELECT * from %s";
        countStatement = "SELECT COUNT(*) from %s";
        System.err.println("Prepared.");

        for (String table : new String[] {tableNarrow, tableWide})
        {
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            cfs.disableAutoCompaction();

            //Warm up
            System.err.println(String.format("Writing to %s...", table));
            if (table == tableNarrow)
            {
                int count = numPartitions * numRows;
                for (long i = 0; i < count; i++)
                    execute(String.format(writeStatement, table), i, i, i);
            }
            else
            {
                for (long j = 0; j < numPartitions; j++)
                    for (long i = 0; i < numRows; i++)
                        execute(String.format(writeStatement, table), j, i, i);
            }

            System.err.println("Flushing... ");
            cfs.forceBlockingFlush();
        }

        dumpMetrics(); // dump the metrics so we can compare before and after the benchmark, excluding write metrics
        System.err.println("Done. ");
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        dumpMetrics();
        JVMStabilityInspector.removeShutdownHooks();
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }

    private static void dumpMetrics()
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
    }

    @Benchmark
    public void readNarrow() throws Throwable
    {
       execute(String.format(readStatement, tableNarrow));
    }

    @Benchmark
    public void countNarrow() throws Throwable
    {
        execute(String.format(countStatement, tableNarrow));
    }

    @Benchmark
    public void readWide() throws Throwable
    {
        execute(String.format(readStatement, tableWide));
    }

    @Benchmark
    public void countWide() throws Throwable
    {
        execute(String.format(countStatement, tableWide));
    }

//    @Override
//    public UntypedResultSet execute(String query, Object... values) throws Throwable
//    {
//        Waiter waiter = new Waiter();
//        executeAsync(query, values).subscribe(waiter);
//        return waiter.get();
//    }
//
//    static class Waiter implements SingleObserver<UntypedResultSet>
//    {
//        volatile UntypedResultSet value;
//        Throwable error = null;
//
//        public UntypedResultSet get() throws Throwable
//        {
//            while (value == null && error == null)
//                Thread.yield();
//
//            if (error != null)
//                throw error;
//
//            return value;
//        }
//
//        public void onSubscribe(Disposable disposable)
//        {
//        }
//
//        public void onSuccess(UntypedResultSet v)
//        {
//            value = v;
//        }
//
//        public void onError(Throwable throwable)
//        {
//            error = throwable;
//        }
//    }
}
