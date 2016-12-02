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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 20, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1
, jvmArgsAppend = {"-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
                   "-Dcassandra.storagedir=/tmp/foobar",
                   "-XX:CompileCommandFile=/home/jake/workspace/cassandra/conf/hotspot_compiler",
                                   "-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder",
                                   "-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints", "-XX:+PreserveFramePointer"
                                   //"-XX:StartFlightRecording=duration=60s,filename=./profiling-data.jfr,name=profile,settings=profile",
                                  // "-XX:FlightRecorderOptions=settings=/home/jake/workspace/cassandra/profiling-advanced.jfc,samplethreads=true"
}
)
@Threads(1)
@State(Scope.Benchmark)
public class ReadWriteTest extends CQLTester
{
    static String keyspace;
    String table;
    PreparedStatement writeStatement;
    PreparedStatement readStatement;
    long numRows = 5000;
    long numReads = 0;

    @Param({"64"})
    int INFLIGHT = 1;
    List<ResultSetFuture> futures;
    ColumnFamilyStore cfs;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {

        RxJavaPlugins.setComputationSchedulerHandler((s) -> NettyRxScheduler.instance());
        RxJavaPlugins.initIoScheduler(Schedulers.from(Executors.newFixedThreadPool(DatabaseDescriptor.getConcurrentWriters())));
        RxJavaPlugins.setErrorHandler(t -> logger.error("RxJava unexpected Exception ", t));

        CQLTester.setUpClass();
        CQLTester.requireNetwork();

        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))");
        executeNet(4, "use "+keyspace+";");
        writeStatement = prepareNet(4, "INSERT INTO "+table+"(userid,picid,commentid)VALUES(?,?,?)");
        readStatement = prepareNet(4, "SELECT * from "+table+" where userid = ? limit 1");

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        //Warm up
        System.err.println("Writing " + numRows);
        for (long i = 0; i < numRows; i++)
            executeNet(4, writeStatement.bind(i,i,i));

        futures = new ArrayList<>(INFLIGHT);
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        StorageService.instance.removeShutdownHook();
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }

    @Benchmark
    public Object read() throws Throwable
    {
        for (int i = 0; i < INFLIGHT; i++)
            futures.add(executeNetAsync(4, readStatement.bind( numReads++ % numRows )));

        FBUtilities.waitOnFutures(futures);

        List<Row> rows = futures.stream().map(f -> f.getUninterruptibly().one()).collect(Collectors.toList());
        futures.clear();

        return rows;
    }

    public static void main(String... args) throws Exception {
        Options opts = new OptionsBuilder()
                       .include(".*"+ReadWriteTest.class.getSimpleName()+".*")
                       .jvmArgs("-server")
                       .forks(1)
                       .mode(Mode.AverageTime)
                       //.addProfiler(LinuxPerfAsmProfiler.class)
                       .build();

        Collection<RunResult> records = new Runner(opts).run();
        for ( RunResult result : records) {
            Result r = result.getPrimaryResult();
            System.out.println("API replied benchmark score: "
                               + r.getScore() + " "
                               + r.getScoreUnit() + " over "
                               + r.getStatistics().getN() + " iterations");
        }
    }
}
