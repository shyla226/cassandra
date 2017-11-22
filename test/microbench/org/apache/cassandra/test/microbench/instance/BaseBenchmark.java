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


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import io.reactivex.Scheduler;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.concurrent.IOScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

import static org.apache.cassandra.test.microbench.instance.Util.printTPCStats;

/**
 * Serves as a base template for instance benchmarks which have the following controls:
 *  - populationSize - initial population in table
 *  - inflight - operations in flight
 *  - throttleMs - sleep between operations, to set a steady load
 *  - flush - flush when write of initial population
 *  - compression - enable/disable compression
 *
 * The instance is setup/torn down as a global state for the benchmark, while threads are expected to use their own
 * session (or a bunch of sessions if you like) maintained as a thread level state.
 *
 * Note: because of how JMH codegen works we need some boilerplate in subclasses to resolve the types correctly
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 20, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class BaseBenchmark
{
    private AtomicLong opCounter = new AtomicLong();

    @Param("1000000")
    long populationSize;

    @Param("1000")
    int inflight;

    /** setting this to 0 is usually a bad idea, but in this case the granularity of the ops makes it ok */
    @Param("100")
    int throttleMs;

    @Param("true")
    boolean flush;

    @Param("false")
    boolean compression;

    @Param("true")
    boolean stabiliseSSCount;

    public static class CassandraSetup extends CQLTester
    {
        String keyspace;
        String table;
        Cluster clientCluster;
        List<Session> sessions = new ArrayList();
        int perThreadInflight;

        public void setup(BaseBenchmark benchmark, BenchmarkParams params) throws Throwable
        {
            perThreadInflight = benchmark.inflight/params.getThreads();

            Scheduler ioScheduler = Schedulers.from(Executors.newFixedThreadPool(IOScheduler.MAX_POOL_SIZE));
            RxJavaPlugins.setComputationSchedulerHandler((s) -> TPC.bestTPCScheduler());
            RxJavaPlugins.initIoScheduler(() -> ioScheduler);
            RxJavaPlugins.setErrorHandler(t -> logger.error("RxJava unexpected Exception ", t));

            CQLTester.setUpClass();
            CQLTester.requireNetwork();

            clientCluster = createClientCluster(ProtocolVersion.CURRENT,
                                                "Test Cluster",
                                                NettyOptions.DEFAULT_INSTANCE,
                                                null,
                                                null);
            keyspace = createKeyspace(benchmark.keyspace());
            table = createTable(keyspace, benchmark.table());
            executeNet(ProtocolVersion.CURRENT, "use " + keyspace + ";");
            benchmark.prePopulateTable(this);
        }

        synchronized Session newSession()
        {
            Session session = clientCluster.connect();
            session.execute("use " + keyspace + ";");
            sessions.add(session);
            return session;
        }

        public synchronized void teardown() throws Throwable
        {
            printTPCStats();

            JVMStabilityInspector.removeShutdownHooks();

            for(Session session : sessions)
            {
                session.close();
            }
            clientCluster.close();
            CQLTester.tearDownClass();
            CQLTester.cleanup();
        }
    }

    public static class PerThreadSession
    {
        Session session;
        int inflight;
        PreparedStatement statement;

        public void setupSessionAndStatements(CassandraSetup globalState, BaseBenchmark benchmark)
        {
            session = globalState.newSession();
            inflight = globalState.perThreadInflight;
            statement = benchmark.createStatement(session, globalState.table);
        }
    }

    protected PreparedStatement createStatement(Session session, String table)
    {
        throw new UnsupportedOperationException();
    }

    protected void prePopulateTable(CassandraSetup cassandraSetup)
    {
        ColumnFamilyStore cfs = Keyspace.open(cassandraSetup.keyspace).getColumnFamilyStore(cassandraSetup.table);
        cfs.disableAutoCompaction();
        List<ResultSetFuture> futures = new ArrayList<>(inflight);
        System.err.println("Writing " + populationSize);
        int lastPrint = 0;
        final Session session = cassandraSetup.sessionNet(ProtocolVersion.CURRENT);
        final PreparedStatement write = prepareWrite(session, cassandraSetup.table);

        for (int i = 0; i < populationSize; i += inflight)
        {
            int max = (int) Math.min(inflight, populationSize - i);
            for (int j = 0; j < max; ++j)
            {
                long v = i + j;
                futures.add(insertRowForValue(session, write, v));
            }
            FBUtilities.waitOnFutures(futures);
            futures.clear();

            if (i - lastPrint > populationSize / 100)
            {
                System.out.print(".");
                lastPrint = i;
            }
        }
        if (flush)
            cfs.forceBlockingFlush();

        // Needed to stabilize sstable count for off-cache sized tests (e.g. count = 100_000_000)
        if (stabiliseSSCount)
        {
            while (cfs.getLiveSSTables().size() >= 15)
            {
                cfs.enableAutoCompaction(true);
                cfs.disableAutoCompaction();
            }
        }
    }

    protected PreparedStatement prepareWrite(Session session, String table)
    {
        return session.prepare("INSERT INTO " + table + "(userid,picid,commentid)VALUES(?,?,?)");
    }

    protected ResultSetFuture insertRowForValue(Session session, PreparedStatement write, long v)
    {
        return session.executeAsync(write.bind(v, v, v));
    }

    protected String table()
    {
        return "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) " +
               (compression ? "with compression = { 'sstable_compression' : 'LZ4Compressor' }" : "with compression = {'enabled': false}");
    }

    protected String keyspace()
    {
        return "CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false";
    }

    @Setup(Level.Invocation)
    public void throttle()
    {
        if (throttleMs == 0)
            return;
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(throttleMs));
    }

    long incrementAndGetOpCounter()
    {
        return opCounter.incrementAndGet();
    }


    public Object executeInflight(Supplier<Long> nextKey, PerThreadSession state) throws Throwable
    {
        List<ResultSetFuture> futures = new ArrayList<>(inflight);
        for (int i = 0; i < state.inflight; i++)
        {
            final long queryKey = nextKey.get();
            futures.add(executeForKey(state, queryKey));
        }

        FBUtilities.waitOnFutures(futures);

        List<Row> rows = futures.stream().map(f -> f.getUninterruptibly().one()).collect(Collectors.toList());
        futures.clear();

        if (rows.size() == 0)
            System.err.println("EMPTY");
        return rows;
    }

    protected ResultSetFuture executeForKey(PerThreadSession state, long key)
    {
        return state.session.executeAsync(state.statement.bind(key));
    }

}
