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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 15, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class ReadWriteTestWide extends CQLTester
{
    static String keyspace;
    String table;
    String writeStatement;
    String readStatement;
    long numRows = 0;
    ColumnFamilyStore cfs;
    static final int count = 2_300_000;
    static final long row_count = 4;
    Random rand = new Random(1);

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}");
        execute("use "+keyspace+";");
        writeStatement = "INSERT INTO "+table+"(userid,picid,commentid)VALUES(?,?,?)";
        readStatement = "SELECT * from "+table+" where userid=? and picid=?";

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        //Warm up
        System.err.println("Writing " + count);
        for (long i = 0; i < count; i++)
            execute(writeStatement, i % row_count, i, i );
        cfs.forceBlockingFlush();
        cfs.enableAutoCompaction(true);
        cfs.disableAutoCompaction();
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CQLTester.cleanup();
    }

    @Benchmark
    public Object write() throws Throwable
    {
        numRows++;
        return execute(writeStatement, numRows, numRows, numRows );
    }


    @Benchmark
    public Object readRandom() throws Throwable
    {
        return executeRead(rand.nextInt(count), 0);
    }

    @Benchmark
    public Object readRandomWOutside() throws Throwable
    {
        return executeRead(rand.nextInt(count), rand.nextInt(6) == 1 ? 1 : 0);
    }

    @Benchmark
    public Object readFixed() throws Throwable
    {
        return executeRead(1234567890123L % count, 0);
    }

    @Benchmark
    public Object readOutside() throws Throwable
    {
        return executeRead(1234567890123L % count, 1);
    }

    public Object executeRead(long arg, int rowofs) throws Throwable
    {
        return execute(readStatement, (arg + rowofs) % row_count, arg);
    }

    @Benchmark
    public Object readGreaterMatch() throws Throwable
    {
        String statement = "SELECT * from "+table+" where userid=? and picid>? limit 1";
        long arg = rand.nextInt(count);
        return execute(statement, arg % row_count, arg);
    }

    @Benchmark
    public Object readReversedMatch() throws Throwable
    {
        String statement = "SELECT * from "+table+" where userid=? and picid<? order by picid desc limit 1";
        long arg = rand.nextInt(count);
        return execute(statement, arg % row_count, arg);
    }

    @Benchmark
    public Object readGreater() throws Throwable
    {
        String statement = "SELECT * from "+table+" where userid=? and picid>? limit 1";
        long arg = rand.nextInt(count);
        return execute(statement, arg % row_count, arg - 1);
    }

    @Benchmark
    public Object readReversed() throws Throwable
    {
        String statement = "SELECT * from "+table+" where userid=? and picid<? order by picid desc limit 1";
        long arg = rand.nextInt(count);
        return execute(statement, arg % row_count, arg + 1);
    }

}
