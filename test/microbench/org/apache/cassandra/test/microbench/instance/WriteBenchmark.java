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
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.utils.FBUtilities;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

public class WriteBenchmark extends BaseBenchmark
{

    /** Boiler plate start */
    @State(Scope.Benchmark)
    public static class GlobalState extends CassandraSetup
    {
        @Setup(Level.Trial)
        public void setup(WriteBenchmark benchmark, BenchmarkParams params) throws Throwable
        {
            super.setup(benchmark, params);
        }

        @TearDown
        public void teardown() throws Throwable
        {
            super.teardown();
        }
    }

    @State(Scope.Thread)
    public static class ThreadState extends PerThreadSession
    {
        @Setup(Level.Trial)
        public void setup(GlobalState g, WriteBenchmark b) throws Throwable
        {
            super.setupSessionAndStatements(g, b);
        }
    }

    /** Boiler plate ends */
    @Override
    protected PreparedStatement createStatement(Session session, String table)
    {
        return prepareWrite(session, table);
    }


    @Benchmark
    public Object insertSequential(ThreadState state) throws Throwable
    {
        return executeInflight(() -> incrementAndGetOpCounter() + populationSize, state);
    }

    @Benchmark
    public Object updateSequential(ThreadState state) throws Throwable
    {
        return executeInflight(() -> incrementAndGetOpCounter() & populationSize, state);
    }

    @Benchmark
    public Object updateRandom(ThreadState state) throws Throwable
    {
        final ThreadLocalRandom tlr = ThreadLocalRandom.current();

        return executeInflight(() -> tlr.nextLong() % populationSize, state);
    }
}
