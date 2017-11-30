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


import java.util.concurrent.ThreadLocalRandom;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.openjdk.jmh.annotations.Benchmark;

public class ReadBenchmark extends BaseBenchmark
{
    @Override
    protected PreparedStatement createStatement(Session session, String table)
    {
        return session.prepare("SELECT * from " + table + " where userid = ? limit 5000");
    }

    @Benchmark
    public Object readSequential(PerThreadSession state) throws Throwable
    {
        return executeInflight(() -> incrementAndGetOpCounter() % populationSize, state);
    }


    @Benchmark
    public Object readFixed(PerThreadSession state) throws Throwable
    {
        return executeInflight(() -> 1231231231L % populationSize, state);
    }

    @Benchmark
    public Object readFail(PerThreadSession state) throws Throwable
    {
        return executeInflight(() -> populationSize + ThreadLocalRandom.current().nextLong(populationSize), state);
    }

    @Benchmark
    public Object readRandomNoFail(PerThreadSession state) throws Throwable
    {
        return executeInflight(() -> ThreadLocalRandom.current().nextLong(populationSize), state);
    }

    @Benchmark
    public Object readRandomHalfFail(PerThreadSession state) throws Throwable
    {
        final ThreadLocalRandom tlr = ThreadLocalRandom.current();
        return executeInflight(() -> (tlr.nextBoolean() ? populationSize : 0) + tlr.nextLong(populationSize), state);
    }
}
