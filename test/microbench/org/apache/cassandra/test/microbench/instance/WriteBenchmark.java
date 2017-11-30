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

import com.datastax.driver.core.*;
import org.openjdk.jmh.annotations.Benchmark;

public class WriteBenchmark extends BaseBenchmark
{
    @Override
    protected PreparedStatement createStatement(Session session, String table)
    {
        return prepareWrite(session, table);
    }

    @Override
    protected ResultSetFuture executeForKey(PerThreadSession state, long key)
    {
        return insertRowForValue(state.session, state.statement, key);
    }

    @Benchmark
    public Object insertSequential(PerThreadSession state) throws Throwable
    {
        return executeInflight(() -> incrementAndGetOpCounter() + populationSize, state);
    }

    @Benchmark
    public Object updateSequential(PerThreadSession state) throws Throwable
    {
        return executeInflight(() -> incrementAndGetOpCounter() & populationSize, state);
    }

    @Benchmark
    public Object updateRandom(PerThreadSession state) throws Throwable
    {
        final ThreadLocalRandom tlr = ThreadLocalRandom.current();

        return executeInflight(() -> tlr.nextLong() % populationSize, state);
    }
}
