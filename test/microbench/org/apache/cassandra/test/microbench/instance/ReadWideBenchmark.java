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
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

public class ReadWideBenchmark  extends ReadBenchmark
{
    @Param("4")
    int partitionCount;

    @Override
    protected ResultSetFuture insertRowForValue(Session session, PreparedStatement write, long v)
    {
        return session.executeAsync(write.bind(v % partitionCount, v, v));
    }

    @Override
    protected ResultSetFuture executeForKey(PerThreadSession state, long key)
    {
        return state.session.executeAsync(state.statement.bind(key % partitionCount, key));
    }

    @Override
    protected PreparedStatement createStatement(Session session, String table)
    {
        return session.prepare("SELECT * from " + table + " where userid = ? and picid = ?");
    }
}
