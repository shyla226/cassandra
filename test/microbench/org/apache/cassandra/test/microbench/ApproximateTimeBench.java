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

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@State(Scope.Benchmark)
public class ApproximateTimeBench
{

    @Benchmark
    public long nanoTime()
    {
        return System.nanoTime();
    }

    @Benchmark
    public long currentTimeMillis()
    {
        return System.currentTimeMillis();
    }
    @Benchmark
    public long approximateNanoTime()
    {
        return ApproximateTime.nanoTime();
    }

    @Benchmark
    public long approximateCurrentTimeMillis()
    {
        return ApproximateTime.currentTimeMillis();
    }

    @Benchmark
    public long approximateMillisTime()
    {
        return ApproximateTime.millisTime();
    }
}
