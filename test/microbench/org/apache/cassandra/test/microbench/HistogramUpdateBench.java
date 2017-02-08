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

import org.apache.cassandra.metrics.Histogram;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import sun.misc.Contended;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = {
      "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.concurrent.MonitoredEpollEventLoopGroup",
      "-Dagrona.disable.bounds.checks=TRUE"
//      ,"-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintAssembly"
//       ,"-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder","-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints",
//       "-XX:StartFlightRecording=duration=60s,filename=./profiling-data.jfr,name=profile,settings=profile",
//       "-XX:FlightRecorderOptions=settings=/home/stefi/profiling-advanced.jfc,samplethreads=true"
})
@Threads(-1)
@State(Scope.Benchmark)
public class HistogramUpdateBench
{
    static final int UPDATE_TIME_MILLIS = 1000;
    static final long testValueLevel = 12340;
    Histogram histogram;

    @Setup
    public void setup()
    {
       histogram = Histogram.make(Histogram.DEFAULT_ZERO_CONSIDERATION, Histogram.DEFAULT_MAX_TRACKABLE_VALUE, UPDATE_TIME_MILLIS, false);
    }

    @State(Scope.Thread)
    public static class ThreadState
    {
        @Contended
        long i;
    }

    @Benchmark
    public void histogramUpdate(ThreadState state)
    {
        histogram.update(testValueLevel + (state.i++ & 0x800));
    }
}
