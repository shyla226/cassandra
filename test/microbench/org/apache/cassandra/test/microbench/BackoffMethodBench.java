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
import java.util.concurrent.locks.LockSupport;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Demostrates observed descheduling for different sleep/deschedule methods one might use for a polling thread.
 * The costs will differ based on OS/HW/JVM. But here's the results collected on recent combination:
 * <code>
 * Benchmark                                               Mode     Cnt       Score     Error  Units
 * BackoffMethodBench.sleepMicro1:sleepMicro1·p0.00      sample            2484.000            ns/op
 * BackoffMethodBench.sleepMicro1:sleepMicro1·p0.50      sample           53312.000            ns/op
 * BackoffMethodBench.sleepMicro1:sleepMicro1·p0.90      sample           53440.000            ns/op
 * BackoffMethodBench.sleepMicro1:sleepMicro1·p0.95      sample           53632.000            ns/op
 * BackoffMethodBench.sleepMicro1:sleepMicro1·p0.99      sample           55040.000            ns/op
 * BackoffMethodBench.sleepMicro1:sleepMicro1·p0.999     sample           60081.920            ns/op
 * BackoffMethodBench.sleepMicro1:sleepMicro1·p0.9999    sample           91895.488            ns/op
 * BackoffMethodBench.sleepMicro1:sleepMicro1·p1.00      sample          327168.000            ns/op
 * BackoffMethodBench.sleepMicro50                       sample   48744  102473.354 ±  28.890  ns/op
 * BackoffMethodBench.sleepMicro50:sleepMicro50·p0.00    sample           58112.000            ns/op
 * BackoffMethodBench.sleepMicro50:sleepMicro50·p0.50    sample          102272.000            ns/op
 * BackoffMethodBench.sleepMicro50:sleepMicro50·p0.90    sample          102528.000            ns/op
 * BackoffMethodBench.sleepMicro50:sleepMicro50·p0.95    sample          103040.000            ns/op
 * BackoffMethodBench.sleepMicro50:sleepMicro50·p0.99    sample          105216.000            ns/op
 * BackoffMethodBench.sleepMicro50:sleepMicro50·p0.999   sample          136257.280            ns/op
 * BackoffMethodBench.sleepMicro50:sleepMicro50·p0.9999  sample          148864.512            ns/op
 * BackoffMethodBench.sleepMicro50:sleepMicro50·p1.00    sample          155904.000            ns/op
 * BackoffMethodBench.sleepNano1                         sample  188135     451.809 ±   0.534  ns/op
 * BackoffMethodBench.sleepNano1:sleepNano1·p0.00        sample             421.000            ns/op
 * BackoffMethodBench.sleepNano1:sleepNano1·p0.50        sample             441.000            ns/op
 * BackoffMethodBench.sleepNano1:sleepNano1·p0.90        sample             486.000            ns/op
 * BackoffMethodBench.sleepNano1:sleepNano1·p0.95        sample             490.000            ns/op
 * BackoffMethodBench.sleepNano1:sleepNano1·p0.99        sample             548.000            ns/op
 * BackoffMethodBench.sleepNano1:sleepNano1·p0.999       sample             698.864            ns/op
 * BackoffMethodBench.sleepNano1:sleepNano1·p0.9999      sample            3812.746            ns/op
 * BackoffMethodBench.sleepNano1:sleepNano1·p1.00        sample            9152.000            ns/op
 * BackoffMethodBench.sleepNano100                       sample   77199   38804.622 ± 271.504  ns/op
 * BackoffMethodBench.sleepNano100:sleepNano100·p0.00    sample             440.000            ns/op
 * BackoffMethodBench.sleepNano100:sleepNano100·p0.50    sample           52352.000            ns/op
 * BackoffMethodBench.sleepNano100:sleepNano100·p0.90    sample           52480.000            ns/op
 * BackoffMethodBench.sleepNano100:sleepNano100·p0.95    sample           52672.000            ns/op
 * BackoffMethodBench.sleepNano100:sleepNano100·p0.99    sample           54080.000            ns/op
 * BackoffMethodBench.sleepNano100:sleepNano100·p0.999   sample           78259.200            ns/op
 * BackoffMethodBench.sleepNano100:sleepNano100·p0.9999  sample           94499.840            ns/op
 * BackoffMethodBench.sleepNano100:sleepNano100·p1.00    sample          176896.000            ns/op
 * BackoffMethodBench.yield                              sample  188225     243.000 ±   0.740  ns/op
 * BackoffMethodBench.yield:yield·p0.00                  sample             209.000            ns/op
 * BackoffMethodBench.yield:yield·p0.50                  sample             241.000            ns/op
 * BackoffMethodBench.yield:yield·p0.90                  sample             244.000            ns/op
 * BackoffMethodBench.yield:yield·p0.95                  sample             256.000            ns/op
 * BackoffMethodBench.yield:yield·p0.99                  sample             333.000            ns/op
 * BackoffMethodBench.yield:yield·p0.999                 sample             491.000            ns/op
 * BackoffMethodBench.yield:yield·p0.9999                sample            3704.967            ns/op
 * BackoffMethodBench.yield:yield·p1.00                  sample           25760.000            ns/op
 * </code>
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1)
@State(Scope.Benchmark)
public class BackoffMethodBench
{
    @Benchmark
    public void yield()
    {
        Thread.yield();
    }

    @Benchmark
    public void sleepNano1()
    {
        LockSupport.parkNanos(1);
    }

    @Benchmark
    public void sleepNano100()
    {
        LockSupport.parkNanos(100);
    }

    @Benchmark
    public void sleepMicro1()
    {
        LockSupport.parkNanos(1000);
    }

    @Benchmark
    public void sleepMicro50()
    {
        LockSupport.parkNanos(50000);
    }
}
