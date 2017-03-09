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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.utils.FBUtilities;
import org.openjdk.jmh.annotations.Benchmark;
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
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-ea", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor" })
@Threads(4) // make sure this matches the number of _physical_cores_
@State(Scope.Benchmark)
public class DynamicEndpointSnitchBench
{
    @Param({ "false", "true" })
    private boolean dynamic = false;
    @Param({ "250" })
    private int nodes = 250;

    private InetAddress[] addresses;
    private IEndpointSnitch snitch;

    @Setup
    public void setup()
    {
        DatabaseDescriptor.toolInitialization();

        IEndpointSnitch parentSnitch = new SimpleSnitch();
        snitch = dynamic ?
                 new DynamicEndpointSnitch(parentSnitch) :
                 parentSnitch;

        addresses = new InetAddress[nodes];
        for (int i = 0; i < nodes; i++)
        {
            try
            {
                addresses[i] = InetAddress.getByAddress(new byte[]{ 1, 0, (byte) (nodes >> 8), (byte) nodes });
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    @State(Scope.Thread)
    public static class LocalBenchState
    {
        int index = (ThreadLocalRandom.current().nextInt() & 0x7ffffff);
    }

    @Benchmark
    public void updateSame()
    {
        // super stupid pseudo-latency
        update(addresses[0]);
    }

    @Benchmark
    public void update(LocalBenchState state)
    {
        int n = (state.index++) % nodes;
        update(addresses[n]);
    }

    private void update(InetAddress address)
    {
        // super stupid pseudo-latency
        long latency = (Thread.currentThread().getId() ^ System.currentTimeMillis()) & 32767;

        ((DynamicEndpointSnitch) snitch).receiveTiming(address, latency);
    }

    @Benchmark
    public void sort(LocalBenchState state, Blackhole bh)
    {
        int n = (state.index++) % nodes;

        List<InetAddress> cmp = new ArrayList<>(3);
        for (int i = 0; i < 3; i++)
        {
            cmp.add(addresses[n++]);
            if (n == addresses.length)
                n = 0;
        }

        snitch.sortByProximity(FBUtilities.getBroadcastAddress(), cmp);
        bh.consume(cmp);
    }

}
