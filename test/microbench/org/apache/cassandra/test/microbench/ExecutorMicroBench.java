
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.concurrent.MonitoredEpollEventLoopGroup;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmark for eventloops
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 10)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class ExecutorMicroBench {
    @State(Scope.Thread)
    public static class MonitoredState {
        @Param({"10"})
        public int count;
        public int value = 0;

        public MonitoredEpollEventLoopGroup executor;

        @Setup
        public void setup() {
            executor = new MonitoredEpollEventLoopGroup(1);
        }

        @TearDown
        public void teardown() {
            executor.shutdown();
        }
    }


    @State(Scope.Thread)
    public static class ExecutorState {

        @Param({"10"})
        public int count;
        public int value = 0;

        public EpollEventLoopGroup executor;

        @Setup
        public void setup() {
            executor = new EpollEventLoopGroup(1);
        }

        @TearDown
        public void teardown() {
            executor.shutdown();
        }
    }

    static void await(int count, CountDownLatch latch) throws Exception {
        if (count < 1000) {
            while (latch.getCount() != 0) ;
        } else {
            latch.await();
        }
    }

    static void run2(ExecutorState state, CountDownLatch cdl)
    {
        if (++state.value == state.count)
        {
            state.value = 0;
            cdl.countDown();
        }
        else
        {
            state.executor.submit(() -> run2(state, cdl));
        }
    }


    @Benchmark
    public void executor(ExecutorState state, Blackhole bh) throws Exception {

        CountDownLatch cdl = new CountDownLatch(1);

        state.executor.submit(() -> run2(state, cdl));
        await(state.count, cdl);
    }


    static void run1(MonitoredState state, CountDownLatch cdl)
    {
        if (++state.value == state.count)
        {
            state.value = 0;
            cdl.countDown();
        }
        else
        {
            state.executor.submit(() -> run1(state, cdl));
        }
    }


    @Benchmark
    public void monitored(MonitoredState state, Blackhole bh) throws Exception {

        CountDownLatch cdl = new CountDownLatch(1);

        state.executor.submit(() -> run1(state, cdl));

        await(state.count, cdl);
    }


   // @Benchmark
    public void forkjoin(ExecutorState state, Blackhole bh) throws Exception {

        CountDownLatch cdl = new CountDownLatch(1);

        ForkJoinPool fj = ForkJoinPool.commonPool();

        int c = state.count;
        for (int i = 0; i < c; i++) {
            int j = i;
            fj.submit(() -> {
                if (j == c - 1)
                {
                    cdl.countDown();
                }
            });
        }

        await(c, cdl);
    }
}

