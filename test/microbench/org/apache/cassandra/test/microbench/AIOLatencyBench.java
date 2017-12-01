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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.AsynchronousChannelProxy;
import org.apache.cassandra.io.util.ChunkReader;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1,  jvmArgsPrepend = {"-XX:+UseSerialGC", "-Xms8G", "-Xmx8G", "-Xmn4G", "-Ddse.io.aio.force=true"})
//@Fork(value = 1,  jvmArgsPrepend = {"-Ddse.io.aio.force=true"})
@Threads(1)
@State(Scope.Benchmark)
public class AIOLatencyBench
{
    private Random random;
    private ByteBuffer[] buffers;
    private File file;
    private AsynchronousChannelProxy channel;
    private AsynchronousChannelProxy batchedChannel;
    private ChunkReader chunkReader;
    private CompletableFuture<ByteBuffer>[] futures;

    public enum BatchingType
    {
        NONE,
        SIMPLE,
        VECTORED
    }

    @Param({ "4096", "8192"})
    private int chunkSize = 4096;

    @Param({"8", "16"})
    private int numBuffers = 8;

    @Param({"NONE", "SIMPLE", "VECTORED"})
    private BatchingType batchingType = BatchingType.NONE;

    //@Param({"1", "5", "10"})
    //private int backOffMillis = 1;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();

        random = new Random(System.currentTimeMillis());
        buffers = new ByteBuffer[numBuffers];
        for (int i = 0; i < numBuffers; i++)
            buffers[i] = ByteBuffer.allocateDirect(chunkSize);

        if (!TPC.USE_AIO)
            throw new IllegalStateException("This benchmark is for AIO reads but AIO is not enabled");

        // if testing different disks, replace "." with the right path, e.g. /mnt/xxx
        file = new File(".", "AIOLatencyBench.tmp");
        file.deleteOnExit();

        try(FileChannel writeChannel = new FileOutputStream(file, false).getChannel())
        {
            for (int i = 0; i < numBuffers; i++)
            {
                byte[] data = new byte[chunkSize];
                random.nextBytes(data);
                writeChannel.write(ByteBuffer.wrap(data));
            }
            writeChannel.force(true);
        }

        channel = new AsynchronousChannelProxy(file, false);
        if (batchingType == BatchingType.NONE)
        {
            chunkReader = ChunkReader.simple(channel, file.length(), BufferType.OFF_HEAP, chunkSize);
        }
        else
        {
            batchedChannel = channel.maybeBatched(batchingType == BatchingType.VECTORED);
            chunkReader = ChunkReader.simple(batchedChannel, file.length(), BufferType.OFF_HEAP, chunkSize);
        }

        futures = new CompletableFuture[numBuffers];
    }

//    @Setup(Level.Invocation)
//    public void backoff() throws InterruptedException
//    {
//        Thread.sleep(backOffMillis);
//    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        channel.close();
        chunkReader.close();

        if (batchedChannel != null)
            batchedChannel.close();
    }

    @Benchmark
    public void readBuffers() throws Throwable
    {
        for (int i = 0; i < numBuffers; i++)
            futures[i] = chunkReader.readChunk(chunkSize * i, buffers[i]);

        if (batchedChannel != null)
            batchedChannel.submitBatch();

        CompletableFuture.allOf(futures).join();
    }
}
