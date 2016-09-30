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

package org.apache.cassandra.concurrent;

import java.io.IOError;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.affinity.CpuLayout;
import net.openhft.affinity.impl.VanillaCpuLayout;
import org.apache.cassandra.utils.FBUtilities;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.MpscChunkedArrayQueue;
import org.jctools.queues.SpscArrayQueue;

/**
 * Created by jake on 3/22/16.
 */
public class MonitoredTPCExecutorService
{
    private static final Logger logger = LoggerFactory.getLogger(MonitoredTPCExecutorService.class);

    private final static MonitoredTPCExecutorService INSTANCE = new MonitoredTPCExecutorService();

    public final static MonitoredTPCExecutorService instance()
    {
        return INSTANCE;
    }

    private final CpuLayout layout;
    private final Thread monitorThread;
    private final SingleCoreExecutor runningCores[];
    private final Thread runningThreads[];

    private MonitoredTPCExecutorService()
    {
        try
        {
            layout = VanillaCpuLayout.fromCpuInfo();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        int nettyThreads = Integer.valueOf(System.getProperty("io.netty.eventLoopThreads", "7"));
        int totalCPUs = Runtime.getRuntime().availableProcessors();
        int allocatedCPUs = FBUtilities.getAvailableProcessors();

        if (nettyThreads > allocatedCPUs)
            throw new RuntimeException("io.netty.eventLoop is set to high. Should be <= 20% of cores");

        int start = (totalCPUs - allocatedCPUs) + nettyThreads;
        int end = totalCPUs;

        runningCores = new SingleCoreExecutor[end - start];
        runningThreads = new Thread[runningCores.length];
        for (int i = 0; i < runningCores.length; i++)
        {
            int cpuId = start + i;

            runningCores[i] = new SingleCoreExecutor(i, cpuId, layout.coreId(cpuId), layout.socketId(cpuId));

            Thread owner = new Thread(runningCores[i]);
            runningThreads[i] = owner;
            owner.setName("monitor-executive-service-cpu-"+cpuId);
            owner.setDaemon(true);
            owner.setPriority(Thread.MAX_PRIORITY);
            owner.start();
        }

        monitorThread = new Thread(() -> {
            int length = runningCores.length;

            while (true)
            {
                for (int i = 0; i < length; i++)
                {
                    LockSupport.parkNanos(1);
                    runningCores[i].checkQueue();
                }
            }
        });

        monitorThread.setName("monitor-executive-service-thread");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    public void shutdown()
    {
        monitorThread.interrupt();
        for (SingleCoreExecutor executor : runningCores)
            executor.shutdown();
    }

    public SingleCoreExecutor any()
    {
        return runningCores[ThreadLocalRandom.current().nextInt(runningCores.length)];
    }

    public SingleCoreExecutor one(int i)
    {
        return runningCores[i % runningCores.length];
    }

    private enum CoreState
    {
        PARKED, WORKING
    }

    public class SingleCoreExecutor implements Runnable
    {
        private static final int maxExtraSpins = 1024 * 10;
        private static final int yieldExtraSpins = 1024 * 8;
        private static final int parkExtraSpins = 1024 * 9;

        private final int threadOffset;
        private final int cpuId;
        private final int coreId;
        private final int socketId;
        private volatile CoreState state;
        private final MessagePassingQueue<FutureTask<?>> runQueue;

        private SingleCoreExecutor(int threadOffset, int cpuId, int coreId, int socketId)
        {
            this.threadOffset = threadOffset;
            this.cpuId = cpuId;
            this.coreId = coreId;
            this.socketId = socketId;
            this.runQueue = new SpscArrayQueue<>(1 << 20);
            this.state = CoreState.WORKING;
        }

        private void park()
        {
            //logger.info("{} parking", cpuId);
            state = CoreState.PARKED;
            LockSupport.park();
        }

        private void unpark()
        {
            //logger.info("{} unparking", cpuId);
            state = CoreState.WORKING;
            LockSupport.unpark(runningThreads[threadOffset]);
        }

        private void checkQueue()
        {
            if (state == CoreState.PARKED && !runQueue.isEmpty())
                unpark();
        }

        public FutureTask<?> addTask(FutureTask<?> futureTask)
        {
            if (!runQueue.offer(futureTask))
                throw new RuntimeException("Backpressure");

            return futureTask;
        }

        protected void onCompletion()
        {

        }


        public void shutdown()
        {
            //runQueue.getRefProcessorThread().stop();
            runningThreads[threadOffset].stop();
        }

        public List<Runnable> shutdownNow()
        {
            return null;
        }

        public boolean isShutdown()
        {
            return false;
        }

        public boolean isTerminated()
        {
            return false;
        }

        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
            return false;
        }

        //Event loop
        public void run()
        {
            try
            {
                logger.info("Assigning {} to cpu {} on core {} on socket {}", Thread.currentThread().getName(), cpuId, coreId, socketId);
                //AffinitySupport.setAffinity(1L << cpuId);
                MessagePassingQueue<FutureTask<?>> runQueue = this.runQueue;

                while (true)
                {
                    //deal with spurious wakeups
                    if (state == CoreState.WORKING)
                    {
                        int spins = 0;
                        int processed = 0;
                        while(true)
                        {
                            processed = runQueue.drain(FutureTask::run);
                            if(processed > 0 || ++spins < yieldExtraSpins)
                                continue;
                            else if (spins < parkExtraSpins)
                                Thread.yield();
                            else if (spins < maxExtraSpins)
                                LockSupport.parkNanos(1);
                            else
                                break;
                        }
                    }

                    //Nothing todo; park
                    park();
                }
            }
            finally
            {
                //AffinitySupport.setAffinity(AffinityLock.BASE_AFFINITY);
                logger.info("Shutting down event loop");
            }
        }
    }
}
