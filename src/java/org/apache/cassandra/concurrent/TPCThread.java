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

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * A TPC thread, that is one of the core threads running our internal event loops.
 * <p>
 * We create exactly as many TPC threads as the number of configured cores (see {@link DatabaseDescriptor#getTPCCores()})
 * and each of those threads has an associated core ID ({@link #coreId}).
 */
public class TPCThread extends FastThreadLocalThread
{
    private final int coreId;

    private EpollTPCEventLoopGroup.SingleCoreEventLoop eventLoop;

    private TPCThread(ThreadGroup group, Runnable target, int coreId)
    {
        super(group, target, "CoreThread-" + coreId);
        this.coreId = coreId;
    }

    public int coreId()
    {
        return coreId;
    }


    /**
     * Creates a new factory to create TPC threads.
     * <p>
     * This factory makes the assumptions that it is only used to create true TPC threads, and thus that exactly
     * {@link DatabaseDescriptor#getTPCCores()} of them will be created. Also note that each call creates a new
     * factory so in practice this should only be called once per-runtime and the factory used to create all the
     * TPC threads. However, some jmh benchmarks reuse our TPC event loop (typically {@link EpollTPCEventLoopGroup})
     * and that will use a new factory every time, which is fine for those tests but means we don't want to make
     * that factory a static singleton.
     */
    static Executor newTPCThreadFactory()
    {
        return new TPCThreadsCreator();
    }

    public TPCMetrics metrics()
    {
        return TPC.metrics(coreId);
    }

    public EpollTPCEventLoopGroup.SingleCoreEventLoop eventLoop()
    {
        return eventLoop;
    }

    public void eventLoop(EpollTPCEventLoopGroup.SingleCoreEventLoop eventLoop)
    {
        this.eventLoop = eventLoop;
    }

    /**
     * An {@link Executor} that creates a new TPC thread for every new command passed. Newly created TPC threads are
     * created with increasing core ID. This is to be used when creating the events loop (within the
     * {@link TPCEventLoopGroup}) and shouldn't be used anywhere else.
     */
    public static class TPCThreadsCreator implements Executor
    {
        private final Factory factory = new Factory();
        private volatile TPCThread lastCreatedThread;

        public void execute(Runnable runnable)
        {
            factory.newThread(runnable).start();
        }

        public TPCThread lastCreatedThread()
        {
            return lastCreatedThread;
        }

        // Implementation note: we extend DefaultThreadFactory because that makes it much easier to create
        // FastThreadLocalThread, but we completely ignore the name-generation part of the class. Instead, thread names
        // are set in the TPCThread ctor above.
        private class Factory extends DefaultThreadFactory
        {
            private final AtomicInteger coreIdGenerator = new AtomicInteger();

            private Factory()
            {
                super(TPCThread.class, true, Thread.MAX_PRIORITY);
            }

            protected Thread newThread(Runnable r, String name)
            {
                lastCreatedThread = new TPCThread(this.threadGroup, r, coreIdGenerator.getAndIncrement());
                return lastCreatedThread;
            }
        }
    }
}
