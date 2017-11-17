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

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.AbstractEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * A TPC event loop group that uses NIO for I/O tasks.
 */
public class NioTPCEventLoopGroup extends NioEventLoopGroup implements TPCEventLoopGroup
{
    private final ImmutableList<SingleCoreEventLoop> eventLoops;

    /**
     * Creates new a {@code NioTPCEventLoopGroup} using the provided number of event loops.
     *
     * @param nThreads the number of event loops to use.
     */
    NioTPCEventLoopGroup(int nThreads)
    {
        super(nThreads, TPCThread.newTPCThreadFactory());
        this.eventLoops = ImmutableList.copyOf(Iterables.transform(this, e -> (SingleCoreEventLoop) e));
    }

    public ImmutableList<? extends TPCEventLoop> eventLoops()
    {
        return eventLoops;
    }

    protected EventLoop newChild(Executor executor, Object... args) throws Exception
    {
        assert executor instanceof TPCThread.TPCThreadsCreator;
        return new SingleCoreEventLoop(super.newChild(executor, args), (TPCThread.TPCThreadsCreator)executor);
    }

    // Overriding to avoid direct cast of SingleCoreEventLoop to NioEventLoop, see SingleCoreEventLoop implementation
    // note for details.
    @Override
    public void setIoRatio(int ioRatio) {
        for (EventExecutor eventExecutor : this)
        {
            SingleCoreEventLoop eventLoop = (SingleCoreEventLoop) eventExecutor;
            ((NioEventLoop) eventLoop.nettyLoop).setIoRatio(ioRatio);
        }
    }

    // Implementation note: we'd really want to extend Netty NioEventLoop here, but its ctor is package protected (and
    // putting this in a Netty package to make it work would be really ugly). So instead, we wrap said NioEventLoop
    // and delegate to it (we extend AbstractEventExecutor, which is largely an empty shell, to simplify a bit).
    // This is a tad ugly but doesn't feel like a huge deal either.
    // In the future, maybe Netty would be amenable to make that ctor protected, which would be more consistent with
    // the EpollEventLoop class, which could also allow us to do optimizations akin to the one in EpollTPCEventLoopGroup
    // if we so wished.
    private static class SingleCoreEventLoop extends AbstractEventExecutor implements TPCEventLoop
    {
        private final EventLoop nettyLoop;
        private final TPCThread thread;

        private SingleCoreEventLoop(EventLoop nettyLoop, TPCThread.TPCThreadsCreator executor)
        {
            super(nettyLoop.parent());

            this.nettyLoop = nettyLoop;

            // Start the loop, which forces the creation of the Thread using 'executor' so we can get a reference to it
            // easily.
            Futures.getUnchecked(nettyLoop.submit(() -> {}));

            this.thread = executor.lastCreatedThread();
            assert this.thread != null;
        }

        public TPCThread thread()
        {
            return thread;
        }

        @Override
        public TPCEventLoopGroup parent()
        {
            return (TPCEventLoopGroup)super.parent();
        }

        public EventLoop next()
        {
            return (EventLoop)super.next();
        }

        public ChannelFuture register(Channel channel)
        {
            return nettyLoop.register(channel);
        }

        public ChannelFuture register(ChannelPromise channelPromise)
        {
            return nettyLoop.register(channelPromise);
        }

        public ChannelFuture register(Channel channel, ChannelPromise channelPromise)
        {
            return nettyLoop.register(channel, channelPromise);
        }

        public boolean isShuttingDown()
        {
            return nettyLoop.isShuttingDown();
        }

        public Future<?> shutdownGracefully(long l, long l1, TimeUnit timeUnit)
        {
            return nettyLoop.shutdownGracefully(l, l1, timeUnit);
        }

        public Future<?> terminationFuture()
        {
            return nettyLoop.terminationFuture();
        }

        public void shutdown()
        {
            nettyLoop.shutdown();
        }

        public boolean isShutdown()
        {
            return nettyLoop.isShutdown();
        }

        public boolean isTerminated()
        {
            return nettyLoop.isTerminated();
        }

        public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException
        {
            return nettyLoop.awaitTermination(l, timeUnit);
        }

        public boolean inEventLoop(Thread thread)
        {
            return nettyLoop.inEventLoop();
        }

        @Override
        public boolean canExecuteImmediately(TPCTaskType taskType)
        {
            if (coreId() != TPC.getCoreId())
                return false;
            return true;
        }

        public void execute(Runnable runnable)
        {
            nettyLoop.execute(runnable);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
            return nettyLoop.schedule(command, delay, unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
        {
            return nettyLoop.schedule(callable, delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
        {
            return nettyLoop.scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
        {
            return nettyLoop.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }
    }
}
