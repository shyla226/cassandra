package org.apache.cassandra.cql3.continuous.paging;

import java.net.SocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.AbstractEventLoop;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import org.mockito.Mockito;

public class ContinuousPagingTestStubs
{
    public static class RecordingScheduler extends TestScheduler
    {
        public List<Runnable> commands = new CopyOnWriteArrayList<>();

        @Override
        public Disposable scheduleDirect(Runnable command)
        {
            commands.add(command);
            return Mockito.mock(Disposable.class);
        }

        @Override
        public Disposable scheduleDirect(Runnable command, long delay, TimeUnit unit)
        {
            commands.add(command);
            return Mockito.mock(Disposable.class);
        }

        public void runAll()
        {
            while(!commands.isEmpty())
            {
                for(Runnable r : commands)
                {
                    r.run();
                    commands.remove(r);
                }
            }
        }

        public void reset()
        {
            commands.clear();
        }
    }

    public static class DirectEventLoop extends TestEventLoop
    {
        DirectEventLoop()
        {
            this(false);
        }

        DirectEventLoop(boolean inEventLoop)
        {
            super(inEventLoop);
        }

        @Override
        public void execute(Runnable command)
        {
            command.run();
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
            command.run();
            return Mockito.mock(ScheduledFuture.class);
        }
    }

    public static class BlackholeEventLoop extends TestEventLoop
    {
        BlackholeEventLoop()
        {
            this(false);
        }

        BlackholeEventLoop(boolean inEventLoop)
        {
            super(inEventLoop);
        }

        @Override
        public void execute(Runnable command)
        {
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
            return Mockito.mock(ScheduledFuture.class);
        }
    }

    public static class RecordingEventLoop extends TestEventLoop
    {
        public List<Runnable> commands = new CopyOnWriteArrayList<>();

        RecordingEventLoop()
        {
            this(false);
        }

        RecordingEventLoop(boolean inEventLoop)
        {
            super(inEventLoop);
        }

        @Override
        public void execute(Runnable command)
        {
            commands.add(command);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
            commands.add(command);
            return Mockito.mock(ScheduledFuture.class);
        }

        public void runAll()
        {
            while(!commands.isEmpty())
            {
                for(Runnable r : commands)
                {
                    r.run();
                    commands.remove(r);
                }
            }
        }

        public void reset()
        {
            commands.clear();
        }
    }

    public static class BlackholeChannel extends TestChannel
    {
        private final EventLoop eventLoop;
        public boolean writable = true;

        public BlackholeChannel(EventLoop eventLoop)
        {
            this.eventLoop = eventLoop;
        }

        @Override
        public EventLoop eventLoop()
        {
            return eventLoop;
        }

        @Override
        public Channel flush()
        {
            return this;
        }

        @Override
        public ChannelFuture write(Object msg)
        {
            return Mockito.mock(ChannelFuture.class);
        }

        @Override
        public boolean isWritable()
        {
            return writable;
        }

        public void reset()
        {
            writable = true;
        }
    }

    public static class RecordingChannel extends TestChannel
    {
        private EventLoop eventLoop;
        public int flushCalls = 0;
        public int writeCalls = 0;
        public List writeObjects = new LinkedList();
        public boolean writable = true;

        public RecordingChannel(EventLoop eventLoop)
        {
            this.eventLoop = eventLoop;
        }

        @Override
        public EventLoop eventLoop()
        {
            return eventLoop;
        }

        @Override
        public Channel flush()
        {
            flushCalls++;
            return this;
        }

        @Override
        public ChannelFuture write(Object msg)
        {
            writeCalls++;
            writeObjects.add(msg);
            return Mockito.mock(ChannelFuture.class);
        }

        @Override
        public boolean isWritable()
        {
            return writable;
        }

        public void setEventLoop(TestEventLoop eventLoop)
        {
            this.eventLoop = eventLoop;
        }

        public void reset()
        {
            flushCalls = writeCalls = 0;
            writeObjects.clear();
            writable = true;
        }
    }

    public abstract static class TestScheduler extends Scheduler
    {
        @Override
        public abstract Disposable scheduleDirect(Runnable run);

        @Override
        public abstract Disposable scheduleDirect(Runnable run, long delay, TimeUnit unit);

        @Override
        public Worker createWorker()
        {
            return Mockito.mock(Worker.class);
        }
    }

    public abstract static class TestEventLoop extends AbstractEventLoop
    {
        private final boolean inEventLoop;

        protected TestEventLoop(boolean inEventLoop)
        {
            this.inEventLoop = inEventLoop;
        }

        @Override
        public abstract void execute(Runnable command);

        @Override
        public abstract ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean inEventLoop(Thread thread)
        {
            return inEventLoop;
        }

        @Override
        public boolean isShutdown()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isShuttingDown()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isTerminated()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture register(Channel channel)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture register(ChannelPromise promise)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture register(Channel channel, ChannelPromise promise)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void shutdown()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Future<?> terminationFuture()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    public abstract static class TestChannel implements Channel
    {
        @Override
        public abstract EventLoop eventLoop();

        @Override
        public abstract Channel flush();

        @Override
        public abstract ChannelFuture write(Object msg);

        @Override
        public abstract boolean isWritable();

        @Override
        public ChannelFuture closeFuture()
        {
            return Mockito.mock(ChannelFuture.class);
        }

        @Override
        public ByteBufAllocator alloc()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public <T> Attribute<T> attr(AttributeKey<T> key)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public long bytesBeforeUnwritable()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public long bytesBeforeWritable()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture close()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture close(ChannelPromise promise)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int compareTo(Channel o)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelConfig config()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture deregister()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture deregister(ChannelPromise promise)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture disconnect()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture disconnect(ChannelPromise promise)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public <T> boolean hasAttr(AttributeKey<T> key)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelId id()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isActive()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isOpen()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isRegistered()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public SocketAddress localAddress()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelMetadata metadata()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture newFailedFuture(Throwable cause)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelProgressivePromise newProgressivePromise()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelPromise newPromise()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture newSucceededFuture()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Channel parent()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelPipeline pipeline()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Channel read()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public SocketAddress remoteAddress()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Unsafe unsafe()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelPromise voidPromise()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture write(Object msg, ChannelPromise promise)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

    }
}
