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

package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.epoll.AIOContext;
import io.netty.channel.epoll.AIOEpollFileChannel;
import io.netty.channel.epoll.EpollEventLoop;
import io.netty.channel.unix.FileDescriptor;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NativeLibrary;

/**
 * A proxy of a FileChannel that:
 *
 * - implements reference counting
 * - exports only thread safe FileChannel operations
 * - wraps IO exceptions into runtime exceptions
 *
 * Tested by RandomAccessReaderTest.
 */
public class AsynchronousChannelProxy extends AbstractChannelProxy<AsynchronousFileChannel>
{
    private static final Logger logger = LoggerFactory.getLogger(AsynchronousChannelProxy.class);

    @SuppressWarnings({"unchecked", "rawtypes"}) // generic array construction
    private static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute[0];
    private static final Set<OpenOption> READ_ONLY = Collections.singleton(StandardOpenOption.READ);
    private static final ExecutorService javaAioGroup;
    static
    {
        if (TPC.USE_AIO || FBUtilities.isWindows)
            javaAioGroup = Executors.newCachedThreadPool(new NamedThreadFactory("java-aio"));
        else
            javaAioGroup = Executors.newFixedThreadPool(32, new NamedThreadFactory("java-aio"));
    }

    private static AsynchronousFileChannel openFileChannel(File file, boolean javaAIO)
    {
        try
        {
            if (!TPC.USE_AIO || javaAIO)
                return AsynchronousFileChannel.open(file.toPath(), READ_ONLY, javaAioGroup, NO_ATTRIBUTES);

            try
            {
                int flags = FileDescriptor.O_RDONLY | FileDescriptor.O_DIRECT;
                return new AIOEpollFileChannel(file, (EpollEventLoop)TPC.bestIOEventLoop(), flags);
            }
            catch (IOException e)
            {
                // We can get a EINVAL error on open() if the underlying file system does not support O_DIRECT (see
                // O_DIRECT sections in http://man7.org/linux/man-pages/man2/open.2.html for details). In that case,
                // we don't want to error out, but rather default to java "fake" asynchronous IO.
                if (e.getMessage().contains("Invalid argument"))
                    return AsynchronousFileChannel.open(file.toPath(), READ_ONLY, javaAioGroup, NO_ATTRIBUTES);
                else
                    throw e;
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    // store a variable to avoid instanceof checks for every read, eventually we should separate the two implementations
    private final AIOEpollFileChannel epollChannel;

    public AsynchronousChannelProxy(String path)
    {
        this (new File(path), false);
    }

    public AsynchronousChannelProxy(String path, boolean javaAIO)
    {
        this (new File(path), javaAIO);
    }

    public AsynchronousChannelProxy(File file, boolean javaAIO)
    {
        this(file.getPath(), openFileChannel(file, javaAIO));
    }

    public AsynchronousChannelProxy(String filePath, AsynchronousFileChannel channel)
    {
        super(filePath, channel);
        this.epollChannel = channel instanceof AIOEpollFileChannel ? (AIOEpollFileChannel)channel : null;
    }

    public AsynchronousChannelProxy(AsynchronousChannelProxy copy)
    {
        super(copy);
        this.epollChannel = channel instanceof AIOEpollFileChannel ? (AIOEpollFileChannel)channel : null;
    }

    boolean requiresAlignment()
    {
        return channel instanceof AIOEpollFileChannel;
    }

    private static final int MAX_RETRIES = 10;

    CompletionHandler<Integer, ByteBuffer> makeRetryingHandler(ByteBuffer dest,
                                                               long offset,
                                                               CompletionHandler<Integer, ByteBuffer> onComplete)
    {
        return new CompletionHandler<Integer, ByteBuffer>()
        {
            int retries = 0;

            public void completed(Integer result, ByteBuffer attachment)
            {
                onComplete.completed(result, attachment);
            }

            public void failed(Throwable exc, ByteBuffer attachment)
            {
                //new RuntimeException("Too many pending requests")
                if (exc instanceof RuntimeException && exc.getMessage().contains("Too many pending requests"))
                {
                    // This error is thrown if there are too many pending requests in the queue.
                    // This should not normally be hit. However, it can happen if the node is
                    // heavily pounded by read requests. Rather than immediately failing (and even marking the sstable
                    // as suspect), retry the read, backing off exponentially longer with each retry.
                    if (++retries < MAX_RETRIES)
                    {
                        logger.warn("Got {}. Retrying {} more times.", exc.getMessage(), MAX_RETRIES - retries);
                        TPC.bestTPCScheduler().scheduleDirect(() -> channel.read(dest, offset, dest, this),
                                                              1 << retries - 1,
                                                              TimeUnit.MILLISECONDS);
                        return;
                    }
                    else
                    {
                        logger.error("Got {} and exhausted all retries.", exc.getMessage());
                    }
                }
                logger.debug("Failed to read {} with exception {}", filePath, exc);
                onComplete.failed(exc, attachment);
            }
        };
    }

    public void read(ByteBuffer dest, long offset, CompletionHandler<Integer, ByteBuffer> onComplete)
    {
        CompletionHandler<Integer, ByteBuffer> retryingHandler = makeRetryingHandler(dest, offset, onComplete);

        try
        {
            if (epollChannel != null)
                epollChannel.read(dest, offset, dest, retryingHandler, (EpollEventLoop)TPC.bestIOEventLoop());
            else
                channel.read(dest, offset, dest, retryingHandler);
        }
        catch (Throwable t)
        {
            retryingHandler.failed(t, dest);
        }
    }

    public long size() throws FSReadError
    {
        try
        {
            return channel.size();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    public ChannelProxy getBlockingChannel()
    {
        return new ChannelProxy(new File(filePath));
    }

    public long transferTo(long position, long count, WritableByteChannel target)
    {
        try (ChannelProxy cp = getBlockingChannel())
        {
            return cp.transferTo(position, count, target);
        }
    }

    public AsynchronousChannelProxy sharedCopy()
    {
        return new AsynchronousChannelProxy(this);
    }

    @Override
    public void tryToSkipCache(long offset, long len)
    {
        int fd;

        if (epollChannel != null)
        {
            if (epollChannel.isDirect())
                return; // direct AIO channels bypass the page cache

            fd = epollChannel.getFd();
        }
        else
        {
            fd = NativeLibrary.getfd(channel);
        }

        NativeLibrary.trySkipCache(fd, offset, len, filePath);
    }

    /**
     * Start a batch of reads. By default each read is submitted immediately and so this is a no-op but
     * {@link AIOEpollBatchedChannelProxy} implements this method by creating a batch to submit later.
     */
    public void startBatch()
    {
        //no op for non-batched channels, since each read request is submitted immediately
    }

    /**
     * Submit a batch of reads. By default each read is submitted immediately and so this is a no-op but
     * {@link AIOEpollBatchedChannelProxy} implements this method by submitting a batch.
     */
    public void submitBatch()
    {
        //no op for non-batched channels, since each read request is submitted immediately
    }

    /**
     * Return a {@link AIOEpollBatchedChannelProxy} if it's possible to create one, otherwise return a copy
     * of this channel, with ref. counting incremented.
     *
     * @param vectored - true if vectored IO should be used in addition to batching.
     * @return a channel proxy that supports batching or a copy of this channel.
     */
    public AsynchronousChannelProxy maybeBatched(boolean vectored)
    {
        if (epollChannel == null)
            return sharedCopy();

        return new AIOEpollBatchedChannelProxy(this, vectored);
    }

    /**
     * Extends {@link AsynchronousChannelProxy} to provide batching functionality.
     * <p>
     * Instead of being sent directly, reads are added to a batch, which must be submitted later on
     * by calling {@link #submitBatch()}.
     * <p>
     * Note that unlike {@link AsynchronousChannelProxy}, this class is not thread safe since it contains some state,
     * the batch. So it must be use only by a single thread or synchronized externally.
     */
    private static class AIOEpollBatchedChannelProxy extends AsynchronousChannelProxy
    {
        private static final FastThreadLocal<AIOContext.Batch<ByteBuffer>> batch = new FastThreadLocal<>();
        private final boolean vectored;
        private final AIOEpollFileChannel epollChannel;

        private AIOEpollBatchedChannelProxy(AsynchronousChannelProxy inner, boolean vectored)
        {
            super(inner);
            this.vectored = vectored;
            this.epollChannel = (AIOEpollFileChannel) inner.channel;
        }

        @Override
        public void read(ByteBuffer dest, long offset, CompletionHandler<Integer, ByteBuffer> onComplete)
        {
            CompletionHandler<Integer, ByteBuffer> handler = makeRetryingHandler(dest, offset, onComplete);
            try
            {
                if (!batch.isSet())
                    epollChannel.read(dest, offset, dest, handler, (EpollEventLoop) TPC.bestIOEventLoop());
                else
                    batch.get().add(offset, dest, dest, handler);
            }
            catch (Throwable err)
            {
                handler.failed(err, dest);
            }
        }

        @Override
        public void startBatch()
        {
            assert !batch.isSet() : "Batch was already started";
            batch.set(epollChannel.newBatch(vectored));
        }

        @Override
        public void submitBatch()
        {
            if (!this.batch.isSet())
                return;

            AIOContext.Batch<ByteBuffer> batch = this.batch.get();

            try
            {
                if (batch.numRequests() > 0)
                    epollChannel.read(batch, (EpollEventLoop) TPC.bestIOEventLoop());
            }
            catch (Throwable err)
            {
                batch.failed(err);
            }
            finally
            {
                this.batch.remove();
            }
        }
    }
}
