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
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.epoll.AIOEpollFileChannel;
import io.netty.channel.epoll.EpollEventLoop;
import io.netty.channel.unix.FileDescriptor;
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
public final class AsynchronousChannelProxy extends AbstractChannelProxy<AsynchronousFileChannel>
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

            EpollEventLoop loop = (EpollEventLoop) TPC.bestTPCScheduler().getExecutor();
            int flags = FileDescriptor.O_RDONLY | FileDescriptor.O_DIRECT;
            return new AIOEpollFileChannel(file, loop, flags);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

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
    }

    public AsynchronousChannelProxy(AsynchronousChannelProxy copy)
    {
        super(copy);
    }

    public void read(ByteBuffer dest, long offset, CompletionHandler<Integer, ByteBuffer> onComplete)
    {
        try
        {
            channel.read(dest, offset, dest, onComplete);
        }
        catch (Throwable t)
        {
            onComplete.failed(t, dest);
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

    public int getFileDescriptor()
    {
        return NativeLibrary.getfd(channel);
    }
}