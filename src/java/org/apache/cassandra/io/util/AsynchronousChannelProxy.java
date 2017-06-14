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
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.epoll.AIOEpollFileChannel;
import io.netty.channel.epoll.EpollEventLoop;
import io.netty.channel.unix.FileDescriptor;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.io.FSReadError;
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

    private static AsynchronousFileChannel openFileChannel(File file, boolean directIO)
    {
        try
        {
            if (!TPC.USE_AIO)
                return AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.READ);

            EpollEventLoop loop = (EpollEventLoop) TPC.bestTPCScheduler().getExecutor();

            int flags = FileDescriptor.O_RDONLY;
            if (directIO && TPC.USE_DIRECT_IO)
                flags |= FileDescriptor.O_DIRECT;

            return new AIOEpollFileChannel(file, loop, flags);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public AsynchronousChannelProxy(String path, boolean directIO)
    {
        this (new File(path), directIO);
    }

    public AsynchronousChannelProxy(File file, boolean directIO)
    {
        this(file.getPath(), openFileChannel(file, directIO));
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