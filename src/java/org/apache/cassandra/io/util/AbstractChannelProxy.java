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

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.WritableByteChannel;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseable;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

abstract class AbstractChannelProxy<T extends Channel> extends SharedCloseableImpl
{
    protected final String filePath;
    protected final T channel;

    AbstractChannelProxy(String filePath, T channel)
    {
        super(new Cleanup(filePath, channel));
        this.filePath = filePath;
        this.channel = channel;
    }

    AbstractChannelProxy(AbstractChannelProxy<T> copy)
    {
        super(copy);

        this.filePath = copy.filePath;
        this.channel = copy.channel;
    }

    public abstract SharedCloseable sharedCopy();

    public abstract long size() throws FSReadError;

    public abstract long transferTo(long position, long count, WritableByteChannel target);

    public abstract int getFileDescriptor();

    public String filePath()
    {
        return filePath;
    }

    @Override
    public String toString()
    {
        return filePath();
    }


    private final static class Cleanup implements RefCounted.Tidy
    {
        final String filePath;
        final Channel channel;

        Cleanup(String filePath, Channel channel)
        {
            this.filePath = filePath;
            this.channel = channel;
        }

        public String name()
        {
            return filePath;
        }

        public void tidy()
        {
            try
            {
                channel.close();
            }
            catch (IOException e)
            {
                throw new FSReadError(e, filePath);
            }
        }
    }

}
