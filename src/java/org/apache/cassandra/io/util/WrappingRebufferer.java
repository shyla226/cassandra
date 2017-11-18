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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import javax.annotation.Nullable;

public class WrappingRebufferer implements Rebufferer
{
    protected final Rebufferer source;
    private final Deque<WrappingBufferHolder> buffers;

    public WrappingRebufferer(Rebufferer source)
    {
        this.source = source;
        this.buffers = new ArrayDeque<>(2);
    }

    @Override
    public BufferHolder rebuffer(long position, ReaderConstraint constraint)
    {
        BufferHolder bufferHolder = source.rebuffer(position, constraint);
        return newBufferHolder().initialize(bufferHolder, bufferHolder.buffer(), bufferHolder.offset());
    }

    protected WrappingBufferHolder newBufferHolder()
    {
        WrappingBufferHolder ret = buffers.pollFirst();
        if (ret == null)
            ret = new WrappingBufferHolder();

        return ret;
    }

    @Override
    public AsynchronousChannelProxy channel()
    {
        return source.channel();
    }

    @Override
    public long fileLength()
    {
        return source.fileLength();
    }

    @Override
    public double getCrcCheckChance()
    {
        return source.getCrcCheckChance();
    }

    @Override
    public void close()
    {
        source.close();
    }

    @Override
    public void closeReader()
    {
        source.closeReader();
    }

    protected String paramsToString()
    {
        return "";
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '[' + paramsToString() + "]:" + source.toString();
    }

    protected final class WrappingBufferHolder implements Rebufferer.BufferHolder
    {
        @Nullable
        private BufferHolder bufferHolder;

        private ByteBuffer buffer;
        private long offset;

        protected WrappingBufferHolder initialize(@Nullable BufferHolder bufferHolder, ByteBuffer buffer, long offset)
        {
            assert this.bufferHolder == null && this.buffer == null && this.offset == 0L : "initialized before release";

            this.bufferHolder = bufferHolder;
            this.buffer = buffer;
            this.offset = offset;

            return this;
        }

        public ByteBuffer buffer()
        {
            return buffer;
        }

        public long offset()
        {
            return offset;
        }

        public void offset(long offet)
        {
            this.offset = offet;
        }

        public int limit()
        {
            return buffer.limit();
        }

        public void limit(int limit)
        {
            this.buffer.limit(limit);
        }

        public void release()
        {
            assert buffer != null : "released twice";

            if (bufferHolder != null)
            {
                bufferHolder.release();
                bufferHolder = null;
            }

            buffer = null;
            offset = 0L;

            buffers.offerFirst(this);
        }
    }
}