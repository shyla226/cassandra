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
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public abstract class WrappingRebufferer implements Rebufferer, Rebufferer.BufferHolder
{
    protected final Rebufferer source;

    protected BufferHolder bufferHolder;
    protected ByteBuffer buffer;
    protected long offset;

    public WrappingRebufferer(Rebufferer source)
    {
        this.source = source;
    }

    @Override
    public BufferHolder rebuffer(long position)
    {
        assert bufferHolder == null;
        bufferHolder = source.rebuffer(position);
        buffer = bufferHolder.buffer();
        offset = bufferHolder.offset();

        return this;
    }

    @Override
    public ChannelProxy channel()
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

    @Override
    public String toString()
    {
        return String.format("%s[]:%s", getClass().getSimpleName(), source.toString());
    }

    @Override
    public ByteBuffer buffer()
    {
        return buffer;
    }

    @Override
    public long offset()
    {
        return offset;
    }

    @Override
    public void release()
    {
        if (bufferHolder != null)
        {
            bufferHolder.release();
            bufferHolder = null;
        }
    }

}
