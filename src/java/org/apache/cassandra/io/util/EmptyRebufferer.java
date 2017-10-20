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

import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.concurrent.TPCUtils;

public class EmptyRebufferer implements Rebufferer, RebuffererFactory
{
    AsynchronousChannelProxy channel;

    public EmptyRebufferer(AsynchronousChannelProxy channel)
    {
        this.channel = channel;
    }

    public AsynchronousChannelProxy channel()
    {
        return channel;
    }

    public long fileLength()
    {
        return 0;
    }

    public double getCrcCheckChance()
    {
        return 0;
    }

    public Rebufferer instantiateRebufferer()
    {
        return this;
    }

    @Override
    public boolean supportsPrefetch()
    {
        return false; // nothing to prefetch
    }

    public BufferHolder rebuffer(long position)
    {
        return EMPTY;
    }

    public CompletableFuture<BufferHolder> rebufferAsync(long position)
    {
        return TPCUtils.completedFuture(EMPTY);
    }

    public int rebufferSize()
    {
        return 0;
    }

    public BufferHolder rebuffer(long position, ReaderConstraint constraint)
    {
        return EMPTY;
    }

    public void close()
    {
        // nothing to do
    }

    public void closeReader()
    {
        // nothing to do
    }
}