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

import io.netty.channel.EventLoop;

/**
 * An event loop for our Thread-Per-Core architecture. Each loop runs on a specific {@link TPCThread} and as a core
 * ID associated to it.
 * <p>
 * This extends Netty {@link EventLoop} for reasons similar to the one mentioned in {@link TPCEventLoopGroup}.
 */
public interface TPCEventLoop extends EventLoop
{
    /**
     * The TPC thread on which the event loop runs.
     */
    public TPCThread thread();

    /**
     * The core ID identifying the event loop.
     */
    default public int coreId()
    {
        return thread().coreId();
    }

    default boolean canExecuteImmediately(TPCRunnable runnable)
    {
        return false;
    }

    @Override
    public TPCEventLoopGroup parent();
}
