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

import com.google.common.collect.ImmutableList;

import io.netty.channel.EventLoopGroup;
import org.apache.cassandra.transport.Server;

/**
 * A group of TPC event loops.
 * <p>
 * This can be mostly thought of as a list of our per-core {@link TPCEventLoop}. This extends netty {@link EventLoopGroup}
 * because 1) there is no point re-inventing the wheel and 2) we use our internal group and event loops to handle
 * I/O tasks as well, which means we pass this to Netty (see the bootstrap group in {@link Server#start} in particular).
 * <p>
 * The exact event loop group (and so the per-core loops it contains) used is initialized and kept in the {@link TPC}
 * class.
 */
public interface TPCEventLoopGroup extends EventLoopGroup
{
    /**
     * Returns an immutable list of TPC event loops. The {@code ith} element of the list is the event loop of coreId
     * {@code i}.
     * <p>
     * Note that through extending {@link EventLoopGroup}, the group is already a {@code Iterable<EventLoop>}, but this
     * methods makes it easier to 1) access the loop for a particular core if we want and 2) get instances of
     * {@link TPCEventLoop} directly, which gives us access to a few additional handy information like the
     * {@link TPCThread} on which the loop runs.
     */
    public ImmutableList<? extends TPCEventLoop> eventLoops();
}
