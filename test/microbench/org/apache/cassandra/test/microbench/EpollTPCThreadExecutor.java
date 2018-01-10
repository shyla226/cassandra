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

package org.apache.cassandra.test.microbench;

import java.util.concurrent.*;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.cassandra.concurrent.EpollTPCEventLoopGroup;
import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Created to test perf of FastThreadLocal
 *
 * Used in some TPC related benchmarks via:
 * jvmArgsAppend = {"-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.EpollTPCEventLoopGroup"}
 */
public class EpollTPCThreadExecutor extends EpollTPCEventLoopGroup
{
    static {
        DatabaseDescriptor.clientInitialization(false);
    }
    public EpollTPCThreadExecutor(int size, String name)
    {
        super(size, name);
    }
}
