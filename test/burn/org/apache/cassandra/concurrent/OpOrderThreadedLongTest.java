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

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.utils.concurrent.OpOrderThreaded;

public class OpOrderThreadedLongTest extends LongOpOrderTester
{
    static AtomicInteger currId = new AtomicInteger();

    static class ProducerThread extends Thread
    {
        int id = currId.getAndIncrement();

        public ProducerThread(Runnable target)
        {
            super(target);
        }
    }

    static class ThreadGen implements ThreadFactory
    {
        public Thread newThread(Runnable runnable)
        {
            return new ProducerThread(runnable);
        }
    }

    static class ThreadId implements OpOrderThreaded.ThreadIdentifier
    {
        public int idFor(Thread d)
        {
            return ((ProducerThread) d).id;
        }

        public boolean barrierPermitted()
        {
            return true;
        }
    }

    static class ThreadId1 implements OpOrderThreaded.ThreadIdentifier
    {
        public int idLimit()
        {
            return 1;
        }

        public int idFor(Thread d)
        {
            return 0;
        }

        public boolean barrierPermitted()
        {
            return true;
        }
    }

    static final int THREAD_COUNT = PRODUCERS + CONSUMERS;

    public OpOrderThreadedLongTest()
    {
//        super(new OpOrderSimple(), Executors.newFixedThreadPool(THREAD_COUNT, new ThreadGen()));
        super(new OpOrderThreaded(null, new ThreadId(), THREAD_COUNT), Executors.newFixedThreadPool(THREAD_COUNT, new ThreadGen()));
//        super(new OpOrderThreaded(null, new ThreadId1(), 1), Executors.newFixedThreadPool(THREAD_COUNT, new ThreadGen()));
    }
}
