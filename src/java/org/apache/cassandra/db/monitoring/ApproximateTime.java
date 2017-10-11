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

package org.apache.cassandra.db.monitoring;

import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoop;
import org.apache.cassandra.concurrent.EpollTPCEventLoopGroup;
import org.apache.cassandra.concurrent.WatcherThread;

/**
 * This is an approximation of System.currentTimeInMillis() and System.nanoTime(),
 * to be used as a faster alternative when we can sacrifice precision.
 *
 * The current nanoTime is updated by the Watcher thread of {@link org.apache.cassandra.concurrent.WatcherThread},
 * which calls {@link java.util.concurrent.locks.LockSupport#parkNanos(long)} with a parameter of 1
 * nanoSecond, and then checks the queues of the single-threaded executors, so the precision should be
 * of approximately 50 to 100 microseconds. To be on the safe side, we set the precision to 200 microseconds.
 */
public class ApproximateTime
{
    private static final long intialCurrentTimeMillis = System.currentTimeMillis();
    private static final long initialNanoTime = System.nanoTime();
    private static volatile long currentNanoTime = initialNanoTime;

    /** register class with the {@link WatcherThread}
     */
    static
    {
        WatcherThread.instance.get().addAction(ApproximateTime::tick);
    }

    /** The precision when called by the {@link WatcherThread}
     */
    private static final long precisionMicros = 200; // see comment in class description

    /**
     * Update the current time. This must be called by the same thread
     */
    public static void tick()
    {
        currentNanoTime = System.nanoTime();
    }

    /**
     * @return an approximate current time in millis with the precision returned by {@link ApproximateTime#precision()}.
     */
    public static long currentTimeMillis()
    {
        return intialCurrentTimeMillis + TimeUnit.MILLISECONDS.convert(currentNanoTime - initialNanoTime, TimeUnit.NANOSECONDS);
    }

    /** @return the approximate current time in nanoseconds, to be compared with another value returned by this method
     */
    public static long nanoTime()
    {
        return currentNanoTime;
    }

    /**
     * @return the precision of the approximate time, see javadoc above.
     */
    public static long precision()
    {
        return TimeUnit.MILLISECONDS.convert(precisionMicros, TimeUnit.MICROSECONDS);
    }

}
