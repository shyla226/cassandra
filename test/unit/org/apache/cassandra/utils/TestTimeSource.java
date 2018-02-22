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
package org.apache.cassandra.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TestTimeSource implements TimeSource
{
    private final AtomicLong timeInMillis = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong timeInNanos = new AtomicLong(System.nanoTime());
    private volatile int advanceCounter;
    private volatile int advanceCalls;
    private volatile long advanceTime;

    public void reset(long timeInMillis, long timeInNanos)
    {
        this.timeInMillis.set(timeInMillis);
        this.timeInNanos.set(timeInNanos);
    }

    @Override
    public long currentTimeMillis()
    {
        maybeAdvance();
        return timeInMillis.get();
    }

    @Override
    public long nanoTime()
    {
        maybeAdvance();
        return timeInNanos.get();
    }

    /**
     * Automatically advance by the given time unit every nth "time" calls.
     */
    public void autoAdvance(int calls, long time, TimeUnit unit)
    {
        advanceCounter = 0;
        advanceCalls = calls;
        advanceTime = TimeUnit.NANOSECONDS.convert(time, unit);
    }

    @Override
    public TimeSource sleep(long sleepFor, TimeUnit unit)
    {
        long currentMillis = timeInMillis.get();
        long currentNanos = timeInNanos.get();
        long sleepInMillis = TimeUnit.MILLISECONDS.convert(sleepFor, unit);
        long sleepInNanos = TimeUnit.NANOSECONDS.convert(sleepFor, unit);
        doSleep(currentMillis, sleepInMillis, timeInMillis);
        doSleep(currentNanos, sleepInNanos, timeInNanos);
        return this;
    }

    private void doSleep(long current, long sleep, AtomicLong time)
    {
        boolean elapsed;
        do
        {
            long newTime = current + sleep;
            elapsed = time.compareAndSet(current, newTime);
            if (!elapsed)
            {
                long updated = time.get();
                if (updated - current >= sleep)
                {
                    elapsed = true;
                }
                else
                {
                    sleep -= updated - current;
                    current = updated;
                }
            }
        }
        while (!elapsed);
    }

    @Override
    public TimeSource sleepUninterruptibly(long sleepFor, TimeUnit unit)
    {
        return sleep(sleepFor, unit);
    }
    
    private void maybeAdvance()
    {
        if (advanceCalls > 0 && ++advanceCounter == advanceCalls)
        {
            sleepUninterruptibly(advanceTime, TimeUnit.NANOSECONDS);
            advanceCounter = 0;
        }
    }
}
