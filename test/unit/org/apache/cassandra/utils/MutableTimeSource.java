/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Controlling time flow like the Master of Universe :) useful for tests depending on timing.
 */
public class MutableTimeSource implements TimeSource
{
    private final AtomicLong timeNs;
    private final AtomicLong totalSleepingTime = new AtomicLong(0);

    public MutableTimeSource(long startingTimeMillis)
    {
        this.timeNs = new AtomicLong(TimeUnit.MILLISECONDS.toNanos(startingTimeMillis));
    }

    @Override
    public long currentTimeMillis()
    {
        return TimeUnit.NANOSECONDS.toMillis(timeNs.get());
    }

    @Override
    public long nanoTime()
    {
        return timeNs.get();
    }

    public void autoAdvance(int calls, long time, TimeUnit unit)
    {
        // not used
    }

    /**
     * Move the clock forward for the given amount of time.
     * @param deltaTime time amount
     * @param unit time unit
     */
    public TimeSource forward(long deltaTime, TimeUnit unit)
    {
        timeNs.addAndGet(unit.toNanos(deltaTime));
        return this;
    }


    /**
     * Set the clock to the given time
     * @param newTime new time value
     * @param unit time unit
     */
    public void set(long newTime, TimeUnit unit)
    {
        this.timeNs.set(unit.toNanos(newTime));
    }

    @Override
    public TimeSource sleepUninterruptibly(long sleepFor, TimeUnit unit)
    {
        totalSleepingTime.addAndGet(TimeUnit.NANOSECONDS.convert(sleepFor, unit));
        forward(sleepFor, unit);
        return this;
    }

    @Override
    public TimeSource sleep(long sleepFor, TimeUnit unit)
    {
        totalSleepingTime.addAndGet(TimeUnit.NANOSECONDS.convert(sleepFor, unit));
        forward(sleepFor, unit);
        return this;
    }

    /**
     *
     * @return total time spent in sleeping in ns
     */
    public long getTotalSleepingTime()
    {
        return totalSleepingTime.get();
    }

    /**
     * reset sleeping to time to zero
     */
    public void resetSleepingTime()
    {
        totalSleepingTime.set(0);
    }
}