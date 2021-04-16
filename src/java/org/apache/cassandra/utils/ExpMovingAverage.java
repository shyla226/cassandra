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
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;

/**
 * An exponential moving average.
 */
public class ExpMovingAverage implements MovingAverage
{
    private enum Status
    {
        NONE,
        NORMAL,
        TICKING
    }

    private final TimeSource timeSource;
    private final long samplingPeriodMillis;
    private final long averagePeriodSec;

    /** A state machine for the management of late initialization and thread concurrency */
    private final AtomicReference<Status> status;
    /** The time of the last tick */
    private volatile long lastTickMillis;
    /** The average in the current period (since the last tick) */
    private final AtomicDouble current;
    /** The long term average with exponential decay */
    private volatile double average;

    /**
     * Create a {@link ExpMovingAverage} that averages over 5 minutes.
     *
     * @return a smoothing filter that averages over 5 minutes
     */
    public static MovingAverage fiveMinutes(TimeSource timeSource)
    {
        return minutes(5, timeSource);
    }

    /**
     * Create a {@link ExpMovingAverage} that averages over a minute.
     *
     * @return a smoothing filter that averages over a minute
     */
    public static ExpMovingAverage oneMinute(TimeSource timeSource)
    {
        return minutes(1, timeSource);
    }

    /**
     * Create a {@link ExpMovingAverage} that averages over the specified number of minutes.
     *
     * @return a smoothing filter that averages over the specified number of minutes
     */
    public static ExpMovingAverage minutes(int minutes, TimeSource timeSource)
    {
        return new ExpMovingAverage(timeSource, 1000, minutes * 60);
    }

    /**
     * Create a {@link ExpMovingAverage} that averages over half a minute.
     *
     * @return a smoothing filter that averages over half a minute
     */
    public static MovingAverage halfMinute(TimeSource timeSource)
    {
        return new ExpMovingAverage(timeSource, 250, 30);
    }

    private ExpMovingAverage(TimeSource timeSource, int samplingPeriodMills, int averagePeriodSec)
    {
        this.timeSource = timeSource;
        this.samplingPeriodMillis = samplingPeriodMills;
        this.averagePeriodSec = averagePeriodSec;

        this.status = new AtomicReference<>(Status.NONE);
        this.current = new AtomicDouble(0);
        this.average = 0;
    }

    @Override
    public MovingAverage update(double val)
    {
        while (status.get() == Status.NONE)
        {
            if (status.compareAndSet(Status.NONE, Status.NORMAL))
            {
                current.set(val);
                average = val;
                lastTickMillis = timeSource.currentTimeMillis();
                return this;
            }
            else
            {
                Thread.yield();
            }
        }

        double c;
        do
        {
            c = current.get();
        } while (!current.compareAndSet(c, (c + val) / 2));

        tickIfNecessary();

        return this;
    }

    private void tickIfNecessary()
    {
        long now = timeSource.currentTimeMillis();
        long age = now - lastTickMillis;

        if (age <= samplingPeriodMillis)
            return;

        tick(now);
    }

    private void tick(long now)
    {
        if (status.compareAndSet(Status.NORMAL, Status.TICKING))
        {
            try
            {
                final long period = now - lastTickMillis;
                if (period == 0)
                    return; // happens when freshly initialized

                //https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average for how alpha is calculated
                double alpha = 1 - Math.exp(-period / (double) TimeUnit.SECONDS.toMillis(1) / averagePeriodSec);
                double currentAverage = current.getAndSet(0);
                average += (alpha * (currentAverage - average));
            }
            finally
            {
                lastTickMillis = timeSource.currentTimeMillis();
                status.set(Status.NORMAL);
            }
        }
        else
        {
            if (status.get() == Status.NONE)
                throw new IllegalStateException("Status was not initialized");
        }
    }

    @VisibleForTesting
    double getCurrent()
    {
        return current.get();
    }

    @Override
    public double get()
    {
        return average;
    }

    @Override
    public String toString()
    {
        return String.format("%.2f", average);
    }
}
