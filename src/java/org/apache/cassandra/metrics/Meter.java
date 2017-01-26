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

package org.apache.cassandra.metrics;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Metered;
import org.apache.cassandra.concurrent.NettyRxScheduler;

/**
 * A meter metric which measures mean throughput and one-, five-, and fifteen-minute
 * exponentially-weighted moving average throughputs.
 *
 * This class is nearly identical to {@link com.codahale.metrics.Meter}, except that
 * we only call {@link EWMA#tick()} every {@link Meter#TICK_INTERVAL_SECONDS} seconds,
 * by comparing the value of the counter with the last value that was used in the
 * previous tick.
 *
 * @see EWMA
 */
public class Meter implements Metered, Composable<Meter>
{
    private static final long TICK_INTERVAL_SECONDS = 5;

    private final EWMA m1Rate = EWMA.oneMinuteEWMA(false);
    private final EWMA m5Rate = EWMA.fiveMinuteEWMA(false);
    private final EWMA m15Rate = EWMA.fifteenMinuteEWMA(false);

    private final Clock clock;
    private final int coreId;

    private final Counter count;
    private final long startTime;
    private long lastCounted;

    /**
     * Creates a new {@link com.codahale.metrics.Meter}.
     */
    public Meter()
    {
        this(false);
    }

    /**
     * Creates a new {@link com.codahale.metrics.Meter}.
     *
     * @param isComposite     whether the counter is aggreagated or not
     */
    public Meter(boolean isComposite)
    {
        this(ApproximateClock.defaultClock(), Counter.make(isComposite));
    }

    /**
     * Creates a new {@link com.codahale.metrics.Meter}.
     *
     * @param clock      the clock to use for the meter ticks
     * @param count      the counter to use for counting
     */
    public Meter(Clock clock, Counter count)
    {
        this.clock = clock;
        this.count = count;
        this.startTime = this.clock.getTick();
        this.coreId = NettyRxScheduler.getNextCore();
        this.lastCounted = 0;

        schedule();
    }

    private void schedule()
    {
        NettyRxScheduler.getForCore(this.coreId).scheduleDirect(this::tick, TICK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Mark the occurrence of an event.
     */
    public void mark()
    {
        mark(1);
    }

    /**
     * Mark the occurrence of a given number of events.
     *
     * @param n the number of events
     */
    public void mark(long n)
    {
        count.inc(n);
    }

    private void tick()
    {
        try
        {
            final long currentCount = getCount();
            final long delta = currentCount - lastCounted;

            m1Rate.tick(delta);
            m5Rate.tick(delta);
            m15Rate.tick(delta);

            lastCounted = currentCount;
        }
        finally
        {
            schedule();
        }
    }

    @Override
    public long getCount()
    {
        return count.getCount();
    }

    @Override
    public double getFifteenMinuteRate()
    {
        return m15Rate.getRate(TimeUnit.SECONDS);
    }

    @Override
    public double getFiveMinuteRate()
    {
        return m5Rate.getRate(TimeUnit.SECONDS);
    }

    @Override
    public double getMeanRate()
    {
        return getMeanRate(getCount());
    }

    public double getMeanRate(long count)
    {
        if (count == 0) {
            return 0.0;
        } else {
            final double elapsed = (clock.getTick() - startTime);
            return count / elapsed * TimeUnit.SECONDS.toNanos(1);
        }
    }

    @Override
    public double getOneMinuteRate()
    {
        return m1Rate.getRate(TimeUnit.SECONDS);
    }

    @Override
    public Type getType()
    {
        return count.getType();
    }

    @Override
    public void compose(Meter metric)
    {
        count.compose(metric.count);
    }
}
