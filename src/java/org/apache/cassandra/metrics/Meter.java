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
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Metered;
import org.apache.cassandra.utils.concurrent.LongAdder;

/**
 * A meter metric which measures mean throughput and one-, five-, and fifteen-minute
 * exponentially-weighted moving average throughputs.
 *
 * This class is nearly identical to {@link com.codahale.metrics.Meter}, except that
 * it maintains a <b>lastCounted</b> {@link AtomicLong} that keeps a snapshot of the
 * counter after every tick that is used to calculate the uncounted delta
 * in the next call to {@link EWMA#tick(long)}.
 *
 * @see EWMA
 */
public class Meter implements Metered
{
    private static final long TICK_INTERVAL = TimeUnit.SECONDS.toNanos(5);

    private final EWMA m1Rate = EWMA.oneMinuteEWMA(false);
    private final EWMA m5Rate = EWMA.fiveMinuteEWMA(false);
    private final EWMA m15Rate = EWMA.fifteenMinuteEWMA(false);

    private final AtomicLong lastCounted = new AtomicLong();
    private final LongAdder count = new LongAdder();
    private final long startTime;
    private final AtomicLong lastTick;
    private final Clock clock;

    /**
     * Creates a new {@link com.codahale.metrics.Meter}.
     */
    public Meter() {
        this(ApproximateClock.defaultClock());
    }

    /**
     * Creates a new {@link com.codahale.metrics.Meter}.
     *
     * @param clock      the clock to use for the meter ticks
     */
    public Meter(Clock clock) {
        this.clock = clock;
        this.startTime = this.clock.getTick();
        this.lastTick = new AtomicLong(startTime);
    }

    /**
     * Mark the occurrence of an event.
     */
    public void mark() {
        mark(1);
    }

    /**
     * Mark the occurrence of a given number of events.
     *
     * @param n the number of events
     */
    public void mark(long n) {
        tickIfNecessary();
        count.add(n);
    }

    private void tickIfNecessary() {
        final long oldTick = lastTick.get();
        final long newTick = clock.getTick();
        final long age = newTick - oldTick;
        if (age > TICK_INTERVAL) {
            final long newIntervalStartTick = newTick - age % TICK_INTERVAL;
            if (lastTick.compareAndSet(oldTick, newIntervalStartTick)) {
                final long requiredTicks = age / TICK_INTERVAL;
                for (long i = 0; i < requiredTicks; i++) {
                    final long count = getUncounted();
                    m1Rate.tick(count);
                    m5Rate.tick(count);
                    m15Rate.tick(count);
                }
            }
        }
    }

    private long getUncounted()
    {
        long current = getCount();
        long previous = lastCounted.getAndSet(current);
        return current - previous;
    }

    @Override
    public long getCount() {
        return count.sum();
    }

    @Override
    public double getFifteenMinuteRate() {
        tickIfNecessary();
        return m15Rate.getRate(TimeUnit.SECONDS);
    }

    @Override
    public double getFiveMinuteRate() {
        tickIfNecessary();
        return m5Rate.getRate(TimeUnit.SECONDS);
    }

    @Override
    public double getMeanRate() {
        return getMeanRate(getCount());
    }

    public double getMeanRate(long count) {
        if (count == 0) {
            return 0.0;
        } else {
            final double elapsed = (clock.getTick() - startTime);
            return count / elapsed * TimeUnit.SECONDS.toNanos(1);
        }
    }

    @Override
    public double getOneMinuteRate() {
        tickIfNecessary();
        return m1Rate.getRate(TimeUnit.SECONDS);
    }
}
