package org.apache.cassandra.metrics;

import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Sampling;
import com.codahale.metrics.Snapshot;

/**
 * A timer metric which aggregates timing durations and that provides duration statistics, plus
 * throughput statistics via a {@link Meter}.
 *
 * This class is nearly identical to {@link com.codahale.metrics.Timer}, except that
 * it replaces the {@link com.codahale.metrics.Histogram} field with our own {@link Histogram}
 * and replaces the {@link com.codahale.metrics.Meter} field with our own {@link Meter}.
 *
 * The underlying histogram and meter may be aggregated, making this timer aggregated, that is
 * uncapable of updating its values directly.
 */
public class Timer implements Metered, Sampling, Composable<Timer>
{
    /**
     * A timing context.
     *
     * @see Timer#time()
     */
    public static class Context implements Closeable
    {
        private final Timer timer;
        private final Clock clock;
        private final long startTime;

        private Context(Timer timer, Clock clock)
        {
            this.timer = timer;
            this.clock = clock;
            this.startTime = clock.getTick();
        }

        /**
         * Updates the timer with the difference between current and start time. Call to this method will
         * not reset the start time. Multiple calls result in multiple updates.
         * @return the elapsed time in nanoseconds
         */
        public long stop()
        {
            final long elapsed = clock.getTick() - startTime;
            timer.update(elapsed, TimeUnit.NANOSECONDS);
            return elapsed;
        }

        /** Equivalent to calling {@link #stop()}. */
        @Override
        public void close()
        {
            stop();
        }
    }

    private final Meter meter;
    private final Histogram histogram;
    private final Composable.Type composableType;
    private final Clock clock;

    /**
     * Creates a new non-composite {@link Timer} using a default {@link Histogram} and the default
     * {@link Clock}.
     */
    public Timer()
    {
        this(false);
    }

    /**
     * Creates a new {@link Timer} using a default {@link Histogram} and the default
     * {@link Clock}.
     */
    public Timer(boolean isComposite)
    {
        this(Histogram.make(isComposite),  Clock.defaultClock(), isComposite);
    }

    /**
     * Creates a new {@link Timer} that uses the given {@link Reservoir} and {@link Clock}.
     *
     * @param histogram the {@link Histogram} implementation the timer should use
     * @param clock  the {@link Clock} implementation the timer should use
     */
    public Timer(Histogram histogram, Clock clock, boolean isComposite)
    {
        this.meter = new Meter(clock, Counter.make(isComposite));
        this.histogram = histogram;
        this.composableType = isComposite ? Type.COMPOSITE : Type.SINGLE;
        this.clock = clock;
    }

    /**
     * Adds a recorded duration.
     *
     * @param duration the length of the duration
     * @param unit     the scale unit of {@code duration}
     */
    public void update(long duration, TimeUnit unit)
    {
        update(unit.toNanos(duration));
    }

    /**
     * Times and records the duration of event.
     *
     * @param event a {@link Callable} whose {@link Callable#call()} method implements a process
     *              whose duration should be timed
     * @param <T>   the type of the value returned by {@code event}
     * @return the value returned by {@code event}
     * @throws Exception if {@code event} throws an {@link Exception}
     */
    public <T> T time(Callable<T> event) throws Exception
    {
        if (composableType == Type.COMPOSITE)
            throw new UnsupportedOperationException("Composite timer cannot time an event");

        final long startTime = clock.getTick();
        try
        {
            return event.call();
        } finally
        {
            update(clock.getTick() - startTime);
        }
    }

    /**
     * Returns a new {@link Context}.
     *
     * @return a new {@link Context}
     * @see Context
     */
    public Context time()
    {
        if (composableType == Type.COMPOSITE)
            throw new UnsupportedOperationException("Composite timer cannot time an event");

        return new Context(this, clock);
    }

    @Override
    public long getCount()
    {
        return histogram.getCount();
    }

    @Override
    public double getFifteenMinuteRate()
    {
        return meter.getFifteenMinuteRate();
    }

    @Override
    public double getFiveMinuteRate()
    {
        return meter.getFiveMinuteRate();
    }

    @Override
    public double getMeanRate()
    {
        return meter.getMeanRate();
    }

    @Override
    public double getOneMinuteRate()
    {
        return meter.getOneMinuteRate();
    }

    @Override
    public Snapshot getSnapshot()
    {
        return histogram.getSnapshot();
    }

    private void update(long duration)
    {
        if (composableType == Type.COMPOSITE)
            throw new UnsupportedOperationException("Composite timer cannot be updated");

        if (duration >= 0)
        {
            histogram.update(duration);
            meter.mark();
        }
    }

    @Override
    public Type getType()
    {
        return composableType;
    }

    @Override
    public void compose(Timer metric)
    {
        if (composableType != Type.COMPOSITE)
            throw new UnsupportedOperationException("Non composite timer cannot be composed with another timer");

        this.meter.compose(metric.meter);
        this.histogram.compose(metric.histogram);
    }

    @VisibleForTesting
    public Histogram getHistogram()
    {
        return histogram;
    }
}