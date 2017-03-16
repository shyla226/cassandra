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

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Counting;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Sampling;
import com.codahale.metrics.Snapshot;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.EstimatedHistogram;

/**
 * A metric which calculates the distribution of a value.
 *
 * @see <a href="http://www.johndcook.com/standard_deviation.html">Accurately computing running
 *      variance</a>
 *
 * This class removes the {@link java.util.concurrent.atomic.LongAdder} of the {@link com.codahale.metrics.Histogram}
 * class and retrieves {@link this#getCount()} from {@link Reservoir}.
 */
public interface Histogram extends Metric, Sampling, Counting, Composable<Histogram>
{
    /**
     * Whether zeros are considered by default.
     */
    boolean DEFAULT_ZERO_CONSIDERATION = false;

    /** The maximum trackable value, 18 TB. This comes from the legacy implementation based on
     * {@link EstimatedHistogram#newOffsets(int, boolean)} with size set to 164 and  considerZeros
     * set to true.*/
    long DEFAULT_MAX_TRACKABLE_VALUE = 18 * (1L << 43);

    /**
     * Adds a recorded value.
     *
     * @param value the length of the value
     */
    public void update(final long value);

    /**
     * @return the number of values recorded
     */
    @Override
    public long getCount();  //from Counting

    /**
     *
     * @return a snapshot of the histogram.
     */
    @Override
    public Snapshot getSnapshot(); //from Sampling

    @VisibleForTesting
    public void clear();

    @VisibleForTesting
    public void aggregate();

    public boolean considerZeroes();

    public long maxTrackableValue();

    public long[] getOffsets();

    public static Histogram make(boolean isComposite)
    {
        return make(DEFAULT_ZERO_CONSIDERATION, isComposite);
    }

    public static Histogram make(boolean considerZeroes, boolean isComposite)
    {
        return make(considerZeroes, DEFAULT_MAX_TRACKABLE_VALUE, isComposite);
    }

    public static Histogram make(boolean considerZeroes, long maxTrackableValue, boolean isComposite)
    {
        return make(considerZeroes, maxTrackableValue, DatabaseDescriptor.getMetricsHistogramUpdateTimeMillis(), isComposite);
    }

    public static Histogram make(boolean considerZeroes, long maxTrackableValue, int updateIntervalMillis, boolean isComposite)
    {
        return isComposite
               ? new CompositeHistogram(DecayingEstimatedHistogram.makeCompositeReservoir(considerZeroes, maxTrackableValue, updateIntervalMillis, ApproximateClock.defaultClock()))
               : new DecayingEstimatedHistogram(considerZeroes, maxTrackableValue, updateIntervalMillis, ApproximateClock.defaultClock());
    }

    interface Reservoir
    {
        boolean considerZeroes();

        long maxTrackableValue();

        long getCount();

        Snapshot getSnapshot();

        void add(Histogram histogram);

        long[] getOffsets();
    }
}
