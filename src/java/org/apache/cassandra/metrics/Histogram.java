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

/**
 * A metric which calculates the distribution of a value.
 *
 * @see <a href="http://www.johndcook.com/standard_deviation.html">Accurately computing running
 *      variance</a>
 *
 * This class removes the {@link java.util.concurrent.atomic.LongAdder} of the {@link com.codahale.metrics.Histogram}
 * class and retrieves {@link this#getCount()} from {@link Reservoir}.
 */
public class Histogram implements Metric, Sampling, Counting
{
    private final DecayingEstimatedHistogramReservoir reservoir;

    /**
     * Creates a new {@link com.codahale.metrics.Histogram} with the given reservoir.
     *
     * @param reservoir the reservoir to create a histogram from
     */
    public Histogram(DecayingEstimatedHistogramReservoir reservoir) {
        this.reservoir = reservoir;
    }

    /**
     * Adds a recorded value.
     *
     * @param value the length of the value
     */
    public final void update(final long value) {
        reservoir.update(value);
    }

    /**
     * Returns the number of values recorded.
     *
     * @return the number of values recorded
     */
    @Override
    public long getCount() {
        return reservoir.getCount();
    }

    @Override
    public Snapshot getSnapshot() {
        return reservoir.getSnapshot();
    }

    @VisibleForTesting
    public void clear()
    {
        reservoir.clear();
    }

    @VisibleForTesting
    public void aggregate()
    {
        reservoir.aggregate();
    }
}
