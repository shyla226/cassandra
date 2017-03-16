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

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.index.sasi.utils.AbstractIterator;

import static org.apache.cassandra.metrics.Histogram.DEFAULT_MAX_TRACKABLE_VALUE;
import static org.apache.cassandra.metrics.Histogram.DEFAULT_ZERO_CONSIDERATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompositeHistogramTest
{
    private static final DecayingEstimatedHistogramTest.TestClock CLOCK = new DecayingEstimatedHistogramTest.TestClock();
    private static final int TEST_UPDATE_INTERVAL_MILLIS = 0; // zero ensures updates are performed on read
    private static final DecayingEstimatedHistogram.Reservoir DEFAULT_RESERVOIR =
        DecayingEstimatedHistogram.makeCompositeReservoir(DEFAULT_ZERO_CONSIDERATION, DEFAULT_MAX_TRACKABLE_VALUE, TEST_UPDATE_INTERVAL_MILLIS, CLOCK);

    private static final double DELTA = 1e-15; // precision for double comparisons

    @Test
    public void testSingleAggregation()
    {
        testAggregation(1, true, identicalSequentialValues(1, 100));
    }

    @Test
    public void testDoubleAggregation()
    {
        testAggregation(2, false, identicalSequentialValues(2, 100));
    }

    @Test
    public void testSmallAggregation()
    {
        testAggregation(10, true, identicalSequentialValues(10, 100));
    }

    @Test
    public void testMediumAggregation()
    {
        testAggregation(100, false, identicalSequentialValues(100, 100));
    }

    @Test
    public void testLargeAggregation()
    {
        testAggregation(1000, true, identicalSequentialValues(1000, 100));
    }

    @Test
    public void testRandomAggregation()
    {
        testAggregation(100, false, randomValues(100, 10000));
    }

    Iterator<Long> identicalSequentialValues(final int numHistograms, final int numValues)
    {
        return new AbstractIterator<Long>()
        {
            int currentHistogram = 0;
            long currentValue = 0;

            protected Long computeNext()
            {
                if (currentHistogram >= numHistograms)
                    return null;

                if (currentValue < numValues)
                {
                    return currentValue++;
                }
                else
                {
                    currentValue = 0;
                    currentHistogram++;
                    return -1L;
                }
            }
        };
    }

    Iterator<Long> randomValues(final int numHistograms, final int bound)
    {
        return new AbstractIterator<Long>()
        {
            int currentHistogram = 0;
            long currentValue = 0;
            int nextMaxVal = ThreadLocalRandom.current().nextInt(1, bound);

            protected Long computeNext()
            {
                if (currentHistogram >= numHistograms)
                    return null;

                if (currentValue < nextMaxVal)
                {
                    currentValue++;
                    return ThreadLocalRandom.current().nextLong(0, DEFAULT_MAX_TRACKABLE_VALUE);
                }
                else
                {
                    currentValue = 0;
                    nextMaxVal = ThreadLocalRandom.current().nextInt(1, bound);
                    currentHistogram++;
                    return -1L;
                }
            }
        };
    }

    /**
     * Create the specified number of histograms and aggregate them. Create a control histogram that
     * receives all updates. The iterator returns values to update in each single histogram, with a negative
     * value signaling that it is time to move on to the next histogram. At the end check that the
     * control and the aggregated histograms are equivalent.
     *
     * @param numHistograms - the number of histograms to aggregate
     * @param considerZeroes - whether initial zeroes are considered
     * @param values - an iterator returning values to insert into each histogram, negative values are used to move on to the next histogram
     */
    private void testAggregation(int numHistograms, boolean considerZeroes, Iterator<Long> values)
    {
        DecayingEstimatedHistogram[] histograms = new DecayingEstimatedHistogram[numHistograms];
        for (int i = 0; i < numHistograms; i++)
            histograms[i] = new DecayingEstimatedHistogram(considerZeroes, DEFAULT_MAX_TRACKABLE_VALUE, TEST_UPDATE_INTERVAL_MILLIS, CLOCK);

        DecayingEstimatedHistogram.Reservoir reservoir = DecayingEstimatedHistogram.makeCompositeReservoir(considerZeroes,
                                                                                                           DEFAULT_MAX_TRACKABLE_VALUE,
                                                                                                           TEST_UPDATE_INTERVAL_MILLIS,
                                                                                                           CLOCK);
        CompositeHistogram compositeHistogram = new CompositeHistogram(reservoir);
        DecayingEstimatedHistogram controlHistogram = new DecayingEstimatedHistogram(considerZeroes,
                                                                                     DEFAULT_MAX_TRACKABLE_VALUE,
                                                                                     TEST_UPDATE_INTERVAL_MILLIS,
                                                                                     CLOCK);

        assertEquals(considerZeroes, compositeHistogram.considerZeroes());
        assertEquals(DEFAULT_MAX_TRACKABLE_VALUE, compositeHistogram.maxTrackableValue());

        for (DecayingEstimatedHistogram histogram : histograms)
            compositeHistogram.compose(histogram);

        int index = 0;
        while (values.hasNext() && index < histograms.length)
        {
            Long v = values.next();
            if (v < 0)
            {
                index++;
                continue;
            }

            histograms[index].update(v);
            controlHistogram.update(v);
        }

        checkHistogramsAreEquivalent(controlHistogram, compositeHistogram);

        // check with a rescale
        CLOCK.addSeconds(DecayingEstimatedHistogram.ForwardDecayingReservoir.HALF_TIME_IN_S);
        checkHistogramsAreEquivalent(controlHistogram, compositeHistogram);
    }

    /**
     * Check that the two histograms are equivalent in terms of values returned to the clients.
     *
     * @param controlHistogram - the control histogram that gets updated all the time
     * @param compositeHistogram - the aggregated histogram to test
     */
    private void checkHistogramsAreEquivalent(DecayingEstimatedHistogram controlHistogram, CompositeHistogram compositeHistogram)
    {
        assertEquals(controlHistogram.getCount(), compositeHistogram.getCount());

        Snapshot controlSnapshot = controlHistogram.getSnapshot();
        Snapshot aggregatedSnapshot = compositeHistogram.getSnapshot();

        assertTrue(controlSnapshot.size() == aggregatedSnapshot.size());
        assertTrue(Arrays.equals(controlSnapshot.getValues(), aggregatedSnapshot.getValues()));
        assertEquals(controlSnapshot.getMean(), aggregatedSnapshot.getMean(), DELTA);
        assertEquals(controlSnapshot.getMin(), aggregatedSnapshot.getMin());
        assertEquals(controlSnapshot.getMax(), aggregatedSnapshot.getMax());
        assertEquals(controlSnapshot.getMedian(), aggregatedSnapshot.getMedian(), DELTA);
        assertEquals(controlSnapshot.get75thPercentile(), aggregatedSnapshot.get75thPercentile(), DELTA);
        assertEquals(controlSnapshot.get95thPercentile(), aggregatedSnapshot.get95thPercentile(), DELTA);
        assertEquals(controlSnapshot.get98thPercentile(), aggregatedSnapshot.get98thPercentile(), DELTA);
        assertEquals(controlSnapshot.get99thPercentile(), aggregatedSnapshot.get99thPercentile(), DELTA);
        assertEquals(controlSnapshot.get999thPercentile(), aggregatedSnapshot.get999thPercentile(), DELTA);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUpdateNotSupported()
    {
        CompositeHistogram histogram = new CompositeHistogram(DEFAULT_RESERVOIR);
        histogram.update(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAggregateNotSupported()
    {
        CompositeHistogram histogram = new CompositeHistogram(DEFAULT_RESERVOIR);
        histogram.aggregate();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testClearNotSupported()
    {
        CompositeHistogram histogram = new CompositeHistogram(DEFAULT_RESERVOIR);
        histogram.clear();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddWithWrongType()
    {
        CompositeHistogram histogram = new CompositeHistogram(DEFAULT_RESERVOIR);
        histogram.compose(new CompositeHistogram(DEFAULT_RESERVOIR));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddWithNull()
    {
        CompositeHistogram histogram = new CompositeHistogram(DEFAULT_RESERVOIR);
        histogram.compose(null);
    }
}
