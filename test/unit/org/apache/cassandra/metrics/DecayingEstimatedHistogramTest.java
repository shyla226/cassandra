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

import org.junit.Test;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class DecayingEstimatedHistogramTest
{
    private static final double DOUBLE_ASSERT_DELTA = 0;
    private static final Clock CLOCK = new TestClock();
    private static final int TEST_UPDATE_INTERVAL_MILLIS = 0; // zero ensures updates are performed on read

    @Test
    public void testSimple()
    {
        {
            // 0 and 1 map to the same, first bucket
            DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(Histogram.DEFAULT_ZERO_CONSIDERATION,
                                                                                  Histogram.DEFAULT_MAX_TRACKABLE_VALUE,
                                                                                  TEST_UPDATE_INTERVAL_MILLIS,
                                                                                  CLOCK);
            histogram.update(0);
            assertEquals(1, histogram.getSnapshot().getValues()[0]);
            histogram.update(1);
            assertEquals(2, histogram.getSnapshot().getValues()[0]);
        }
        {
            // 0 and 1 map to different buckets
            DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(true,
                                                                                  Histogram.DEFAULT_MAX_TRACKABLE_VALUE,
                                                                                  TEST_UPDATE_INTERVAL_MILLIS,
                                                                                  CLOCK);
            histogram.update(0);
            assertEquals(1, histogram.getSnapshot().getValues()[0]);
            histogram.update(1);
            Snapshot snapshot = histogram.getSnapshot();
            assertEquals(1, snapshot.getValues()[0]);
            assertEquals(1, snapshot.getValues()[1]);
        }
    }

    @Test
    public void testOverflow()
    {
        DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(Histogram.DEFAULT_ZERO_CONSIDERATION, 1, TEST_UPDATE_INTERVAL_MILLIS, CLOCK);
        histogram.update(100);
        assert histogram.isOverflowed();
        assertEquals(Long.MAX_VALUE, histogram.getSnapshot().getMax());
    }

    @Test
    public void testMinMax()
    {
        DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(Histogram.DEFAULT_ZERO_CONSIDERATION,
                                                                              Histogram.DEFAULT_MAX_TRACKABLE_VALUE,
                                                                              TEST_UPDATE_INTERVAL_MILLIS,
                                                                              CLOCK);
        histogram.update(16);
        Snapshot snapshot = histogram.getSnapshot();
        assertEquals(16, snapshot.getMin());
        assertEquals(19, snapshot.getMax());
    }

    @Test
    public void testMean()
    {
        {
            DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(Histogram.DEFAULT_ZERO_CONSIDERATION,
                                                                                  Histogram.DEFAULT_MAX_TRACKABLE_VALUE,
                                                                                  TEST_UPDATE_INTERVAL_MILLIS,
                                                                                  CLOCK);
            for (int i = 0; i < 40; i++)
                histogram.update(0);
            for (int i = 0; i < 20; i++)
                histogram.update(1);
            for (int i = 0; i < 10; i++)
                histogram.update(2);
            assertEquals(1.14D, histogram.getSnapshot().getMean(), 0.1D);
        }
        {
            DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(true,
                                                                                  Histogram.DEFAULT_MAX_TRACKABLE_VALUE,
                                                                                  TEST_UPDATE_INTERVAL_MILLIS,
                                                                                  CLOCK);
            for (int i = 0; i < 40; i++)
                histogram.update(0);
            for (int i = 0; i < 20; i++)
                histogram.update(1);
            for (int i = 0; i < 10; i++)
                histogram.update(2);
            assertEquals(0.57D, histogram.getSnapshot().getMean(), 0.1D);
        }
    }

    @Test
    public void testStdDev()
    {
        {
            DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(Histogram.DEFAULT_ZERO_CONSIDERATION,
                                                                                  Histogram.DEFAULT_MAX_TRACKABLE_VALUE,
                                                                                  TEST_UPDATE_INTERVAL_MILLIS,
                                                                                  CLOCK);
            for (int i = 0; i < 20; i++)
                histogram.update(10);
            for (int i = 0; i < 40; i++)
                histogram.update(20);
            for (int i = 0; i < 20; i++)
                histogram.update(30);

            Snapshot snapshot = histogram.getSnapshot();
            assertEquals(20.0D, snapshot.getMean(), 2.0D);
            assertEquals(7.07D, snapshot.getStdDev(), 2.0D);
        }
    }

    @Test
    public void testFindingCorrectBuckets()
    {
        DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(Histogram.DEFAULT_ZERO_CONSIDERATION,
                                                                              23282687,
                                                                              TEST_UPDATE_INTERVAL_MILLIS,
                                                                              CLOCK);
        histogram.update(23282687);
        assertFalse(histogram.isOverflowed());
        assertEquals(1, histogram.getSnapshot().getValues()[92]);

        histogram.update(9);
        assertEquals(1, histogram.getSnapshot().getValues()[7]);

        histogram.update(21);
        histogram.update(22);
        Snapshot snapshot = histogram.getSnapshot();
        assertEquals(2, snapshot.getValues()[12]);
        assertEquals(5767181.0D, snapshot.getMean(), DOUBLE_ASSERT_DELTA);
    }

    @Test
    public void testPercentile()
    {
        {
            DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(Histogram.DEFAULT_ZERO_CONSIDERATION,
                                                                                  Histogram.DEFAULT_MAX_TRACKABLE_VALUE,
                                                                                  TEST_UPDATE_INTERVAL_MILLIS,
                                                                                  CLOCK);
            // percentile of empty histogram is 0
            assertEquals(0D, histogram.getSnapshot().getValue(0.99), DOUBLE_ASSERT_DELTA);

            histogram.update(1);
            // percentile of a histogram with one element should be that element
            assertEquals(1D, histogram.getSnapshot().getValue(0.99), DOUBLE_ASSERT_DELTA);

            histogram.update(10);
            assertEquals(10D, histogram.getSnapshot().getValue(0.99), DOUBLE_ASSERT_DELTA);
        }

        {
            DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(Histogram.DEFAULT_ZERO_CONSIDERATION,
                                                                                  Histogram.DEFAULT_MAX_TRACKABLE_VALUE,
                                                                                  TEST_UPDATE_INTERVAL_MILLIS,
                                                                                  CLOCK);

            histogram.update(1);
            histogram.update(2);
            histogram.update(3);
            histogram.update(4);
            histogram.update(5);

            Snapshot snapshot = histogram.getSnapshot();
            assertEquals(0, snapshot.getValue(0.00), DOUBLE_ASSERT_DELTA);
            assertEquals(3, snapshot.getValue(0.50), DOUBLE_ASSERT_DELTA);
            assertEquals(3, snapshot.getValue(0.60), DOUBLE_ASSERT_DELTA);
            assertEquals(5, snapshot.getValue(1.00), DOUBLE_ASSERT_DELTA);
        }

        {
            DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(Histogram.DEFAULT_ZERO_CONSIDERATION,
                                                                                  Histogram.DEFAULT_MAX_TRACKABLE_VALUE,
                                                                                  TEST_UPDATE_INTERVAL_MILLIS,
                                                                                  CLOCK);

            for (int i = 11; i <= 20; i++)
                histogram.update(i);

            // Right now the histogram looks like:
            //    10, 12, 14, 16, 20
            //     1   2   2   4   1
            // %:  10  30  50  90  100
            Snapshot snapshot = histogram.getSnapshot();
            assertEquals(10, snapshot.getValue(0.01), DOUBLE_ASSERT_DELTA);
            assertEquals(12, snapshot.getValue(0.30), DOUBLE_ASSERT_DELTA);
            assertEquals(14, snapshot.getValue(0.50), DOUBLE_ASSERT_DELTA);
            assertEquals(16, snapshot.getValue(0.60), DOUBLE_ASSERT_DELTA);
            assertEquals(16, snapshot.getValue(0.80), DOUBLE_ASSERT_DELTA);
            assertEquals(20, snapshot.getValue(1.00), DOUBLE_ASSERT_DELTA);
        }
        {
            DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(true,
                                                                                  Histogram.DEFAULT_MAX_TRACKABLE_VALUE,
                                                                                  TEST_UPDATE_INTERVAL_MILLIS,
                                                                                  CLOCK);
            histogram.update(0);
            histogram.update(0);
            histogram.update(1);

            Snapshot snapshot = histogram.getSnapshot();
            assertEquals(0, snapshot.getValue(0.5), DOUBLE_ASSERT_DELTA);
            assertEquals(1, snapshot.getValue(0.99), DOUBLE_ASSERT_DELTA);
        }
    }

    @Test
    public void testDecayingPercentile()
    {
        {
            TestClock clock = new TestClock();

            DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(Histogram.DEFAULT_ZERO_CONSIDERATION,
                                                                                  Histogram.DEFAULT_MAX_TRACKABLE_VALUE,
                                                                                  TEST_UPDATE_INTERVAL_MILLIS,
                                                                                  clock);
            // percentile of empty histogram is 0
            assertEquals(0, histogram.getSnapshot().getValue(1.0), DOUBLE_ASSERT_DELTA);

            for (int v = 1; v <= 100; v++)
            {
                for (int i = 0; i < 10_000; i++)
                    histogram.update(v);
            }

            // 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 19, 22, 26, 31, 37, 44, 52, 62, 74, 88, 105,
            // 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 28, 32, 40, 48, 56, 64, 80, 96, 112,

            Snapshot snapshot = histogram.getSnapshot();
            assertEstimatedQuantile(05, snapshot.getValue(0.05));
            assertEstimatedQuantile(20, snapshot.getValue(0.20));
            assertEstimatedQuantile(40, snapshot.getValue(0.40));
            assertEstimatedQuantile(96, snapshot.getValue(0.99));

            clock.addSeconds(DecayingEstimatedHistogram.ForwardDecayingReservoir.HALF_TIME_IN_S);
            snapshot = histogram.getSnapshot();
            assertEstimatedQuantile(05, snapshot.getValue(0.05));
            assertEstimatedQuantile(20, snapshot.getValue(0.20));
            assertEstimatedQuantile(40, snapshot.getValue(0.40));
            assertEstimatedQuantile(96, snapshot.getValue(0.99));

            for (int v = 1; v <= 50; v++)
            {
                for (int i = 0; i < 10_000; i++)
                    histogram.update(v);
            }

            snapshot = histogram.getSnapshot();
            assertEstimatedQuantile(04, snapshot.getValue(0.05));
            assertEstimatedQuantile(14, snapshot.getValue(0.20));
            assertEstimatedQuantile(24, snapshot.getValue(0.40));
            assertEstimatedQuantile(96, snapshot.getValue(0.99));

            clock.addSeconds(DecayingEstimatedHistogram.ForwardDecayingReservoir.HALF_TIME_IN_S);
            snapshot = histogram.getSnapshot();
            assertEstimatedQuantile(04, snapshot.getValue(0.05));
            assertEstimatedQuantile(14, snapshot.getValue(0.20));
            assertEstimatedQuantile(24, snapshot.getValue(0.40));
            assertEstimatedQuantile(96, snapshot.getValue(0.99));

            for (int v = 1; v <= 50; v++)
            {
                for (int i = 0; i < 10_000; i++)
                    histogram.update(v);
            }

            snapshot = histogram.getSnapshot();
            assertEstimatedQuantile(03, snapshot.getValue(0.05));
            assertEstimatedQuantile(12, snapshot.getValue(0.20));
            assertEstimatedQuantile(20, snapshot.getValue(0.40));
            assertEstimatedQuantile(96, snapshot.getValue(0.99));

            clock.addSeconds(DecayingEstimatedHistogram.ForwardDecayingReservoir.HALF_TIME_IN_S);
            snapshot = histogram.getSnapshot();
            assertEstimatedQuantile(03, snapshot.getValue(0.05));
            assertEstimatedQuantile(12, snapshot.getValue(0.20));
            assertEstimatedQuantile(20, snapshot.getValue(0.40));
            assertEstimatedQuantile(96, snapshot.getValue(0.99));

            for (int v = 11; v <= 20; v++)
            {
                for (int i = 0; i < 5_000; i++)
                    histogram.update(v);
            }

            snapshot = histogram.getSnapshot();
            assertEstimatedQuantile(04, snapshot.getValue(0.05));
            assertEstimatedQuantile(12, snapshot.getValue(0.20));
            assertEstimatedQuantile(20, snapshot.getValue(0.40));
            assertEstimatedQuantile(95, snapshot.getValue(0.99));

            clock.addSeconds(DecayingEstimatedHistogram.ForwardDecayingReservoir.HALF_TIME_IN_S);
            snapshot = histogram.getSnapshot();
            assertEstimatedQuantile(04, snapshot.getValue(0.05));
            assertEstimatedQuantile(12, snapshot.getValue(0.20));
            assertEstimatedQuantile(20, snapshot.getValue(0.40));
            assertEstimatedQuantile(95, snapshot.getValue(0.99));

        }

        {
            TestClock clock = new TestClock();

            DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(Histogram.DEFAULT_ZERO_CONSIDERATION,
                                                                                  Histogram.DEFAULT_MAX_TRACKABLE_VALUE,
                                                                                  TEST_UPDATE_INTERVAL_MILLIS,
                                                                                  clock);
            // percentile of empty histogram is 0
            assertEquals(0, histogram.getSnapshot().getValue(0.99), DOUBLE_ASSERT_DELTA);

            for (int m = 0; m < 40; m++)
            {
                for (int i = 0; i < 1_000_000; i++)
                {
                    histogram.update(2);
                }

                // percentile of a histogram with one element should be that element
                clock.addSeconds(DecayingEstimatedHistogram.ForwardDecayingReservoir.HALF_TIME_IN_S);
                assertEquals(2, histogram.getSnapshot().getValue(0.99), DOUBLE_ASSERT_DELTA);
            }

            clock.addSeconds(DecayingEstimatedHistogram.ForwardDecayingReservoir.HALF_TIME_IN_S * 100);
            assertEquals(0, histogram.getSnapshot().getValue(0.99), DOUBLE_ASSERT_DELTA);
        }

        {
            TestClock clock = new TestClock();

            DecayingEstimatedHistogram histogram = new DecayingEstimatedHistogram(Histogram.DEFAULT_ZERO_CONSIDERATION,
                                                                                  Histogram.DEFAULT_MAX_TRACKABLE_VALUE,
                                                                                  TEST_UPDATE_INTERVAL_MILLIS,
                                                                                  clock);

            histogram.update(20);
            histogram.update(21);
            histogram.update(25);
            histogram.update(26);
            Snapshot snapshot = histogram.getSnapshot();
            assertEquals(2, snapshot.getValues()[12]);
            assertEquals(2, snapshot.getValues()[13]);

            clock.addSeconds(DecayingEstimatedHistogram.ForwardDecayingReservoir.HALF_TIME_IN_S);

            histogram.update(20);
            histogram.update(21);
            histogram.update(25);
            histogram.update(26);
            snapshot = histogram.getSnapshot();
            assertEquals(4, snapshot.getValues()[12]);
            assertEquals(4, snapshot.getValues()[13]);
        }
    }

    private void assertEstimatedQuantile(long expectedValue, double actualValue)
    {
        assertTrue("Expected at least [" + expectedValue + "] but actual is [" + actualValue + "]",
                   actualValue >= expectedValue);
        assertTrue("Expected less than [" + Math.round(expectedValue * 1.2) + "] but actual is [" + actualValue + "]",
                   actualValue < Math.round(expectedValue * 1.2));
    }

    public static class TestClock extends Clock {
        private long tick = 0;

        public void addSeconds(long seconds)
        {
            tick += seconds * 1_000_000_000L;
        }

        public long getTick()
        {
            return tick;
        }

        public long getTime()
        {
            return tick / 1_000_000L;
        };
    }
}
