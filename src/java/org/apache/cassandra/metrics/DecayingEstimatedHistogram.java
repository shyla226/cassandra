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

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Clock;
import net.nicoulaj.compilecommand.annotations.Inline;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.apache.cassandra.utils.EstimatedHistogram;

/**
 * A decaying histogram where values collected during each minute will be twice as significant as the values
 * collected in the previous minute. Measured values are collected in variable sized buckets, using small buckets in the
 * lower range and larger buckets in the upper range. Use this histogram when you want to know if the distribution of
 * the underlying data stream has changed recently and you want high resolution on values in the lower range.
 *
 * The histogram use forward decay [1] to make recent values more significant. The forward decay factor will be doubled
 * every minute (half-life time set to 60 seconds) [2]. The forward decay landmark is reset every 30 minutes (or at
 * first read/update after 30 minutes).
 *
 * The 30 minute rescale interval is used based on the assumption that in an extreme case we would have to collect a
 * metric 1M times for a single bucket each second. By the end of the 30:th minute all collected values will roughly
 * add up to 1.000.000 * 60 * pow(2, 30) which can be represented with 56 bits giving us some head room in a signed
 * 64 bit long.
 *
 * Internally two histograms are maintained, one with decay and one without decay. All public getters in a snapshot
 * will expose the decay functionality with the exception of the {@link com.codahale.metrics.Snapshot#getValues()}
 * which will return values from the histogram without decay. This makes it possible for the caller to maintain precise
 * deltas in an interval of its choise.
 *
 * Each bucket represents values from [previous bucket offset, current offset). See {@link BucketProperties} for details
 * on how the bucket offsets are calcualted.
 *
 * Internally the histogram will store updates in temporary buffers, one per core thread plus another one for every other thread.
 * Periodically this temporary buffers are aggregated into the final buckets and decaying buckets by scheduling a task
 * always on the same core. The core is chosen in a round robin fashion. Optionally aggregation is performed also before
 * a read, if the thread reading is the same as the aggregation thread or if the thread reading is not a core thread. This is
 * mostly to avoid breaking unit tests (timers internally use a histogram).
 *
 * The temporary buffers that are dedicated to a core thread will be updated by performing a volatile read and an ordered write,
 * not a volatile write since this would be too expensive. The temporary buffer shared by all threads is instead updated with
 * a CAS.
 *
 * The aggregation thread will read and update all temporary buffers with a CAS that sets the values to zero. Because of the
 * ordered write in the dedicated buffers, it is possible to rarely miss out on the latest update, but not very likely since
 * an ordered write guarantes any reading thread will see the value in a matter of nanoseconds.
 * See {@link sun.misc.Unsafe#putOrderedLong(Object, long, long)}.
 *
 * [1]: http://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf
 * [2]: https://en.wikipedia.org/wiki/Half-life
 * [3]: https://github.com/dropwizard/metrics/blob/v3.1.2/metrics-core/src/main/java/com/codahale/metrics/ExponentiallyDecayingReservoir.java
 */
final class DecayingEstimatedHistogram implements Histogram
{
    /**
     * The reservoir contains the buckets with and without decay, and they are periodically
     * aggregated by looking at the values in the recorder.
     */
    private final ForwardDecayingReservoir reservoir;

    /** The recorders takes care of performing real-time updates, there is typically one recorder
     * for simple histogram (at the table level) and multiple recorders for aggregated histograms
     * (at the keyspace or global level). */
    private final Recorder recorder;



    DecayingEstimatedHistogram(boolean considerZeroes, long maxTrackableValue, int updateTimeMillis, Clock clock)
    {
        BucketProperties bucketProperties = new BucketProperties(maxTrackableValue);
        this.reservoir = new ForwardDecayingReservoir(bucketProperties, clock, considerZeroes, updateTimeMillis, false);
        this.recorder = new Recorder(bucketProperties, reservoir);
    }

    static Reservoir makeCompositeReservoir(boolean considerZeroes, long maxTrackableValue, int updateIntervalMillis, Clock clock)
    {
        return new ForwardDecayingReservoir(new BucketProperties(maxTrackableValue),
                                            clock,
                                            considerZeroes,
                                            updateIntervalMillis,
                                            true);
    }

    /**
     * Increments the count of the bucket closest to n, rounding UP.
     *
     * @param value the data point to add to the histogram
     */
    public final void update(final long value)
    {
        recorder.update(value);
    }

    /**
     * Return the number of buckets where recorded values are stored.
     *
     * @return the number of buckets
     */
    public int size()
    {
        return reservoir.bucketProperties.size();
    }

    /**
     * Returns a snapshot of the decaying values in this histogram.
     *
     * @return the snapshot
     */
    public com.codahale.metrics.Snapshot getSnapshot()
    {
        return reservoir.getSnapshot();
    }

    /**
     * @return true if this histogram has overflowed -- that is, a value larger than our largest bucket could bound was added
     */
    @VisibleForTesting
    boolean isOverflowed()
    {
        return reservoir.isOverflowed();
    }

    @VisibleForTesting
    public void clear()
    {
        recorder.clear();
        reservoir.clear();
    }

    public void aggregate()
    {
        reservoir.onReadAggregate();
    }

    public boolean considerZeroes()
    {
        return reservoir.considerZeroes();
    }

    public long maxTrackableValue()
    {
        return reservoir.maxTrackableValue();
    }

    public long[] getOffsets()
    {
        return reservoir.getOffsets();
    }

    public long getCount()
    {
        return reservoir.getCount();
    }

    @Override
    public Type getType()
    {
        return Type.SINGLE;
    }

    /**
     * Store any properties of the buckets, and creates the offsets on the fly, when required.
     *
     * Bucket sizes are exponentially increasing so that smaller values have a better resolution.
     * In the legacy implementation bucket sizes were increasing by multiplying the previous size by 1.2
     * and the index was found via a binary search. This was too slow and the bucket sizes have been replaced
     * by sizes that increase by 2 so that we can find an index more quickly. To best approximate the existing
     * behavior, the first 8 buckets have width 1, then the next 4 have width 2, the next 4 have width 4 and so on.
     * We can divide our buckets into buckets with sub-buckets, where the first bucket contains the initial 8 buckets
     * and then each other bucket overlaps withe the previous bucket by adding 4 new buckets, all with the same (double)
     * width. With this trick we can find an index as explained by the comments in getIndex(). This mechanism is similar
     * to what's been implemented in https://github.com/HdrHistogram/HdrHistogram, except there it is more
     * generic and it allows to specify the desired histogram precision (by using more buckets than we do
     * with our fixed properties).
     *
     */
    static final class BucketProperties
    {
        /** The maximum value that can be tracked by the histogram */
        final long maxTrackableValue;

        /** The number of buckets needed to cover the maximum trackable value */
        final int numBuckets;

        /** Values for calculating buckets and indexes */
        final static int subBucketCount = 8;  // number of sub-buckets in each bucket
        final static int subBucketHalfCount = subBucketCount / 2;
        final static int unitMagnitude = 0; // power of two of the unit in bucket zero (2^0 = 1)
        final static int subBucketCountMagnitude = 3; // power of two of the number of sub buckets
        final static int subBucketHalfCountMagnitude = subBucketCountMagnitude - 1; // power of two of half the number of sub-buckets

        final long subBucketMask;
        final int leadingZeroCountBase;

        BucketProperties(long maxTrackableValue)
        {
            this.maxTrackableValue = maxTrackableValue;
            this.numBuckets = makeOffsets(true).length;
            this.subBucketMask = (long)(subBucketCount - 1) << unitMagnitude;
            this.leadingZeroCountBase = 64 - unitMagnitude - subBucketHalfCountMagnitude - 1;
        }

        /**
         * Return a number of buckets sufficient to track up to maxTrackableValue.
         * @param considerZeroes when true add zero as the first value to the offsets
         * @return the number of buckets required (the array size)
         */
        long[] makeOffsets(boolean considerZeroes)
        {
            ArrayList<Long> ret = new ArrayList<>();
            if (considerZeroes)
               ret.add(0L);

            for (int i = 1; i <= subBucketCount; i++)
            {
                ret.add((long) i);
                if (i >= maxTrackableValue)
                    break;
            }

            long last = subBucketCount;
            long unit = 1 << (unitMagnitude + 1);

            while (last < maxTrackableValue)
            {
                for (int i = 0; i < subBucketHalfCount; i++)
                {
                    last += unit;
                    ret.add(last);
                    if (last >= maxTrackableValue)
                        break;
                }
                unit *= 2;
            }

            return ret.stream().mapToLong(i->i).toArray();
        }

        /**
         * Given a valud, return the index of the bucket where this value is located.
         */
        @Inline
        final int getIndex(final long value) {
            if (value < 0) {
                throw new ArrayIndexOutOfBoundsException("Histogram recorded value cannot be negative.");
            }

            // Calculates the number of powers of two by which the value is greater than the biggest value that fits in
            // bucket 0. This is the bucket index since each successive bucket can hold a value 2x greater.
            // The mask maps small values to bucket 0.
            final int bucketIndex = leadingZeroCountBase - Long.numberOfLeadingZeros(value | subBucketMask);

            // For bucketIndex 0, this is just value, so it may be anywhere in 0 to subBucketCount.
            // For other bucketIndex, this will always end up in the top half of subBucketCount: assume that for some bucket
            // k > 0, this calculation will yield a value in the bottom half of 0 to subBucketCount. Then, because of how
            // buckets overlap, it would have also been in the top half of bucket k-1, and therefore would have
            // returned k-1 in getBucketIndex(). Since we would then shift it one fewer bits here, it would be twice as big,
            // and therefore in the top half of subBucketCount.
            final int subBucketIndex = (int)(value >>> (bucketIndex + unitMagnitude));

            //assert(subBucketIndex < subBucketCount);
            //assert(bucketIndex == 0 || (subBucketIndex >= subBucketHalfCount));
            // Calculate the index for the first entry that will be used in the bucket (halfway through subBucketCount).
            // For bucketIndex 0, all subBucketCount entries may be used, but bucketBaseIndex is still set in the middle.
            final int bucketBaseIndex = (bucketIndex + 1) << subBucketHalfCountMagnitude;

            // Calculate the offset in the bucket. This subtraction will result in a positive value in all buckets except
            // the 0th bucket (since a value in that bucket may be less than half the bucket's 0 to subBucketCount range).
            // However, this works out since we give bucket 0 twice as much space.
            final int offsetInBucket = subBucketIndex - subBucketHalfCount;

            // The following is the equivalent of ((subBucketIndex  - subBucketHalfCount) + bucketBaseIndex;
            return bucketBaseIndex + offsetInBucket;
        }

        private int size()
        {
            return numBuckets;
        }
    }

    /**
     * This is the reservoir for DecayingEstimatedHistogram. It will periodically aggregate
     * the value of the recorders and perform forward decay.
     */
    final static class ForwardDecayingReservoir implements Reservoir
    {
        static final long HALF_TIME_IN_S = 60L;
        static final double MEAN_LIFETIME_IN_S = HALF_TIME_IN_S / Math.log(2.0);
        static final long LANDMARK_RESET_INTERVAL_IN_MS = 30L * 60L * 1000L;

        /** Stores the properties of the buckets, such as offsets and how to index into the buckets. */
        private final BucketProperties bucketProperties;

        /**
         * Provides the current time. Encapsulated into a class for testing purposes.
         */
        private final Clock clock;

        /** True if the first bucket starts with an offset of zero. This only affects the snapshots and is kept
         * for backward compatibility, internally offsets will always start at zero.
         */
        private final boolean considerZeroes;

        /** How ofter the buckets are updated by reading from the recorders and performing forward decay. */
        private final int updateIntervalMillis;

        /** This is the last time forward decay was rescaled */
        private long decayLandmark;

        /** The recorders take care of performing real-time updates, there is typically one recorder
         * for a simple histogram (at the table level) and multiple recorders for aggregated histograms
         * (at the keyspace or global level). Recorders are shared by reservoirs and must in no way be
         * altered.
         * */
        private final CopyOnWriteArrayList<Recorder> recorders;

        /** The core on which the aggregating thread is running */
        private final int coreId;

        /** The aggregated buckets without forward decaying */
        private final long[] buckets;

        /** The aggregated buckets with forward decaying */
        private final long[] decayingBuckets;

        /** This is published after a periodic aggregation */
        private volatile Snapshot snapshot;

        /** This is true when this is the reservoir of a composite histogram */
        private final boolean isComposite;

        /** Composite reservoirs are scheduled eagerly, but single reservoir are
         * scheduled by the recorder.
         */
        private final AtomicBoolean scheduled;

        /**
         * This is set to true by the aggregating thread when at least one of the local buckets
         * is overflowed, that is it could not track a value becasue it was too high even for
         * the largest bucket.
         */
        private volatile boolean isOverflowed;

        ForwardDecayingReservoir(BucketProperties bucketProperties, Clock clock, boolean considerZeroes, int updateIntervalMillis, boolean isComposite)
        {
            this.bucketProperties = bucketProperties;
            this.clock = clock;
            this.considerZeroes = considerZeroes;
            this.updateIntervalMillis = updateIntervalMillis;
            this.isOverflowed = false;
            this.buckets = new long[bucketProperties.numBuckets];
            this.decayingBuckets = new long[bucketProperties.numBuckets];
            this.recorders = new CopyOnWriteArrayList<>();
            this.decayLandmark = clock.getTime();
            this.snapshot = new Snapshot(this);
            this.coreId = NettyRxScheduler.getNextCore();
            this.scheduled = new AtomicBoolean(false);
            this.isComposite = isComposite;

            scheduleIfComposite();
        }

        void add(Recorder recorder)
        {
            recorders.add(recorder);
        }

        void maybeSchedule()
        {
            if (updateIntervalMillis <= 0 || scheduled.get())
                return;

            if (scheduled.compareAndSet(false, true))
                NettyRxScheduler.getForCore(coreId).scheduleDirect(this::aggregate, updateIntervalMillis, TimeUnit.MILLISECONDS);
        }

        void scheduleIfComposite()
        {
            if (updateIntervalMillis > 0 && isComposite)
                NettyRxScheduler.getForCore(coreId).scheduleDirect(this::aggregate, updateIntervalMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public boolean considerZeroes()
        {
            return considerZeroes;
        }

        @Override
        public long maxTrackableValue()
        {
            return bucketProperties.maxTrackableValue;
        }

        /**
         * Perform an aggregation before a read.
         *
         * This is only done if the update interval is <= 0, which means that no updates are scheduled.
         * Aggregation is not thread safe and therefore the callers must take care of serializing calls
         * to read methods that in turn call this method. This is currently performed only by unit tests,
         * which are single threaded, or by {@link org.apache.cassandra.locator.DynamicEndpointSnitch},
         * which only consults the histogram in a periodic task, hence from the same thread.
         */
        void onReadAggregate()
        {
            if (updateIntervalMillis <= 0)
                aggregate();
        }

        boolean isOverflowed()
        {
            onReadAggregate();
            return isOverflowed;
        }

        public long getCount()
        {
            onReadAggregate();
            return snapshot.getCount();
        }

        public Snapshot getSnapshot()
        {
            onReadAggregate();
            return snapshot;
        }

        /**
         * Read values from the thread local buffers and aggregate them into the buckets. Perform any forward decay or
         * rescaling. This method is suppossed to be called always from only one thread at a time by scheduling a task
         * on the same core.
         */
        @VisibleForTesting
        void aggregate()
        {
            try
            {
                scheduled.set(false);

                final long now = clock.getTime();
                rescaleIfNeeded(now);

                final long weight = Math.round(forwardDecayWeight(now, decayLandmark));
                this.isOverflowed = recorders.stream().map(Recorder::isOverFlowed).reduce(Boolean.FALSE, Boolean::logicalOr);

                for (int i = 0; i < buckets.length; i++)
                {
                    final int index = i;
                    long value = recorders.stream().map(recorder -> recorder.getValue(index)).reduce(0L, Long::sum);
                    long delta = value - buckets[i];
                    if (delta > 0)
                    {
                        buckets[i] += delta;
                        decayingBuckets[i] += delta * weight;
                    }

                }

                this.snapshot = new Snapshot(this);
            }
            finally
            {
                scheduleIfComposite();
            }
        }

        private boolean isCompatible(Reservoir other)
        {
            if (!(other instanceof ForwardDecayingReservoir))
                return false;

            ForwardDecayingReservoir otherFDReservoir = (ForwardDecayingReservoir)other;
            return otherFDReservoir.considerZeroes == this.considerZeroes &&
                   otherFDReservoir.buckets.length == this.buckets.length &&
                   otherFDReservoir.decayingBuckets.length == this.decayingBuckets.length;
        }

        public void add(Histogram histogram)
        {
            if (!(histogram instanceof DecayingEstimatedHistogram))
                throw new IllegalArgumentException("Histogram is not compatible");

            DecayingEstimatedHistogram decayingEstimatedHistogram = (DecayingEstimatedHistogram)histogram;

            if (!isCompatible(decayingEstimatedHistogram.reservoir))
                throw new IllegalArgumentException("Histogram reservoir is not compatible");

            add(decayingEstimatedHistogram.recorder);
        }

        public long[] getOffsets()
        {
            return bucketProperties.makeOffsets(considerZeroes);
        }

        void clear()
        {
            isOverflowed = false;
            for (int i = 0; i < bucketProperties.size(); i++)
            {
                buckets[i] = 0;
                decayingBuckets[i] = 0;
            }

            snapshot = new Snapshot(this);
        }

        private void rescaleIfNeeded(long now)
        {
            if (needRescale(now))
            {
                rescale(now);
            }
        }

        private void rescale(long now)
        {
            final double rescaleFactor = forwardDecayWeight(now, decayLandmark);
            decayLandmark = now;

            for (int i = 0; i < decayingBuckets.length; i++)
                decayingBuckets[i] = Math.round((decayingBuckets[i] / rescaleFactor));
        }

        private boolean needRescale(long now)
        {
            return (now - decayLandmark) > LANDMARK_RESET_INTERVAL_IN_MS;
        }

        private double forwardDecayWeight()
        {
            return forwardDecayWeight(clock.getTime(), decayLandmark);
        }

        private static double forwardDecayWeight(long now, long decayLandmark)
        {
            return Math.exp(((now - decayLandmark) / 1000.0) / MEAN_LIFETIME_IN_S);
        }
    }


    /**
     * A recorder takes care of updating values. It owns up to N+1 buffers,
     * where N is the number of cores. Depending on the thread that performs
     * the update, the corresponding buffer is updated. The thread-per-core
     * threads use a private buffer, updated with an Unsafe.putOrderedWrong
     * (which is an atomic write, withouth the memory ordering of volatile writes)
     * whilst all other threads share a final buffer which is updated with a CAS.
     *
     * The values in the buffers are then aggreagated on read.
     *
     * We allocate memory for buffers only as needed for two reasons: to reduce
     * memory footprint and, most importantly, to reduce the likehood that
     * two buffer pointers will share the same cache line. On allocation we
     * use the UNSAFE to atomically set the buffer pointer into the array.
     *
     * An histogram then aggregates the values from one or more recorders.
     * A simple histogram owns a single Recorder whilst an aggregate histogram
     * references multiple recorders from its child histograms.
     */
    private static final class Recorder
    {
        private final BucketProperties bucketProperties;
        private final int numCores;
        private final Buffer[] buffers;
        private final ForwardDecayingReservoir reservoir;

        Recorder(BucketProperties bucketProperties, ForwardDecayingReservoir reservoir)
        {
            this.bucketProperties = bucketProperties;
            this.numCores = NettyRxScheduler.getNumCores();
            this.buffers = new Buffer[numCores + 1];
            this.reservoir = reservoir;

            // the last buffer is allocated eagerly because it is shared by
            // multiple threads, and we risk two threads racing in allocating a buffer
            this.buffers[numCores] = new Buffer(bucketProperties);

            // add the recorder to the reservoir
            reservoir.add(this);
        }

        // thread-safe: called by many threads (the updating threads)
        void update(final long value)
        {
            reservoir.maybeSchedule();

            int coreId = NettyRxScheduler.getCoreId();

            Buffer buffer = buffers[coreId];
            if (buffer == null)
            {
                assert NettyRxScheduler.isValidCoreId(coreId);
                buffer = buffers[coreId] = new Buffer(bucketProperties);
            }

            buffer.update(value, coreId < numCores);
        }

        // not thread-safe: must only be called by the aggregating thread
        boolean isOverFlowed()
        {
            return Arrays.stream(buffers)
                         .filter(Objects::nonNull).map(b -> b.isOverflowed)
                         .reduce(Boolean.FALSE, Boolean::logicalOr);
        }

        // not thread-safe: must only be called by the aggregating thread
        long getValue(final int index)
        {
            return Arrays.stream(buffers)
                         .filter(Objects::nonNull)
                         // here we may loose an update at most I think, since a lazy set
                         // aka putOrderedLong takes a few nanoseconds to be visible but for
                         // metrics it should be acceptable as long as it is atomic
                         .map(b -> b.getLongVolatile(index))
                         .reduce(0L, Long::sum);
        }

        // not thread-safe: testing only
        void clear()
        {
            Arrays.stream(buffers).filter(Objects::nonNull).forEach(Buffer::clear);
        }
    }

    /**
     * A buffer that stores buckets off-heap and that can update
     * items atomically with lazy set or CAS.
     *
     */
    private static final class Buffer
    {
        private final AtomicBuffer buffer;
        private final BucketProperties bucketProperties;
        private volatile boolean isOverflowed;

        Buffer(BucketProperties bucketProperties)
        {
            this.buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(bucketProperties.numBuckets << 3));
            this.bucketProperties = bucketProperties;
            this.isOverflowed = false;
        }

        /**
         * Update a valud in the buffer. When lazy is true, this assumes the
         * same thread is doing the lazy updates and so we can rely on a get long volatile.
         * @param value - the value to set
         * @param lazy - whether lazy updates are OK (single updating thread)
         */
        void update(long value, boolean lazy)
        {
            if (value > bucketProperties.maxTrackableValue)
            {
                isOverflowed = true;
                return;
            }

            int index = bucketProperties.getIndex(value) << 3;
            if (lazy)
                buffer.putLongOrdered(index, buffer.getLongVolatile(index) + 1);
            else
                buffer.getAndAddLong(index, 1);
        }

        long getAndSet(int index, long value)
        {
            return buffer.getAndSetLong(index << 3, value);
        }

        long getLongVolatile(int index)
        {
            return buffer.getLongVolatile(index << 3);
        }

        void clear()
        {
            for (int i = 0; i < bucketProperties.size(); i++)
                getAndSet(i, 0);
        }
    }

    /**
     * A snapshot of the decaying histogram.
     *
     * The decaying buckets are copied into a snapshot array to give a consistent view for all getters. However, the
     * copy is made without a write-lock and so other threads may change the buckets while the array is copied,
     * probably causign a slight skew up in the quantiles and mean values.
     *
     * The decaying buckets will be used for quantile calculations and mean values, but the non decaying buckets will be
     * exposed for calls to {@link com.codahale.metrics.Snapshot#getValues()}.
     */
    @VisibleForTesting
    public static class Snapshot extends com.codahale.metrics.Snapshot
    {
        private final long[] bucketOffsets;
        private final boolean isOverflowed;
        private final long[] buckets;
        private final long[] decayingBuckets;
        private final long count;
        private final long countWithDecay;

        public Snapshot(final ForwardDecayingReservoir reservoir)
        {
            this.bucketOffsets = reservoir.getOffsets();
            this.isOverflowed = reservoir.isOverflowed;
            this.buckets = copyBuckets(reservoir.buckets, reservoir.considerZeroes);
            this.decayingBuckets = rescaleBuckets(copyBuckets(reservoir.decayingBuckets, reservoir.considerZeroes),
                                                  reservoir.forwardDecayWeight());
            this.count = countBuckets(buckets);
            this.countWithDecay = countBuckets(decayingBuckets);
        }

        private long[] copyBuckets(long[] buckets, boolean considerZeroes)
        {
            long[] ret = new long[bucketOffsets.length];

            int index = 0;
            if (considerZeroes)
            {
                ret[index++] = buckets[0];
                ret[index++] = buckets[1];
            }
            else
            {
                ret[index++] = buckets[0] + buckets[1];
            }

            for (int i = 2; i < buckets.length; i++)
            {
                ret[index++] = buckets[i];
            }
            return ret;
        }

        private long[] rescaleBuckets(long[] buckets, final double rescaleFactor)
        {
            final int length = buckets.length;
            for (int i = 0; i < length; i++)
                buckets[i] = Math.round(buckets[i] / rescaleFactor);

            return buckets;
        }

        private long countBuckets(long[] buckets)
        {
            long sum = 0L;
            for (int i = 0; i < buckets.length; i++)
                sum += buckets[i];
            return sum;
        }

        long getCount()
        {
            return count;
        }

        /**
         * Get the estimated value at the specified quantile in the distribution.
         *
         * @param quantile the quantile specified as a value between 0.0 (zero) and 1.0 (one)
         * @return estimated value at given quantile
         * @throws IllegalStateException in case the histogram overflowed
         */
        public double getValue(double quantile)
        {
            assert quantile >= 0 && quantile <= 1.0;

            if (isOverflowed)
                throw new IllegalStateException("Unable to compute when histogram overflowed");

            final long qcount = (long) Math.ceil(countWithDecay * quantile);
            if (qcount == 0)
                return 0;

            long elements = 0;
            for (int i = 0; i < decayingBuckets.length; i++)
            {
                elements += decayingBuckets[i];
                if (elements >= qcount)
                    return bucketOffsets[i];
            }
            return 0;
        }

        @VisibleForTesting
        public long[] getOffsets()
        {
            return bucketOffsets;
        }

        /**
         * Will return a snapshot of the non-decaying buckets.
         *
         * The values returned will not be consistent with the quantile and mean values. The caller must be aware of the
         * offsets created by {@link EstimatedHistogram#getBucketOffsets()} to make use of the values returned.
         *
         * @return a snapshot of the non-decaying buckets.
         */
        public long[] getValues()
        {
            return this.buckets;
        }

        /**
         * Return the number of buckets where recorded values are stored.
         *
         * This method does not return the number of recorded values as suggested by the {@link com.codahale.metrics.Snapshot} interface.
         *
         * @return the number of buckets
         */
        public int size()
        {
            return decayingBuckets.length;
        }

        /**
         * Get the estimated max-value that could have been added to this histogram.
         *
         * As values are collected in variable sized buckets, the actual max value recored in the histogram may be less
         * than the value returned.
         *
         * @return the largest value that could have been added to this histogram, or Long.MAX_VALUE if the histogram
         * overflowed
         */
        public long getMax()
        {
            if (isOverflowed)
                return Long.MAX_VALUE;

            for (int i = decayingBuckets.length - 1; i >= 0; i--)
            {
                if (decayingBuckets[i] > 0)
                    return bucketOffsets[i + 1] - 1;
            }
            return 0;
        }

        /**
         * Get the estimated mean value in the distribution.
         *
         * @return the mean histogram value (average of bucket offsets, weighted by count)
         * @throws IllegalStateException if any values were greater than the largest bucket threshold
         */
        public double getMean()
        {
            if (isOverflowed)
                throw new IllegalStateException("Unable to compute when histogram overflowed");

            long elements = 0;
            long sum = 0;
            for (int i = 0; i < decayingBuckets.length; i++)
            {
                long bCount = decayingBuckets[i];
                elements += bCount;
                long delta = bucketOffsets[i] - (i == 0 ? 0 : bucketOffsets[i-1]);
                sum += bCount * (bucketOffsets[i] + (delta >> 1));  // smallest possible value plus half delta, rounded down
            }

            return (double) sum / elements;
        }

        /**
         * Get the estimated min-value that could have been added to this histogram.
         *
         * As values are collected in variable sized buckets, the actual min value recored in the histogram may be
         * higher than the value returned.
         *
         * @return the smallest value that could have been added to this histogram
         */
        public long getMin()
        {
            for (int i = 0; i < decayingBuckets.length; i++)
            {
                if (decayingBuckets[i] > 0)
                    return bucketOffsets[i];
            }
            return 0;
        }

        /**
         * Get the estimated standard deviation of the values added to this histogram.
         *
         * As values are collected in variable sized buckets, the actual deviation may be more or less than the value
         * returned.
         *
         * @return an estimate of the standard deviation
         */
        public double getStdDev()
        {
            if (isOverflowed)
                throw new IllegalStateException("Unable to compute when histogram overflowed");

            if(countWithDecay <= 1)
            {
                return 0.0D;
            }
            else
            {
                double mean = this.getMean();
                double sum = 0.0D;

                for(int i = 0; i < decayingBuckets.length; ++i)
                {
                    long value = bucketOffsets[i];
                    double diff = value - mean;
                    sum += diff * diff * decayingBuckets[i];
                }

                return Math.sqrt(sum / (countWithDecay - 1));
            }
        }

        public void dump(OutputStream output)
        {
            try (PrintWriter out = new PrintWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8)))
            {
                for(int i = 0; i < decayingBuckets.length; ++i)
                {
                    out.printf("%d%n", decayingBuckets[i]);
                }
            }
        }
    }
}
