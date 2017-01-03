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

package org.apache.cassandra.utils.concurrent;

import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.cassandra.concurrent.NettyRxScheduler;
import sun.misc.Contended;

/**
 * A class equivalent to {@link java.util.concurrent.atomic.LongAdder} except that
 * it uses an offheap buffer to store an array of longs, one per thread assigned to a core
 * plus one. At position zero, we store a long that is updated with CAS, this is used for
 * all threads not assigned to a core. At positions > 0 we store values reserved to threads
 * assigned to a core, so at position 1 we have a long value that will be updated by the thread
 * assigned to core zero, and so forth. In this case a simple read and volatile write is sufficient.
 *
 * The sum can be recovered from any thread, via a volatile read of all values.
 */
public class LongAdder
{
    // Unsafe mechanics
    private static final sun.misc.Unsafe UNSAFE;
    private static final long valueOffset;

    static
    {
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) field.get(null);

            Class<?> ak = Cell.class;
            valueOffset = UNSAFE.objectFieldOffset(ak.getDeclaredField("value"));
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    static final class Cell {
        @Contended
        long value;

        Cell(long x) { value = x; }

        /** Lazyness does not guarantee the value that is written
         * will be immediately visible but it does ensure that the
         * update will be atomic (i.e. either visible or not at all)
         * and typically a reading thread will have access in a few
         * nanoseconds. It is suitable when only a single thread updates
         * a value, even for incrementing, since we know the current
         * value in this case.
         */
        void add(long x, boolean lazy)
        {
            if (lazy)
                UNSAFE.putOrderedLong(this, valueOffset, value + x);
            else
                UNSAFE.getAndAddLong(this, valueOffset, x);
        }

        void set(long x, boolean lazy)
        {
            if (lazy)
                UNSAFE.putOrderedLong(this, valueOffset, x);
            else
                UNSAFE.getAndSetLong(this, valueOffset, x);
        }

        long get()
        {
            // TODO - is the loadFence the correct way to emulate a volatile read? If not, we can just accept that
            // we may miss the last update on read
            UNSAFE.loadFence();
            return UNSAFE.getLong(this, valueOffset);
        }
    }

    private final int numCores;
    private final Cell[] values;


    public LongAdder()
    {
        this.numCores = NettyRxScheduler.getNumCores();
        this.values = new Cell[numCores + 1];
    }

    public void add(long x)
    {
       int index = NettyRxScheduler.getCoreId();

        // allocate lazily to conserve space and to  make sure they
        // are not on the same cache line, even with the @contended
        // annotation performace drops by 10x if allocating all together
        // when the array is created
        if (values[index] == null)
            values[index] = new Cell(0L);

        // only the last index requires a cas, for the rest a lazy put
        // is sufficient since only one thread will update this values
        values[index].add(x, index < numCores);
    }

    public void increment() {
        add(1L);
    }

    public void decrement() {
        add(-1L);
    }

    public long sum()
    {
        return Arrays.stream(values).filter(cell -> cell != null).map(cell -> cell.get()).reduce(0L, Long::sum);
    }

    public void reset()
    {
        Arrays.stream(values).filter(cell -> cell != null).forEach(cell -> cell.set(0L, false));
    }

    public String toString() {
        return Long.toString(sum());
    }

    /**
     * Equivalent to {@link #sum}.
     *
     * @return the sum
     */
    public long longValue() {
        return sum();
    }

    /**
     * Returns the {@link #sum} as an {@code int} after a narrowing
     * primitive conversion.
     */
    public int intValue() {
        return (int)sum();
    }

    /**
     * Returns the {@link #sum} as a {@code float}
     * after a widening primitive conversion.
     */
    public float floatValue() {
        return (float)sum();
    }

    /**
     * Returns the {@link #sum} as a {@code double} after a widening
     * primitive conversion.
     */
    public double doubleValue() {
        return (double)sum();
    }
}
