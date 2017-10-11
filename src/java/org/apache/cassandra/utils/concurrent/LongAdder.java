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

import java.util.Arrays;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.utils.memory.MemoryUtil;
import sun.misc.Contended;

/**
 * A class equivalent to {@link java.util.concurrent.atomic.LongAdder} except that
 * it creates a Cell per core-thread as soon as needed, and a further Cell to be
 * shared by every other thread. The dedicated cells are updated with an ordered put,
 * aka as lazy set, rather than a full volatile write, whilst the shared Cell is
 * updated with CAS.
 *
 * The ordered put guarantees atomicity, just like a volatile write, but not
 * total ordering. So we may miss the latest value when reading, but we can
 * tolerate this (the reading thread will see the value in a matter of nanoseconds).
 * */
public class LongAdder
{
    private static final long valueOffset;

    static
    {
        try
        {
            Class<?> ak = Cell.class;
            valueOffset = MemoryUtil.unsafe.objectFieldOffset(ak.getDeclaredField("value"));
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
         * will be immediately visible to another thread but it does
         * ensure that the update will be atomic (i.e. either visible
         * or not at all) and typically a reading thread will have access
         * in a few nanoseconds. It is suitable when only a single thread
         * updates a value, even for incrementing, since we know the current
         * value in this case.
         */
        void add(long x, boolean lazy)
        {
            if (lazy)
                MemoryUtil.unsafe.putOrderedLong(this, valueOffset, value + x);
            else
                MemoryUtil.unsafe.getAndAddLong(this, valueOffset, x);
        }

        void set(long x)
        {
            MemoryUtil.unsafe.getAndSetLong(this, valueOffset, x);
        }

        long get()
        {
            return MemoryUtil.unsafe.getLongVolatile(this, valueOffset);
        }
    }

    private final int numCores;
    private final Cell[] values;

    public LongAdder()
    {
        this.numCores = TPC.getNumCores();
        this.values = new Cell[numCores + 1];

        // The last value is shared by multiple threads and so it is
        // initialized early to avoid races during lazy initialization
        this.values[numCores] = new Cell(0L);
    }

    public void add(long x)
    {
       int coreId = TPC.getCoreId();

        // allocate lazily to conserve space and to make sure they
        // are not on the same cache line, even with the @contended
        // annotation performace drops by 10x if allocating all together
        // when the array is created
        if (values[coreId] == null)
        {
            assert TPC.isValidCoreId(coreId);
            values[coreId] = new Cell(0L);
        }

        // only the last index requires a cas, for the rest a lazy put
        // is sufficient since only one thread will update this values
        values[coreId].add(x, coreId < numCores);
    }

    public void increment() {
        add(1L);
    }

    public void decrement() {
        add(-1L);
    }

    public long sum()
    {
        long sum = 0;
        for (Cell c : values)
            if (c != null)
                sum += c.get();

        return sum;
    }

    public void reset()
    {
        Arrays.stream(values).filter(cell -> cell != null).forEach(cell -> cell.set(0L));
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
