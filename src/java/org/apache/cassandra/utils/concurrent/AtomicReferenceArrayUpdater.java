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

import sun.misc.Unsafe;

public class AtomicReferenceArrayUpdater<V>
{

    final long offset;
    final int shift;

    public AtomicReferenceArrayUpdater(Class<V[]> arrayType)
    {
        if (!arrayType.isArray())
            throw new IllegalArgumentException();
        // this is the base offset within the array type that indexes begin at
        offset = unsafe.arrayBaseOffset(arrayType);
        // this is the interval at which indexes occur
        int scale = unsafe.arrayIndexScale(arrayType);
        // it should be a power of 2, which corresponds to 1 set bit
        assert Integer.bitCount(scale) == 1;
        // and as such, this can be translated into a shift, by counting how many zero bits lower than the set bit
        // (or bits set after subtracting one)
        shift = Integer.bitCount(scale - 1);
    }

    /**
     * Atomically change the value at an index in the array if it currently contains the value we expect
     *
     * @param array the array to modify
     * @param index the index to modify
     * @param expect the value that we expect to be present at the index
     * @param update the value to set the index to if it currently contains "expect"
     * @return success/failure
     */
    public boolean compareAndSet(V[] array, int index, V expect, V update)
    {
        assert index >= 0 && index < array.length;
        return unsafe.compareAndSwapObject(array, offset + (index << shift), expect, update);
    }

    static final Unsafe unsafe;
    static
    {
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }
}
