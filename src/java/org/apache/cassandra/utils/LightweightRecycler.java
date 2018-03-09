/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.utils;

import java.util.*;
import java.util.function.Supplier;

import io.netty.util.concurrent.FastThreadLocal;

/**
 * A simple thread local object reuse facility with limited capacity and no attempt at rebalancing pooling between
 * threads. This is meant to be put in place where churn is high, but single object allocation and footprint are not
 * so high to justify a more sofisticated approach.
 * <p>
 * We extend {@link FastThreadLocal} to reduce indirection. This class is not for further extension, and direct use of
 * parent methods is is discouraged.
 * <p>
 * <b>Internal use only</b>
 *
 * @param <T>
 */
public final class LightweightRecycler<T> extends FastThreadLocal<ArrayDeque<T>>
{
    private final int capacity;

    public LightweightRecycler(int capacity)
    {
        this.capacity = capacity;
    }

    protected ArrayDeque<T> initialValue() throws Exception
    {
        return new ArrayDeque<>(capacity);
    }

    /**
     * @return a reusable instance, or null if none is available
     */
    public T reuse()
    {
        return get().pollFirst();
    }

    /**
     * @param supplier
     * @return a reusable instance, or allocate one via the provided supplier
     */
    public T reuseOrAllocate(Supplier<T> supplier)
    {
        final T reuse = reuse();
        return reuse != null ? reuse : supplier.get();
    }

    /**
     * @param t to be recycled, if t is a collection it will be cleared before recycling, but not cleared if not
     *          recycled
     * @return true if t was recycled, false otherwise
     */
    public boolean tryRecycle(T t)
    {
        Objects.requireNonNull(t);

        final ArrayDeque<T> pool = get();
        if (pool.size() < capacity)
        {
            if (t instanceof Collection)
                ((Collection) t).clear();
            pool.offerFirst(t);
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * @return maximum capacity of the recycler
     */
    public int capacity()
    {
        return capacity;
    }

    /**
     * @return current count of available instances for reuse
     */
    public int available()
    {
        return get().size();
    }
}
