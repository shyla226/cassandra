/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.utils;


import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.*;

public class LightweightRecyclerTest
{
    @Test
    public void testBasicInteraction()
    {
        int capacity = 100;
        LightweightRecycler<Integer> recycler = new LightweightRecycler<>(capacity);
        assertEquals(capacity, recycler.capacity());
        assertEquals(0, recycler.available());
        List<Integer> allocated = new ArrayList<>();
        for (int i=0; i < capacity; i++)
        {
            int finalI = i;
            allocated.add(recycler.reuseOrAllocate(() -> Integer.valueOf(finalI)));
        }
        // still nothing in the recycler
        assertEquals(0, recycler.available());
        // nothing to reuse
        assertNull(recycler.reuse());
        for (Integer i : allocated)
        {
            assertTrue(recycler.tryRecycle(i));
        }
        assertEquals(capacity, recycler.available());

        // we reject recycled items beyond capacity
        assertFalse(recycler.tryRecycle(666));
    }

    @Test
    public void recyclingClearsCollections()
    {
        LightweightRecycler<List> recycler = new LightweightRecycler<>(1);
        assertEquals(1, recycler.capacity());
        assertEquals(0, recycler.available());

        List list1 = recycler.reuseOrAllocate(ArrayList::new);
        List list2 = recycler.reuseOrAllocate(ArrayList::new);

        list1.add(new Object());
        list2.add(new Object());

        assertTrue(recycler.tryRecycle(list1));
        assertFalse(recycler.tryRecycle(list2));

        assertEquals(1, recycler.available());
        assertTrue(list1.isEmpty());
        assertFalse(list2.isEmpty());
    }
}
