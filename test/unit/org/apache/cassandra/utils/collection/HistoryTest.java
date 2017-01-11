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
package org.apache.cassandra.utils.collection;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.*;

public class HistoryTest
{
    @Test
    public void testHistory()
    {
        History<Integer> h = new History<>(10);

        assertEquals(10, h.capacity());
        assertEquals(0, h.size());
        assertFalse(h.iterator().hasNext());
        assertTrue(h.last(1).isEmpty());

        // Add elements, but less than capacity
        List<Integer> added = new ArrayList<>();
        for (int i = 0; i < 8; i++)
        {
            h.add(i);
            added.add(i);
        }

        assertEquals(10, h.capacity());
        assertEquals(added.size(), h.size());
        assertTrue(Iterables.elementsEqual(Lists.reverse(added), h));
        assertEquals(Lists.reverse(added).subList(0, 4), h.last(4));
        assertEquals(Lists.reverse(added), h.last(8));
        assertEquals(Lists.reverse(added), h.last(10));
        assertEquals(Lists.reverse(added), h.last(15));

        // Add more elements to get over capacity
        for (int i = 8; i < 15; i++)
        {
            h.add(i);
            added.add(i);
        }

        assertEquals(10, h.capacity());
        assertEquals(h.capacity(), h.size());
        assertTrue(Iterables.elementsEqual(Lists.reverse(added).subList(0, h.capacity()), h));
        assertEquals(Lists.reverse(added).subList(0, 4), h.last(4));
        assertEquals(Lists.reverse(added).subList(0, 8), h.last(8));
        assertEquals(Lists.reverse(added).subList(0, 10), h.last(10));
        assertEquals(Lists.reverse(added).subList(0, 10), h.last(15));

        // Clear and check we're indeed empty
        h.clear();
        assertEquals(10, h.capacity());
        assertEquals(0, h.size());
        assertFalse(h.iterator().hasNext());
        assertTrue(h.last(1).isEmpty());
    }
}