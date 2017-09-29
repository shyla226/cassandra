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

import java.util.AbstractCollection;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A collection storing a given number of the last values that has been added to it, automatically removing older values
 * when new ones are added, and where iteration is from the latest added value to the oldest that is stored.
 * <p>
 * An {@link History} is created with a given capacity {@code n}, which cannot be changed afterwards, and that
 * represents how many historical data will be kept. {@link History} does not support the removal of specific element,
 * the oldest (in term of addition) element is automatically removed when a new one is added as soon as at least
 * {@code n} elements has been added. The collection does support the {@link #clear} method however.
 * <p>
 * This collection is not thread-safe.
 */
public class History<E> extends AbstractCollection<E>
{
    private final E[] array;

    private int idx; // Points to the next slot in which to insert; once we get to capacity, this cycle over the array
    private boolean atCapacity; // Whether we hold 'capacity' elements or not. Because we don't support removals other
                                // that full clear, we don't need anything more fancy to deal with less-than-capacity.

    /**
     * Creates a new {@link History} collection that will keep the {@code capacity} latest added elements.
     *
     * @param capacity the number of elements the history will keep.
     */
    public History(int capacity)
    {
        @SuppressWarnings("unchecked")
        final E[] a = (E[])new Object[capacity];
        this.array = a;
    }

    /**
     * Returns an immutable list view of the last {@code n} elements in this history (from the newest to the oldest, so
     * {@code this.last(3).get(0)} is the last element added to the history).
     *
     * @param n the number of elements to include in the view. If {@code n > size()} (including if {@code n > capacity()),
     *          then the returned view will only contain {@code size()} elements.
     * @return a list view of the most recent {@code Math.min(n, size())} elements in this history. Note that updates to
     *         this object are reflected in the returned list.
     */
    public List<E> last(int n)
    {
        return new View(n);
    }

    /**
     * Returns the last element of this history (if any).
     *
     * @return the most recent element in this history, or {@code null} if the history is empty.
     */
    public E last()
    {
        if (isEmpty())
            return null;

        return array[(idx - 1 + array.length) % array.length];
    }

    /**
     * Returns an immutable list view of the elements in this history (from the newest to the oldest).
     * <p>
     * This is a shortcut for {@code last(capacity())}.
     *
     * @return a list view of this history.
     */
    public List<E> listView()
    {
        return new View(capacity());
    }

    /**
     * An iterator over the elements stored in this history. The elements are returned by the created iterator from the
     * most recently inserted one to the oldest one.
     *
     * @return an iterator over the elements of history from the most recent to the oldest one.
     */
    public Iterator<E> iterator()
    {
        return last(capacity()).iterator();
    }

    /**
     * The maximum number of recent elements this history stores. If {@code size() == capacity(}}, then adding a new
     * element automatically remove the oldest one (in term of insertion) in the history.
     *
     * @return the capacity of the history (which is fixed at creation).
     */
    public int capacity()
    {
        return array.length;
    }

    /**
     * The size of the history, which cannot be greater than the capacity the history was created with.
     *
     * @return the number of element this history collection stores.
     */
    public int size()
    {
        return atCapacity ? array.length : idx;
    }

    /**
     * Whether the history is "at capacity", that is if {@code capacity() == size()}.
     * <p>
     * Once the history is at capacity, any new addition will evict the oldest element stored.
     *
     * @return {@code true} if the history is at capacity, {@code false} otherwise.
     */
    public boolean isAtCapacity()
    {
        return atCapacity;
    }

    /**
     * Adds a new element to the history. If the history already
     *
     * @param e the element to add.
     * @return
     */
    public boolean add(E e)
    {
        array[idx++] = e;
        if (idx == array.length)
        {
            atCapacity = true; // if we were already at capacity, it's harmless
            idx = 0;
        }
        return true;
    }

    /**
     * Clears the history.
     */
    @Override
    public void clear()
    {
        Arrays.fill(array, null);
        idx = 0;
        atCapacity = false;
    }

    private class View extends AbstractList<E>
    {
        private final int n; // the (maximum) number of elements for the view

        private View(int n)
        {
            this.n = n;
        }

        public E get(int i)
        {
            if (i < 0 || i >= size())
                throw new IndexOutOfBoundsException();

            return array[(idx - 1 - i + array.length) % array.length];
        }

        public int size()
        {
            return Math.min(History.this.size(), n);
        }
    }
}
