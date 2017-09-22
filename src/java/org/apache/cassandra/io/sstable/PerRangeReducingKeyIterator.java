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
package org.apache.cassandra.io.sstable;

import java.util.Collection;
import java.util.Iterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Iterates through all keys on a given set of sstables via merge reduce. The iteration algorithm iterates keys over
 * multiple ranges (bounds): each key is globally iterated by range, with the next key coming from the next range,
 * in circular fashion: i.e., if there are 2 ranges, the first key will come from the first range, the second key from
 * the second range, the third key again from the first range again and so on.
 * <p>
 * Caller must acquire and release references to the sstables used here.
 */
public class PerRangeReducingKeyIterator implements KeyIterator
{
    private final ReducingKeyIterator[] iterators;
    private final Collection<? extends AbstractBounds<Token>> bounds;
    private int current = -1;
    private int next = -1;

    public PerRangeReducingKeyIterator(Collection<SSTableReader> sstables, Collection<? extends AbstractBounds<Token>> bounds)
    {
        Preconditions.checkArgument(!sstables.isEmpty(), "There must be at least one sstable to iterate.");
        Preconditions.checkArgument(!bounds.isEmpty(), "There must be at least one bound to iterate through.");

        this.bounds = ImmutableList.copyOf(bounds);
        this.iterators = new ReducingKeyIterator[bounds.size()];

        initIterators(sstables, bounds);
    }

    public void close()
    {
        for (ReducingKeyIterator it : iterators)
        {
            it.close();
        }
    }

    public long getTotalBytes()
    {
        return iterators.length == 0 ? 0 : iterators[0].bytesTotal;
    }

    public long getBytesRead()
    {
        long bytesRead = 0;
        for (ReducingKeyIterator it : iterators)
        {
            bytesRead += it.bytesRead;
        }

        return bytesRead;
    }

    public boolean hasNext()
    {
        return computeNext();
    }

    public DecoratedKey next()
    {
        boolean hasNext = computeNext();
        if (hasNext)
        {
            current = next;
            return iterators[current].next();
        }
        throw new IllegalStateException("This iterator has no next element!");
    }

    public Collection<? extends AbstractBounds<Token>> getBounds()
    {
        return bounds;
    }

    private void initIterators(Collection<SSTableReader> sstables, Collection<? extends AbstractBounds<Token>> bounds)
    {
        Iterator<? extends AbstractBounds<Token>> boundsIt = bounds.iterator();
        int idx = 0;
        while (boundsIt.hasNext())
        {
            AbstractBounds<Token> bound = boundsIt.next();
            iterators[idx++] = new ReducingKeyIterator(sstables, bound);
        }
    }

    private boolean computeNext()
    {
        boolean hasNext = false;
        int remaining = iterators.length;
        next = current;
        do
        {
            next = ++next % iterators.length;
            hasNext = iterators[next].hasNext();
        }
        while (!hasNext && --remaining > 0);
        return hasNext;
    }
}
