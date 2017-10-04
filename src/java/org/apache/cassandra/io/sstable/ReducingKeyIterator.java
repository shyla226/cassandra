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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.IMergeIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Reducer;

/**
 * Iterates through all keys on a given set of sstables via merge reduce.
 * <p>
 * Caller must acquire and release references to the sstables used here.
 */
public class ReducingKeyIterator implements KeyIterator
{
    private IMergeIterator<DecoratedKey, DecoratedKey> mi;
    long bytesRead;
    long bytesTotal;

    public ReducingKeyIterator(Collection<SSTableReader> sstables)
    {
        this(sstables, null);
    }

    // Package protected to allow implementing per range iteration, not meant to be used by external callers
    ReducingKeyIterator(Collection<SSTableReader> sstables, AbstractBounds<Token> range)
    {
        List<Iter> iters = new ArrayList<>(sstables.size());
        for (SSTableReader sstable : sstables)
            iters.add(new Iter(sstable, range));

        mi = MergeIterator.get(iters, DecoratedKey.comparator, new Reducer<DecoratedKey, DecoratedKey>()
        {
            DecoratedKey reduced = null;

            @Override
            public boolean trivialReduceIsTrivial()
            {
                return true;
            }

            public void reduce(int idx, DecoratedKey current)
            {
                reduced = current;
            }

            public DecoratedKey getReduced()
            {
                return reduced;
            }
        });
    }

    class Iter implements CloseableIterator<DecoratedKey>
    {
        PartitionIndexIterator source;
        SSTableReader sstable;
        AbstractBounds<Token> range;
        final long total;

        public Iter(SSTableReader sstable, AbstractBounds<Token> range)
        {
            this.sstable = sstable;
            this.range = range;
            bytesTotal += total = sstable.uncompressedLength();
        }

        @Override
        public void close()
        {
            if (source != null)
                source.close();
        }

        @Override
        public boolean hasNext()
        {
            if (source == null)
            {
                try
                {
                    if (range == null)
                        source = sstable.allKeysIterator();
                    else
                        source = sstable.coveredKeysIterator(range.left.minKeyBound(), range.inclusiveLeft(),
                                                             range.inclusiveRight() ? range.right.maxKeyBound() : range.right.minKeyBound(), range.inclusiveRight());
                }
                catch (IOException e)
                {
                    throw new FSReadError(e, sstable.getFilename());
                }
            }

            return source.key() != null;
        }

        @Override
        public DecoratedKey next()
        {
            if (!hasNext())
                throw new AssertionError();

            try
            {
                DecoratedKey key = source.key();
                long prevPos = source.dataPosition();

                source.advance();

                long pos = source.key() != null
                           ? source.dataPosition()
                           : computeEndPosition(key);

                bytesRead += pos - prevPos;

                return key;
            }
            catch (IOException e)
            {
                throw new FSReadError(e, sstable.getFilename());
            }
        }

        private long computeEndPosition(DecoratedKey lastKey)
        {
            // If we are restricted by a range, such range might finish before the actual last key on the sstable, so
            // the end position is equal to the next key after the last key of the range:
            if (range != null)
            {
                RowIndexEntry entry = sstable.getPosition(lastKey, SSTableReader.Operator.GT);
                if (entry != null)
                    return entry.position;
            }
            // Otherwise the end position is just the total number of bytes:
            return total;
        }
    }

    public void close()
    {
        mi.close();
    }

    public long getTotalBytes()
    {
        return bytesTotal;
    }

    public long getBytesRead()
    {
        return bytesRead;
    }

    public boolean hasNext()
    {
        return mi.hasNext();
    }

    public DecoratedKey next()
    {
        return mi.next();
    }
}
