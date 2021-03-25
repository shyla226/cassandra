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

package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import java.util.concurrent.CompletionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.IndexFileEntry;
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;

/**
 * A flow of {@link IndexFileEntry} for the
 * trie index table format.
 */
public class TrieIndexFileIterator extends AbstractIterator<IndexFileEntry>
{
    private static final Logger logger = LoggerFactory.getLogger(TrieIndexFileIterator.class);

    private final RandomAccessReader dataFileReader;
    private final TrieIndexSSTableReader sstable;
    private final PartitionIndex partitionIndex;
    private final IPartitioner partitioner;
    private final PartitionPosition left;
    private final int inclusiveLeft;
    private final PartitionPosition right;
    private final int exclusiveRight;
    private final FileHandle rowIndexFile;

    private FileDataInput rowIndexFileReader;
    private PartitionIndex.IndexPosIterator posIterator;
    private long pos = PartitionIndex.NOT_FOUND;
    private IndexFileEntry next;

    public TrieIndexFileIterator(RandomAccessReader dataFileReader,
                                 TrieIndexSSTableReader sstable,
                                 PartitionPosition left,
                                 int inclusiveLeft,
                                 PartitionPosition right,
                                 int exclusiveRight)
    {
        this.dataFileReader = dataFileReader;
        this.sstable = sstable;
        this.partitionIndex = sstable.partitionIndex;
        this.partitioner = sstable.metadata().partitioner;
        this.left = left;
        this.inclusiveLeft = inclusiveLeft;
        this.right = right;
        this.exclusiveRight = exclusiveRight;
        this.rowIndexFile = sstable.rowIndexFile;
    }

    @Override
    public IndexFileEntry computeNext()
    {
        try
        {

            IndexFileEntry current = next == null ? readFirst() : readNext();
            if (current != IndexFileEntry.EMPTY)
                return current;
            else
                         return null;
        }
        catch (CorruptSSTableException | IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    /**
     * Perform late initialization, that is initialization that is deferred until the first item is requested,
     * so that we don't leak resources if the flow is never subscribed to.
     */
    private void lateInitialization()
    {
        if (rowIndexFileReader == null)
            rowIndexFileReader = rowIndexFile.createReader();

        if (posIterator == null)
            posIterator = new PartitionIndex.IndexPosIterator(partitionIndex, left, right); // can throw NotInCacheException
    }

    private IndexFileEntry readFirst() throws IOException
    {
        lateInitialization(); // can throw NotInCacheException

        next = readEntry(); // can throw NotInCacheException
        if (next.key != null && !(next.key.compareTo(left) > inclusiveLeft))
        {
            next = null;
            next = readEntry(); // can throw NotInCacheException
        }

        return readNext(); // can throw NotInCacheException
    }

    private IndexFileEntry readNext() throws IOException
    {
        IndexFileEntry ret = next;

        if (ret == IndexFileEntry.EMPTY)
            return ret; // we are done

        next = readEntry(); // can throw NotInCacheException

        // if next is empty, then ret is the last partition to be published, in this case we check against
        // any right limit and suppress the last partition if it is beyond the limit
        if (next == IndexFileEntry.EMPTY && right != null && ret.key.compareTo(right) > exclusiveRight)
            return IndexFileEntry.EMPTY; // exclude last partition outside range

        return ret;
    }

    /**
     * Returns the next index file entry by reading the position first, then reading either the
     * row index file or the data file depending on where the position points to.
     */
    private IndexFileEntry readEntry() throws IOException
    {
        if (pos == PartitionIndex.NOT_FOUND)
            pos = posIterator.nextIndexPos();

        IndexFileEntry ret;
        if (pos != PartitionIndex.NOT_FOUND)
        {
            if (pos >= 0)
            {
                if (pos != rowIndexFileReader.getFilePointer())
                    rowIndexFileReader.seek(pos);

                ret = new IndexFileEntry(partitioner.decorateKey(ByteBufferUtil.readWithShortLength(rowIndexFileReader)),
                                         TrieIndexEntry.deserialize(rowIndexFileReader, rowIndexFileReader.getFilePointer()));
            }
            else
            {
                long dataPos = ~pos;
                if (dataPos != dataFileReader.getFilePointer())
                    dataFileReader.seek(dataPos);

                ret = new IndexFileEntry(partitioner.decorateKey(ByteBufferUtil.readWithShortLength(dataFileReader)),
                                         new RowIndexEntry<>(dataPos));
            }

            pos = PartitionIndex.NOT_FOUND; // make sure next time we get the next pos
        }
        else
        {
            ret = IndexFileEntry.EMPTY;
        }

        //logger.debug("{} - Read @pos {}: {}", hashCode(), pos, ret);
        return ret;
    }


    public void close()
    {
        Throwable err = Throwables.close(null, posIterator, rowIndexFileReader);
        Throwables.maybeFail(err);
    }
}
