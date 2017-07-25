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
package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.util.Comparator;

import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.io.sstable.format.AbstractSSTableIterator.Reader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;

// Used by indexed readers to store where they are of the index.
public class IndexState implements AutoCloseable
{
    private final Reader reader;

    private final BigRowIndexEntry indexEntry;
    private final BigRowIndexEntry.IndexInfoRetriever indexInfoRetriever;
    private final boolean reversed;

    private int currentIndexIdx;
    private int lastIndexIdx;

    // Marks the beginning of the block corresponding to currentIndexIdx.
    private long startOfBlock;
    private Comparator<IndexInfo> indexComparator;

    public IndexState(Reader reader, ClusteringComparator comparator, BigRowIndexEntry indexEntry, boolean reversed, FileHandle indexFile, Rebufferer.ReaderConstraint rc)
    {
        this.reader = reader;
        this.indexEntry = indexEntry;
        this.indexInfoRetriever = indexEntry.openWithIndex(indexFile, rc);
        this.reversed = reversed;
        this.currentIndexIdx = reversed ? indexEntry.rowIndexCount() : -1;
        this.lastIndexIdx = currentIndexIdx;

        this.indexComparator = reversed
                ? (o1, o2) -> comparator.compare(o1.firstName, o2.firstName)
                : (o1, o2) -> comparator.compare(o1.lastName, o2.lastName);
    }

    /**
     * Resets the state back to last known
     * entry.
     */
    public void reset()
    {
        this.currentIndexIdx = lastIndexIdx;
    }

    public boolean isDone()
    {
        return reversed ? currentIndexIdx < 0 : currentIndexIdx >= indexEntry.rowIndexCount();
    }

    // Sets the reader to the beginning of blockIdx.
    public void setToBlock(int blockIdx) throws IOException
    {
        if (blockIdx >= 0 && blockIdx < indexEntry.rowIndexCount())
            reader.seekToPosition(columnOffset(blockIdx));

        currentIndexIdx = blockIdx;
        reader.openMarker = blockIdx > 0 ? index(blockIdx - 1).endOpenMarker : null;
        startOfBlock = reader.file.getFilePointer();
        lastIndexIdx = currentIndexIdx;
    }

    private long columnOffset(int i) throws IOException
    {
        return indexEntry.position + index(i).offset;
    }

    public int blocksCount()
    {
        return indexEntry.rowIndexCount();
    }

    // Update the block idx based on the current reader position if we're past the current block.
    // This only makes sense for forward iteration (for reverse ones, when we reach the end of a block we
    // should seek to the previous one, not update the index state and continue).
    public void updateBlock() throws IOException
    {
        assert !reversed;

        // If we get here with currentBlockIdx < 0, it means setToBlock() has never been called, so it means
        // we're about to read from the beginning of the partition, but haven't "prepared" the IndexState yet.
        // Do so by setting us on the first block.
        if (currentIndexIdx < 0)
        {
            setToBlock(0);
            return;
        }

        while (currentIndexIdx + 1 < indexEntry.rowIndexCount() && isPastCurrentBlock())
        {
            reader.openMarker = currentIndex().endOpenMarker;
            ++currentIndexIdx;

            startOfBlock = columnOffset(currentIndexIdx);

            //Finished, save state
            lastIndexIdx = currentIndexIdx;
        }
    }

    // Check if we've crossed an index boundary (based on the mark on the beginning of the index block).
    public boolean isPastCurrentBlock() throws IOException
    {
        assert reader.deserializer != null;
        return reader.file.getFilePointer() - startOfBlock >= currentIndex().width;
    }

    public int currentBlockIdx()
    {
        return currentIndexIdx;
    }

    public IndexInfo currentIndex() throws IOException
    {
        return index(currentIndexIdx);
    }

    public IndexInfo index(int i) throws IOException
    {
        return indexInfoRetriever.columnsIndex(i);
    }

    // Finds the index of the first block containing the provided bound, starting at the provided index.
    // Will be -1 if the bound is before any block, and blocksCount() if it is after every block.
    public int findBlockIndex(ClusteringBound bound, int fromIdx) throws IOException
    {
        if (bound == ClusteringBound.BOTTOM)
            return -1;
        if (bound == ClusteringBound.TOP)
            return blocksCount();

        return indexFor(bound, fromIdx);
    }

    public int indexFor(ClusteringPrefix name, int lastIndex) throws IOException
    {
        IndexInfo target = new IndexInfo(name, name, 0, 0, null);
        /*
        Take the example from the unit test, and say your index looks like this:
        [0..5][10..15][20..25]
        and you look for the slice [13..17].

        When doing forward slice, we are doing a binary search comparing 13 (the start of the query)
        to the lastName part of the index slot. You'll end up with the "first" slot, going from left to right,
        that may contain the start.

        When doing a reverse slice, we do the same thing, only using as a start column the end of the query,
        i.e. 17 in this example, compared to the firstName part of the index slots.  bsearch will give us the
        first slot where firstName > start ([20..25] here), so we subtract an extra one to get the slot just before.
        */
        int startIdx = 0;
        int endIdx = indexEntry.rowIndexCount() - 1;

        if (reversed)
        {
            if (lastIndex < endIdx)
            {
                endIdx = lastIndex;
            }
        }
        else
        {
            if (lastIndex > 0)
            {
                startIdx = lastIndex;
            }
        }

        int index = binarySearch(target, indexComparator, startIdx, endIdx);
        return (index < 0 ? -index - (reversed ? 2 : 1) : index);
    }

    private int binarySearch(IndexInfo key, Comparator<IndexInfo> c, int low, int high) throws IOException
    {
        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            IndexInfo midVal = index(mid);
            int cmp = c.compare(midVal, key);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

    @Override
    public String toString()
    {
        return String.format("IndexState(indexSize=%d, currentBlock=%d, reversed=%b)", indexEntry.rowIndexCount(), currentIndexIdx, reversed);
    }

    @Override
    public void close() throws IOException
    {
        indexInfoRetriever.close();
    }
}