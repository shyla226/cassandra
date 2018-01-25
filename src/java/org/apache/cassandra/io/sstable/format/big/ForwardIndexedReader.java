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

import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.Rebufferer;

class ForwardIndexedReader extends ForwardReader
{
    private final IndexState indexState;

    private int lastBlockIdx; // the last index block that has data for the current query

    ForwardIndexedReader(SSTableReader sstable,
                         BigRowIndexEntry indexEntry,
                         Slices slices,
                         FileDataInput file,
                         boolean shouldCloseFile,
                         SerializationHelper helper,
                         Rebufferer.ReaderConstraint rc)
    {
        super(sstable, slices, file, shouldCloseFile, helper);
        this.indexState = new IndexState(this,
                                         metadata.comparator,
                                         indexEntry,
                                         false,
                                         ((BigTableReader) sstable).ifile,
                                         rc);
        this.lastBlockIdx = indexState.blocksCount(); // if we never call setForSlice, that's where we want to stop
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            super.close();
        }
        finally
        {
            this.indexState.close();
        }
    }

    @Override
    public void resetReaderState() throws IOException
    {
        super.resetReaderState();
        this.indexState.reset();
    }

    @Override
    public boolean setForSlice(Slice slice) throws IOException
    {
        super.setForSlice(slice);

        // if our previous slicing already got us the biggest row in the sstable, we're done
        if (indexState.isDone())
            return false;

        // Find the first index block we'll need to read for the slice.
        int startIdx = indexState.findBlockIndex(slice.start(), indexState.currentBlockIdx());
        if (startIdx >= indexState.blocksCount())
            return false;

        // Find the last index block we'll need to read for the slice.
        lastBlockIdx = indexState.findBlockIndex(slice.end(), startIdx);

        // If the slice end is before the very first block, we have nothing for that slice
        if (lastBlockIdx < 0)
        {
            assert startIdx < 0;
            return false;
        }

        // If we start before the very first block, just read from the first one.
        if (startIdx < 0)
            startIdx = 0;

        // If that's the last block we were reading, we're already where we want to be. Otherwise,
        // seek to that first block
        if (startIdx != indexState.currentBlockIdx())
            indexState.setToBlock(startIdx);

        // The index search is based on the last name of the index blocks, so at that point we have that:
        //   1) indexes[currentIdx - 1].lastName < slice.start <= indexes[currentIdx].lastName
        //   2) indexes[lastBlockIdx - 1].lastName < slice.end <= indexes[lastBlockIdx].lastName
        // so if currentIdx == lastBlockIdx and slice.end < indexes[currentIdx].firstName, we're guaranteed that the
        // whole slice is between the previous block end and this block start, and thus has no corresponding
        // data. One exception is if the previous block ends with an openMarker as it will cover our slice
        // and we need to return it.
        if (indexState.currentBlockIdx() == lastBlockIdx
            && metadata.comparator.compare(slice.end(), indexState.currentIndex().firstName) < 0
            && openMarker == null)
        {
            return false;
        }

        return true;
    }

    @Override
    protected Unfiltered nextInSlice() throws IOException
    {
        while (true)
        {
            // Our previous read might have made us cross an index block boundary. If so, update our informations.
            // If we read from the beginning of the partition, this is also what will initialize the index state.
            indexState.updateBlock();

            // Return the next unfiltered unless we've reached the end, or we're beyond our slice
            // end (note that unless we're on the last block for the slice, there is no point
            // in checking the slice end).
            if (indexState.isDone()
                || indexState.currentBlockIdx() > lastBlockIdx
                || !deserializer.hasNext()
                || (indexState.currentBlockIdx() == lastBlockIdx && deserializer.compareNextTo(end) >= 0))
                return null;


            Unfiltered next = deserializer.readNext();
            // We may get empty row for the same reason expressed on UnfilteredSerializer.deserializeOne.
            // This should be rare (i.e. does not warrant saving state).
            if (next.isEmpty())
                continue;

            if (next.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
                updateOpenMarker((RangeTombstoneMarker) next);
            return next;
        }
    }
}
