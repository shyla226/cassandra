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
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.Rebufferer;

class ReverseIndexedReader extends ReverseReader
{
    private final IndexState indexState;

    // The last index block to consider for the slice
    private int lastBlockIdx;

    ReverseIndexedReader(BigTableReader sstable,
                         BigRowIndexEntry indexEntry,
                         Slices slices,
                         FileDataInput file,
                         boolean shouldCloseFile,
                         SerializationHelper helper,
                         Rebufferer.ReaderConstraint rc)
    {
        super(sstable, slices, file, shouldCloseFile, helper);
        this.indexState = new IndexState(this, metadata.comparator, indexEntry, true, sstable.ifile, rc);
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

        // if our previous slicing already got us past the beginning of the sstable, we're done
        if (indexState.isDone())
            return false;

        // Find the first index block we'll need to read for the slice.
        int startIdx = indexState.findBlockIndex(slice.end(), indexState.currentBlockIdx());
        if (startIdx < 0)
            return false;

        lastBlockIdx = indexState.findBlockIndex(slice.start(), startIdx);

        // If the last block to look (in reverse order) is after the very last block, we have nothing for that slice
        if (lastBlockIdx >= indexState.blocksCount())
        {
            assert startIdx >= indexState.blocksCount();
            return false;
        }

        // If we start (in reverse order) after the very last block, just read from the last one.
        if (startIdx >= indexState.blocksCount())
            startIdx = indexState.blocksCount() - 1;

        // Note that even if we were already set on the proper block (which would happen if the previous slice
        // requested ended on the same block this one start), we can't reuse it because when reading the previous
        // slice we've only read that block from the previous slice start. Re-reading also handles
        // skipFirstIteratedItem/skipLastIteratedItem that we would need to handle otherwise.
        indexState.setToBlock(startIdx);
        prepStage = PrepStage.OPEN;
        return true;
    }

    @Override
    protected boolean preSliceStep() throws IOException
    {
        if (indexState.currentBlockIdx() != lastBlockIdx)
            return true;

        return skipSmallerRow(start);
    }

    protected boolean slicePrepStep() throws IOException
    {
        if (buffer == null)
            buffer = createBuffer(indexState.blocksCount());

        return prepStep(indexState.currentBlockIdx() != lastBlockIdx, false);
    }

    protected boolean preBlockStep() throws IOException
    {
        if (indexState.currentBlockIdx() != lastBlockIdx)
            return true;

        return skipSmallerRow(start);
    }

    protected boolean blockPrepStep() throws IOException
    {
        return prepStep(indexState.currentBlockIdx() != lastBlockIdx, true);
    }

    @Override
    public boolean advanceBlock() throws IOException
    {
        int nextBlockIdx = indexState.currentBlockIdx() - 1;
        if (nextBlockIdx < 0 || nextBlockIdx < lastBlockIdx)
            return false;

        // The slice start can be in
        indexState.setToBlock(nextBlockIdx);
        prepStage = PrepStage.OPEN;
        return true;
    }

    @Override
    protected boolean stopReadingDisk() throws IOException
    {
        return indexState.isPastCurrentBlock();
    }
}
