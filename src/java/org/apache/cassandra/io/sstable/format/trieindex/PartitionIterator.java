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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

class PartitionIterator extends PartitionIndex.IndexPosIterator implements PartitionIndexIterator
{
    final PartitionPosition limit;
    final int exclusiveLimit;
    DecoratedKey currentKey;
    RowIndexEntry currentEntry;
    DecoratedKey nextKey;
    RowIndexEntry nextEntry;
    final FileHandle dataFile;
    final FileHandle rowIndexFile;
    final PartitionIndex partitionIndex;
    final IPartitioner partitioner;
    boolean closeHandles = false;

    /**
     * Note: For performance reasons this class does not request a reference of the files it uses.
     * If it is the only reference to the data, caller must request shared copies and apply closeHandles().
     * See {@link org.apache.cassandra.io.sstable.format.trieindex.TrieIndexFormat.ReaderFactory#keyIterator(org.apache.cassandra.io.sstable.Descriptor, org.apache.cassandra.schema.TableMetadata)}
     */
    PartitionIterator(PartitionIndex partitionIndex, IPartitioner partitioner, FileHandle rowIndexFile, FileHandle dataFile,
                      PartitionPosition left, int inclusiveLeft, PartitionPosition right, int exclusiveRight, Rebufferer.ReaderConstraint rc) throws IOException
    {
        super(partitionIndex, left, right, rc);
        this.partitionIndex = partitionIndex;
        this.partitioner = partitioner;
        this.limit = right;
        this.exclusiveLimit = exclusiveRight;
        this.rowIndexFile = rowIndexFile;
        this.dataFile = dataFile;
        readNext();
        // first value can be off
        if (nextKey != null && !(nextKey.compareTo(left) > inclusiveLeft))
            readNext();
        advance();
    }

    /**
     * Note: For performance reasons this class does not request a reference of the files it uses.
     * If it is the only reference to the data, caller must request shared copies and apply closeHandles().
     * See {@link org.apache.cassandra.io.sstable.format.trieindex.TrieIndexFormat.ReaderFactory#keyIterator(org.apache.cassandra.io.sstable.Descriptor, org.apache.cassandra.schema.TableMetadata)}
     */
    PartitionIterator(PartitionIndex partitionIndex, IPartitioner partitioner, FileHandle rowIndexFile, FileHandle dataFile, Rebufferer.ReaderConstraint rc) throws IOException
    {
        super(partitionIndex, rc);
        this.partitionIndex = partitionIndex;
        this.partitioner = partitioner;
        this.limit = null;
        this.exclusiveLimit = 0;
        this.rowIndexFile = rowIndexFile;
        this.dataFile = dataFile;
        readNext();
        advance();
    }

    public PartitionIterator closeHandles()
    {
        this.closeHandles = true;
        return this;
    }

    public void close()
    {
        try
        {
            if (closeHandles)
                FBUtilities.closeAll(ImmutableList.of(partitionIndex, dataFile, rowIndexFile));
        }
        catch (Exception e)
        {
            Throwables.propagate(e);
        }
        finally
        {
            super.close();
        }
    }

    public DecoratedKey key()
    {
        return currentKey;
    }

    public long dataPosition()
    {
        return currentEntry != null ? currentEntry.position : -1;
    }

    public RowIndexEntry entry()
    {
        return currentEntry;
    }

    public void advance() throws IOException
    {
        currentKey = nextKey;
        currentEntry = nextEntry;
        if (currentKey != null)
        {
            readNext();
            // if nextKey is null, then currentKey is the last key to be published, therefore check against any limit
            // and suppress the partition if it is beyond the limit
            if (nextKey == null && limit != null && currentKey.compareTo(limit) > exclusiveLimit)
            { // exclude last partition outside range
                currentKey = null;
                currentEntry = null;
            }
        }
    }

    private void readNext() throws IOException
    {
        long pos = nextIndexPos();
        if (pos != PartitionIndex.NOT_FOUND)
        {
            if (pos >= 0)
                try (FileDataInput in = rowIndexFile.createReader(pos, rc))
                {
                    nextKey = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
                    nextEntry = TrieIndexEntry.deserialize(in, in.getFilePointer());
                }
            else
                try (FileDataInput in = dataFile.createReader(~pos, rc))
                {
                    nextKey = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
                    nextEntry = new RowIndexEntry(~pos);
                }
        }
        else
        {
            nextKey = null;
            nextEntry = null;
        }
    }
}