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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.ByteBufferUtil;

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
    final IPartitioner partitioner;

    PartitionIterator(PartitionIndex partitionIndex, IPartitioner partitioner, FileHandle rowIndexFile, FileHandle dataFile,
                      PartitionPosition left, int inclusiveLeft, PartitionPosition right, int exclusiveRight) throws IOException
    {
        super(partitionIndex, left, right);
        this.partitioner = partitioner;
        this.limit = right;
        this.exclusiveLimit = exclusiveRight;
        this.rowIndexFile = rowIndexFile.sharedCopy();
        this.dataFile = dataFile.sharedCopy();
        readNext();
        // first value can be off
        if (nextKey != null && !(nextKey.compareTo(left) > inclusiveLeft))
            readNext();
        advance();
    }

    PartitionIterator(PartitionIndex partitionIndex, IPartitioner partitioner, FileHandle rowIndexFile, FileHandle dataFile) throws IOException
    {
        super(partitionIndex);
        this.partitioner = partitioner;
        this.limit = null;
        this.exclusiveLimit = 0;
        this.rowIndexFile = rowIndexFile.sharedCopy();
        this.dataFile = dataFile.sharedCopy();
        readNext();
        advance();
    }

    public void close()
    {
        super.close();
        dataFile.close();
        rowIndexFile.close();
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
            if (nextKey == null && limit != null && currentKey.compareTo(limit) > exclusiveLimit)
            {
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
                try (FileDataInput in = rowIndexFile.createReader(pos))
                {
                    nextKey = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
                    nextEntry = TrieIndexEntry.deserialize(in, in.getFilePointer());
                }
            else
                try (FileDataInput in = dataFile.createReader(~pos))
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