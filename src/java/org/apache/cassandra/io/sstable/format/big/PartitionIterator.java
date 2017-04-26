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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class PartitionIterator implements PartitionIndexIterator
{
    private final FileHandle ifile;
    private final RandomAccessReader reader;
    private final IPartitioner partitioner;
    private final BigRowIndexEntry.IndexSerializer rowIndexEntrySerializer;
    private final Version version;
    private final PartitionPosition limit;
    private final int exclusiveLimit;

    private DecoratedKey key;
    private RowIndexEntry entry;
    private long dataPosition;

    public PartitionIterator(FileHandle ifile, IPartitioner partitioner, BigRowIndexEntry.IndexSerializer rowIndexEntrySerializer, Version version) throws IOException
    {
        this.ifile = ifile.sharedCopy();
        this.reader = this.ifile.createReader();
        this.partitioner = partitioner;
        this.rowIndexEntrySerializer = rowIndexEntrySerializer;
        this.limit = null;
        this.exclusiveLimit = 0;
        this.version = version;
        advance();
    }

    public PartitionIterator(BigTableReader sstable) throws IOException
    {
        this(sstable.ifile, sstable.getPartitioner(), sstable.rowIndexEntrySerializer, sstable.descriptor.version);
    }

    PartitionIterator(BigTableReader sstable,
                      PartitionPosition left, int inclusiveLeft,
                      PartitionPosition right, int exclusiveRight) throws IOException
    {
        this.limit = right;
        this.exclusiveLimit = exclusiveRight;
        this.ifile = sstable.ifile.sharedCopy();
        this.reader = this.ifile.createReader();
        this.rowIndexEntrySerializer = sstable.rowIndexEntrySerializer;
        this.partitioner = sstable.getPartitioner();
        this.version = sstable.descriptor.version;
        seekTo(sstable, left, inclusiveLeft);
    }

    @Override
    public void close()
    {
        reader.close();
        ifile.close();
    }

    private void seekTo(BigTableReader sstable, PartitionPosition left, int inclusiveLeft) throws IOException
    {
        reader.seek(sstable.getIndexScanPosition(left));

        entry = null;
        dataPosition = -1;
        while (!reader.isEOF())
        {
            DecoratedKey indexDecoratedKey = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(reader));
            if (indexDecoratedKey.compareTo(left) > inclusiveLeft)
            {
                if (indexDecoratedKey.compareTo(limit) > exclusiveLimit)
                    break;

                key = indexDecoratedKey;
                return;
            }
            else
            {
                BigRowIndexEntry.Serializer.skip(reader, version);
            }
        }
        key = null;
    }

    @Override
    public void advance() throws IOException
    {
        if (entry == null)
            BigRowIndexEntry.Serializer.skip(reader, version);

        entry = null;
        dataPosition = -1;

        if (!reader.isEOF())
        {
            DecoratedKey indexDecoratedKey = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(reader));
            if (limit == null || indexDecoratedKey.compareTo(limit) <= exclusiveLimit)
            {
                key = indexDecoratedKey;
                return;
            }
        }
        key = null;
        entry = null;
    }

    @Override
    public DecoratedKey key()
    {
        return key;
    }

    @Override
    public RowIndexEntry entry() throws IOException
    {
        if (entry == null)
        {
            if (key == null)
                return null;
            assert rowIndexEntrySerializer != null : "Cannot use entry() without specifying rowIndexSerializer";
            entry = rowIndexEntrySerializer.deserialize(reader, reader.getFilePointer());
            dataPosition = entry.position;
        }
        return entry;
    }

    @Override
    public long dataPosition() throws IOException
    {
        if (dataPosition == -1)
        {
            if (key == null)
                return -1;
            long pos = reader.getFilePointer();
            dataPosition = BigRowIndexEntry.Serializer.readPosition(reader);
            reader.seek(pos);
        }
        return dataPosition;
    }
}
