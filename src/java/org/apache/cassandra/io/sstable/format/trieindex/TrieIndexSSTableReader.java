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
import java.nio.ByteBuffer;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.AbstractSSTableIterator;
import org.apache.cassandra.io.sstable.format.IndexFileEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener.SelectionReason;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener.SkippingReason;
import org.apache.cassandra.io.sstable.format.ScrubPartitionIterator;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.MmapRebufferer;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.flow.Flow;

/**
 * SSTableReaders are open()ed by Keyspace.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
class TrieIndexSSTableReader extends SSTableReader
{
    private static final Logger logger = LoggerFactory.getLogger(TrieIndexSSTableReader.class);

    protected FileHandle rowIndexFile;
    protected PartitionIndex partitionIndex;

    TrieIndexSSTableReader(Descriptor desc, Set<Component> components, TableMetadataRef metadata, Long maxDataAge, StatsMetadata sstableMetadata, OpenReason openReason, SerializationHeader header)
    {
        super(desc, components, metadata, maxDataAge, sstableMetadata, openReason, header);
    }

    protected void loadIndex(boolean preloadIfMemmapped) throws IOException
    {
        if (components.contains(Component.PARTITION_INDEX))
            try (
                FileHandle.Builder rowIndexBuilder = indexFileHandleBuilder(Component.ROW_INDEX);
                FileHandle.Builder partitionIndexBuilder = indexFileHandleBuilder(Component.PARTITION_INDEX);
                )
            {
                rowIndexFile = rowIndexBuilder.complete();
                // only preload if memmapped
                boolean preload = preloadIfMemmapped && rowIndexFile.rebuffererFactory() instanceof MmapRebufferer;
                partitionIndex = PartitionIndex.load(partitionIndexBuilder, metadata().partitioner, preload, Rebufferer.ReaderConstraint.NONE);
                first = partitionIndex.firstKey();
                last = partitionIndex.lastKey();
            }
    }

    protected void releaseIndex()
    {
        if (rowIndexFile != null)
        {
            rowIndexFile.close();
            rowIndexFile = null;
        }

        if (partitionIndex != null)
        {
            partitionIndex.close();
            partitionIndex = null;
        }
    }

    /**
     * Clone this reader with the new open reason and set the clone as replacement.
     *
     * @param reason the {@code OpenReason} for the replacement.
     *
     * @return the cloned reader. That reader is set as a replacement by the method.
     */
    protected SSTableReader clone(OpenReason reason)
    {
        TrieIndexSSTableReader replacement = internalOpen(descriptor,
                                                          components,
                                                          metadata,
                                                          rowIndexFile.sharedCopy(),
                                                          dataFile.sharedCopy(),
                                                          partitionIndex.sharedCopy(),
                                                          bf.sharedCopy(),
                                                          maxDataAge,
                                                          sstableMetadata,
                                                          reason,
                                                          header);
        replacement.first = first;
        replacement.last = last;
        replacement.isSuspect.set(isSuspect.get());
        return replacement;
    }

    /**
     * Open a RowIndexedReader which already has its state initialized (by SSTableWriter).
     */
    static TrieIndexSSTableReader internalOpen(Descriptor desc,
                                               Set<Component> components,
                                               TableMetadataRef metadata,
                                               FileHandle ifile,
                                               FileHandle dfile,
                                               PartitionIndex partitionIndex,
                                               IFilter bf,
                                               long maxDataAge,
                                               StatsMetadata sstableMetadata,
                                               OpenReason openReason,
                                               SerializationHeader header)
    {
        assert desc != null && ifile != null && dfile != null && partitionIndex != null && bf != null && sstableMetadata != null;

        // Make sure the SSTableReader internalOpen part does the same.
        assert desc.getFormat() == TrieIndexFormat.instance;
        TrieIndexSSTableReader reader = TrieIndexFormat.readerFactory.open(desc, components, metadata, maxDataAge, sstableMetadata, openReason, header);

        reader.bf = bf;
        reader.rowIndexFile = ifile;
        reader.dataFile = dfile;
        reader.partitionIndex = partitionIndex;
        reader.setup(true);

        return reader;
    }

    @Override
    protected void setup(boolean trackHotness)
    {
        super.setup(trackHotness);
        tidy.addCloseable(partitionIndex);
        tidy.addCloseable(rowIndexFile);
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        rowIndexFile.addTo(identities);
        partitionIndex.addTo(identities);
    }

    public long estimatedKeys()
    {
        return partitionIndex.size();
    }

    public UnfilteredRowIterator iterator(DecoratedKey key,
                                          Slices slices,
                                          ColumnFilter selectedColumns,
                                          boolean reversed,
                                          SSTableReadsListener listener)
    {
        RowIndexEntry rie = getExactPosition(key, listener, Rebufferer.ReaderConstraint.NONE);
        return iterator(null, key, rie, slices, selectedColumns, reversed, Rebufferer.ReaderConstraint.NONE);
    }

    public UnfilteredRowIterator iterator(FileDataInput file,
                                          DecoratedKey key,
                                          RowIndexEntry indexEntry,
                                          Slices slices,
                                          ColumnFilter selectedColumns,
                                          boolean reversed,
                                          Rebufferer.ReaderConstraint readerConstraint)
    {
        if (indexEntry == null)
            return UnfilteredRowIterators.noRowsIterator(metadata(), key, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, reversed);
        return reversed
             ? new SSTableReversedIterator(this, file, key, indexEntry, slices, selectedColumns, readerConstraint)
             : new SSTableIterator(this, file, key, indexEntry, slices, selectedColumns, readerConstraint);
    }

    @Override
    public RowIndexEntry getPosition(PartitionPosition key, Operator op, SSTableReadsListener listener, Rebufferer.ReaderConstraint rc)
    {
        if (op == Operator.EQ)
        {
            return getExactPosition((DecoratedKey) key, listener, rc);
        }
        else
        {
            if (filterLast() && last.compareTo(key) < 0)
                return null;
            boolean filteredLeft = (filterFirst() && first.compareTo(key) > 0);
            final PartitionPosition searchKey = filteredLeft ? first : key;
            final Operator searchOp = filteredLeft ? Operator.GE : op;

            try (PartitionIndex.Reader reader = partitionIndex.openReader(rc))
            {
                return reader.ceiling(searchKey,
                                      (pos, assumeGreater, compareKey) -> retrieveEntryIfAcceptable(searchOp, compareKey, pos, assumeGreater, rc));
            }
            catch (IOException e)
            {
                markSuspect();
                throw new CorruptSSTableException(e, rowIndexFile.path());
            }
        }
    }

    /**
     * Called by getPosition above (via Reader.ceiling) to check if the position satisfies the full key constraint.
     * This is called once if there is a prefix match (which can be in any relationship with the sought key, thus
     * assumeGreater: false), and if it returns null it is called again for the closest greater position
     * (with assumeGreater: true).
     * Returns the index entry at this position, or null if the search op rejects it.
     */
    private RowIndexEntry retrieveEntryIfAcceptable(Operator searchOp, PartitionPosition searchKey, long pos, boolean assumeGreater, Rebufferer.ReaderConstraint rc) throws IOException
    {
        if (pos >= 0)
        {
            try (FileDataInput in = rowIndexFile.createReader(pos, rc))
            {
                if (assumeGreater)
                    ByteBufferUtil.skipShortLength(in);
                else
                {
                    ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
                    DecoratedKey decorated = decorateKey(indexKey);
                    if (searchOp.apply(decorated.compareTo(searchKey)) != 0)
                        return null;
                }
                return TrieIndexEntry.deserialize(in, in.getFilePointer());
            }
        }
        else
        {
            pos = ~pos;
            if (!assumeGreater)
            {
                try (FileDataInput in = dataFile.createReader(pos, rc))
                {
                    ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
                    DecoratedKey decorated = decorateKey(indexKey);
                    if (searchOp.apply(decorated.compareTo(searchKey)) != 0)
                        return null;
                }
            }
            return new RowIndexEntry(pos);
        }
    }

    public boolean contains(DecoratedKey dk, Rebufferer.ReaderConstraint rc)
    {
        if (!bf.isPresent(dk))
            return false;
        if (filterFirst() && first.compareTo(dk) > 0)
            return false;
        if (filterLast() && last.compareTo(dk) < 0)
            return false;

        try (PartitionIndex.Reader reader = partitionIndex.openReader(rc))
        {
            long indexPos = reader.exactCandidate(dk);
            if (indexPos == PartitionIndex.NOT_FOUND)
                return false;

            try (FileDataInput in = createIndexOrDataReader(indexPos, rc))
            {
                return ByteBufferUtil.equalsWithShortLength(in, dk.getKey());
            }
        }
        catch (IOException e)
        {
            markSuspect();
            throw new CorruptSSTableException(e, rowIndexFile.path());
        }
    }

    FileDataInput createIndexOrDataReader(long indexPos, Rebufferer.ReaderConstraint rc)
    {
        if (indexPos >= 0)
            return rowIndexFile.createReader(indexPos, rc);
        else
            return dataFile.createReader(~indexPos, rc);
    }

    public DecoratedKey keyAt(long dataPosition, Rebufferer.ReaderConstraint rc) throws IOException
    {
        DecoratedKey key;
        try (FileDataInput in = dataFile.createReader(dataPosition, rc))
        {
            if (in.isEOF())
                return null;

            key = decorateKey(ByteBufferUtil.readWithShortLength(in));
        }

        return key;
    }

    @Override
    public RowIndexEntry getExactPosition(DecoratedKey dk, Rebufferer.ReaderConstraint rc)
    {
        return getExactPosition(dk, SSTableReadsListener.NOOP_LISTENER, rc);
    }

    private RowIndexEntry getExactPosition(DecoratedKey dk, SSTableReadsListener listener, Rebufferer.ReaderConstraint rc)
    {
        if (!bf.isPresent(dk))
        {
            listener.onSSTableSkipped(this, SkippingReason.BLOOM_FILTER);
            Tracing.trace("Bloom filter allows skipping sstable {}", descriptor.generation);
            return null;
        }

        if ((filterFirst() && first.compareTo(dk) > 0) || (filterLast() && last.compareTo(dk) < 0))
        {
            bloomFilterTracker.addFalsePositive();
            listener.onSSTableSkipped(this, SkippingReason.MIN_MAX_KEYS);
            return null;
        }

        try (PartitionIndex.Reader reader = partitionIndex.openReader(rc))
        {
            long indexPos = reader.exactCandidate(dk);
            if (indexPos == PartitionIndex.NOT_FOUND)
            {
                bloomFilterTracker.addFalsePositive();
                listener.onSSTableSkipped(this, SkippingReason.PARTITION_INDEX_LOOKUP);
                return null;
            }

            try (FileDataInput in = createIndexOrDataReader(indexPos, rc))
            {
                if (!ByteBufferUtil.equalsWithShortLength(in, dk.getKey()))
                {
                    bloomFilterTracker.addFalsePositive();
                    listener.onSSTableSkipped(this, SkippingReason.INDEX_ENTRY_NOT_FOUND);
                    return null;
                }

                bloomFilterTracker.addTruePositive();
                RowIndexEntry entry = indexPos >= 0 ? TrieIndexEntry.deserialize(in, in.getFilePointer())
                                                    : new RowIndexEntry(~indexPos);

                listener.onSSTableSelected(this, entry, SelectionReason.INDEX_ENTRY_FOUND);
                return entry;
            }
        }
        catch (IOException e)
        {
            markSuspect();
            throw new CorruptSSTableException(e, rowIndexFile.path());
        }
    }

    protected FileHandle[] getFilesToBeLocked()
    {
        return new FileHandle[] { dataFile, rowIndexFile, partitionIndex.getFileHandle() };
    }

    public PartitionIterator coveredKeysIterator(PartitionPosition left, boolean inclusiveLeft, PartitionPosition right, boolean inclusiveRight) throws IOException
    {
        return new PartitionIterator(partitionIndex, metadata().partitioner, rowIndexFile, dataFile, left, inclusiveLeft ? -1 : 0, right, inclusiveRight ? 0 : -1, Rebufferer.ReaderConstraint.NONE);
    }

    public PartitionIterator allKeysIterator() throws IOException
    {
        return new PartitionIterator(partitionIndex, metadata().partitioner, rowIndexFile, dataFile, Rebufferer.ReaderConstraint.NONE);
    }

    public ScrubPartitionIterator scrubPartitionsIterator() throws IOException
    {
        if (partitionIndex == null)
            return null;
        return new ScrubIterator(partitionIndex, rowIndexFile);
    }

    @Override
    public Flow<IndexFileEntry> coveredKeysFlow(RandomAccessReader dataFileReader,
                                                PartitionPosition left,
                                                boolean inclusiveLeft,
                                                PartitionPosition right,
                                                boolean inclusiveRight)
    {
        return new TrieIndexFileFlow(dataFileReader, this, left, inclusiveLeft ? -1 : 0, right, inclusiveRight ? 0 : -1);
    }
}
