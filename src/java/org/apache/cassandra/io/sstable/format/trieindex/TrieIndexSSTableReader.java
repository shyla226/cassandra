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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.Downsampling;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.format.IndexFileEntry;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener.SelectionReason;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener.SkippingReason;
import org.apache.cassandra.io.sstable.format.big.BigTableRowIndexEntry;
import org.apache.cassandra.io.sstable.format.trieindex.PartitionIndex.IndexPosIterator;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.BloomFilterSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.io.sstable.format.SSTableReader.Operator.EQ;
import static org.apache.cassandra.io.sstable.format.SSTableReader.Operator.GE;
import static org.apache.cassandra.io.sstable.format.SSTableReader.Operator.GT;
import static org.apache.cassandra.io.sstable.format.SSTableReader.Operator.LT;
import static org.apache.cassandra.io.sstable.format.SSTableReaderBuilder.defaultIndexHandleBuilder;

/**
 * SSTableReaders are open()ed by Keyspace.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
class TrieIndexSSTableReader extends SSTableReader
{
    private static final Logger logger = LoggerFactory.getLogger(TrieIndexSSTableReader.class);

    protected FileHandle rowIndexFile;
    protected PartitionIndex partitionIndex;

    TrieIndexSSTableReader(Descriptor desc, Set<Component> components, TableMetadataRef metadata, Long maxDataAge, StatsMetadata sstableMetadata, OpenReason openReason, SerializationHeader header, FileHandle dfile, IFilter bf)
    {
        super(desc, components, metadata, maxDataAge, sstableMetadata, openReason, header, null, dfile, null, bf);
    }

    protected void loadIndex(boolean preload) throws IOException
    {
        if (components.contains(Component.PARTITION_INDEX))
        {
            try (
                    FileHandle.Builder rowIndexBuilder = defaultIndexHandleBuilder(descriptor, Component.ROW_INDEX);
                    FileHandle.Builder partitionIndexBuilder = defaultIndexHandleBuilder(descriptor, Component.PARTITION_INDEX);
            )
            {
                rowIndexFile = rowIndexBuilder.complete();
                // only preload if memmapped
                partitionIndex = PartitionIndex.load(partitionIndexBuilder, metadata().partitioner, preload);
                first = partitionIndex.firstKey();
                last = partitionIndex.lastKey();
            }
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
                                                          dfile.sharedCopy(),
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
        TrieIndexSSTableReader reader = TrieIndexFormat.readerFactory.open(desc, components, metadata, maxDataAge, sstableMetadata, openReason, header, dfile, bf);

        reader.rowIndexFile = ifile;
        reader.partitionIndex = partitionIndex;
        reader.setup(true);

        return reader;
    }

    @Override
    protected void setup(boolean trackHotness) {
        super.setup(trackHotness);
        tidy.setup(this, trackHotness, Arrays.asList(bf, dfile, partitionIndex, rowIndexFile));
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        rowIndexFile.addTo(identities);
        partitionIndex.addTo(identities);
    }

    @Override
    public void verifyComponent(Component component) throws IOException
    {
        switch (component.type)
        {
            case PARTITION_INDEX:
                verifyPartitionIndex();
                break;
            case ROW_INDEX:
                verifyRowIndex();
                break;
            case FILTER:
                verifyBloomFilter();
                break;
            default:
                // just ignore anything else
                break;
        }
    }

    private void verifyBloomFilter() throws IOException
    {
        try (DataInputStream stream = new DataInputStream(new BufferedInputStream(Files.newInputStream(Paths.get(descriptor.filenameFor(Component.FILTER)))));
             IFilter bf = BloomFilterSerializer.deserialize(stream, descriptor.version.hasOldBfFormat()))
        {}
    }

    private void verifyRowIndex() throws IOException
    {
        try (PartitionIndexIterator keyIter = TrieIndexFormat.readerFactory.keyIterator(descriptor, metadata()))
        {
            while (true) {
                keyIter.advance();
                DecoratedKey key = metadata().partitioner.decorateKey(keyIter.key());
                if (key == null)
                    break;
            }
        }
    }

    private void verifyPartitionIndex() throws IOException
    {
        StatsMetadata statsMetadata = (StatsMetadata) descriptor.getMetadataSerializer().deserialize(descriptor, MetadataType.STATS);
        try (FileHandle.Builder builder = forVerify(defaultIndexHandleBuilder(descriptor, Component.PARTITION_INDEX));
             PartitionIndex index = PartitionIndex.load(builder, metadata().partitioner, false);
             IndexPosIterator iter = index.allKeysIterator())
        {
            while (iter.nextIndexPos() != PartitionIndex.NOT_FOUND)
            {}
        }
    }

    private boolean filterFirst()
    {
        return openReason == OpenReason.MOVED_START;
    }

    private boolean filterLast()
    {
        return false;
    }

    public long estimatedKeys()
    {
        return partitionIndex == null ? 0 : partitionIndex.size();
    }

    public PartitionReader reader(FileDataInput file,
                                  boolean shouldCloseFile,
                                  RowIndexEntry<?> indexEntry,
                                  SerializationHelper helper,
                                  Slices slices,
                                  boolean reversed)
    throws IOException
    {
        return indexEntry.isIndexed()
               ? reversed
                 ? new ReverseIndexedReader(this, (TrieIndexEntry) indexEntry, slices, file, shouldCloseFile, helper)
                 : new ForwardIndexedReader(this, (TrieIndexEntry) indexEntry, slices, file, shouldCloseFile, helper)
               : reversed
                 ? new ReverseReader(this, slices, file, shouldCloseFile, helper)
                 : new ForwardReader(this, slices, file, shouldCloseFile, helper);
    }

    @Override
    protected RowIndexEntry<?> getPosition(PartitionPosition key, Operator op, boolean updateCacheAndStats, boolean permitMatchPastLast, SSTableReadsListener listener)
    {
        // todo update bloom filter if updateCacheAndStats
        // todo handle permitMatchPastLast

        PartitionPosition searchKey;
        Operator searchOp;

        if (op == EQ)
            return getExactPosition((DecoratedKey) key, listener);

        if (op == GT || op == GE)
        {
            if (filterLast() && last.compareTo(key) < 0)
                return null;
            boolean filteredLeft = (filterFirst() && first.compareTo(key) > 0);
            searchKey = filteredLeft ? first : key;
            searchOp = filteredLeft ? GE : op;

            try (PartitionIndex.Reader reader = partitionIndex.openReader())
            {
                return reader.ceiling(searchKey,
                                      (pos, assumeNoMatch, compareKey) -> retrieveEntryIfAcceptable(searchOp, compareKey, pos, assumeNoMatch));
            }
            catch (IOException e)
            {
                markSuspect();
                throw new CorruptSSTableException(e, rowIndexFile.path());
            }
        }

        if (op == LT)
        {
            if (filterFirst() && first.compareTo(key) >= 0)
                return null;
            boolean filteredRight = filterLast() && last.compareTo(key) < 0;
            searchKey = filteredRight ? last : key;
            searchOp = LT;

            try (PartitionIndex.Reader reader = partitionIndex.openReader())
            {
                return reader.floor(searchKey,
                                    (pos, assumeNoMatch, compareKey) -> retrieveEntryIfAcceptable(searchOp, compareKey, pos, assumeNoMatch));
            }
            catch (IOException e)
            {
                markSuspect();
                throw new CorruptSSTableException(e, rowIndexFile.path());
            }
        }

        throw new UnsupportedOperationException("Unsupported op: " + op);
    }

    /**
     * Called by getPosition above (via Reader.ceiling/floor) to check if the position satisfies the full key constraint.
     * This is called once if there is a prefix match (which can be in any relationship with the sought key, thus
     * assumeNoMatch: false), and if it returns null it is called again for the closest greater position
     * (with assumeNoMatch: true).
     * Returns the index entry at this position, or null if the search op rejects it.
     */
    private RowIndexEntry<?> retrieveEntryIfAcceptable(Operator searchOp, PartitionPosition searchKey, long pos, boolean assumeNoMatch) throws IOException
    {
        if (pos >= 0)
        {
            try (FileDataInput in = rowIndexFile.createReader(pos))
            {
                if (assumeNoMatch)
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
            if (!assumeNoMatch)
            {
                try (FileDataInput in = dfile.createReader(pos))
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

    protected boolean inBloomFilter(DecoratedKey dk)
    {
        return first.compareTo(dk) <= 0 && last.compareTo(dk) >= 0 && bf.isPresent(dk);
    }

    public boolean contains(DecoratedKey dk)
    {
        if (!inBloomFilter(dk))
            return false;
        if (filterFirst() && first.compareTo(dk) > 0)
            return false;
        if (filterLast() && last.compareTo(dk) < 0)
            return false;

        try (PartitionIndex.Reader reader = partitionIndex.openReader())
        {
            long indexPos = reader.exactCandidate(dk);
            if (indexPos == PartitionIndex.NOT_FOUND)
                return false;

            try (FileDataInput in = createIndexOrDataReader(indexPos))
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

    FileDataInput createIndexOrDataReader(long indexPos)
    {
        if (indexPos >= 0)
            return rowIndexFile.createReader(indexPos);
        else
            return dfile.createReader(~indexPos);
    }

    @Override
    public DecoratedKey keyAt(RandomAccessReader reader, long dataPosition) throws IOException
    {
        reader.seek(dataPosition);
        if (reader.isEOF()) return null;
        return decorateKey(ByteBufferUtil.readWithShortLength(reader));
    }

    @Override
    public DecoratedKey keyAt(long dataPosition) throws IOException
    {
        try (FileDataInput in = dfile.createReader(dataPosition))
        {
            if (in.isEOF()) return null;
            return decorateKey(ByteBufferUtil.readWithShortLength(in));
        }
    }

    @Override
    public DecoratedKey keyAt(FileDataInput reader) throws IOException
    {
        return null;
    }

    public RowIndexEntry<?> getExactPosition(DecoratedKey dk,
                                          SSTableReadsListener listener,
                                          FileDataInput rowIndexInput,
                                          FileDataInput dataInput)
    {
        if (!inBloomFilter(dk))
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

        try (PartitionIndex.Reader reader = partitionIndex.openReader())
        {
            long indexPos = reader.exactCandidate(dk);
            if (indexPos == PartitionIndex.NOT_FOUND)
            {
                bloomFilterTracker.addFalsePositive();
                listener.onSSTableSkipped(this, SkippingReason.PARTITION_INDEX_LOOKUP);
                return null;
            }

            FileDataInput toClose = null;
            try
            {
                FileDataInput in;
                if (indexPos >= 0)
                {
                    in = rowIndexInput;
                    if (in == null)
                    {
                        in = rowIndexFile.createReader(indexPos);
                        toClose = in;
                    }
                    else
                        in.seek(indexPos);
                }
                else
                {
                    in = dataInput;
                    if (in == null)
                    {
                        in = dfile.createReader(~indexPos);
                        toClose = in;
                    }
                    else
                        in.seek(~indexPos);
                }

                if (!ByteBufferUtil.equalsWithShortLength(in, dk.getKey()))
                {
                    bloomFilterTracker.addFalsePositive();
                    listener.onSSTableSkipped(this, SkippingReason.INDEX_ENTRY_NOT_FOUND);
                    return null;
                }

                bloomFilterTracker.addTruePositive();
                RowIndexEntry<?> entry = indexPos >= 0 ? TrieIndexEntry.deserialize(in, in.getFilePointer())
                                                    : new RowIndexEntry<>(~indexPos);

                listener.onSSTableSelected(this, entry, SelectionReason.INDEX_ENTRY_FOUND);
                return entry;
            }
            finally
            {
                if (toClose != null)
                    toClose.close();
            }
        }
        catch (IOException e)
        {
            markSuspect();
            throw new CorruptSSTableException(e, rowIndexFile.path());
        }
    }

    public RowIndexEntry<?> getExactPosition(DecoratedKey dk, SSTableReadsListener listener)
    {
        return getExactPosition(dk, listener, null, null);
    }

    protected FileHandle[] getFilesToBeLocked()
    {
        return new FileHandle[] { partitionIndex.getFileHandle(), rowIndexFile, dfile };
    }

    public PartitionIterator coveredKeysIterator(PartitionPosition left, boolean inclusiveLeft, PartitionPosition right, boolean inclusiveRight) throws IOException
    {
        AbstractBounds<PartitionPosition> cover = Bounds.bounds(left, inclusiveLeft, right, inclusiveRight);
        boolean isLeftInSStableRange = !filterFirst() || first.compareTo(left) <= 0 && last.compareTo(left) >= 0;
        boolean isRightInSStableRange = !filterLast()|| first.compareTo(right) <= 0 && last.compareTo(right) >= 0;
        if (isLeftInSStableRange || isRightInSStableRange || (cover.contains(first) && cover.contains(last)))
        {
            inclusiveLeft = isLeftInSStableRange ? inclusiveLeft : true;
            inclusiveRight = isRightInSStableRange ? inclusiveRight : true;
            return new PartitionIterator(partitionIndex,
                                         metadata().partitioner,
                                         rowIndexFile, dfile,
                                         isLeftInSStableRange ? left : first, inclusiveLeft ? -1 : 0,
                                         isRightInSStableRange ? right : last, inclusiveRight ? 0 : -1);
        }
        else
            return PartitionIterator.empty(partitionIndex);
    }

    public PartitionIterator allKeysIterator() throws IOException
    {
        return new PartitionIterator(partitionIndex, metadata().partitioner, rowIndexFile, dfile);
    }

    public ScrubIterator scrubPartitionsIterator() throws IOException
    {
        if (partitionIndex == null)
            return null;
        return new ScrubIterator(partitionIndex, rowIndexFile);
    }

    public Iterator<IndexFileEntry> coveredKeysFlow(RandomAccessReader dfileReader,
                                                PartitionPosition left,
                                                boolean inclusiveLeft,
                                                PartitionPosition right,
                                                boolean inclusiveRight)
    {
        boolean isLeftInSStableRange = !filterFirst() || first.compareTo(left) <= 0 && last.compareTo(left) >= 0;
        boolean isRightInSStableRange = !filterLast() || first.compareTo(right) <= 0 && last.compareTo(right) >= 0;
        if (isLeftInSStableRange || isRightInSStableRange)
        {
            inclusiveLeft = isLeftInSStableRange ? inclusiveLeft : true;
            inclusiveRight = isRightInSStableRange ? inclusiveRight : true;
            return new TrieIndexFileIterator(dfileReader,
                                             this,
                                         isLeftInSStableRange ? left : first, inclusiveLeft ? -1 : 0,
                                         isRightInSStableRange ? right : last, inclusiveRight ? 0 : -1);
        }
        else
        {
            return Collections.emptyIterator();
        }
    }

    @Override
    public Iterable<DecoratedKey> getKeySamples(final Range<Token> range)
        {
        Iterator<IndexPosIterator> partitionKeyIterators = SSTableScanner.makeBounds(this,
                                                                                     Collections.singleton(range))
                                                                         .stream()
                                                                         .map(bound -> indexPosIteratorForRange(bound))
                                                                         .iterator();

        if (!partitionKeyIterators.hasNext())
            return Collections.emptyList();

        return new Iterable<DecoratedKey>()
        {
            @Override
            public Iterator<DecoratedKey> iterator()
            {
                return new AbstractIterator<DecoratedKey>()
                {
                    IndexPosIterator currentItr = partitionKeyIterators.next();
                    long count = -1;

                    private long getNextPos() throws IOException
                    {
                        long pos = PartitionIndex.NOT_FOUND;
                        while ((pos = currentItr.nextIndexPos()) == PartitionIndex.NOT_FOUND
                                && partitionKeyIterators.hasNext())
                        {
                            closeCurrentIt();
                            currentItr = partitionKeyIterators.next();
                        }
                        return pos;
                    }

                    private void closeCurrentIt()
                    {
                        if (currentItr != null)
                            currentItr.close();
                        currentItr = null;
                    }

                    @Override
                    protected DecoratedKey computeNext()
                    {
                        try
                        {
                            while (true)
                            {
                                long pos = getNextPos();
                                count++;
                                if (pos == PartitionIndex.NOT_FOUND)
                                    break;
                                if (count % Downsampling.BASE_SAMPLING_LEVEL == 0)
                                {
                                    // handle exclusive start and exclusive end
                                    DecoratedKey key = getKeyByPos(pos);
                                    if (range.contains(key.getToken()))
                                        return key;
                                    count--;
                                }
                            }
                            closeCurrentIt();
                            return endOfData();
                        }
                        catch (IOException e)
                        {
                            closeCurrentIt();
                            markSuspect();
                            throw new CorruptSSTableException(e, dfile.path());
                        }
                    }
                };
            }
        };
    }

    private DecoratedKey getKeyByPos(long pos) throws IOException
    {
        assert pos != PartitionIndex.NOT_FOUND;

        if (pos >= 0)
            try (FileDataInput in = rowIndexFile.createReader(pos))
            {
                return metadata().partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
            }
        else
            try (FileDataInput in = dfile.createReader(~pos))
            {
                return metadata().partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
            }
    }

    private IndexPosIterator indexPosIteratorForRange(AbstractBounds<PartitionPosition> bound)
    {
        return new IndexPosIterator(partitionIndex, bound.left, bound.right);
    }

    @Override
    public long estimatedKeysForRanges(Collection<Range<Token>> ranges)
    {
        // Estimate the number of partitions by calculating the bytes of the sstable that are covered by the specified
        // ranges and using the mean partition size to obtain a number of partitions from that.
        long selectedDataSize = 0;
        for (Range<Token> range : Range.normalize(ranges))
        {
            PartitionPosition left = range.left.minKeyBound();
            if (left.compareTo(first) <= 0)
                left = null;
            else if (left.compareTo(last) > 0)
                continue;   // no intersection

            PartitionPosition right = range.right.minKeyBound();
            if (range.right.isMinimum() || right.compareTo(last) >= 0)
                right = null;
            else if (right.compareTo(first) < 0)
                continue;   // no intersection

            if (left == null && right == null)
                return partitionIndex.size();   // sstable is fully covered, return full partition count to avoid rounding errors

            if (left == null && filterFirst())
                left = first;
            if (right == null && filterLast())
                right = last;

            long startPos = left != null ? getPosition(left, GE).position : 0;
            long endPos = right != null ? getPosition(right, GE).position : uncompressedLength();
            selectedDataSize += endPos - startPos;
        }
        return (long) (selectedDataSize / sstableMetadata.estimatedPartitionSize.rawMean());
    }

    // todo must be overridden
    @Override
    public UnfilteredRowIterator iterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener)
    {
        return null;
    }

    // todo must be overridden
    @Override
    public UnfilteredRowIterator simpleIterator(Supplier<FileDataInput> dfile, DecoratedKey key, boolean tombstoneOnly)
    {
        return null;
    }

    // todo must be overridden
    @Override
    public ISSTableScanner getScanner()
    {
        return null;
    }

    // todo must be overridden
    @Override
    public ISSTableScanner getScanner(Collection<Range<Token>> ranges)
    {
        return null;
    }

    // todo must be overridden
    @Override
    public ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> rangeIterator)
    {
        return null;
    }

    // todo must be overridden
    @Override
    public ISSTableScanner getScanner(ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener)
    {
        return null;
    }

    // todo must be overridden
    protected TrieIndexSSTableReader(SSTableReaderBuilder builder)
    {
        super(builder);
    }

    // todo must be overridden
    protected TrieIndexSSTableReader(Descriptor desc, Set<Component> components, TableMetadataRef metadata, long maxDataAge, StatsMetadata sstableMetadata, OpenReason openReason, SerializationHeader header, IndexSummary summary, FileHandle dfile, FileHandle ifile, IFilter bf)
    {
        super(desc, components, metadata, maxDataAge, sstableMetadata, openReason, header, summary, dfile, ifile, bf);
    }

    // -------------

    // todo must be overridden
    @Override
    public void setupOnline()
    {
        super.setupOnline();
    }

    // todo must be overridden
    @Override
    public SSTableReader cloneAndReplace(IFilter newBloomFilter)
    {
        return super.cloneAndReplace(newBloomFilter);
    }

    // todo must be overridden
    @Override
    public SSTableReader cloneWithRestoredStart(DecoratedKey restoredStart)
    {
        return super.cloneWithRestoredStart(restoredStart);
    }

    // todo must be overridden
    @Override
    public SSTableReader cloneWithNewStart(DecoratedKey newStart, Runnable runOnClose)
    {
        return super.cloneWithNewStart(newStart, runOnClose);
    }

    // todo must be overridden
    @Override
    public SSTableReader cloneWithNewSummarySamplingLevel(ColumnFamilyStore parent, int samplingLevel) throws IOException
    {
        return super.cloneWithNewSummarySamplingLevel(parent, samplingLevel);
    }

    @Override
    public int getIndexSummarySamplingLevel()
    {
        return 0; // tries do not have index summaries
    }

    @Override
    public long getIndexSummaryOffHeapSize()
    {
        return 0; // tries do not have index summaries
    }

    @Override
    public int getMinIndexInterval()
    {
        return 0; // tries do not have index summaries
    }

    @Override
    public double getEffectiveIndexInterval()
    {
        return 0; // tries do not have index summaries
    }

    @Override
    public void releaseSummary()
    {
        // no-op - tries do not have index summaries
    }

    @Override
    public long getIndexScanPosition(PartitionPosition key)
    {
        // TODO check this
        throw new UnsupportedOperationException();
    }

    @Override
    public int getIndexSummarySize()
    {
        return 0; // tries do not have index summaries
    }

    @Override
    public int getMaxIndexSummarySize()
    {
        return 0; // tries do not have index summaries
    }

    @Override
    public byte[] getIndexSummaryKey(int index)
    {
        // TODO check this
        throw new UnsupportedOperationException();
    }

    @Override
    public void cacheKey(DecoratedKey key, BigTableRowIndexEntry info)
    {
        // no-op - tries do not have index summaries
    }

    @Override
    public BigTableRowIndexEntry getCachedPosition(DecoratedKey key, boolean updateStats)
    {
        return null; // tries do not have index summaries
    }

    @Override
    protected BigTableRowIndexEntry getCachedPosition(KeyCacheKey unifiedKey, boolean updateStats)
    {
        return null; // tries do not have index summaries
    }

    @Override
    public boolean isKeyCacheEnabled()
    {
        return false; // tries do not have index summaries
    }

    // todo must be overridden
    @Override
    public DecoratedKey firstKeyBeyond(PartitionPosition token)
    {
        return super.firstKeyBeyond(token);
    }

    @Override
    public InstrumentingCache<KeyCacheKey, BigTableRowIndexEntry> getKeyCache()
    {
        return null; // tries do not have key cache
    }

    @Override
    public long getKeyCacheHit()
    {
        return 0L; // tries do not have key cache
    }

    @Override
    public long getKeyCacheRequest()
    {
        return 0L;  // tries do not have key cache
    }

    @Override
    public ChannelProxy getIndexChannel()
    {
        throw new UnsupportedOperationException("tries do not have primary index");
    }

    @Override
    public RandomAccessReader openIndexReader()
    {
        throw new UnsupportedOperationException("tries do not have primary index");
    }

    @Override
    public FileHandle getIndexFile()
    {
        throw new UnsupportedOperationException("tries do not have primary index");
    }

}
