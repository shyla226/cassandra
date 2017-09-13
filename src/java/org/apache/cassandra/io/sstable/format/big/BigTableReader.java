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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.MemoryOnlyStrategy;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.IndexFileEntry;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.ScrubPartitionIterator;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener.SelectionReason;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener.SkippingReason;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.flow.Flow;

/**
 * SSTableReaders are open()ed by Keyspace.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class BigTableReader extends SSTableReader
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableReader.class);

    protected FileHandle ifile;
    protected IndexSummary indexSummary;

    public final BigRowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    protected InstrumentingCache<KeyCacheKey, BigRowIndexEntry> keyCache;

    protected final AtomicLong keyCacheHit = new AtomicLong(0);
    protected final AtomicLong keyCacheRequest = new AtomicLong(0);

    BigTableReader(Descriptor desc, Set<Component> components, TableMetadataRef metadata, Long maxDataAge, StatsMetadata sstableMetadata, OpenReason openReason, SerializationHeader header)
    {
        super(desc, components, metadata, maxDataAge, sstableMetadata, openReason, header);

        this.rowIndexEntrySerializer = new BigRowIndexEntry.Serializer(descriptor.version,
                                                                       header);
    }

    protected void loadIndex(boolean preloadIfMemmapped) throws IOException
    {
        if (!components.contains(Component.PRIMARY_INDEX))
        {
            // avoid any reading of the missing primary index component.
            // this should only happen during StandaloneScrubber
            return;
        }

        try(FileHandle.Builder ibuilder = new FileHandle.Builder(descriptor.filenameFor(Component.PRIMARY_INDEX))
                .mmapped(DatabaseDescriptor.getIndexAccessMode() != Config.DiskAccessMode.standard && metadata().params.compaction.klass().equals(MemoryOnlyStrategy.class))
                .withChunkCache(ChunkCache.instance))
        {
            loadSummary();

            long indexFileLength = new File(descriptor.filenameFor(Component.PRIMARY_INDEX)).length();
            int indexBufferSize = optimizationStrategy.bufferSize(indexFileLength / indexSummary.size());
            ifile = ibuilder.bufferSize(indexBufferSize).complete();
        }
    }

    /**
     * Load index summary from Summary.db file if it exists.
     *
     * if loaded index summary has different index interval from current value stored in schema,
     * then Summary.db file will be deleted and this returns false to rebuild summary.
     *
     * @return true if index summary is loaded successfully from Summary.db file.
     */
    @SuppressWarnings("resource")
    public boolean loadSummary()
    {
        File summariesFile = new File(descriptor.filenameFor(Component.SUMMARY));
        if (!summariesFile.exists())
            return false;

        DataInputStream iStream = null;
        try
        {
            iStream = new DataInputStream(Files.newInputStream(summariesFile.toPath()));
            indexSummary = IndexSummary.serializer.deserialize(
                    iStream, getPartitioner(),
                    metadata().params.minIndexInterval, metadata().params.maxIndexInterval);
            first = decorateKey(ByteBufferUtil.readWithLength(iStream));
            last = decorateKey(ByteBufferUtil.readWithLength(iStream));
        }
        catch (IOException e)
        {
            if (indexSummary != null)
                indexSummary.close();
            logger.trace("Cannot deserialize SSTable Summary File {}: {}", summariesFile.getPath(), e.getMessage());
            // corrupted; delete it and fall back to creating a new summary
            FileUtils.closeQuietly(iStream);
            // delete it and fall back to creating a new summary
            FileUtils.deleteWithConfirm(summariesFile);
            return false;
        }
        finally
        {
            FileUtils.closeQuietly(iStream);
        }

        return true;
    }

    protected void releaseIndex()
    {
        if (ifile != null)
        {
            ifile.close();
            ifile = null;
        }

        if (indexSummary != null)
        {
            indexSummary.close();
            indexSummary = null;
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
        BigTableReader replacement = internalOpen(descriptor,
                                                  components,
                                                  metadata,
                                                  ifile.sharedCopy(),
                                                  dataFile.sharedCopy(),
                                                  indexSummary.sharedCopy(),
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
    static BigTableReader internalOpen(Descriptor desc,
                                      Set<Component> components,
                                      TableMetadataRef metadata,
                                      FileHandle ifile,
                                      FileHandle dfile,
                                      IndexSummary indexSummary,
                                      IFilter bf,
                                      long maxDataAge,
                                      StatsMetadata sstableMetadata,
                                      OpenReason openReason,
                                      SerializationHeader header)
    {
        assert desc != null && ifile != null && dfile != null && indexSummary != null && bf != null && sstableMetadata != null;

        // Make sure the SSTableReader internalOpen part does the same.
        assert desc.getFormat() == BigFormat.instance;
        BigTableReader reader = BigFormat.readerFactory.open(desc, components, metadata, maxDataAge, sstableMetadata, openReason, header);

        reader.bf = bf;
        reader.ifile = ifile;
        reader.dataFile = dfile;
        reader.indexSummary = indexSummary;
        reader.setup(true);

        return reader;
    }

    @Override
    protected void setup(boolean trackHotness)
    {
        super.setup(trackHotness);
        tidy.addCloseable(ifile);
        tidy.addCloseable(indexSummary);
    }

    @Override
    public void setupOnline()
    {
        super.setupOnline();
        // under normal operation we can do this at any time, but SSTR is also used outside C* proper,
        // e.g. by BulkLoader, which does not initialize the cache.  As a kludge, we set up the cache
        // here when we know we're being wired into the rest of the server infrastructure.
        keyCache = CacheService.instance.keyCache;
        logger.trace("key cache contains {}/{} keys", keyCache.size(), keyCache.getCapacity());
    }

    public boolean isKeyCacheSetup()
    {
        return keyCache != null;
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        ifile.addTo(identities);
        indexSummary.addTo(identities);
    }

    public UnfilteredRowIterator iterator(DecoratedKey key,
                                          Slices slices,
                                          ColumnFilter selectedColumns,
                                          boolean reversed,
                                          SSTableReadsListener listener)
    {
        BigRowIndexEntry rie = getPosition(key, SSTableReader.Operator.EQ, listener, Rebufferer.ReaderConstraint.NONE);
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
             ? new SSTableReversedIterator(this, file, key, (BigRowIndexEntry) indexEntry, slices, selectedColumns, readerConstraint)
             : new SSTableIterator(this, file, key, (BigRowIndexEntry) indexEntry, slices, selectedColumns, readerConstraint);
    }

    /**
     * Gets the position in the index file to start scanning to find the given key (at most indexInterval keys away,
     * modulo downsampling of the index summary). Always returns a {@code value >= 0}
     */
    public long getIndexScanPosition(PartitionPosition key)
    {
        if (openReason == OpenReason.MOVED_START && key.compareTo(first) < 0)
            key = first;

        return getIndexScanPositionFromBinarySearchResult(indexSummary.binarySearch(key), indexSummary);
    }

    public static long getIndexScanPositionFromBinarySearchResult(int binarySearchResult, IndexSummary referencedIndexSummary)
    {
        if (binarySearchResult == -1)
            return 0;
        else
            return referencedIndexSummary.getPosition(getIndexSummaryIndexFromBinarySearchResult(binarySearchResult));
    }

    public static int getIndexSummaryIndexFromBinarySearchResult(int binarySearchResult)
    {
        if (binarySearchResult < 0)
        {
            // binary search gives us the first index _greater_ than the key searched for,
            // i.e., its insertion position
            int greaterThan = (binarySearchResult + 1) * -1;
            if (greaterThan == 0)
                return -1;
            return greaterThan - 1;
        }
        else
        {
            return binarySearchResult;
        }
    }

    public DecoratedKey keyAt(long indexPosition, Rebufferer.ReaderConstraint rc) throws IOException
    {
        DecoratedKey key;
        try (FileDataInput in = ifile.createReader(indexPosition, rc))
        {
            if (in.isEOF())
                return null;

            key = decorateKey(ByteBufferUtil.readWithShortLength(in));

            // hint read path about key location if caching is enabled
            // this saves index summary lookup and index file iteration which whould be pretty costly
            // especially in presence of promoted column indexes
            if (isKeyCacheSetup())
                cacheKey(key, rowIndexEntrySerializer.deserialize(in, in.getFilePointer()));
        }

        return key;
    }

    public KeyCacheKey getCacheKey(DecoratedKey key)
    {
        return new KeyCacheKey(metadata(), descriptor, key.getKey());
    }

    public void cacheKey(DecoratedKey key, BigRowIndexEntry info)
    {
        CachingParams caching = metadata().params.caching;

        if (!caching.cacheKeys() || keyCache == null || keyCache.getCapacity() == 0)
            return;

        KeyCacheKey cacheKey = new KeyCacheKey(metadata(), descriptor, key.getKey());
        logger.trace("Adding cache entry for {} -> {}", cacheKey, info);
        keyCache.put(cacheKey, info);
    }

    public BigRowIndexEntry getCachedPosition(DecoratedKey key, boolean updateStats)
    {
        return getCachedPosition(new KeyCacheKey(metadata(), descriptor, key.getKey()), updateStats);
    }

    public BigRowIndexEntry getCachedPosition(KeyCacheKey unifiedKey, boolean updateStats)
    {
        if (keyCacheEnabled())
        {
            if (updateStats)
            {
                BigRowIndexEntry cachedEntry = keyCache.get(unifiedKey);
                keyCacheRequest.incrementAndGet();
                if (cachedEntry != null)
                {
                    keyCacheHit.incrementAndGet();
                    bloomFilterTracker.addTruePositive();
                }
                return cachedEntry;
            }
            else
            {
                return keyCache.getInternal(unifiedKey);
            }
        }
        return null;
    }

    private boolean keyCacheEnabled()
    {
        return keyCache != null && keyCache.getCapacity() > 0 && metadata().params.caching.cacheKeys();
    }

    public InstrumentingCache<KeyCacheKey, BigRowIndexEntry> getKeyCache()
    {
        return keyCache;
    }

    /**
     * @return Number of key cache hit
     */
    public long getKeyCacheHit()
    {
        return keyCacheHit.get();
    }

    /**
     * @return Number of key cache request
     */
    public long getKeyCacheRequest()
    {
        return keyCacheRequest.get();
    }

    /**
     * Get position updating key cache and stats.
     * @see #getPosition(PartitionPosition, SSTableReader.Operator, Rebufferer.ReaderConstraint)
     */
    public BigRowIndexEntry getPosition(PartitionPosition key, Operator op, Rebufferer.ReaderConstraint rc)
    {
        return getPosition(key, op, true, false, SSTableReadsListener.NOOP_LISTENER, rc);
    }

    @Override
    public BigRowIndexEntry getPosition(PartitionPosition key, Operator op, SSTableReadsListener listener, Rebufferer.ReaderConstraint rc)
    {
        return getPosition(key, op, true, false, listener, rc);
    }

    /**
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     * allow key selection by token bounds but only if op != * EQ
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @param updateCacheAndStats true if updating stats and cache
     * @return The index entry corresponding to the key, or null if the key is not present
     */
    protected BigRowIndexEntry getPosition(PartitionPosition key,
                                           Operator op,
                                           boolean updateCacheAndStats,
                                           boolean permitMatchPastLast,
                                           SSTableReadsListener listener,
                                           Rebufferer.ReaderConstraint rc)
    {
        if (op == Operator.EQ)
        {
            assert key instanceof DecoratedKey; // EQ only make sense if the key is a valid row key
            if (!bf.isPresent((DecoratedKey)key))
            {
                listener.onSSTableSkipped(this, SkippingReason.BLOOM_FILTER);
                Tracing.trace("Bloom filter allows skipping sstable {}", descriptor.generation);
                return null;
            }
        }

        // next, the key cache (only make sense for valid row key)
        if ((op == Operator.EQ || op == Operator.GE) && (key instanceof DecoratedKey))
        {
            DecoratedKey decoratedKey = (DecoratedKey)key;
            KeyCacheKey cacheKey = new KeyCacheKey(metadata(), descriptor, decoratedKey.getKey());
            BigRowIndexEntry cachedPosition = getCachedPosition(cacheKey, updateCacheAndStats);
            if (cachedPosition != null)
            {
                listener.onSSTableSelected(this, cachedPosition, SelectionReason.KEY_CACHE_HIT);
                Tracing.trace("Key cache hit for sstable {}", descriptor.generation);
                return cachedPosition;
            }
        }

        // check the smallest and greatest keys in the sstable to see if it can't be present
        boolean skip = false;
        if (key.compareTo(first) < 0)
        {
            if (op == Operator.EQ)
                skip = true;
            else
                key = first;

            op = Operator.EQ;
        }
        else
        {
            int l = last.compareTo(key);
            // l <= 0  => we may be looking past the end of the file; we then narrow our behaviour to:
            //             1) skipping if strictly greater for GE and EQ;
            //             2) skipping if equal and searching GT, and we aren't permitting matching past last
            skip = l <= 0 && (l < 0 || (!permitMatchPastLast && op == Operator.GT));
        }
        if (skip)
        {
            if (op == Operator.EQ && updateCacheAndStats)
                bloomFilterTracker.addFalsePositive();
            listener.onSSTableSkipped(this, SkippingReason.MIN_MAX_KEYS);
            Tracing.trace("Check against min and max keys allows skipping sstable {}", descriptor.generation);
            return null;
        }

        int binarySearchResult = indexSummary.binarySearch(key);
        long sampledPosition = getIndexScanPositionFromBinarySearchResult(binarySearchResult, indexSummary);
        int sampledIndex = getIndexSummaryIndexFromBinarySearchResult(binarySearchResult);

        int effectiveInterval = indexSummary.getEffectiveIndexIntervalAfterIndex(sampledIndex);

        if (ifile == null)
            return null;

        // scan the on-disk index, starting at the nearest sampled position.
        // The check against IndexInterval is to be exit the loop in the EQ case when the key looked for is not present
        // (bloom filter false positive). But note that for non-EQ cases, we might need to check the first key of the
        // next index position because the searched key can be greater the last key of the index interval checked if it
        // is lesser than the first key of next interval (and in that case we must return the position of the first key
        // of the next interval).
        int i = 0;
        String path = null;
        try (FileDataInput in = ifile.createReader(sampledPosition, rc))
        {
            path = in.getPath();
            while (!in.isEOF())
            {
                i++;

                ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);

                boolean opSatisfied; // did we find an appropriate position for the op requested
                boolean exactMatch; // is the current position an exact match for the key, suitable for caching

                // Compare raw keys if possible for performance, otherwise compare decorated keys.
                if (op == Operator.EQ && i <= effectiveInterval)
                {
                    opSatisfied = exactMatch = indexKey.equals(((DecoratedKey) key).getKey());
                }
                else
                {
                    DecoratedKey indexDecoratedKey = decorateKey(indexKey);
                    int comparison = indexDecoratedKey.compareTo(key);
                    int v = op.apply(comparison);
                    opSatisfied = (v == 0);
                    exactMatch = (comparison == 0);
                    if (v < 0)
                    {
                        listener.onSSTableSkipped(this, SkippingReason.PARTITION_INDEX_LOOKUP);
                        Tracing.trace("Partition index lookup allows skipping sstable {}", descriptor.generation);
                        return null;
                    }
                }

                if (opSatisfied)
                {
                    // read data position from index entry
                    BigRowIndexEntry indexEntry = rowIndexEntrySerializer.deserialize(in, in.getFilePointer());
                    if (exactMatch && updateCacheAndStats)
                    {
                        assert key instanceof DecoratedKey; // key can be == to the index key only if it's a true row key
                        DecoratedKey decoratedKey = (DecoratedKey)key;

                        if (logger.isTraceEnabled())
                        {
                            // expensive sanity check!  see CASSANDRA-4687
                            try (FileDataInput fdi = dataFile.createReader(indexEntry.position, rc))
                            {
                                DecoratedKey keyInDisk = decorateKey(ByteBufferUtil.readWithShortLength(fdi));
                                if (!keyInDisk.equals(key))
                                    throw new AssertionError(String.format("%s != %s in %s", keyInDisk, key, fdi.getPath()));
                            }
                        }

                        // store exact match for the key
                        cacheKey(decoratedKey, indexEntry);
                    }
                    if (op == Operator.EQ && updateCacheAndStats)
                        bloomFilterTracker.addTruePositive();
                    listener.onSSTableSelected(this, indexEntry, SelectionReason.INDEX_ENTRY_FOUND);
                    Tracing.trace("Partition index with {} entries found for sstable {}", indexEntry.rowIndexCount(), descriptor.generation);
                    return indexEntry;
                }

                BigRowIndexEntry.Serializer.skip(in, descriptor.version);
            }
        }
        catch (IOException e)
        {
            markSuspect();
            throw new CorruptSSTableException(e, path);
        }

        if (op == SSTableReader.Operator.EQ && updateCacheAndStats)
            bloomFilterTracker.addFalsePositive();
        listener.onSSTableSkipped(this, SkippingReason.INDEX_ENTRY_NOT_FOUND);
        Tracing.trace("Partition index lookup complete (bloom filter false positive) for sstable {}", descriptor.generation);
        return null;
    }

    @Override
    public long estimatedKeys()
    {
        return indexSummary.getEstimatedKeyCount();
    }

    @Override
    public RowIndexEntry getExactPosition(DecoratedKey key, Rebufferer.ReaderConstraint rc)
    {
        return getPosition(key, Operator.EQ, rc);
    }

    @Override
    public boolean contains(DecoratedKey key, Rebufferer.ReaderConstraint rc)
    {
        return getExactPosition(key, rc) != null;
    }

    @Override
    public PartitionIterator coveredKeysIterator(PartitionPosition left, boolean inclusiveLeft, PartitionPosition right, boolean inclusiveRight) throws IOException
    {
        return new PartitionIterator(this, left, inclusiveLeft ? -1 : 0, right, inclusiveRight ? 0 : -1);
    }

    @Override
    public PartitionIndexIterator allKeysIterator() throws IOException
    {
        return new PartitionIterator(this);
    }

    public ScrubPartitionIterator scrubPartitionsIterator() throws IOException
    {
        if (ifile == null)
            return null;
        return new ScrubIterator(ifile, rowIndexEntrySerializer);
    }

    @Override
    public Flow<IndexFileEntry> coveredKeysFlow(RandomAccessReader dfile,
                                                PartitionPosition left,
                                                boolean inclusiveLeft,
                                                PartitionPosition right,
                                                boolean inclusiveRight)
    {
        return new BigIndexFileFlow(this, left, inclusiveLeft ? -1 : 0, right, inclusiveRight ? 0 : -1);
    }

    protected FileHandle[] getFilesToBeLocked()
    {
        return new FileHandle[] { dataFile, ifile };
    }
}
