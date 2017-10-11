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

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.MemoryOnlyStrategy;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.*;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.Transactional;

@VisibleForTesting
public class TrieIndexSSTableWriter extends SSTableWriter
{
    private static final Logger logger = LoggerFactory.getLogger(TrieIndexSSTableWriter.class);

    private final PartitionWriter partitionWriter;
    private final IndexWriter iwriter;
    private final FileHandle.Builder dbuilder;
    protected final SequentialWriter dataFile;
    private DecoratedKey lastWrittenKey;
    private DataPosition dataMark;
    private long lastEarlyOpenLength = 0;
    private final Optional<ChunkCache> chunkCache = Optional.ofNullable(ChunkCache.instance);

    private static final SequentialWriterOption WRITER_OPTION = SequentialWriterOption.newBuilder()
                                                                .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                                .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024)
                                                                .bufferType(BufferType.OFF_HEAP)
                                                                .build();

    public TrieIndexSSTableWriter(Descriptor descriptor,
                          long keyCount,
                          long repairedAt,
                          UUID pendingRepair,
                          TableMetadataRef metadata,
                          MetadataCollector metadataCollector, 
                          SerializationHeader header,
                          Collection<SSTableFlushObserver> observers,
                          LifecycleTransaction txn)
    {
        super(descriptor, keyCount, repairedAt, pendingRepair, metadata, metadataCollector, header, observers);
        txn.trackNew(this); // must track before any files are created

        if (compression)
        {
            dataFile = new CompressedSequentialWriter(new File(getFilename()),
                                             descriptor.filenameFor(Component.COMPRESSION_INFO),
                                             new File(descriptor.filenameFor(Component.DIGEST)),
                                             WRITER_OPTION,
                                             metadata().params.compression,
                                             metadataCollector);
        }
        else
        {
            dataFile = new ChecksummedSequentialWriter(new File(getFilename()),
                    new File(descriptor.filenameFor(Component.CRC)),
                    new File(descriptor.filenameFor(Component.DIGEST)),
                    WRITER_OPTION);
        }
        dbuilder = new FileHandle.Builder(descriptor.filenameFor(Component.DATA)).compressed(compression)
                                                                                 .mmapped(DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.mmap &&
                                                                                          metadata().params.compaction.klass().equals(MemoryOnlyStrategy.class));
        chunkCache.ifPresent(dbuilder::withChunkCache);
        iwriter = new IndexWriter(keyCount);

        partitionWriter = new PartitionWriter(this.header, metadata().comparator, dataFile, iwriter.rowIndexFile, descriptor.version, this.observers);
    }

    public void mark()
    {
        dataMark = dataFile.mark();
        iwriter.mark();
    }

    public void resetAndTruncate()
    {
        dataFile.resetAndTruncate(dataMark);
        iwriter.resetAndTruncate();
    }

    /**
     * Perform sanity checks on @param decoratedKey and @return the position in the data file before any data is written
     */
    protected long beforeAppend(DecoratedKey decoratedKey)
    {
        assert decoratedKey != null : "Keys must not be null"; // empty keys ARE allowed b/c of indexed row values
        if (lastWrittenKey != null && lastWrittenKey.compareTo(decoratedKey) >= 0)
            throw new RuntimeException("Last written key " + lastWrittenKey + " >= current key " + decoratedKey + " writing into " + getFilename());
        return (lastWrittenKey == null) ? 0 : dataFile.position();
    }

    private long afterAppend(DecoratedKey decoratedKey, RowIndexEntry index) throws IOException
    {
        metadataCollector.addKey(decoratedKey.getKey());
        lastWrittenKey = decoratedKey;
        last = lastWrittenKey;
        if (first == null)
            first = lastWrittenKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote {} at {}", decoratedKey, index.position);
        return iwriter.append(decoratedKey, index);
    }

    /**
     * Appends partition data to this writer.
     *
     * @param iterator the partition to write
     * @return the created index entry if something was written, that is if {@code iterator}
     * wasn't empty, {@code null} otherwise.
     *
     * @throws FSWriteError if a write to the dataFile fails
     */
    public RowIndexEntry append(UnfilteredRowIterator iterator)
    {
        DecoratedKey key = iterator.partitionKey();

        if (key.getKey().remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            logger.error("Key size {} exceeds maximum of {}, skipping row", key.getKey().remaining(), FBUtilities.MAX_UNSIGNED_SHORT);
            return null;
        }

        if (iterator.isEmpty())
            return null;

        long startPosition = beforeAppend(key);
        observers.forEach((o) -> {
            o.startPartition(key, startPosition);
        });

        //Reuse the writer for each row
        partitionWriter.reset();

        try (UnfilteredRowIterator collecting = Transformation.apply(iterator, new StatsCollector(metadataCollector)))
        {
            long trieRoot = partitionWriter.writePartition(collecting);

            RowIndexEntry entry = TrieIndexEntry.create(startPosition, trieRoot,
                                                        collecting.partitionLevelDeletion(),
                                                        partitionWriter.rowIndexCount);

            long endPosition = dataFile.position();
            long rowSize = endPosition - startPosition;
            maybeLogLargePartitionWarning(key, rowSize);
            metadataCollector.addPartitionSizeInBytes(rowSize);
            afterAppend(key, entry);
            return entry;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }
    }

    private void maybeLogLargePartitionWarning(DecoratedKey key, long rowSize)
    {
        if (rowSize > DatabaseDescriptor.getCompactionLargePartitionWarningThreshold())
        {
            String keyString = metadata().partitionKeyType.getString(key.getKey());
            logger.warn("Writing large partition {}/{}:{} ({}) to sstable {}", metadata.keyspace, metadata.name, keyString, FBUtilities.prettyPrintMemory(rowSize), getFilename());
        }
    }

    private static class StatsCollector extends Transformation
    {
        private final MetadataCollector collector;

        StatsCollector(MetadataCollector collector)
        {
            this.collector = collector;
        }

        @Override
        public Row applyToStatic(Row row)
        {
            if (!row.isEmpty())
                Rows.collectStats(row, collector);
            return row;
        }

        @Override
        public Row applyToRow(Row row)
        {
            collector.updateClusteringValues(row.clustering());
            Rows.collectStats(row, collector);
            return row;
        }

        @Override
        public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            collector.updateClusteringValues(marker.clustering());
            if (marker.isBoundary())
            {
                RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker)marker;
                collector.update(bm.endDeletionTime());
                collector.update(bm.startDeletionTime());
            }
            else
            {
                collector.update(((RangeTombstoneBoundMarker)marker).deletionTime());
            }
            return marker;
        }

        @Override
        public void onPartitionClose()
        {
            collector.addCellPerPartitionCount();
        }

        @Override
        public DeletionTime applyToDeletion(DeletionTime deletionTime)
        {
            collector.update(deletionTime);
            return deletionTime;
        }
    }

    @SuppressWarnings("resource")
    public boolean openEarly(Consumer<SSTableReader> callWhenReady)
    {
        long dataLength = dataFile.position();
        return iwriter.buildPartial(dataLength, partitionIndex ->
        {
            StatsMetadata stats = statsMetadata();
            FileHandle ifile = iwriter.rowIndexFHBuilder.complete();        // not necessary to limit size
            if (compression)
                dbuilder.withCompressionMetadata(((CompressedSequentialWriter) dataFile).open(dataLength));
            int dataBufferSize = optimizationStrategy.bufferSize(stats.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
            FileHandle dfile = dbuilder.bufferSize(dataBufferSize).complete(dataLength);
            invalidateCacheAtBoundary(dfile);
            SSTableReader sstable = TrieIndexSSTableReader.internalOpen(descriptor,
                                                               components, metadata,
                                                               ifile, dfile, partitionIndex, iwriter.bf.sharedCopy(),
                                                               maxDataAge, stats, SSTableReader.OpenReason.EARLY, header);

            sstable.first = getMinimalKey(partitionIndex.firstKey());
            sstable.last = getMinimalKey(partitionIndex.lastKey());
            callWhenReady.accept(sstable);
        });
    }

    void invalidateCacheAtBoundary(FileHandle dfile)
    {
        chunkCache.ifPresent(cache -> {
            if (lastEarlyOpenLength != 0 && dfile.dataLength() > lastEarlyOpenLength)
                cache.invalidatePosition(dfile, lastEarlyOpenLength);
        });
        lastEarlyOpenLength = dfile.dataLength();
    }

    public SSTableReader openFinalEarly()
    {
        // we must ensure the data is completely flushed to disk
        dataFile.sync();
        iwriter.rowIndexFile.sync();

        return openFinal(SSTableReader.OpenReason.EARLY);
    }

    @SuppressWarnings("resource")
    private SSTableReader openFinal(SSTableReader.OpenReason openReason)
    {
        if (maxDataAge < 0)
            maxDataAge = System.currentTimeMillis();

        StatsMetadata stats = statsMetadata();
        // finalize in-memory state for the reader
        PartitionIndex partitionIndex = iwriter.completedPartitionIndex();
        FileHandle rowIndexFile = iwriter.rowIndexFHBuilder.complete();
        int dataBufferSize = optimizationStrategy.bufferSize(stats.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
        if (compression)
            dbuilder.withCompressionMetadata(((CompressedSequentialWriter) dataFile).open(0));
        FileHandle dfile = dbuilder.bufferSize(dataBufferSize).complete();
        invalidateCacheAtBoundary(dfile);
        SSTableReader sstable = TrieIndexSSTableReader.internalOpen(descriptor,
                                                            components,
                                                            this.metadata,
                                                            rowIndexFile,
                                                            dfile,
                                                            partitionIndex,
                                                            iwriter.bf.sharedCopy(),
                                                            maxDataAge,
                                                            stats,
                                                            openReason,
                                                            header);
        sstable.first = getMinimalKey(first);
        sstable.last = getMinimalKey(last);
        return sstable;
    }

    protected SSTableWriter.TransactionalProxy txnProxy()
    {
        return new TransactionalProxy();
    }

    class TransactionalProxy extends SSTableWriter.TransactionalProxy
    {
        // finalise our state on disk, including renaming
        protected void doPrepare()
        {
            iwriter.prepareToCommit();

            // write sstable statistics
            dataFile.prepareToCommit();
            writeMetadata(descriptor, finalizeMetadata());

            // save the table of components
            SSTable.appendTOC(descriptor, components);

            if (openResult)
                finalReader = openFinal(SSTableReader.OpenReason.NORMAL);
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            accumulate = dataFile.commit(accumulate);
            accumulate = iwriter.commit(accumulate);
            return accumulate;
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            accumulate = dbuilder.close(accumulate);
            return accumulate;
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            accumulate = iwriter.abort(accumulate);
            accumulate = dataFile.abort(accumulate);
            return accumulate;
        }
    }

    private void writeMetadata(Descriptor desc, Map<MetadataType, MetadataComponent> components)
    {
        File file = new File(desc.filenameFor(Component.STATS));
        try (SequentialWriter out = new SequentialWriter(file, WRITER_OPTION))
        {
            desc.getMetadataSerializer().serialize(components, out, desc.version);
            out.finish();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file.getPath());
        }
    }

    public long getFilePointer()
    {
        return dataFile.position();
    }

    public long getOnDiskFilePointer()
    {
        return dataFile.getOnDiskFilePointer();
    }

    public long getEstimatedOnDiskBytesWritten()
    {
        return dataFile.getEstimatedOnDiskBytesWritten();
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    class IndexWriter extends AbstractTransactional implements Transactional
    {
        private final SequentialWriter rowIndexFile;
        public final FileHandle.Builder rowIndexFHBuilder;
        private final SequentialWriter partitionIndexFile;
        public final FileHandle.Builder partitionIndexFHBuilder;
        public final PartitionIndexBuilder partitionIndex;
        public final IFilter bf;
        boolean partitionIndexCompleted = false;
        private DataPosition riMark, piMark;

        IndexWriter(long keyCount)
        {
            rowIndexFile = new SequentialWriter(new File(descriptor.filenameFor(Component.ROW_INDEX)), WRITER_OPTION);
            rowIndexFHBuilder = SSTableReader.indexFileHandleBuilder(descriptor, metadata(), Component.ROW_INDEX);
            partitionIndexFile = new SequentialWriter(new File(descriptor.filenameFor(Component.PARTITION_INDEX)), WRITER_OPTION);
            partitionIndexFHBuilder = SSTableReader.indexFileHandleBuilder(descriptor, metadata(), Component.PARTITION_INDEX);
            partitionIndex = new PartitionIndexBuilder(partitionIndexFile, partitionIndexFHBuilder);
            bf = FilterFactory.getFilter(keyCount, metadata().params.bloomFilterFpChance, true);
            // register listeners to be alerted when the data files are flushed
            partitionIndexFile.setPostFlushListener(() -> partitionIndex.markPartitionIndexSynced(partitionIndexFile.getLastFlushOffset()));
            rowIndexFile.setPostFlushListener(() -> partitionIndex.markRowIndexSynced(rowIndexFile.getLastFlushOffset()));
            dataFile.setPostFlushListener(() -> partitionIndex.markDataSynced(dataFile.getLastFlushOffset()));
        }

        public long append(DecoratedKey key, RowIndexEntry indexEntry) throws IOException
        {
            bf.add(key);
            long position;
            if (indexEntry.isIndexed())
            {
                long indexStart = rowIndexFile.position();
                try
                {
                    ByteBufferUtil.writeWithShortLength(key.getKey(), rowIndexFile);
                    indexEntry.serialize(rowIndexFile, rowIndexFile.position());
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e, rowIndexFile.getPath());
                }

                if (logger.isTraceEnabled())
                    logger.trace("wrote index entry: {} at {}", indexEntry, indexStart);
                position = indexStart;
            }
            else
            {
                // Write data position directly in trie.
                position = ~indexEntry.position;
            }
            partitionIndex.addEntry(key, position);
            return position;
        }

        public boolean buildPartial(long dataPosition, Consumer<PartitionIndex> callWhenReady)
        {
            return partitionIndex.buildPartial(callWhenReady, rowIndexFile.position(), dataPosition);
        }

        /**
         * Closes the index and bloomfilter, making the public state of this writer valid for consumption.
         */
        void flushBf()
        {
            if (components.contains(Component.FILTER))
            {
                String path = descriptor.filenameFor(Component.FILTER);
                try (SeekableByteChannel fos = Files.newByteChannel(Paths.get(path), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
                     DataOutputStreamPlus stream = new BufferedDataOutputStreamPlus(fos))
                {
                    // bloom filter
                    FilterFactory.serialize(bf, stream);
                    stream.flush();
                    SyncUtil.sync((FileChannel) fos);
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e, path);
                }
            }
        }

        public void mark()
        {
            riMark = rowIndexFile.mark();
            piMark = partitionIndexFile.mark();
        }

        public void resetAndTruncate()
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in afterappend so
            // we assume that if that worked then we won't be trying to reset.
            rowIndexFile.resetAndTruncate(riMark);
            partitionIndexFile.resetAndTruncate(piMark);
        }

        protected void doPrepare()
        {
            flushBf();

            // truncate index file
            long position = rowIndexFile.position();
            rowIndexFile.prepareToCommit();
            FileUtils.truncate(rowIndexFile.getPath(), position);

            complete();
        }

        void complete() throws FSWriteError
        {
            if (partitionIndexCompleted)
                return;

            try
            {
                partitionIndex.complete();
                partitionIndexCompleted = true;
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, partitionIndexFile.getPath());
            }
        }

        PartitionIndex completedPartitionIndex()
        {
            complete();
            try
            {
                return PartitionIndex.load(partitionIndexFHBuilder, getPartitioner(), false, Rebufferer.ReaderConstraint.NONE);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, partitionIndexFile.getPath());
            }
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            return rowIndexFile.commit(accumulate);
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            return rowIndexFile.abort(accumulate);
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            return Throwables.close(accumulate, ImmutableList.of(bf, partitionIndex, rowIndexFile, rowIndexFHBuilder, partitionIndexFile, partitionIndexFHBuilder));
        }
    }
}
