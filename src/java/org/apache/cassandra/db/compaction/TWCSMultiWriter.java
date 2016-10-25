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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.SimpleSSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

/**
 * A special sstable multi writer for time window compaction
 *
 * It inspects all parts of all partitions during flush to make sure that an sstable is never in more than one
 * time window.
 *
 * It splits by creating new UnfilteredRowIterators that only contain data from a single window and then writes those
 * partitions in sequence to disk.
 *
 * The partition is at most split in MAX_BUCKETS (10) parts, these buckets are based on the current time to get a limit to
 * how many sstables we write during flush. Too many will take a very long time to write. If the TWCS table is configured
 * to have single-day windows, we will flush into separate sstables that go back 10 days. We also keep 2 extra buckets,
 * one for all data older that 10 days, and one for data written with timestamps in the future.
 *
 */
public class TWCSMultiWriter implements SSTableMultiWriter
{
    private static final int MAX_BUCKETS = 10;
    private static final int ALL_BUCKETS = MAX_BUCKETS + 2;
    private static final int OLD_BUCKET_INDEX = MAX_BUCKETS;
    private static final int FUTURE_BUCKET_INDEX = MAX_BUCKETS + 1;
    private final ColumnFamilyStore cfs;
    private final TimeUnit windowTimeUnit;
    private final int windowTimeSize;
    private final TimeUnit timestampResolution;
    private final Descriptor descriptor;
    private final long keyCount;
    private final long repairedAt;
    private final MetadataCollector meta;
    private final SerializationHeader header;
    private final Collection<Index> indexes;
    private final LifecycleTransaction txn;
    private final BucketIndexer bucketIndex;
    private final SSTableMultiWriter [] writers = new SSTableMultiWriter[ALL_BUCKETS];

    public TWCSMultiWriter(ColumnFamilyStore cfs,
                           TimeUnit windowTimeUnit,
                           int windowTimeSize,
                           TimeUnit timestampResolution,
                           Descriptor descriptor,
                           long keyCount,
                           long repairedAt,
                           MetadataCollector meta,
                           SerializationHeader header,
                           Collection<Index> indexes,
                           LifecycleTransaction txn)
    {
        this.cfs = cfs;
        this.windowTimeUnit = windowTimeUnit;
        this.windowTimeSize = windowTimeSize;
        this.timestampResolution = timestampResolution;
        this.descriptor = descriptor;
        this.keyCount = keyCount;
        this.repairedAt = repairedAt;
        this.meta = meta;
        this.header = header;
        this.indexes = indexes;
        this.txn = txn;
        bucketIndex = createBucketIndexes(windowTimeUnit, windowTimeSize);
    }

    public static BucketIndexer createBucketIndexes(TimeUnit windowTimeUnit, int windowTimeSize)
    {
        long tmpMaxBucket = 0;
        long tmpMinBucket = Long.MAX_VALUE;
        long now = System.currentTimeMillis();
        long [] buckets = new long[MAX_BUCKETS];
        for (int i = 0; i < buckets.length; i++)
        {
            long bucket = TimeWindowCompactionStrategy.getWindowBoundsInMillis(windowTimeUnit, windowTimeSize, now - TimeUnit.MILLISECONDS.convert(windowTimeSize * i, windowTimeUnit)).left;
            // add the most recent bucket last in the array (i = 0 => most recent bucket)
            buckets[buckets.length - i - 1] = bucket;
            tmpMaxBucket = Math.max(tmpMaxBucket, bucket);
            tmpMinBucket = Math.min(tmpMinBucket, bucket);
        }
        return new BucketIndexer(buckets, tmpMinBucket, tmpMaxBucket);
    }

    public boolean append(UnfilteredRowIterator partition)
    {
        UnfilteredRowIterator [] iterators = splitPartitionOnTime(partition, bucketIndex, new TWCSConfig(windowTimeUnit, windowTimeSize, timestampResolution));
        boolean ret = false;
        try
        {
            for (int i = 0; i < iterators.length; i++)
            {
                UnfilteredRowIterator iterator = iterators[i];
                if (iterator == null)
                    continue;
                SSTableMultiWriter writer = writers[i];
                if (writer == null)
                {
                    Descriptor newDesc = cfs.newSSTableDescriptor(descriptor.directory);
                    long minTimestamp = header.stats().minTimestamp;
                    // if have data in the old bucket, this means that the min timestamp of the memtable is correct for this bucket.
                    if (i != OLD_BUCKET_INDEX)
                        minTimestamp = bucketIndex.getTimestampForIndex(i);

                    writer = SimpleSSTableMultiWriter.create(newDesc,
                                                             keyCount,
                                                             repairedAt,
                                                             cfs.metadata,
                                                             meta.copy(),
                                                             new SerializationHeader(header.isForSSTable(),
                                                                                     cfs.metadata,
                                                                                     header.columns(),
                                                                                     new EncodingStats(minTimestamp, header.stats().minLocalDeletionTime, header.stats().minTTL)),
                                                             indexes,
                                                             txn);
                    writers[i] = writer;
                }
                ret |= writer.append(iterator);
            }
        }
        finally
        {
            for (int i = 0; i < iterators.length; i++)
            {
                if (iterators[i] != null)
                    iterators[i].close();
                iterators[i] = null;
            }
        }
        return ret;
    }


    public Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult)
    {
        List<SSTableReader> sstables = new ArrayList<>(writers.length);
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                sstables.addAll(writer.finish(repairedAt, maxDataAge, openResult));
        return sstables;
    }

    public Collection<SSTableReader> finish(boolean openResult)
    {
        List<SSTableReader> sstables = new ArrayList<>(writers.length);
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                sstables.addAll(writer.finish(openResult));
        return sstables;
    }

    public Collection<SSTableReader> finished()
    {
        List<SSTableReader> sstables = new ArrayList<>(writers.length);
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                sstables.addAll(writer.finished());
        return sstables;
    }

    public SSTableMultiWriter setOpenResult(boolean openResult)
    {
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                writer.setOpenResult(openResult);
        return this;
    }

    public String getFilename()
    {
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                return writer.getFilename();
        return "";
    }

    public long getFilePointer()
    {
        long filepointerSum = 0;
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                filepointerSum += writer.getFilePointer();
        return filepointerSum;
    }

    public UUID getCfId()
    {
        return cfs.metadata.cfId;
    }

    public Throwable commit(Throwable accumulate)
    {
        Throwable t = accumulate;
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                t = writer.commit(t);
        return t;
    }

    public Throwable abort(Throwable accumulate)
    {
        Throwable t = accumulate;
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                t = writer.abort(t);
        return t;
    }

    public void prepareToCommit()
    {
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                writer.prepareToCommit();
    }

    public void close()
    {
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                writer.close();
    }

    /**
     * Split a given UnfilteredRowIterator based on the timestamps of the various components of the partition.
     *
     * It inspects:
     *  - Static rows
     *  - Actual rows
     *  -- Row deletion
     *  -- Complex deletions
     *  - Range tombstones
     *  -- if the range tombstone open marker is in a separate bucket from the close marker, new
     *     markers are created and they are put in separate buckets
     *  - Partition level deletion
     * @param partition
     * @param bucketIndex
     * @return
     */
    @VisibleForTesting
    public static UnfilteredRowIterator [] splitPartitionOnTime(UnfilteredRowIterator partition, BucketIndexer bucketIndex, TWCSConfig config)
    {
        List<Unfiltered> [] rows = new List[ALL_BUCKETS];
        Unfiltered [] staticRows = splitRow(partition.staticRow(), bucketIndex, config);

        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();
            if (unfiltered.kind() == Unfiltered.Kind.ROW)
            {
                Row r = (Row) unfiltered;
                Unfiltered [] splitRows = splitRow(r, bucketIndex, config);
                for (int i = 0; i < splitRows.length; i++)
                {
                    if (splitRows[i] != null)
                    {
                        if (rows[i] == null)
                            rows[i] = new ArrayList<>();
                        rows[i].add(splitRows[i]);
                    }
                }
            }
            else
            {
                RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
                boolean isReversed = partition.isReverseOrder();
                long opentwcsBucket = TimeWindowCompactionStrategy.getWindowBoundsInMillis(config.windowTimeUnit,
                                                                                           config.windowTimeSize,
                                                                                           toMillis(marker.openDeletionTime(isReversed).markedForDeleteAt(), config.timestampResolution)).left;
                long closetwcsBucket = TimeWindowCompactionStrategy.getWindowBoundsInMillis(config.windowTimeUnit,
                                                                                            config.windowTimeSize,
                                                                                            toMillis(marker.closeDeletionTime(isReversed).markedForDeleteAt(), config.timestampResolution)).left;
                if (opentwcsBucket == closetwcsBucket)
                {
                    // open and close marker are in the same bucket, just add them to the correct row
                    int idx = bucketIndex.get(opentwcsBucket);
                    if (rows[idx] == null)
                        rows[idx] = new ArrayList<>();
                    rows[idx].add(marker);
                }
                else
                {
                    // open and close are in different buckets, need to create new markers and add the to the different rows
                    int idx = bucketIndex.get(opentwcsBucket);
                    if (rows[idx] == null)
                        rows[idx] = new ArrayList<>();
                    rows[idx].add(new RangeTombstoneBoundMarker(marker.openBound(isReversed), marker.openDeletionTime(isReversed)));
                    idx = bucketIndex.get(closetwcsBucket);
                    if (rows[idx] == null)
                        rows[idx] = new ArrayList<>();
                    rows[idx].add(new RangeTombstoneBoundMarker(marker.closeBound(isReversed), marker.closeDeletionTime(isReversed)));
                }
            }
        }

        UnfilteredRowIterator [] ret = new UnfilteredRowIterator[ALL_BUCKETS];
        // used to keep track of which bucket contains a partition level deletion
        // then when we iterate all the row buckets below, we add it to the correct bucket
        int partitionDeletionBucket = -1;
        if (!partition.partitionLevelDeletion().isLive())
        {
            partitionDeletionBucket = bucketIndex.get(TimeWindowCompactionStrategy.getWindowBoundsInMillis(config.windowTimeUnit,
                                                                                                           config.windowTimeSize,
                                                                                                           toMillis(partition.partitionLevelDeletion().markedForDeleteAt(), config.timestampResolution)).left);
        }

        for (int i = 0; i < rows.length; i++)
        {
            // if we have a static row for this bucket, write it
            Row staticRow = Rows.EMPTY_STATIC_ROW;
            if (staticRows[i] != null)
                staticRow = (Row) staticRows[i];

            DeletionTime partitionLevelDeletion = DeletionTime.LIVE;
            if (partitionDeletionBucket == i)
            {
                partitionLevelDeletion = partition.partitionLevelDeletion();
            }
            List<Unfiltered> row = rows[i];
            if (row == null)
            {
                if (staticRow != Rows.EMPTY_STATIC_ROW || partitionLevelDeletion != DeletionTime.LIVE)
                    ret[i] = new UnfilteredRowIteratorBucket(Collections.emptyList(), partition.metadata(), partition.partitionKey(), partitionLevelDeletion, partition.columns(), staticRow, partition.isReverseOrder(), partition.stats());
            }
            else
            {
                ret[i] = new UnfilteredRowIteratorBucket(row, partition.metadata(), partition.partitionKey(), partitionLevelDeletion, partition.columns(), staticRow, partition.isReverseOrder(), partition.stats());
            }
        }
        return ret;
    }

    private static Unfiltered [] splitRow(Row r, BucketIndexer bucketIndex, TWCSConfig config)
    {
        Row.Builder [] rowBuilders = new Row.Builder[ALL_BUCKETS];
        boolean hasBuilder = false;
        long [] maxTimestampPerRow = new long[ALL_BUCKETS];
        if (!r.deletion().isLive())
        {
            long ts = r.deletion().time().markedForDeleteAt();
            Row.Builder builder = getBuilder(ts, rowBuilders, bucketIndex, r.clustering(), maxTimestampPerRow, config);
            builder.addRowDeletion(r.deletion());
            hasBuilder = true;
        }

        for (Cell c : r.cells())
        {
            if (r.hasComplexDeletion() && !r.getComplexColumnData(c.column()).complexDeletion().isLive())
            {
                DeletionTime dt = r.getComplexColumnData(c.column()).complexDeletion();
                Row.Builder builder = getBuilder(dt.markedForDeleteAt(), rowBuilders, bucketIndex, r.clustering(), maxTimestampPerRow, config);
                builder.addComplexDeletion(c.column(), dt);
            }

            Row.Builder builder = getBuilder(c.timestamp(), rowBuilders, bucketIndex, r.clustering(), maxTimestampPerRow, config);
            builder.addCell(c);
            hasBuilder = true;
        }

        if (!hasBuilder && !r.primaryKeyLivenessInfo().isEmpty())
            getBuilder(r.primaryKeyLivenessInfo().timestamp(), rowBuilders, bucketIndex, r.clustering(), maxTimestampPerRow, config);

        Unfiltered [] splitRows = new Unfiltered[rowBuilders.length];
        for (int i = 0; i < rowBuilders.length; i++)
        {
            Row.Builder rowBuilder = rowBuilders[i];
            if (rowBuilder != null)
            {
                if (!r.primaryKeyLivenessInfo().isEmpty())
                {
                    LivenessInfo li = r.primaryKeyLivenessInfo().withUpdatedTimestamp(maxTimestampPerRow[i]);
                    rowBuilder.addPrimaryKeyLivenessInfo(li);
                }
                splitRows[i] = rowBuilder.build();
            }
        }
        return splitRows;
    }

    private static long toMillis(long timestamp, TimeUnit timestampResolution)
    {
        return TimeUnit.MILLISECONDS.convert(timestamp, timestampResolution);
    }

    /**
     * get the row builder for the given TWCS bucket
     *
     * note that it mutates the arrays passed in so that we can reuse the builders
     */
    private static Row.Builder getBuilder(long timestamp, Row.Builder [] rowBuilders, BucketIndexer indexes, Clustering c, long [] maxTimestampPerRow, TWCSConfig config)
    {
        long twcsBucket = TimeWindowCompactionStrategy.getWindowBoundsInMillis(config.windowTimeUnit,config.windowTimeSize, toMillis(timestamp, config.timestampResolution)).left;
        int idx = indexes.get(twcsBucket);
        maxTimestampPerRow[idx] = Math.max(timestamp, maxTimestampPerRow[idx]);
        Row.Builder builder = rowBuilders[idx];
        if (builder == null)
        {
            builder = BTreeRow.sortedBuilder();
            rowBuilders[idx] = builder;
            builder.newRow(c);
        }
        return builder;
    }

    private static class UnfilteredRowIteratorBucket extends AbstractUnfilteredRowIterator
    {
        private final Iterator<Unfiltered> unfiltereds;

        UnfilteredRowIteratorBucket(List<Unfiltered> uris, CFMetaData metadata, DecoratedKey partitionKey, DeletionTime partitionLevelDeletion, PartitionColumns columns, Row staticRow, boolean isReverseOrder, EncodingStats stats)
        {
            super(metadata, partitionKey, partitionLevelDeletion, columns, staticRow, isReverseOrder, stats);
            this.unfiltereds = uris.iterator();
        }

        protected Unfiltered computeNext()
        {
            if (!unfiltereds.hasNext())
                return endOfData();
            return unfiltereds.next();
        }
    }

    public static class BucketIndexer
    {
        @VisibleForTesting
        public final long [] buckets;
        private final long minBucket;
        private final long maxBucket;

        public BucketIndexer(long [] buckets, long minBucket, long maxBucket)
        {
            this.buckets = buckets;
            this.minBucket = minBucket;
            this.maxBucket = maxBucket;
        }

        public int get(long twcsBucket)
        {
            if (twcsBucket < minBucket)
                return OLD_BUCKET_INDEX;
            if (twcsBucket > maxBucket)
                return FUTURE_BUCKET_INDEX;
            // I benchmarked having a hashmap here instead of the bin search and the performance seems to be a tiny bit
            // better with the binary searching (due to autoboxing I guess)
            int index = Arrays.binarySearch(buckets, twcsBucket);
            assert index >= 0;
            return index;
        }

        long getTimestampForIndex(int bucket)
        {
            if (bucket == OLD_BUCKET_INDEX)
                return Long.MIN_VALUE; // caller needs to make sure we dont use this directly
            else if (bucket == FUTURE_BUCKET_INDEX)
                return maxBucket;
            else
                return buckets[bucket];
        }
    }

    @VisibleForTesting
    public static class TWCSConfig
    {
        private final TimeUnit windowTimeUnit;
        private final int windowTimeSize;
        private final TimeUnit timestampResolution;

        public TWCSConfig(TimeUnit windowTimeUnit, int windowTimeSize, TimeUnit timestampResolution)
        {
            this.windowTimeUnit = windowTimeUnit;
            this.windowTimeSize = windowTimeSize;
            this.timestampResolution = timestampResolution;
        }
    }
}
