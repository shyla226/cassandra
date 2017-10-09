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
package org.apache.cassandra.io.sstable.format;

import java.io.*;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.MemoryOnlyStrategy;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.mos.MemoryLockedBuffer;
import org.apache.cassandra.db.mos.MemoryOnlyStatus;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.metadata.*;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.io.util.FileHandle.Builder;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.SelfRefCounted;
import org.apache.cassandra.utils.flow.Flow;

import static org.apache.cassandra.db.Directories.SECONDARY_INDEX_NAME_SEPARATOR;

/**
 * An SSTableReader can be constructed in a number of places, but typically is either
 * read from disk at startup, or constructed from a flushed memtable, or after compaction
 * to replace some existing sstables. However once created, an sstablereader may also be modified.
 *
 * A reader's OpenReason describes its current stage in its lifecycle, as follows:
 *
 *
 * <pre> {@code
 * NORMAL
 * From:       None        => Reader has been read from disk, either at startup or from a flushed memtable
 *             EARLY       => Reader is the final result of a compaction
 *             MOVED_START => Reader WAS being compacted, but this failed and it has been restored to NORMAL status
 *
 * EARLY
 * From:       None        => Reader is a compaction replacement that is either incomplete and has been opened
 *                            to represent its partial result status, or has been finished but the compaction
 *                            it is a part of has not yet completed fully
 *             EARLY       => Same as from None, only it is not the first time it has been
 *
 * MOVED_START
 * From:       NORMAL      => Reader is being compacted. This compaction has not finished, but the compaction result
 *                            is either partially or fully opened, to either partially or fully replace this reader.
 *                            This reader's start key has been updated to represent this, so that reads only hit
 *                            one or the other reader.
 *
 * METADATA_CHANGE
 * From:       NORMAL      => Reader has seen low traffic and the amount of memory available for index summaries is
 *                            constrained, so its index summary has been downsampled.
 *         METADATA_CHANGE => Same
 * } </pre>
 *
 * Note that in parallel to this, there are two different Descriptor types; TMPLINK and FINAL; the latter corresponds
 * to NORMAL state readers and all readers that replace a NORMAL one. TMPLINK is used for EARLY state readers and
 * no others.
 *
 * When a reader is being compacted, if the result is large its replacement may be opened as EARLY before compaction
 * completes in order to present the result to consumers earlier. In this case the reader will itself be changed to
 * a MOVED_START state, where its start no longer represents its on-disk minimum key. This is to permit reads to be
 * directed to only one reader when the two represent the same data. The EARLY file can represent a compaction result
 * that is either partially complete and still in-progress, or a complete and immutable sstable that is part of a larger
 * macro compaction action that has not yet fully completed.
 *
 * Currently ALL compaction results at least briefly go through an EARLY open state prior to completion, regardless
 * of if early opening is enabled.
 *
 * Since a reader can be created multiple times over the same shared underlying resources, and the exact resources
 * it shares between each instance differ subtly, we track the lifetime of any underlying resource with its own
 * reference count, which each instance takes a Ref to. Each instance then tracks references to itself, and once these
 * all expire it releases its Refs to these underlying resources.
 *
 * There is some shared cleanup behaviour needed only once all sstablereaders in a certain stage of their lifecycle
 * (i.e. EARLY or NORMAL opening), and some that must only occur once all readers of any kind over a single logical
 * sstable have expired. These are managed by the TypeTidy and GlobalTidy classes at the bottom, and are effectively
 * managed as another resource each instance tracks its own Ref instance to, to ensure all of these resources are
 * cleaned up safely and can be debugged otherwise.
 *
 * TODO: fill in details about Tracker and lifecycle interactions for tools, and for compaction strategies
 */
public abstract class SSTableReader extends SSTable implements SelfRefCounted<SSTableReader>
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableReader.class);

    public static final ScheduledThreadPoolExecutor readHotnessTrackerExecutor = initSyncExecutor();
    private static ScheduledThreadPoolExecutor initSyncExecutor()
    {
        if (DatabaseDescriptor.isClientOrToolInitialized())
            return null;

        // Do NOT start this thread pool in client mode

        ScheduledThreadPoolExecutor syncExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("read-hotness-tracker"));
        // Immediately remove readMeter sync task when cancelled.
        syncExecutor.setRemoveOnCancelPolicy(true);
        return syncExecutor;
    }
    private static final RateLimiter meterSyncThrottle = RateLimiter.create(100.0);

    public static final Comparator<SSTableReader> maxTimestampComparator = (o1, o2) -> Long.compare(o1.getMaxTimestamp(), o2.getMaxTimestamp());

    // it's just an object, which we use regular Object equality on; we introduce a special class just for easy recognition
    public static final class UniqueIdentifier {}

    public static final Comparator<SSTableReader> sstableComparator = (o1, o2) -> o1.first.compareTo(o2.first);

    public static final Comparator<SSTableReader> generationReverseComparator = (o1, o2) -> -Integer.compare(o1.descriptor.generation, o2.descriptor.generation);

    public static final Ordering<SSTableReader> sstableOrdering = Ordering.from(sstableComparator);

    public static final Comparator<SSTableReader> sizeComparator = new Comparator<SSTableReader>()
    {
        public int compare(SSTableReader o1, SSTableReader o2)
        {
            return Longs.compare(o1.onDiskLength(), o2.onDiskLength());
        }
    };

    /**
     * maxDataAge is a timestamp in local server time (e.g. System.currentTimeMilli) which represents an upper bound
     * to the newest piece of data stored in the sstable. In other words, this sstable does not contain items created
     * later than maxDataAge.
     *
     * The field is not serialized to disk, so relying on it for more than what truncate does is not advised.
     *
     * When a new sstable is flushed, maxDataAge is set to the time of creation.
     * When a sstable is created from compaction, maxDataAge is set to max of all merged sstables.
     *
     * The age is in milliseconds since epoc and is local to this host.
     */
    public final long maxDataAge;

    public enum OpenReason
    {
        NORMAL,
        EARLY,
        METADATA_CHANGE,
        MOVED_START
    }

    public final OpenReason openReason;
    public final UniqueIdentifier instanceId = new UniqueIdentifier();

    // datafile: might be null before a call to load()
    protected FileHandle dataFile;
    protected IFilter bf;

    protected final BloomFilterTracker bloomFilterTracker = new BloomFilterTracker();

    // technically isCompacted is not necessary since it should never be unreferenced unless it is also compacted,
    // but it seems like a good extra layer of protection against reference counting bugs to not delete data based on that alone
    protected final AtomicBoolean isSuspect = new AtomicBoolean(false);

    // not final since we need to be able to change level on a file.
    protected volatile StatsMetadata sstableMetadata;

    protected final EncodingStats stats;

    public final SerializationHeader header;

    protected final InstanceTidier tidy;
    private final Ref<SSTableReader> selfRef;

    private RestorableMeter readMeter;

    private volatile double crcCheckChance;

    /**
     * Calculate approximate key count.
     * If cardinality estimator is available on all given sstables, then this method use them to estimate
     * key count.
     * If not, then this uses index summaries.
     *
     * @param sstables SSTables to calculate key count
     * @return estimated key count
     */
    public static long getApproximateKeyCount(Iterable<SSTableReader> sstables)
    {
        long count = -1;

        if (Iterables.isEmpty(sstables))
            return count;

        boolean failed = false;
        ICardinality cardinality = null;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.openReason == OpenReason.EARLY)
                continue;

            try
            {
                CompactionMetadata metadata = (CompactionMetadata) sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, MetadataType.COMPACTION);
                // If we can't load the CompactionMetadata, we are forced to estimate the keys using the index
                // summary. (CASSANDRA-10676)
                if (metadata == null)
                {
                    logger.warn("Reading cardinality from Statistics.db failed for {}", sstable.getFilename());
                    failed = true;
                    break;
                }

                if (cardinality == null)
                    cardinality = metadata.cardinalityEstimator;
                else
                    cardinality = cardinality.merge(metadata.cardinalityEstimator);
            }
            catch (IOException e)
            {
                logger.warn("Reading cardinality from Statistics.db failed.", e);
                failed = true;
                break;
            }
            catch (CardinalityMergeException e)
            {
                logger.warn("Cardinality merge failed.", e);
                failed = true;
                break;
            }
        }
        if (cardinality != null && !failed)
            count = cardinality.cardinality();

        // if something went wrong above or cardinality is not available, calculate using index summary
        if (count < 0)
        {
            for (SSTableReader sstable : sstables)
                count += sstable.estimatedKeys();
        }
        return count;
    }

    /**
     * Estimates how much of the keys we would keep if the sstables were compacted together
     */
    public static double estimateCompactionGain(Set<SSTableReader> overlapping)
    {
        Set<ICardinality> cardinalities = new HashSet<>(overlapping.size());
        for (SSTableReader sstable : overlapping)
        {
            try
            {
                ICardinality cardinality = ((CompactionMetadata) sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, MetadataType.COMPACTION)).cardinalityEstimator;
                if (cardinality != null)
                    cardinalities.add(cardinality);
                else
                    logger.trace("Got a null cardinality estimator in: {}", sstable.getFilename());
            }
            catch (IOException e)
            {
                logger.warn("Could not read up compaction metadata for {}", sstable, e);
            }
        }
        long totalKeyCountBefore = 0;
        for (ICardinality cardinality : cardinalities)
        {
            totalKeyCountBefore += cardinality.cardinality();
        }
        if (totalKeyCountBefore == 0)
            return 1;

        long totalKeyCountAfter = mergeCardinalities(cardinalities).cardinality();
        logger.trace("Estimated compaction gain: {}/{}={}", totalKeyCountAfter, totalKeyCountBefore, ((double)totalKeyCountAfter)/totalKeyCountBefore);
        return ((double)totalKeyCountAfter)/totalKeyCountBefore;
    }

    private static ICardinality mergeCardinalities(Collection<ICardinality> cardinalities)
    {
        ICardinality base = new HyperLogLogPlus(13, 25); // see MetadataCollector.cardinality
        try
        {
            base = base.merge(cardinalities.toArray(new ICardinality[cardinalities.size()]));
        }
        catch (CardinalityMergeException e)
        {
            logger.warn("Could not merge cardinalities", e);
        }
        return base;
    }

    public static SSTableReader open(Descriptor descriptor)
    {
        TableMetadataRef metadata;
        if (descriptor.cfname.contains(SECONDARY_INDEX_NAME_SEPARATOR))
        {
            int i = descriptor.cfname.indexOf(SECONDARY_INDEX_NAME_SEPARATOR);
            String indexName = descriptor.cfname.substring(i + 1);
            metadata = Schema.instance.getIndexTableMetadataRef(descriptor.ksname, indexName);
            if (metadata == null)
                throw new AssertionError("Could not find index metadata for index cf " + i);
        }
        else
        {
            metadata = Schema.instance.getTableMetadataRef(descriptor.ksname, descriptor.cfname);
        }
        return open(descriptor, metadata);
    }

    public static SSTableReader open(Descriptor desc, TableMetadataRef metadata)
    {
        return open(desc, componentsFor(desc), metadata);
    }

    public static SSTableReader open(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata)
    {
        return open(descriptor, components, metadata, true, true);
    }

    // use only for offline or "Standalone" operations
    public static SSTableReader openNoValidation(Descriptor descriptor, Set<Component> components, ColumnFamilyStore cfs)
    {
        return open(descriptor, components, cfs.metadata, false, false); // do not track hotness
    }

    // use only for offline or "Standalone" operations
    public static SSTableReader openNoValidation(Descriptor descriptor, TableMetadataRef metadata)
    {
        return open(descriptor, componentsFor(descriptor), metadata, false, false); // do not track hotness
    }

    /**
     * Open SSTable reader to be used in batch mode(such as sstableloader).
     *
     * @param descriptor
     * @param components
     * @param metadata
     * @return opened SSTableReader
     * @throws IOException
     */
    public static SSTableReader openForBatch(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata)
    {
        checkRequiredComponents(descriptor, components, true);

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata;
        try
        {
             sstableMetadata = descriptor.getMetadataSerializer().deserialize(descriptor, types);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, descriptor.filenameFor(Component.STATS));
        }

        ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
        StatsMetadata statsMetadata = (StatsMetadata) sstableMetadata.get(MetadataType.STATS);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);

        // Check if sstable is created using same partitioner.
        // Partitioner can be null, which indicates older version of sstable or no stats available.
        // In that case, we skip the check.
        String partitionerName = metadata.get().partitioner.getClass().getCanonicalName();
        if (validationMetadata != null && !partitionerName.equals(validationMetadata.partitioner))
        {
            logger.error("Cannot open {}; partitioner {} does not match system partitioner {}.  Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, so you will need to edit that to match your old partitioner if upgrading.",
                         descriptor, validationMetadata.partitioner, partitionerName);
            System.exit(1);
        }

        long fileLength = new File(descriptor.filenameFor(Component.DATA)).length();
        logger.debug("Opening {} ({})", descriptor, FBUtilities.prettyPrintMemory(fileLength));
        SSTableReader sstable = internalOpen(descriptor,
                                             components,
                                             metadata,
                                             System.currentTimeMillis(),
                                             statsMetadata,
                                             OpenReason.NORMAL,
                                             header.toHeader(metadata.get()));

        try(FileHandle.Builder dbuilder = sstable.dataFileHandleBuilder())
        {
            sstable.bf = FilterFactory.AlwaysPresent;
            sstable.loadIndex(false);
            sstable.dataFile = dbuilder.complete();
            sstable.setup(false);
            return sstable;
        }
        catch(IOException e)
        {
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    public static void checkRequiredComponents(Descriptor descriptor, Set<Component> components, boolean validate)
    {
        if (validate)
        {
            // Minimum components without which we can't do anything
            assert components.containsAll(requiredComponents(descriptor))
            : "Required components " + Sets.difference(requiredComponents(descriptor), components) +
              " missing for sstable " + descriptor;
        }
        else
        {
            // Scrub-only case, we just need data file.
            assert components.contains(Component.DATA);
        }
    }

    public static Set<Component> requiredComponents(Descriptor descriptor)
    {
        return descriptor.getFormat().getReaderFactory().requiredComponents();
    }

    public static SSTableReader open(Descriptor descriptor,
                                     Set<Component> components,
                                     TableMetadataRef metadata,
                                     boolean validate,
                                     boolean trackHotness)
    {
        checkRequiredComponents(descriptor, components, validate);

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);

        Map<MetadataType, MetadataComponent> sstableMetadata;
        try
        {
            sstableMetadata = descriptor.getMetadataSerializer().deserialize(descriptor, types);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, descriptor.filenameFor(Component.STATS));
        }
        ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
        StatsMetadata statsMetadata = (StatsMetadata) sstableMetadata.get(MetadataType.STATS);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
        assert header != null;

        // Check if sstable is created using same partitioner.
        // Partitioner can be null, which indicates older version of sstable or no stats available.
        // In that case, we skip the check.
        String partitionerName = metadata.get().partitioner.getClass().getCanonicalName();
        if (validationMetadata != null && !partitionerName.equals(validationMetadata.partitioner))
        {
            logger.error("Cannot open {}; partitioner {} does not match system partitioner {}.  Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, so you will need to edit that to match your old partitioner if upgrading.",
                         descriptor, validationMetadata.partitioner, partitionerName);
            System.exit(1);
        }

        long fileLength = new File(descriptor.filenameFor(Component.DATA)).length();
        logger.debug("Opening {} ({})", descriptor, FBUtilities.prettyPrintMemory(fileLength));
        SSTableReader sstable = internalOpen(descriptor,
                                             components,
                                             metadata,
                                             System.currentTimeMillis(),
                                             statsMetadata,
                                             OpenReason.NORMAL,
                                             header.toHeader(metadata.get()));

        try
        {
            // load index and filter
            long start = System.nanoTime();
            sstable.load(validationMetadata);
            logger.trace("INDEX LOAD TIME for {}: {} ms.", descriptor, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

            sstable.setup(trackHotness);
            if (validate)
                sstable.validate();

            return sstable;
        }
        catch (IOException e)
        {
            sstable.selfRef().release();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
        catch (Throwable t)
        {
            sstable.selfRef().release();
            throw t;
        }
    }

    public static Collection<SSTableReader> openAll(Set<Map.Entry<Descriptor, Set<Component>>> entries,
                                                    final TableMetadataRef metadata)
    {
        final Collection<SSTableReader> sstables = new LinkedBlockingQueue<>();
        long start = System.nanoTime();

        // TODO: Send open tasks to device-specific thread. Opening in parallel is a bad idea as devices have to
        // seek to service each thread's read requests (for bloom filter or index preload).
        int threadCount = FBUtilities.getAvailableProcessors();
        ExecutorService executor = DebuggableThreadPoolExecutor.createWithFixedPoolSize("SSTableBatchOpen", threadCount);
        for (final Map.Entry<Descriptor, Set<Component>> entry : entries)
        {
            Runnable runnable = new Runnable()
            {
                public void run()
                {
                    SSTableReader sstable;
                    try
                    {
                        sstable = open(entry.getKey(), entry.getValue(), metadata);
                    }
                    catch (CorruptSSTableException ex)
                    {
                        FileUtils.handleCorruptSSTable(ex);
                        logger.error("Corrupt sstable {}; skipping table", entry, ex);
                        return;
                    }
                    catch (FSError ex)
                    {
                        FileUtils.handleFSError(ex);
                        logger.error("Cannot read sstable {}; file system error, skipping table", entry, ex);
                        return;
                    }
                    sstables.add(sstable);
                }
            };
            executor.submit(runnable);
        }

        executor.shutdown();
        try
        {
            executor.awaitTermination(7, TimeUnit.DAYS);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        long timeTaken = System.nanoTime() - start;
        logger.info(String.format("openAll time for table %s using %d threads: %,.3fms", metadata.name, threadCount, timeTaken * 1.0e-6));

        return sstables;

    }

    protected static SSTableReader internalOpen(final Descriptor descriptor,
                                            Set<Component> components,
                                            TableMetadataRef metadata,
                                            Long maxDataAge,
                                            StatsMetadata sstableMetadata,
                                            OpenReason openReason,
                                            SerializationHeader header)
    {
        Factory readerFactory = descriptor.getFormat().getReaderFactory();

        return readerFactory.open(descriptor, components, metadata, maxDataAge, sstableMetadata, openReason, header);
    }

    protected SSTableReader(final Descriptor desc,
                            Set<Component> components,
                            TableMetadataRef metadata,
                            long maxDataAge,
                            StatsMetadata sstableMetadata,
                            OpenReason openReason,
                            SerializationHeader header)
    {
        super(desc, components, metadata, DatabaseDescriptor.getDiskOptimizationStrategy());
        this.sstableMetadata = sstableMetadata;
        this.stats = new EncodingStats(sstableMetadata.minTimestamp, sstableMetadata.minLocalDeletionTime, sstableMetadata.minTTL);
        this.header = header;
        this.maxDataAge = maxDataAge;
        this.openReason = openReason;
        tidy = new InstanceTidier(descriptor, metadata.id);
        selfRef = new Ref<>(this, tidy);
    }

    public static long getTotalBytes(Iterable<SSTableReader> sstables)
    {
        long sum = 0;
        for (SSTableReader sstable : sstables)
            sum += sstable.onDiskLength();
        return sum;
    }

    public static long getTotalUncompressedBytes(Iterable<SSTableReader> sstables)
    {
        long sum = 0;
        for (SSTableReader sstable : sstables)
            sum += sstable.uncompressedLength();

        return sum;
    }

    public boolean equals(Object that)
    {
        return that instanceof SSTableReader && ((SSTableReader) that).descriptor.equals(this.descriptor);
    }

    public int hashCode()
    {
        return this.descriptor.hashCode();
    }

    public String getFilename()
    {
        return dataFile.path();
    }

    public void setupOnline()
    {
        final ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata().id);
        if (cfs != null)
            setCrcCheckChance(cfs.getCrcCheckChance());
    }

    private void load(ValidationMetadata validation) throws IOException
    {
        load();
    }

    /**
     * Loads bloom filter and prepares index.
     */
    private void load() throws IOException
    {
        try (FileHandle.Builder dbuilder = dataFileHandleBuilder())
        {
            loadBloomFilter();
            loadIndex(bf == FilterFactory.AlwaysPresent);       // Try to preload index if we don't have a bloom filter.
            dataFile = dbuilder.complete();
        }
        catch (Throwable t)
        { // Because the tidier has not been set-up yet in SSTableReader.open(), we must release the files in case of error
            if (dataFile != null)
            {
                dataFile.close();
                dataFile = null;
            }
            releaseIndex();

            throw t;
        }
    }

    /**
     * Load bloom filter from Filter.db file.
     *
     * @throws IOException
     */
    private void loadBloomFilter() throws IOException
    {
        if (!components.contains(Component.FILTER))
        {
            bf = FilterFactory.AlwaysPresent;
            return;
        }
        try (DataInputStream stream = new DataInputStream(new BufferedInputStream(Files.newInputStream(Paths.get(descriptor.filenameFor(Component.FILTER))))))
        {
            bf = FilterFactory.deserialize(stream, true);
        }
    }

    abstract protected void loadIndex(boolean preloadIfMemmapped) throws IOException;
    abstract protected void releaseIndex();

    protected Builder indexFileHandleBuilder(Component component)
    {
        return indexFileHandleBuilder(descriptor, metadata(), component);
    }

    public static Builder indexFileHandleBuilder(Descriptor descriptor, TableMetadata metadata, Component component)
    {
        return new FileHandle.Builder(descriptor.filenameFor(component))
                   .withChunkCache(ChunkCache.instance)
                   .mmapped(DatabaseDescriptor.getIndexAccessMode() != Config.DiskAccessMode.standard && metadata.params.compaction.klass().equals(MemoryOnlyStrategy.class))
                   .bufferSize(PageAware.PAGE_SIZE)
                   .withChunkCache(ChunkCache.instance);
    }

    public static Builder dataFileHandleBuilder(Descriptor descriptor, TableMetadata metadata, boolean compression)
    {
        return new FileHandle.Builder(descriptor.filenameFor(Component.DATA))
                   .compressed(compression)
                   .mmapped(DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.mmap && metadata.params.compaction.klass().equals(MemoryOnlyStrategy.class))
                   .withChunkCache(ChunkCache.instance);
    }

    Builder dataFileHandleBuilder()
    {
        int dataBufferSize = optimizationStrategy.bufferSize(sstableMetadata.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
        return dataFileHandleBuilder(descriptor, metadata(), compression)
                   .bufferSize(dataBufferSize);
    }

    public void setReplaced()
    {
        synchronized (tidy.global)
        {
            assert !tidy.isReplaced;
            tidy.isReplaced = true;
        }
    }

    public boolean isReplaced()
    {
        synchronized (tidy.global)
        {
            return tidy.isReplaced;
        }
    }

    protected boolean filterFirst()
    {
        return openReason == OpenReason.MOVED_START;
    }

    protected boolean filterLast()
    {
        return false;
    }

    /**
     * Clone this reader with the provided start and open reason, and set the clone as replacement.
     *
     * @param newFirst the first key for the replacement (which can be different from the original due to the pre-emptive
     * opening of compaction results).
     * @param reason the {@code OpenReason} for the replacement.
     *
     * @return the cloned reader. That reader is set as a replacement by the method.
     */
    private SSTableReader cloneAndReplace(DecoratedKey newFirst, OpenReason reason)
    {
        SSTableReader replacement = clone(reason);
        replacement.first = newFirst;
        return replacement;
    }

    abstract protected SSTableReader clone(OpenReason reason);

    public SSTableReader cloneWithRestoredStart(DecoratedKey restoredStart)
    {
        synchronized (tidy.global)
        {
            return cloneAndReplace(restoredStart, OpenReason.NORMAL);
        }
    }

    // DropPageCache must NOT be an anonymous or non-static inner class, nor must it retain a reference chain to this reader
    @SuppressWarnings("resource")       // Closeable closed by tidy
    public SSTableReader cloneWithNewStart(DecoratedKey newStart)
    {
        synchronized (tidy.global)
        {
            assert openReason != OpenReason.EARLY;
            // TODO: merge with caller's firstKeyBeyond() work,to save time
            if (newStart.compareTo(first) > 0)
            {
                long dataStart = getExactPosition(newStart, Rebufferer.ReaderConstraint.NONE).position;
                this.tidy.addCloseable(new DropPageCache(dataFile, dataStart, null, 0));
            }

            return cloneAndReplace(newStart, OpenReason.MOVED_START);
        }
    }

    private static class DropPageCache implements Closeable
    {
        final FileHandle dfile;
        final long dfilePosition;
        final FileHandle ifile;
        final long ifilePosition;

        private DropPageCache(FileHandle dfile, long dfilePosition, FileHandle ifile, long ifilePosition)
        {
            this.dfile = dfile;
            this.dfilePosition = dfilePosition;
            this.ifile = ifile;
            this.ifilePosition = ifilePosition;
        }

        public void close()
        {
            dfile.dropPageCache(dfilePosition);

            if (ifile != null)
                ifile.dropPageCache(ifilePosition);
        }
    }

    /**
     * Returns a new SSTableReader with the same properties as this SSTableReader except that a new IndexSummary will
     * be built at the target samplingLevel.  This (original) SSTableReader instance will be marked as replaced, have
     * its DeletingTask removed, and have its periodic read-meter sync task cancelled.
     * @param samplingLevel the desired sampling level for the index summary on the new SSTableReader
     * @return a new SSTableReader
     * @throws IOException
     */
    @SuppressWarnings("resource")
    public SSTableReader cloneWithNewSummarySamplingLevel(ColumnFamilyStore parent, int samplingLevel) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public RestorableMeter getReadMeter()
    {
        return readMeter;
    }

    private void validate()
    {
        if (this.first.compareTo(this.last) > 0)
        {
            throw new CorruptSSTableException(new IllegalStateException(String.format("SSTable first key %s > last key %s", this.first, this.last)), getFilename());
        }
    }

    /**
     * Returns the compression metadata for this sstable.
     * @throws IllegalStateException if the sstable is not compressed
     */
    public CompressionMetadata getCompressionMetadata()
    {
        if (!compression)
            throw new IllegalStateException(this + " is not compressed");

        return dataFile.compressionMetadata().get();
    }

    /**
     * Returns the amount of memory in bytes used off heap by the compression meta-data.
     * @return the amount of memory in bytes used off heap by the compression meta-data
     */
    public long getCompressionMetadataOffHeapSize()
    {
        if (!compression)
            return 0;

        return getCompressionMetadata().offHeapSize();
    }

    /**
     * For testing purposes only.
     */
    public void forceFilterFailures()
    {
        bf = FilterFactory.AlwaysPresent;
    }

    public IFilter getBloomFilter()
    {
        return bf;
    }

    public long getBloomFilterSerializedSize()
    {
        return bf.serializedSize();
    }

    /**
     * Returns the amount of memory in bytes used off heap by the bloom filter.
     * @return the amount of memory in bytes used off heap by the bloom filter
     */
    public long getBloomFilterOffHeapSize()
    {
        return bf.offHeapSize();
    }

    /**
     * @return An estimate of the number of keys in this SSTable based on the index summary.
     */
    abstract public long estimatedKeys();

    /**
     * @param ranges
     * @return An estimate of the number of keys for given ranges in this SSTable.
     */
    public long estimatedKeysForRanges(Collection<Range<Token>> ranges)
    {
        return getKeySamplesInternal(ranges).size();
    }

    public Iterable<DecoratedKey> getKeySamples(final Range<Token> range)
    {
        return getKeySamplesInternal(Collections.singleton(range));
    }

    private Collection<DecoratedKey> getKeySamplesInternal(final Collection<Range<Token>> ranges)
    {
        try
        {
            ArrayList<DecoratedKey> keys = new ArrayList<>();
            for (AbstractBounds<PartitionPosition> bound : SSTableScanner.makeBounds(this, ranges))
            {
                try (PartitionIndexIterator iter = coveredKeysIterator(bound))
                {
                    while (iter.key() != null)
                    {
                        keys.add(iter.key());
                        iter.advance();
                    }
                }
            }
            return keys;
        }
        catch (IOException e)
        {
            markSuspect();
            throw new CorruptSSTableException(e, dataFile.path());
        }
    }

    /**
     * Determine the minimal set of sections that can be extracted from this SSTable to cover the given ranges.
     * @return A sorted list of (offset,end) pairs that cover the given ranges in the datafile for this SSTable.
     */
    public List<Pair<Long,Long>> getPositionsForRanges(Collection<Range<Token>> ranges)
    {
        // use the index to determine a minimal section for each range
        List<Pair<Long,Long>> positions = new ArrayList<>();
        for (Range<Token> range : Range.normalize(ranges))
        {
            assert !range.isTrulyWrapAround();
            // truncate the range so it at most covers the sstable
            AbstractBounds<PartitionPosition> bounds = Range.makeRowRange(range);
            PartitionPosition leftBound = bounds.left.compareTo(first) > 0 ? bounds.left : first.getToken().minKeyBound();
            PartitionPosition rightBound = bounds.right.isMinimum() ? last.getToken().maxKeyBound() : bounds.right;

            if (leftBound.compareTo(last) > 0 || rightBound.compareTo(first) < 0)
                continue;

            long left = getPosition(leftBound, Operator.GT).position;
            long right = (rightBound.compareTo(last) > 0)
                         ? uncompressedLength()
                         : getPosition(rightBound, Operator.GT).position;

            if (left == right)
                // empty range
                continue;

            assert left < right : String.format("Range=%s openReason=%s first=%s last=%s left=%d right=%d", range, openReason, first, last, left, right);
            positions.add(Pair.create(left, right));
        }
        return positions;
    }

    /**
     * Retrieves the position while updating the key cache and the stats.
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     * allow key selection by token bounds but only if op != * EQ
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     */
    public RowIndexEntry getPosition(PartitionPosition key, Operator op)
    {
        return getPosition(key, op, SSTableReadsListener.NOOP_LISTENER, Rebufferer.ReaderConstraint.NONE);
    }

    /**
     * Retrieves the position while updating the key cache and the stats.
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     * allow key selection by token bounds but only if op != * EQ
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @param listener the {@code SSTableReaderListener} that must handle the notifications.
     */
    public abstract RowIndexEntry getPosition(PartitionPosition key, Operator op, SSTableReadsListener listener, Rebufferer.ReaderConstraint rc);
    public abstract RowIndexEntry getExactPosition(DecoratedKey key, Rebufferer.ReaderConstraint rc);
    public abstract boolean contains(DecoratedKey key, Rebufferer.ReaderConstraint rc);

    public abstract UnfilteredRowIterator iterator(DecoratedKey key,
                                                   Slices slices,
                                                   ColumnFilter selectedColumns,
                                                   boolean reversed,
                                                   SSTableReadsListener listener);
    public abstract UnfilteredRowIterator iterator(FileDataInput file,
                                                   DecoratedKey key,
                                                   RowIndexEntry indexEntry,
                                                   Slices slices,
                                                   ColumnFilter selectedColumns,
                                                   boolean reversed,
                                                   Rebufferer.ReaderConstraint readerConstraint);

    public abstract PartitionIndexIterator coveredKeysIterator(PartitionPosition left, boolean inclusiveLeft, PartitionPosition right, boolean inclusiveRight) throws IOException;
    public abstract PartitionIndexIterator allKeysIterator() throws IOException;
    public abstract ScrubPartitionIterator scrubPartitionsIterator() throws IOException;

    public Flow<FlowableUnfilteredPartition> flow(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener)
    {
        return AsyncPartitionReader.create(this, listener, key, slices, selectedColumns, reversed);
    }

    public Flow<FlowableUnfilteredPartition> flow(IndexFileEntry indexEntry, FileDataInput dfile, SSTableReadsListener listener)
    {
        return AsyncPartitionReader.create(this, dfile, listener, indexEntry);
    }

    public Flow<FlowableUnfilteredPartition> flow(IndexFileEntry indexEntry, FileDataInput dfile, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener)
    {
        return AsyncPartitionReader.create(this, dfile, listener, indexEntry, slices, selectedColumns, reversed);
    }

    public abstract Flow<IndexFileEntry> coveredKeysFlow(RandomAccessReader dfile,
                                                         PartitionPosition left,
                                                         boolean inclusiveLeft,
                                                         PartitionPosition right,
                                                         boolean inclusiveRight);

    private final class KeysRange
    {
        PartitionPosition left;
        boolean inclusiveLeft;
        PartitionPosition right;
        boolean inclusiveRight;

        KeysRange(AbstractBounds<PartitionPosition> bounds)
        {
            assert !AbstractBounds.strictlyWrapsAround(bounds.left, bounds.right) : "[" + bounds.left + "," + bounds.right + "]";

            left = bounds.left;
            inclusiveLeft = bounds.inclusiveLeft();
            if (filterFirst() && first.compareTo(left) > 0)
            {
                left = first;
                inclusiveLeft = true;
            }

            right = bounds.right;
            inclusiveRight = bounds.inclusiveRight();
            if (filterLast() && last.compareTo(right) < 0)
            {
                right = last;
                inclusiveRight = true;
            }
        }

        PartitionIndexIterator iterator() throws IOException
        {
            return coveredKeysIterator(left, inclusiveLeft, right, inclusiveRight);
        }

        public Flow<IndexFileEntry> flow(RandomAccessReader dataFileReader)
        {
            return coveredKeysFlow(dataFileReader, left, inclusiveLeft, right, inclusiveRight);
        }
    }

   /**
     * @param bounds Must not be wrapped around ranges
     * @return PartitionIndexIterator within the given bounds
     * @throws IOException
     */
    public PartitionIndexIterator coveredKeysIterator(AbstractBounds<PartitionPosition> bounds) throws IOException
    {
        return new KeysRange(bounds).iterator();
    }

    public Flow<IndexFileEntry> coveredKeysFlow(RandomAccessReader dataFileReader, AbstractBounds<PartitionPosition> bounds)
    {
        return new KeysRange(bounds).flow(dataFileReader);
    }

    /**
     * @param columns the columns to return.
     * @param dataRange filter to use when reading the columns
     * @param listener a listener used to handle internal read events
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public ISSTableScanner getScanner(ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener)
    {
        return SSTableScanner.getScanner(this, columns, dataRange, listener);
    }

    /**
     * Return an asynchronous table scanner, retrieving the selected rows in the selected
     * partitions as a flow of {@link FlowableUnfilteredPartition}.
     *
     * @param columns the columns to return.
     * @param dataRange filter to use when reading the columns
     * @param listener a listener used to handle internal read events
     *
     * @return An asynchronous flow of partitions for seeking over the rows of the SSTable.
     */
    public Flow<FlowableUnfilteredPartition> getAsyncScanner(ColumnFilter columns,
                                                             DataRange dataRange,
                                                             SSTableReadsListener listener)
    {
        return AsyncSSTableScanner.getScanner(this, columns, dataRange, listener);
    }

    /**
     * Direct I/O SSTableScanner over an iterator of bounds.
     *
     * @param boundsIterator the keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> boundsIterator)
    {
        return SSTableScanner.getScanner(this, boundsIterator);
    }

    /**
     * Direct I/O SSTableScanner over the full sstable.
     *
     * @return A Scanner for reading the full SSTable.
     */
    public ISSTableScanner getScanner()
    {
        return SSTableScanner.getScanner(this);
    }

    /**
     * Return an asynchronous table scanner, retrieving the full sstable
     * as a flow of {@link FlowableUnfilteredPartition}.
     *
     * @return An asynchronous flow of partitions for seeking over the rows of the SSTable.
     */
    public Flow<FlowableUnfilteredPartition> getAsyncScanner()
    {
        return AsyncSSTableScanner.getScanner(this);
    }

    /**
     * Direct I/O SSTableScanner over a defined collection of ranges of tokens.
     *
     * @param ranges the range of keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public Flow<FlowableUnfilteredPartition> getAsyncScanner(Collection<Range<Token>> ranges)
    {
        if (ranges != null)
            return AsyncSSTableScanner.getScanner(this, ranges);
        else
            return getAsyncScanner();
    }

    /**
     * Direct I/O SSTableScanner over a defined collection of ranges of tokens.
     *
     * @param ranges the range of keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public ISSTableScanner getScanner(Collection<Range<Token>> ranges)
    {
        if (ranges != null)
            return SSTableScanner.getScanner(this, ranges);
        else
            return getScanner();
    }

    @SuppressWarnings("resource") // caller to close
    public UnfilteredRowIterator simpleIterator(FileDataInput dfile, DecoratedKey key, RowIndexEntry position, boolean tombstoneOnly)
    {
        return SSTableIdentityIterator.create(this, dfile, position, key, tombstoneOnly);
    }

    public boolean couldContain(DecoratedKey dk)
    {
        return !(bf instanceof AlwaysPresentFilter)
                ? bf.isPresent(dk)
                : contains(dk, Rebufferer.ReaderConstraint.NONE);
    }

    /**
     * Finds and returns the first key beyond a given token in this SSTable or null if no such key exists.
     */
    public DecoratedKey firstKeyBeyond(PartitionPosition token)
    {
        try
        {
            RowIndexEntry pos = getPosition(token, Operator.GT);
            if (pos == null)
                return null;

            try (FileDataInput in = dataFile.createReader(pos.position, Rebufferer.ReaderConstraint.NONE))
            {
                ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
                DecoratedKey indexDecoratedKey = decorateKey(indexKey);
                return indexDecoratedKey;
            }
        }
        catch (IOException e)
        {
            markSuspect();
            throw new CorruptSSTableException(e, dataFile.path());
        }
    }

    /**
     * @return The length in bytes of the data for this SSTable. For
     * compressed files, this is not the same thing as the on disk size (see
     * onDiskLength())
     */
    public long uncompressedLength()
    {
        return dataFile.dataLength();
    }

    /**
     * @return The length in bytes of the on disk size for this SSTable. For
     * compressed files, this is not the same thing as the data length (see
     * length())
     */
    public long onDiskLength()
    {
        return dataFile.onDiskLength;
    }

    @VisibleForTesting
    public double getCrcCheckChance()
    {
        return crcCheckChance;
    }

    /**
     * Set the value of CRC check chance. The argument supplied is obtained
     * from the the property of the owning CFS. Called when either the SSTR
     * is initialized, or the CFS's property is updated via JMX
     * @param crcCheckChance
     */
    public void setCrcCheckChance(double crcCheckChance)
    {
        this.crcCheckChance = crcCheckChance;
        dataFile.compressionMetadata().ifPresent(metadata -> metadata.parameters.setCrcCheckChance(crcCheckChance));
    }

    /**
     * Mark the sstable as obsolete, i.e., compacted into newer sstables.
     *
     * When calling this function, the caller must ensure that the SSTableReader is not referenced anywhere
     * except for threads holding a reference.
     *
     * multiple times is usually buggy (see exceptions in Tracker.unmarkCompacting and removeOldSSTablesSize).
     */
    public void markObsolete(Runnable tidier)
    {
        if (logger.isTraceEnabled())
            logger.trace("Marking {} compacted", getFilename());

        synchronized (tidy.global)
        {
            assert !tidy.isReplaced;
            assert tidy.global.obsoletion == null: this + " was already marked compacted";

            tidy.global.obsoletion = tidier;
            tidy.global.stopReadMeterPersistence();
        }
    }

    public boolean isMarkedCompacted()
    {
        return tidy.global.obsoletion != null;
    }

    public void markSuspect()
    {
        if (logger.isTraceEnabled())
            logger.trace("Marking {} as a suspect for blacklisting.", getFilename());

        isSuspect.getAndSet(true);
    }

    public boolean isMarkedSuspect()
    {
        return isSuspect.get();
    }

    /**
     * Direct I/O SSTableScanner over a defined range of tokens.
     *
     * @param range the range of keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public ISSTableScanner getScanner(Range<Token> range)
    {
        if (range == null)
            return getScanner();
        return getScanner(Collections.singletonList(range));
    }

    public FileDataInput getFileDataInput(long position, Rebufferer.ReaderConstraint rc)
    {
        return dataFile.createReader(position, rc);
    }

    /**
     * Tests if the sstable contains data newer than the given age param (in localhost currentMilli time).
     * This works in conjunction with maxDataAge which is an upper bound on the create of data in this sstable.
     * @param age The age to compare the maxDataAre of this sstable. Measured in millisec since epoc on this host
     * @return True iff this sstable contains data that's newer than the given age parameter.
     */
    public boolean newSince(long age)
    {
        return maxDataAge > age;
    }

    public void createLinks(String snapshotDirectoryPath)
    {
        for (Component component : components)
        {
            File sourceFile = new File(descriptor.filenameFor(component));
            if (!sourceFile.exists())
                continue;
            File targetLink = new File(snapshotDirectoryPath, sourceFile.getName());
            FileUtils.createHardLink(sourceFile, targetLink);
        }
    }

    public boolean isRepaired()
    {
        return sstableMetadata.repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE;
    }

    /**
     * Retrieves the key at the given position, as specified by PartitionIndexIterator.keyPosition() as well as
     * SSTableFlushObserver.startPartition().
     */
    abstract public DecoratedKey keyAt(long position, Rebufferer.ReaderConstraint rc) throws IOException;

    public boolean isPendingRepair()
    {
        return sstableMetadata.pendingRepair != ActiveRepairService.NO_PENDING_REPAIR;
    }

    public UUID getPendingRepair()
    {
        return sstableMetadata.pendingRepair;
    }

    public long getRepairedAt()
    {
        return sstableMetadata.repairedAt;
    }

    public boolean intersects(Collection<Range<Token>> ranges)
    {
        Bounds<Token> range = new Bounds<>(first.getToken(), last.getToken());
        return Iterables.any(ranges, r -> r.intersects(range));
    }

    /**
     * TODO: Move someplace reusable
     */
    public enum Operator
    {
        EQ
        {
            public int apply(int comparison) { return -comparison; }
        },

        GE
        {
            public int apply(int comparison) { return comparison >= 0 ? 0 : 1; }
        },

        GT
        {
            public int apply(int comparison) { return comparison > 0 ? 0 : 1; }
        };

        /**
         * @param comparison The result of a call to compare/compareTo, with the desired field on the rhs.
         * @return less than 0 if the operator cannot match forward, 0 if it matches, greater than 0 if it might match forward.
         */
        public abstract int apply(int comparison);
    }

    public long getBloomFilterFalsePositiveCount()
    {
        return bloomFilterTracker.getFalsePositiveCount();
    }

    public long getRecentBloomFilterFalsePositiveCount()
    {
        return bloomFilterTracker.getRecentFalsePositiveCount();
    }

    public long getBloomFilterTruePositiveCount()
    {
        return bloomFilterTracker.getTruePositiveCount();
    }

    public long getRecentBloomFilterTruePositiveCount()
    {
        return bloomFilterTracker.getRecentTruePositiveCount();
    }

    public EstimatedHistogram getEstimatedPartitionSize()
    {
        return sstableMetadata.estimatedPartitionSize;
    }

    public EstimatedHistogram getEstimatedColumnCount()
    {
        return sstableMetadata.estimatedColumnCount;
    }

    public double getEstimatedDroppableTombstoneRatio(int gcBefore)
    {
        return sstableMetadata.getEstimatedDroppableTombstoneRatio(gcBefore);
    }

    public double getDroppableTombstonesBefore(int gcBefore)
    {
        return sstableMetadata.getDroppableTombstonesBefore(gcBefore);
    }

    public double getCompressionRatio()
    {
        return sstableMetadata.compressionRatio;
    }

    public long getMinTimestamp()
    {
        return sstableMetadata.minTimestamp;
    }

    public long getMaxTimestamp()
    {
        return sstableMetadata.maxTimestamp;
    }

    public int getMinLocalDeletionTime()
    {
        return sstableMetadata.minLocalDeletionTime;
    }

    public int getMaxLocalDeletionTime()
    {
        return sstableMetadata.maxLocalDeletionTime;
    }

    /**
     * Whether the sstable may contain tombstones or if it is guaranteed to not contain any.
     * <p>
     * Note that having that method return {@code false} guarantees the sstable has no tombstones whatsoever (so no
     * cell tombstone, no range tombstone maker and no expiring columns), but having it return {@code true} doesn't
     * guarantee it contains any as it may simply have non-expired cells.
     */
    public boolean mayHaveTombstones()
    {
        // A sstable is guaranteed to have no tombstones if minLocalDeletionTime is still set to its default,
        // Cell.NO_DELETION_TIME, which is bigger than any valid deletion times.
        return getMinLocalDeletionTime() != Cell.NO_DELETION_TIME;
    }

    public int getMinTTL()
    {
        return sstableMetadata.minTTL;
    }

    public int getMaxTTL()
    {
        return sstableMetadata.maxTTL;
    }

    public long getTotalColumnsSet()
    {
        return sstableMetadata.totalColumnsSet;
    }

    public long getTotalRows()
    {
        return sstableMetadata.totalRows;
    }

    public int getAvgColumnSetPerRow()
    {
        return sstableMetadata.totalRows < 0
             ? -1
             : (sstableMetadata.totalRows == 0 ? 0 : (int)(sstableMetadata.totalColumnsSet / sstableMetadata.totalRows));
    }

    public int getSSTableLevel()
    {
        return sstableMetadata.sstableLevel;
    }

    /**
     * Reloads the sstable metadata from disk.
     *
     * Called after level is changed on sstable, for example if the sstable is dropped to L0
     *
     * Might be possible to remove in future versions
     *
     * @throws IOException
     */
    public void reloadSSTableMetadata() throws IOException
    {
        this.sstableMetadata = (StatsMetadata) descriptor.getMetadataSerializer().deserialize(descriptor, MetadataType.STATS);
    }

    public StatsMetadata getSSTableMetadata()
    {
        return sstableMetadata;
    }

    public RandomAccessReader openDataReader(RateLimiter limiter)
    {
        assert limiter != null;
        return dataFile.createReader(limiter);
    }

    public RandomAccessReader openDataReader()
    {
        return dataFile.createReader();
    }

    public RandomAccessReader openDataReader(Rebufferer.ReaderConstraint rc)
    {
        return dataFile.createReader(rc);
    }

    public AsynchronousChannelProxy getDataChannel()
    {
        return dataFile.channel;
    }

    /**
     * @param component component to get timestamp.
     * @return last modified time for given component. 0 if given component does not exist or IO error occurs.
     */
    public long getCreationTimeFor(Component component)
    {
        return new File(descriptor.filenameFor(component)).lastModified();
    }

    /**
     * @return Number of key cache hit
     */
    public long getKeyCacheHit()
    {
        return 0;
    }

    /**
     * @return Number of key cache request
     */
    public long getKeyCacheRequest()
    {
        return 0;
    }

    /**
     * Increment the total read count and read rate for this SSTable.  This should not be incremented for non-query reads,
     * like compaction.
     */
    public void incrementReadCount()
    {
        if (readMeter != null)
            readMeter.mark();
    }

    public EncodingStats stats()
    {
       return stats;
    }

    public Ref<SSTableReader> tryRef()
    {
        return selfRef.tryRef();
    }

    public Ref<SSTableReader> selfRef()
    {
        return selfRef;
    }

    public Ref<SSTableReader> ref()
    {
        return selfRef.ref();
    }

    // These runnables must NOT be an anonymous or non-static inner class, nor must it retain a reference chain to this reader
    public void runOnClose(AutoCloseable runOnClose)
    {
        synchronized (tidy.global)
        {
            tidy.addCloseable(runOnClose);
        }
    }

    protected void setup(boolean trackHotness)
    {
        tidy.setup(this, trackHotness);
        tidy.addCloseable(dataFile);
        tidy.addCloseable(bf);
    }

    @VisibleForTesting
    public void overrideReadMeter(RestorableMeter readMeter)
    {
        this.readMeter = tidy.global.readMeter = readMeter;
    }

    public void addTo(Ref.IdentityCollection identities)
    {
        identities.add(this);
        identities.add(tidy.globalRef);
        dataFile.addTo(identities);
        bf.addTo(identities);
        }

    /**
     * Lock memory mapped segments in RAM, see APOLLO-342.
     * @param instance - the memory only status instance, that keeps track of global limits
     */
    public void lock(MemoryOnlyStatus instance)
    {
        Throwable ret = Throwables.perform(null, Arrays.stream(getFilesToBeLocked()).map(f -> () -> f.lock(instance)));
        if (ret != null)
        {
            JVMStabilityInspector.inspectThrowable(ret);
            logger.error("Failed to lock {}", this, ret);
        }
    }

    /**
     * Unlock memory mapped segments that were locked in RAM, if any, see APOLLO-342.
     * @param instance - the memory only status instance, that keeps track of global limits
     */
    public void unlock(MemoryOnlyStatus instance)
    {
        Throwable ret = Throwables.perform(null, Arrays.stream(getFilesToBeLocked()).map(f -> () -> f.unlock(instance)));
        if (ret != null)
        {
            JVMStabilityInspector.inspectThrowable(ret);
            logger.error("Failed to unlock {}", this, ret);
        }
    }

    /**
     * @return - the buffers that were locked in memory, if any.
     */
    public Iterable<MemoryLockedBuffer> getLockedMemory()
    {
        return Iterables.concat(Arrays.stream(getFilesToBeLocked()).map(f -> f.getLockedMemory()).collect(Collectors.toList()));
    }

    /**
     * Return the file handles to the files on disk that should be locked in RAM when said files are memory mapped.
     *
     * APOLLO-342: it's kind of ugly that we need to expose {@link MemoryOnlyStatus} to the sstable
     * reader, so I prefer to only limit it to the base class by asking the sub-classes for their
     * files, rather than polluting them with MemoryOnlyStatus by making the {@link SSTableReader#lock(MemoryOnlyStatus)}
     * and related methods abstract, this also avoids duplicating code. See APOLLO-342 and the follow up ticket
     * for possible improvements.
     *
     * @return - the file handles to the files on disk that can be locked in RAM.
     */
    protected abstract FileHandle[] getFilesToBeLocked();

    /**
     * One instance per SSTableReader we create.
     *
     * We can create many InstanceTidiers (one for every time we reopen an sstable with MOVED_START for example),
     * but there can only be one GlobalTidy for one single logical sstable.
     *
     * When the InstanceTidier cleansup, it releases its reference to its GlobalTidy; when all InstanceTidiers
     * for that type have run, the GlobalTidy cleans up.
     */
    protected static final class InstanceTidier implements Tidy
    {
        private final Descriptor descriptor;
        private final TableId tableId;
        List<AutoCloseable> toClose;

        private boolean isReplaced = false;

        // a reference to our shared tidy instance, that
        // we will release when we are ourselves released
        private Ref<GlobalTidy> globalRef;
        private GlobalTidy global;

        // this ensures that the readMeter is available when it is required.
        // It needs to be awaited before cleaning.
        private volatile CompletableFuture<Void> setupFuture;

        void setup(SSTableReader reader, boolean trackHotness)
        {
            toClose = new ArrayList<>();
            // get a new reference to the shared descriptor-type tidy
            this.globalRef = GlobalTidy.get(reader);
            this.global = globalRef.get();
            this.setupFuture = ensureReadMeter(trackHotness)
                               // this does mean we may miss some read updates but so be it, as
                               // long as we wait on the future before cleaing so that the readMeter
                               // is cleaned
                               .thenAccept(done ->  reader.readMeter = global.readMeter);
        }

        CompletableFuture<Void> ensureReadMeter(boolean trackHotness)
        {
            if (trackHotness)
                return global.ensureReadMeter();
            else
                return TPCUtils.completedFuture(null);
        }

        InstanceTidier(Descriptor descriptor, TableId tableId)
        {
            this.descriptor = descriptor;
            this.tableId = tableId;
        }

        public void addCloseable(AutoCloseable closeable)
        {
            if (closeable != null)
                toClose.add(closeable); // Last added is first to be closed.
        }

        public void tidy()
        {
            if (logger.isTraceEnabled())
                logger.trace("Running instance tidier for {} with setup {}", descriptor, setupFuture != null);

            // don't try to cleanup if the sstablereader was never fully constructed
            if (setupFuture == null)
                return;

            final ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tableId);
            final OpOrder.Barrier barrier;
            if (cfs != null)
            {
                barrier = cfs.readOrdering.newBarrier();
                barrier.issue();
            }
            else
                barrier = null;

            ScheduledExecutors.nonPeriodicTasks.execute(new Runnable()
            {
                public void run()
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Async instance tidier for {}, before barrier", descriptor);

                    // wait on setup future, just in case it has not completed yet
                    TPCUtils.blockingAwait(setupFuture);

                    if (barrier != null)
                        barrier.await();

                    if (logger.isTraceEnabled())
                        logger.trace("Async instance tidier for {}, after barrier", descriptor);

                    Throwables.maybeFail(Throwables.close(null, Lists.reverse(toClose)));
                    globalRef.release();

                    if (logger.isTraceEnabled())
                        logger.trace("Async instance tidier for {}, completed", descriptor);
                }
            });
        }

        public String name()
        {
            return descriptor.toString();
        }
    }

    /**
     * One instance per logical sstable. This both tracks shared cleanup and some shared state related
     * to the sstable's lifecycle.
     *
     * All InstanceTidiers, on setup(), ask the static get() method for their shared state,
     * and stash a reference to it to be released when they are. Once all such references are
     * released, this shared tidy will be performed.
     */
    static final class GlobalTidy implements Tidy
    {
        static WeakReference<ScheduledFuture<?>> NULL = new WeakReference<>(null);
        // keyed by descriptor, mapping to the shared GlobalTidy for that descriptor
        static final ConcurrentMap<Descriptor, Ref<GlobalTidy>> lookup = new ConcurrentHashMap<>();

        private final Descriptor desc;
        // the readMeter that is shared between all instances of the sstable, and can be overridden in all of them
        // at once also, for testing purposes
        private RestorableMeter readMeter;
        // the scheduled persistence of the readMeter, that we will cancel once all instances of this logical
        // sstable have been released
        private WeakReference<ScheduledFuture<?>> readMeterSyncFuture = NULL;
        // shared state managing if the logical sstable has been compacted; this is used in cleanup
        private volatile Runnable obsoletion;

        GlobalTidy(final SSTableReader reader)
        {
            this.desc = reader.descriptor;
        }

        CompletableFuture<Void> ensureReadMeter()
        {
            if (readMeter != null)
                return TPCUtils.completedFuture(null);

            // Don't track read rates for tables in the system keyspace and don't bother trying to load or persist
            // the read meter when in client mode.
            // Also, do not track read rates when running in client or tools mode (syncExecuter isn't available in these modes)
            if (SchemaConstants.isSystemKeyspace(desc.ksname) || DatabaseDescriptor.isClientOrToolInitialized())
            {
                readMeter = null;
                readMeterSyncFuture = NULL;
                return TPCUtils.completedFuture(null);
            }

            return SystemKeyspace.getSSTableReadMeter(desc.ksname, desc.cfname, desc.generation)
                                 .thenAccept(this::setReadMeter);
        }

        // sync the average read rate to system.sstable_activity every five minutes, starting one minute from now
        private void setReadMeter(RestorableMeter readMeter)
        {
            this.readMeter = readMeter;
            try
            {
                readMeterSyncFuture = new WeakReference<>(readHotnessTrackerExecutor.scheduleAtFixedRate(new Runnable()
                {
                    public void run()
                    {
                        if (obsoletion == null)
                        {
                            meterSyncThrottle.acquire();
                            TPCUtils.blockingAwait(SystemKeyspace.persistSSTableReadMeter(desc.ksname, desc.cfname, desc.generation, readMeter));
                        }
                    }
                }, 1, 5, TimeUnit.MINUTES));
            }
            catch (RejectedExecutionException e)
            {
                // That's ok, that just mean we're shutting down the node and the read meter executor has been shut down.
                // Note that while we shutdown that executor relatively late, we also do it _before_ we flush the
                // system keyspace so as to ensure it doesn't re-add new mutations that wouldn't be flushed, so
                // new sstables created by those last flushes could trigger this.
            }
        }

        private void stopReadMeterPersistence()
        {
            ScheduledFuture<?> readMeterSyncFutureLocal = readMeterSyncFuture.get();
            if (readMeterSyncFutureLocal != null)
            {
                readMeterSyncFutureLocal.cancel(true);
                readMeterSyncFuture = NULL;
            }
        }

        public void tidy()
        {
            lookup.remove(desc);

            if (obsoletion != null)
                obsoletion.run();

            // don't ideally want to dropPageCache for the file until all instances have been released
            NativeLibrary.trySkipCache(desc.filenameFor(Component.DATA), 0, 0);
            NativeLibrary.trySkipCache(desc.filenameFor(Component.ROW_INDEX), 0, 0);
            NativeLibrary.trySkipCache(desc.filenameFor(Component.PARTITION_INDEX), 0, 0);
        }

        public String name()
        {
            return desc.toString();
        }

        // get a new reference to the shared GlobalTidy for this sstable
        @SuppressWarnings("resource")
        public static Ref<GlobalTidy> get(SSTableReader sstable)
        {
            Descriptor descriptor = sstable.descriptor;
            Ref<GlobalTidy> refc = lookup.get(descriptor);
            if (refc != null)
                return refc.ref();
            final GlobalTidy tidy = new GlobalTidy(sstable);
            refc = new Ref<>(tidy, tidy);
            Ref<?> ex = lookup.putIfAbsent(descriptor, refc);
            if (ex != null)
            {
                refc.close();
                throw new AssertionError();
            }
            return refc;
        }
    }

    @VisibleForTesting
    public static void resetTidying()
    {
        GlobalTidy.lookup.clear();
    }

    public static abstract class Factory
    {
        public abstract SSTableReader open(final Descriptor descriptor,
                                           Set<Component> components,
                                           TableMetadataRef metadata,
                                           Long maxDataAge,
                                           StatsMetadata sstableMetadata,
                                           OpenReason openReason,
                                           SerializationHeader header);

        public abstract Set<Component> requiredComponents();

        public abstract PartitionIndexIterator keyIterator(Descriptor descriptor, TableMetadata metadata);

        public abstract Pair<DecoratedKey, DecoratedKey> getKeyRange(Descriptor descriptor, IPartitioner partitioner) throws IOException;
    }
}
