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
import java.util.Collection;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.io.sstable.format.SSTableReaderBuilder.defaultDataHandleBuilder;
import static org.apache.cassandra.io.sstable.format.SSTableReaderBuilder.defaultIndexHandleBuilder;

/**
 * Bigtable format with trie indices
 */
public class TrieIndexFormat implements SSTableFormat
{
    // Data, primary index and row index (which may be 0-length) are required.
    // For the 3.0+ sstable format, the (misnomed) stats component hold the serialization header which we need to deserialize the sstable content
    private static final Set<Component> REQUIRED_COMPONENTS = ImmutableSet.of(Component.DATA,
                                                                Component.PARTITION_INDEX,
                                                                Component.ROW_INDEX,
                                                                Component.STATS);

    private final static Set<Component> SUPPORTED_COMPONENTS = ImmutableSet.of(Component.DATA,
                                                                               Component.PARTITION_INDEX,
                                                                               Component.ROW_INDEX,
                                                                               Component.FILTER,
                                                                               Component.COMPRESSION_INFO,
                                                                               Component.STATS,
                                                                               Component.DIGEST,
                                                                               Component.CRC,
                                                                               Component.TOC);

    private final static Set<Component> STREAMING_COMPONENTS = ImmutableSet.of(Component.DATA,
                                                                               Component.PARTITION_INDEX,
                                                                               Component.ROW_INDEX,
                                                                               Component.STATS,
                                                                               Component.COMPRESSION_INFO,
                                                                               Component.FILTER,
                                                                               Component.DIGEST,
                                                                               Component.CRC);
    public static final TrieIndexFormat instance = new TrieIndexFormat();
    public static final Version latestVersion = new TrieIndexVersion(TrieIndexVersion.current_version);
    static final ReaderFactory readerFactory = new ReaderFactory();
    static final WriterFactory writerFactory = new WriterFactory();


    private TrieIndexFormat()
    {

    }

    @Override
    public Type getType()
    {
        return Type.TRIE_INDEX;
    }

    @Override
    public Version getLatestVersion()
    {
        return latestVersion;
    }

    @Override
    public Version getVersion(String version)
    {
        return new TrieIndexVersion(version);
    }


    @Override
    public SSTableWriter.Factory getWriterFactory()
    {
        return writerFactory;
    }

    @Override
    public SSTableReader.Factory getReaderFactory()
    {
        return readerFactory;
    }

    @Override
    public Set<Component> requiredComponents()
    {
        return REQUIRED_COMPONENTS;
    }

    @Override
    public Set<Component> supportedComponents()
    {
        return SUPPORTED_COMPONENTS;
    }

    @Override
    public Set<Component> streamingComponents()
    {
        return STREAMING_COMPONENTS;
    }
    static class WriterFactory extends SSTableWriter.Factory
    {
        @Override
        public SSTableWriter open(Descriptor descriptor,
                                  SSTableWriterCreationHelper helper,
                                  Collection<SSTableFlushObserver> observers,
                                  Set<Component> indexComponents,
                                  SSTableTracker sstableTracker)
        {
            return new TrieIndexSSTableWriter(descriptor, helper, observers, indexComponents, sstableTracker);
        }
    }

    static class ReaderFactory implements SSTableReader.Factory
    {
        @Override
        public TrieIndexSSTableReader open(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata, Long maxDataAge, StatsMetadata sstableMetadata, SSTableReader.OpenReason openReason, SerializationHeader header)
        {
            return new TrieIndexSSTableReader(descriptor, components, metadata, maxDataAge, sstableMetadata, openReason, header);
        }

        @Override
        public PartitionIndexIterator keyIterator(Descriptor desc, TableMetadata metadata)
        {
            IPartitioner partitioner = metadata.partitioner;
            boolean compressedData = desc.fileFor(Component.COMPRESSION_INFO).exists();
            try
            {
                StatsMetadata stats = (StatsMetadata) desc.getMetadataSerializer().deserialize(desc, MetadataType.STATS);

                try (FileHandle.Builder piBuilder = defaultIndexHandleBuilder(desc, Component.PARTITION_INDEX);
                     FileHandle.Builder riBuilder = defaultIndexHandleBuilder(desc, Component.ROW_INDEX);
                     FileHandle.Builder dBuilder = defaultDataHandleBuilder(desc).compressed(compressedData);
                     PartitionIndex index = PartitionIndex.load(piBuilder, partitioner, false);
                     FileHandle dFile = dBuilder.complete();
                     FileHandle riFile = riBuilder.complete())
                {
                    return new PartitionIterator(index.sharedCopy(),
                                                 partitioner,
                                                 riFile.sharedCopy(),
                                                 dFile.sharedCopy())
                            .closeHandles();
                }
            }
            catch (IOException e)
            {
                throw Throwables.cleaned(e);
            }
        }

        @Override
        public Pair<DecoratedKey, DecoratedKey> getKeyRange(Descriptor descriptor, IPartitioner partitioner) throws IOException
        {
            File indexFile = descriptor.fileFor(Component.PARTITION_INDEX);
            if (!indexFile.exists())
                return null;
            boolean compressedData = descriptor.fileFor(Component.COMPRESSION_INFO).exists();

            StatsMetadata stats = (StatsMetadata) descriptor.getMetadataSerializer().deserialize(descriptor, MetadataType.STATS);


            try (FileHandle.Builder fhBuilder = defaultIndexHandleBuilder(descriptor, Component.PARTITION_INDEX);
                 PartitionIndex pIndex = PartitionIndex.load(fhBuilder, partitioner, false))
            {
                return Pair.create(pIndex.firstKey(), pIndex.lastKey());
            }
        }

    }

    // versions are denoted as [major][minor].  Minor versions must be forward-compatible:
    // new fields are allowed in e.g. the metadata component, but fields can't be removed
    // or have their size changed.
    //
    static class TrieIndexVersion extends Version
    {
        public static final String current_version = "ca";
        public static final String earliest_supported_version = "aa";

        // aa (DSE 6.0): trie index format
        // ab (DSE pre-6.8): ILLEGAL - handled as 'b' (predates 'ba'). Pre-GA "LABS" releases of DSE 6.8 used this
        //                   sstable version.
        // ac (DSE 6.0.11, 6.7.6): corrected sstable min/max clustering (DB-3691/CASSANDRA-14861)
        // ad (DSE 6.0.14, 6.7.11): added hostId of the node from which the sstable originated (DB-4629)
        // b  (DSE early 6.8 "LABS") has some of 6.8 features but not all
        // ba (DSE 6.8): encrypted indices and metadata
        //               new BloomFilter serialization format
        //               add incremental NodeSync information to metadata
        //               improved min/max clustering representation
        //               presence marker for partition level deletions
        // bb (DSE 6.8.5): added hostId of the node from which the sstable originated (DB-4629)
        // ca (DSE-DB aka Stargazer based on OSS 4.0): all bb fields  + all OSS fields
        // NOTE: when adding a new version, please add that to LegacySSTableTest, too.

        private final boolean isLatestVersion;

        /**
         * DB-2648/CASSANDRA-9067: DSE 6.8/OSS 4.0 bloom filter representation changed (bitset data is no longer stored
         * as BIG_ENDIAN longs, which avoids some redundant bit twiddling).
         */
        private final boolean hasOldBfFormat;
        private final boolean hasAccurateLegacyMinMax;
        private final boolean hasOriginatingHostId;
        private final boolean hasMaxColumnValueLengths;

        private final int correspondingMessagingVersion;

        TrieIndexVersion(String version)
        {
            super(instance, version = mapAb(version));

            isLatestVersion = version.compareTo(current_version) == 0;
            hasOldBfFormat = version.compareTo("b") < 0;
            hasAccurateLegacyMinMax = version.compareTo("ac") >= 0;
            hasOriginatingHostId = version.matches("(a[d-z])|(b[b-z])"); // TODO TBD
            hasMaxColumnValueLengths = version.matches("b[a-z]"); // TODO TBD
            correspondingMessagingVersion = version.compareTo("ca") >= 0 ? MessagingService.VERSION_40 : MessagingService.VERSION_3014;
        }

        // this is for the ab version which was used in the LABS, and then has been renamed to ba
        private static String mapAb(String version)
        {
            return "ab".equals(version) ? "ba" : version;
        }

        @Override
        public boolean isLatestVersion()
        {
            return isLatestVersion;
        }

        @Override
        public boolean hasCommitLogLowerBound()
        {
            return true;
        }

        @Override
        public boolean hasCommitLogIntervals()
        {
            return true;
        }

        @Override
        public boolean hasMaxCompressedLength()
        {
            return true;
        }

        @Override
        public boolean hasPendingRepair()
        {
            return true;
        }

        @Override
        public boolean hasMetadataChecksum()
        {
            return true;
        }

        @Override
        public boolean hasZeroCopyMetadata()
        {
            return version.compareTo("b") >= 0 && version.compareTo("c") < 0;
        }

        @Override
        public boolean hasIncrementalNodeSyncMetadata()
        {
            return version.compareTo("b") >= 0 && version.compareTo("c") < 0;
        }

        @Override
        public boolean hasAccurateMinMax()
        {
            return hasAccurateLegacyMinMax;
        }

        @Override
        public boolean hasPartitionLevelDeletionsPresenceMarker()
        {
            return version.compareTo("ba") >= 0;
        }

        @Override
        public boolean hasImprovedMinMax()
        {
            return version.compareTo("ba") >= 0;
        }

        // TODO TBD
        @Override
        public boolean hasMaxColumnValueLengths()
        {
            return hasMaxColumnValueLengths;
        }

        // TODO TBD
        @Override
        public boolean hasOriginatingHostId()
        {
            return hasOriginatingHostId;
        }

        @Override
        public boolean isCompatible()
        {
            return version.compareTo(earliest_supported_version) >= 0 && version.charAt(0) <= current_version.charAt(0);
        }

        @Override
        public boolean hasOldBfFormat()
        {
            return hasOldBfFormat;
        }

        // this field is not present in DSE
        @Override
        public int correspondingMessagingVersion()
        {
            return correspondingMessagingVersion;
        }

        // this field is not present in DSE
        @Override
        public boolean isCompatibleForStreaming()
        {
            return isCompatible() && version.charAt(0) == current_version.charAt(0);
        }

        // this field is not present in DSE
        @Override
        public boolean hasIsTransient()
        {
            return version.compareTo("ca") >= 0;
        }
    }
}
