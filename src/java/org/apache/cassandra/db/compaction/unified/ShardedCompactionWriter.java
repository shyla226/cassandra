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

package org.apache.cassandra.db.compaction.unified;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A {@link CompactionAwareWriter} that splits the output sstable at the partition boundaries of the compaction
 * shards used by {@link org.apache.cassandra.db.compaction.UnifiedCompactionStrategy} as long as the size of
 * the sstable so far is sufficiently large.
 */
public class ShardedCompactionWriter extends CompactionAwareWriter
{
    protected final static Logger logger = LoggerFactory.getLogger(ShardedCompactionWriter.class);

    private final long minSstableSizeInBytes;
    private final List<PartitionPosition> boundaries;
    private final long estimatedSSTables;

    private Directories.DataDirectory sstableDirectory;
    private int currentIndex;

    public ShardedCompactionWriter(ColumnFamilyStore cfs,
                                   Directories directories,
                                   LifecycleTransaction txn,
                                   Set<SSTableReader> nonExpiredSSTables,
                                   boolean keepOriginals,
                                   long minSstableSizeInBytes,
                                   List<PartitionPosition> boundaries)
    {
        super(cfs, directories, txn, nonExpiredSSTables, keepOriginals);

        this.minSstableSizeInBytes = minSstableSizeInBytes;
        this.boundaries = boundaries;
        // Calculate the estimated sstables by dividing the expected output size by minSstableSizeInBytes (add minSstableSizeInBytes -1 to the numerator in order to round up)
        this.estimatedSSTables = (cfs.getExpectedCompactedFileSize(nonExpiredSSTables, txn.opType()) + minSstableSizeInBytes - 1) / minSstableSizeInBytes;
        this.sstableDirectory = null;
        this.currentIndex = 0;
    }

    @Override
    protected boolean realAppend(UnfilteredRowIterator partition)
    {
        DecoratedKey key = partition.partitionKey();

        boolean boundaryCrossed = false;
        while (currentIndex < boundaries.size() && key.compareTo(boundaries.get(currentIndex)) >= 0)
        {
            currentIndex++;
            boundaryCrossed = true;
        }

        if (boundaryCrossed && sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten() >= minSstableSizeInBytes)
        {
            logger.debug("Switching writer at boundary {}/{} index {}, with size {} for {}.{}",
                         key.getToken(), boundaries.get(currentIndex-1), currentIndex-1,
                         FBUtilities.prettyPrintMemory(sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten()),
                         cfs.getKeyspaceName(), cfs.getTableName());
            switchCompactionLocation(sstableDirectory);
        }

        RowIndexEntry rie = sstableWriter.append(partition);
        return rie != null;
    }

    @Override
    protected void switchCompactionLocation(Directories.DataDirectory directory)
    {
        sstableDirectory = directory;
        @SuppressWarnings("resource")
        SSTableWriter writer = SSTableWriter.create(cfs.newSSTableDescriptor(getDirectories().getLocationForDisk(sstableDirectory)),
                                                    estimatedTotalKeys / estimatedSSTables,
                                                    minRepairedAt,
                                                    pendingRepair,
                                                    isTransient,
                                                    cfs.metadata,
                                                    new MetadataCollector(txn.originals(), cfs.metadata().comparator, 0),
                                                    SerializationHeader.make(cfs.metadata(), nonExpiredSSTables),
                                                    cfs.indexManager.listIndexGroups(),
                                                    txn);

        sstableWriter.switchWriter(writer);
    }
}