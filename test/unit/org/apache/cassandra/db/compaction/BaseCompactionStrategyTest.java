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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.random.JDKRandomGenerator;

import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * A class that contains common mocks and test utilities for unit tests of compaction strategies
 * that involve mocking compactions and sstables.
 */
public class BaseCompactionStrategyTest
{
    static final double epsilon = 0.00000001;
    static final JDKRandomGenerator random = new JDKRandomGenerator();

    final String keyspace = "ks";
    final String table = "tbl";

    @Mock
    ColumnFamilyStore cfs;

    @Mock
    CompactionStrategyFactory strategyFactory;

    Tracker dataTracker;

    long repairedAt;

    CompactionLogger compactionLogger;

    IPartitioner partitioner;

    protected static void setUpClass()
    {
        long seed = System.currentTimeMillis();
        random.setSeed(seed);
        System.out.println("Random seed: " + seed);

        DatabaseDescriptor.daemonInitialization(); // because of all the static initialization in CFS
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    protected void setUp()
    {
        MockitoAnnotations.initMocks(this);

        TableMetadata metadata = TableMetadata.builder(keyspace, table)
                                              .addPartitionKeyColumn("pk", AsciiType.instance)
                                              .build();

        dataTracker = Tracker.newDummyTracker();
        repairedAt = System.currentTimeMillis();
        partitioner = DatabaseDescriptor.getPartitioner();

        when(cfs.metadata()).thenReturn(metadata);
        when(cfs.getKeyspaceName()).thenReturn(keyspace);
        when(cfs.getTableName()).thenReturn(table);
        when(cfs.getTracker()).thenReturn(dataTracker);
        when(cfs.getPartitioner()).thenReturn(partitioner);

        // use a real compaction logger to execute that code too, even though we don't really check
        // the content of the files, at least we cover the code. The files will be overwritten next
        // time the test is run or by a gradle clean task, so they will not grow indefinitely
        compactionLogger = new CompactionLogger(cfs.metadata());
        compactionLogger.enable();

        when(strategyFactory.getCfs()).thenReturn(cfs);
        when(strategyFactory.getCompactionLogger()).thenReturn(compactionLogger);
    }

    SSTableReader mockSSTable(int level, long bytesOnDisk, long timestamp, double hotness, DecoratedKey first, DecoratedKey last)
    {
        return mockSSTable(level, bytesOnDisk, timestamp, hotness, first, last, false);
    }

    SSTableReader mockSSTable(long bytesOnDisk, long timestamp, DecoratedKey first, DecoratedKey last)
    {
        return mockSSTable(0, bytesOnDisk, timestamp, 0, first, last, false);
    }

    SSTableReader mockSSTable(ICardinality cardinality, long timestamp, int valueSize)
    {
        long keyCount = cardinality.cardinality();
        long bytesOnDisk = valueSize * keyCount;
        DecoratedKey first = new BufferDecoratedKey(partitioner.getMinimumToken(), ByteBuffer.allocate(0));
        DecoratedKey last = new BufferDecoratedKey(partitioner.getMinimumToken(), ByteBuffer.allocate(0));

        SSTableReader ret = mockSSTable(0, bytesOnDisk, timestamp, 0, first, last, true);
        when(ret.keyCardinalityEstimator()).thenReturn(cardinality);
        return ret;
    }

    private SSTableReader mockSSTable(int level, long bytesOnDisk, long timestamp, double hotness, DecoratedKey first, DecoratedKey last, boolean stubOnly)
    {
        // stubOnly will save tons of memory but we'll make it impossible to verify invocations later on
        SSTableReader ret = Mockito.mock(SSTableReader.class, stubOnly ? withSettings().stubOnly() : withSettings());

        when(ret.getSSTableLevel()).thenReturn(level);
        when(ret.bytesOnDisk()).thenReturn(bytesOnDisk);
        when(ret.onDiskLength()).thenReturn(bytesOnDisk);
        when(ret.uncompressedLength()).thenReturn(bytesOnDisk); // let's assume no compression
        when(ret.hotness()).thenReturn(hotness);
        when(ret.getMaxTimestamp()).thenReturn(timestamp);
        when(ret.getMinTimestamp()).thenReturn(timestamp);
        when(ret.getFirst()).thenReturn(first);
        when(ret.getLast()).thenReturn(last);
        when(ret.isMarkedSuspect()).thenReturn(false);
        when(ret.isRepaired()).thenReturn(true);
        when(ret.getRepairedAt()).thenReturn(repairedAt);
        when(ret.getColumnFamilyName()).thenReturn(table);
        when(ret.getGeneration()).thenReturn(0);
        when(ret.toString()).thenReturn(String.format("Bytes on disk: %s, level %d, hotness %f, timestamp %d, first %s, last %s",
                                                      FBUtilities.prettyPrintMemory(bytesOnDisk), level, hotness, timestamp, first, last));

        return ret;
    }

    List<SSTableReader> mockSSTables(int numSSTables, long bytesOnDisk, double hotness, long timestamp)
    {
        DecoratedKey first = new BufferDecoratedKey(partitioner.getMinimumToken(), ByteBuffer.allocate(0));
        DecoratedKey last = new BufferDecoratedKey(partitioner.getMinimumToken(), ByteBuffer.allocate(0));

        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0; i < numSSTables; i++)
        {
            long b = (long)(bytesOnDisk * 0.8 + bytesOnDisk * 0.05 * random.nextDouble()); // leave 5% variability
            double h = hotness * 0.8 + hotness * 0.05 * random.nextDouble(); // leave 5% variability
            sstables.add(mockSSTable(0, b, timestamp, h, first, last));
        }

        return sstables;
    }

    List<SSTableReader> mockNonOverlappingSSTables(int numSSTables, int level, long bytesOnDisk)
    {
        if (!partitioner.splitter().isPresent())
            throw new IllegalStateException(String.format("Cannot split ranges with current partitioner %s", partitioner));

        Range<Token> range = new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken());
        Splitter.WeightedRange weightedRange = new Splitter.WeightedRange(1.0, range);
        Splitter splitter = partitioner.splitter().get();
        List<Token> boundaries = splitter.splitOwnedRanges(numSSTables,
                                                           ImmutableList.of(weightedRange),
                                                           false);
        boundaries.add(0, partitioner.getMinimumToken());
        ByteBuffer emptyBuffer = ByteBuffer.allocate(0);

        long timestamp = System.currentTimeMillis();
        List<SSTableReader> sstables = new ArrayList<>(numSSTables);
        for (int i = 0; i < numSSTables; i++)
        {
            DecoratedKey first = new BufferDecoratedKey(boundaries.get(i).increaseSlightly(), emptyBuffer);
            DecoratedKey last =  new BufferDecoratedKey(boundaries.get(i+1), emptyBuffer);
            sstables.add(mockSSTable(level, bytesOnDisk, timestamp, 0., first, last));

            timestamp+=10;
        }

        return sstables;
    }

    CompactionProgress mockCompletedCompactionProgress(Set<SSTableReader> compacting, UUID id)
    {
        CompactionProgress progress = Mockito.mock(CompactionProgress.class);

        long compactingLen = totUncompressedLength(compacting);
        when(progress.operationId()).thenReturn(id);
        when(progress.inSSTables()).thenReturn(compacting);
        when(progress.uncompressedBytesRead()).thenReturn(compactingLen);
        when(progress.uncompressedBytesWritten()).thenReturn(compactingLen);
        when(progress.durationInNanos()).thenReturn(TimeUnit.SECONDS.toNanos(30));

        return progress;
    }

    void addSizeTieredOptions(Map<String, String> options)
    {
        addSizeTieredOptions(options, SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE);
    }

    void addSizeTieredOptions(Map<String, String> options, long minSSTableSize)
    {
        options.put(SizeTieredCompactionStrategyOptions.MIN_SSTABLE_SIZE_KEY, Long.toString(minSSTableSize));
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, Double.toString(SizeTieredCompactionStrategyOptions.DEFAULT_BUCKET_LOW));
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_HIGH_KEY, Double.toString(SizeTieredCompactionStrategyOptions.DEFAULT_BUCKET_HIGH));
    }

    void addTimeTieredOptions(Map<String, String> options)
    {
        addSizeTieredOptions(options, SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE);

        options.put(TimeWindowCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, TimeUnit.MILLISECONDS.toString());
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "30");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        options.put(TimeWindowCompactionStrategyOptions.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, Long.toString(Long.MAX_VALUE)); // disable check for expired sstables
    }

    void addLeveledOptions(Map<String, String> options, long maxSSTableSizeBytes)
    {
        addLeveledOptions(options, SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE, maxSSTableSizeBytes, 10);
    }

    void addLeveledOptions(Map<String, String> options, long minSSTableSizeBytes, long maxSSTableSizeBytes, int fanout)
    {
        addSizeTieredOptions(options, minSSTableSizeBytes);

        options.put(LeveledCompactionStrategy.SSTABLE_SIZE_OPTION, Long.toString(maxSSTableSizeBytes >> 20)); // Bytes to MB
        options.put(LeveledCompactionStrategy.LEVEL_FANOUT_SIZE_OPTION, Integer.toString(fanout));
    }

    long totUncompressedLength(Collection<SSTableReader> sstables)
    {
        long ret = 0;
        for (SSTableReader sstable : sstables)
            ret += sstable.uncompressedLength();

        return ret;
    }

    double totHotness(Collection<SSTableReader> sstables)
    {
        double ret = 0;
        for (SSTableReader sstable : sstables)
            ret += sstable.hotness();

        return ret;
    }

}
