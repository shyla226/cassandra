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

package org.apache.cassandra.test.microbench;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.reactivex.Single;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.TWCSMultiWriter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1
       , jvmArgsAppend = {"-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"
       ,"-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder","-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints",
        "-XX:StartFlightRecording=duration=60s,filename=./profiling-data.jfr,name=profile,settings=profile",
       // "-XX:FlightRecorderOptions=settings=/home/jake/workspace/cassandra/profiling-advanced.jfc,samplethreads=true"
     }
)
@Threads(1)
@State(Scope.Benchmark)
public class TWCSSplitPartitionBench extends CQLTester
{
    static
    {
        DatabaseDescriptor.clientInitialization(false);
        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    static String keyspace = "keyspace1";

    @Param({"10", "100", "1000", "10000"})
    int rows;

    private ColumnFamilyStore cfs;
    private MBROnHeapUnfilteredPartitions cachedPartition;
    private UnfilteredRowIterator rowToSplit;
    private final TWCSMultiWriter.BucketIndexer indexes = TWCSMultiWriter.createBucketIndexes(TimeUnit.MINUTES, 1);
    @Setup
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s (id int, id2 int, t text, PRIMARY KEY(id, id2))");
        execute("use " + keyspace + ";");
        Random random = new Random();
        long [] timestamps = new long[12];
        int x = 0;
        for (long ts : indexes.buckets)
            timestamps[x++] = ts;

        for (int i = 0; i < rows; i++)
            execute(String.format("insert into %s (id, id2, t) values (1, %d, 'muuh') USING TIMESTAMP %d", table, i, timestamps[random.nextInt(10)]));
        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        Range<Token> r = new Range<>(cfs.getPartitioner().getMinimumToken(), cfs.getPartitioner().getMaximumToken());
        DataRange dr = new DataRange(Range.makeRowRange(r), new ClusteringIndexSliceFilter(Slices.ALL, false));
        PartitionRangeReadCommand rc = new PartitionRangeReadCommand(cfs.metadata(),
                                                                     FBUtilities.nowInSeconds(),
                                                                     ColumnFilter.all(cfs.metadata()),
                                                                     RowFilter.NONE,
                                                                     DataLimits.NONE,
                                                                     dr,
                                                                     Optional.empty());


        try (ReadExecutionController executionController = rc.executionController();
             UnfilteredPartitionIterator pi = rc.executeLocally(executionController).blockingGet())
        {
            cachedPartition = new MBROnHeapUnfilteredPartitions(pi);
        }
    }

    @Setup(Level.Invocation)
    public void setupIter()
    {
        rowToSplit = cachedPartition.iterator().next().blockingGet();
    }

    @Benchmark
    public void split() throws IOException
    {
        TWCSMultiWriter.splitPartitionOnTime(rowToSplit, indexes, new TWCSMultiWriter.TWCSConfig(TimeUnit.MINUTES, 1, TimeUnit.MILLISECONDS));
    }

    public static void main(String... args) throws Exception {
        Options opts = new OptionsBuilder()
                       .include(".*" + TWCSSplitPartitionBench.class.getSimpleName() + ".*")
                       .jvmArgs("-server")
                       .forks(1)
                       .mode(Mode.Throughput)
                       .addProfiler(StackProfiler.class)
                       .build();

        Collection<RunResult> records = new Runner(opts).run();
        for ( RunResult result : records) {
            Result r = result.getPrimaryResult();
            System.out.println("API replied benchmark score: "
                               + r.getScore() + " "
                               + r.getScoreUnit() + " over "
                               + r.getStatistics().getN() + " iterations");
        }
    }


    static class MBROnHeapUnfilteredPartitions
    {
        private final TableMetadata metadata;
        private final List<MBRUnfilteredRowHolder> rows = new ArrayList<>();
        private final UnfilteredPartitionIterator iter;

        MBROnHeapUnfilteredPartitions(UnfilteredPartitionIterator iter)
        {
            metadata = iter.metadata();
            this.iter = iter;
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator row = iter.next().blockingGet())
                {
                    List<Unfiltered> content = new ArrayList<>();
                    while (row.hasNext())
                        content.add(row.next());

                    rows.add(new MBRUnfilteredRowHolder(metadata, row.partitionKey(), row.partitionLevelDeletion(), row.columns(), row.staticRow(), row.isReverseOrder(), row.stats(), content));
                }
            }
        }

        public UnfilteredPartitionIterator iterator()
        {
            return new MBROnHeapUnfilteredPartitionIterator(metadata, iter, rows);
        }

        private static class MBROnHeapUnfilteredPartitionIterator extends AbstractIterator<Single<UnfilteredRowIterator>> implements UnfilteredPartitionIterator
        {
            private final TableMetadata metadata;
            private final Iterator<MBRUnfilteredRowHolder> it;

            private MBROnHeapUnfilteredPartitionIterator(TableMetadata metadata, UnfilteredPartitionIterator iter, List<MBRUnfilteredRowHolder> rows)
            {
                this.metadata = metadata;
                it = rows.iterator();
            }

            public TableMetadata metadata()
            {
                return metadata;
            }

            public void close()
            {
            }

            protected Single<UnfilteredRowIterator> computeNext()
            {
                if (it.hasNext())
                    return Single.just(it.next().iterator());
                return endOfData();
            }
        }

        private static class MBRUnfilteredRowHolder
        {
            private final TableMetadata metadata;
            private final DecoratedKey key;
            private final DeletionTime partitionLevelDeletion;
            private final RegularAndStaticColumns partitionColumns;
            private final Row staticRow;
            private final boolean reversed;
            private final EncodingStats stats;
            private final List<Unfiltered> content;

            public MBRUnfilteredRowHolder(TableMetadata metadata, DecoratedKey key, DeletionTime partitionLevelDeletion, RegularAndStaticColumns partitionColumns, Row staticRow, boolean reversed, EncodingStats stats, List<Unfiltered> content)
            {
                this.metadata = metadata;
                this.key = key;
                this.partitionLevelDeletion = partitionLevelDeletion;
                this.partitionColumns = partitionColumns;
                this.staticRow = staticRow;
                this.reversed = reversed;
                this.stats = stats;
                this.content = content;
            }

            public UnfilteredRowIterator iterator()
            {
                return new AbstractUnfilteredRowIterator(metadata, key, partitionLevelDeletion, partitionColumns, staticRow, reversed, stats)
                {
                    Iterator<Unfiltered> iterator = content.iterator();
                    protected Unfiltered computeNext()
                    {
                        if (iterator.hasNext())
                            return iterator.next();
                        return endOfData();
                    }
                };
            }
        }
    }
}
