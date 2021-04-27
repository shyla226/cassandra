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
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.utils.FBUtilities;

/**
 * The statistics for size tiered compaction.
 * <p/>
 * Implements serializable to allow structured info to be returned via JMX.
 */
public class UnifiedCompactionStatistics extends CompactionAggregateStatistics
{
    private static final Collection<String> HEADER = ImmutableList.copyOf(Iterables.concat(ImmutableList.of("Bucket", "W", "T", "F", "min size", "max size"),
                                                                                           CompactionAggregateStatistics.HEADER,
                                                                                           ImmutableList.of("Tot/Read/Written")));

    private static final long serialVersionUID = 3695927592357345266L;

    /** The bucket S */
    private final int bucket;

    /** The survival factor o */
    private final double o;

    /** The survival factor W */
    private final int W;

    /** The number of sorted runs T that trigger a compaction */
    private final int T;

    /** The fanout size F */
    private final int F;

    /** The minimum size for a sorted run that belongs to this bucket */
    private final long minSizeBytes;

    /** The maximum size for a sorted run that belongs to this bucket */
    private final long maxSizeBytes;

    /** The name of the shard */
    private final String shard;

    /** Total uncompressed bytes of the sstables */
    protected final long tot;

    /** Total bytes read by ongoing compactions */
    protected final long read;

    /** Total bytes written by ongoing compactions */
    protected final long written;

    UnifiedCompactionStatistics(int bucketIndex,
                                double o,
                                int W,
                                int T,
                                int F,
                                long minSizeBytes,
                                long maxSizeBytes,
                                String shard,
                                int numCompactions,
                                int numCompactionsInProgress,
                                int numSSTables,
                                int numCandidateSSTables,
                                int numCompactingSSTables,
                                long sizeInBytes,
                                double readThroughput,
                                double writeThroughput,
                                long tot,
                                long read,
                                long written)
    {
        super(numCompactions, numCompactionsInProgress, numSSTables, numCandidateSSTables, numCompactingSSTables, sizeInBytes, readThroughput, writeThroughput);

        this.bucket = bucketIndex;
        this.o = o;
        this.W = W;
        this.T = T;
        this.F = F;
        this.minSizeBytes = minSizeBytes;
        this.maxSizeBytes = maxSizeBytes;
        this.shard = shard;
        this.tot = tot;
        this.read = read;
        this.written = written;
    }

    /** The bucket Number */
    @JsonProperty
    public int bucket()
    {
        return bucket;
    }

    /** The survival factor o, currently always one */
    @JsonProperty
    public double o()
    {
        return o;
    }

    /** The survival factor W */
    @JsonProperty
    public int W()
    {
        return W;
    }

    /** The number of sorted runs T that trigger a compaction */
    @JsonProperty
    public int T()
    {
        return T;
    }

    /** The fanout size F */
    @JsonProperty
    public int F()
    {
        return F;
    }

    /** The minimum size for a sorted run that belongs to this bucket */
    @JsonProperty
    public long minSizeBytes()
    {
        return minSizeBytes;
    }

    /** The maximum size for a sorted run that belongs to this bucket */
    @JsonProperty
    public long maxSizeBytes()
    {
        return maxSizeBytes;
    }

    /** Total uncompressed bytes of the sstables */
    @JsonProperty
    public long tot()
    {
        return tot;
    }

    /** Uncompressed bytes read by compactions so far. */
    @JsonProperty
    public long read()
    {
        return read;
    }

    /** Uncompressed  bytes written by compactions so far. */
    @JsonProperty
    public long written()
    {
        return written;
    }

    /** The name of the shard, empty if the compaction is not sharded (the default). */
    @JsonProperty
    @Override
    public String shard()
    {
        return shard;
    }

    @Override
    protected Collection<String> header()
    {
        return HEADER;
    }

    @Override
    protected Collection<String> data()
    {
        List<String> data = new ArrayList<>(HEADER.size());
        data.add(Integer.toString(bucket()));
        data.add(Integer.toString(W));
        data.add(Integer.toString(T));
        data.add(Integer.toString(F));
        data.add(FBUtilities.prettyPrintMemory(minSizeBytes));
        data.add(FBUtilities.prettyPrintMemory(maxSizeBytes));

        data.addAll(super.data());

        data.add(toString(tot) + '/' + toString(read) + '/' + toString(written));
        return data;
    }
}