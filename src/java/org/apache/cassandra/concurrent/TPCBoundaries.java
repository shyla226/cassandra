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
package org.apache.cassandra.concurrent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;

/**
 * Holds boundaries (tokens) used to map a particular token (so partition key) to a TPC core ID.
 * In practice, each keyspace has its associated boundaries, see {@link Keyspace}.
 * <p>
 * Technically, if we use {@code n} cores, this is a list of {@code n-1} tokens and each token {@code tk} gets assigned
 * to the core ID corresponding to the slot of the smallest token in the list that is greater to {@code tk}, or {@code n}
 * if {@code tk} is bigger than any token in the list.
 */
public class TPCBoundaries
{
    private static final Token[] EMPTY_TOKEN_ARRAY = new Token[0];

    // Special boundaries that map all tokens to core 0. These boundaries should only be used when the local ranges
    // aren't available:
    //  - StorageService not yet initialized and there were no sstables from which we could extract local ranges on startup.
    //  - the default partitioner in DD does not support splitting, e.g. OrderPreservingPartitioner
    public static TPCBoundaries NONE = new TPCBoundaries(EMPTY_TOKEN_ARRAY);

    // Special boundaries for LOCAL tables, for which we know all the data is local, so we can split
    // the entire partition range supported by the partitioner. We need these boundaries very early on during
    // startup and they do not change, therefore they are stored in this static field. The partitioner
    // needs to support the maximum token, but this is a requirement for partitioners with a splitter.
    public static final TPCBoundaries LOCAL = computeLocalRanges(DatabaseDescriptor.getPartitioner(), TPC.getNumCores());

    private final Token[] boundaries;

    private TPCBoundaries(Token[] boundaries)
    {
        this.boundaries = boundaries;
    }

    private static TPCBoundaries computeLocalRanges(IPartitioner partitioner, int numCores)
    {
        if (!partitioner.splitter().isPresent())
            return NONE;

        return compute(Collections.singletonList(new Range(partitioner.getMinimumToken(), partitioner.getMaximumToken())), numCores);
    }
    /**
     * Computes TPC boundaries for data distributed over the provided ranges.
     *
     * @param localRanges the ranges that must be distributed evenly on the available cores.
     * @param numCores the number of cores used.
     * @return the computed boundaries.
     */
    public static TPCBoundaries compute(List<Range<Token>> localRanges, int numCores)
    {
        assert numCores > 0;
        if (numCores == 1)
            return NONE;

        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        assert partitioner.splitter().isPresent() : partitioner.getClass().getName() + " doesn't support cpu boundary splitting";
        Splitter splitter = partitioner.splitter().get();
        List<Token> boundaries = splitter.splitOwnedRanges(numCores,
                                                           localRanges,
                                                           false);
        // Note that boundaries will contain up-to numCores tokens (it can contain less in very rare cases, but will contain
        // at least 1) and that the last token it always partitioner.getMaximumToken(). So in practice we want to use those
        // token we leave out the last one.
        return new TPCBoundaries(boundaries.subList(0, boundaries.size() - 1).toArray(EMPTY_TOKEN_ARRAY));
    }

    /**
     * Computes the core to use for the provided token.
     */
    int getCoreFor(Token tk)
    {
        for (int i = 0; i < boundaries.length; i++)
        {
            if (tk.compareTo(boundaries[i]) < 0)
                return i;
        }
        return boundaries.length;
    }

    /**
     * The number of cores that this boundaries support, that is how many different core ID {@link #getCoreFor} might
     * possibly return.
     *
     * @return the number of core supported by theses boundaries. This will always be strictly positive (it's at least 1)
     * and in practice it will be equal to {@link TPC#getNumCores()} for the boundaries of normal keyspace and equal to
     * 1 for the system keyspace.
     */
    public int supportedCores()
    {
        return boundaries.length + 1;
    }

    public List<Range<Token>> asRanges()
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>(boundaries.length + 1);
        Token left = partitioner.getMinimumToken();
        for (Token right : boundaries)
        {
            ranges.add(new Range<>(left, right));
            left = right;
        }
        ranges.add(new Range<>(left, partitioner.getMaximumToken()));
        return ranges;
    }

    @Override
    public String toString()
    {
        if (boundaries.length == 0)
            return "core 0: (min, max)";

        StringBuilder sb = new StringBuilder();
        sb.append("core 0: (min, ").append(boundaries[0]).append(") ");
        for (int i = 0; i < boundaries.length - 1; i++)
            sb.append("core ").append(i+1).append(": (").append(boundaries[i]).append(", ").append(boundaries[i+1]).append("] ");
        sb.append("core ").append(boundaries.length).append(": (").append(boundaries[boundaries.length-1]).append(", max)");
        return sb.toString();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TPCBoundaries that = (TPCBoundaries) o;

        return Arrays.equals(boundaries, that.boundaries);
    }

    public int hashCode()
    {
        return Arrays.hashCode(boundaries);
    }
}
