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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowSource;
import org.apache.cassandra.utils.flow.FlowSubscriber;
import org.apache.cassandra.utils.flow.FlowSubscription;
import org.apache.cassandra.utils.flow.FlowSubscriptionRecipient;

/**
 * Asynchronous version of {@link SSTableScanner}.
 */
public class AsyncSSTableScanner extends FlowSource<FlowableUnfilteredPartition> implements FlowSubscriptionRecipient
{
    private final SSTableReader sstable;
    private final RandomAccessReader dfile;
    private final List<AbstractBounds<PartitionPosition>> ranges;
    private final ColumnFilter columns;
    private final DataRange dataRange;
    private final SSTableReadsListener listener;
    private final Flow<FlowableUnfilteredPartition> sourceFlow;

    private FlowSubscription source;

    private AsyncSSTableScanner(SSTableReader sstable,
                                ColumnFilter columns,
                                DataRange dataRange,
                                List<AbstractBounds<PartitionPosition>> ranges,
                                SSTableReadsListener listener)
    {
        assert sstable != null;

        this.sstable = sstable;
        this.dfile = sstable.openDataReader(Rebufferer.ReaderConstraint.IN_CACHE_ONLY);
        this.columns = columns;
        this.dataRange = dataRange;
        this.ranges = ranges;
        this.listener = listener;
        this.sourceFlow = flow();
    }

    public static AsyncSSTableScanner getScanner(SSTableReader sstable)
    {
        return getScanner(sstable, Collections.singletonList(SSTableScanner.fullRange(sstable)));
    }

    public static AsyncSSTableScanner getScanner(SSTableReader sstable, Collection<Range<Token>> ranges)
    {
        return getScanner(sstable, SSTableScanner.makeBounds(sstable, ranges));
    }

    public static AsyncSSTableScanner getScanner(SSTableReader sstable, List<AbstractBounds<PartitionPosition>> bounds)
    {
        return new AsyncSSTableScanner(sstable,
                                       ColumnFilter.all(sstable.metadata()),
                                       null,
                                       bounds,
                                       SSTableReadsListener.NOOP_LISTENER);
    }

    public static AsyncSSTableScanner getScanner(SSTableReader sstable,
                                                 ColumnFilter columns,
                                                 DataRange dataRange,
                                                 SSTableReadsListener listener)
    {
        return new AsyncSSTableScanner(sstable, columns, dataRange, SSTableScanner.makeBounds(sstable, dataRange), listener);
    }


    private Flow<FlowableUnfilteredPartition> flow()
    {
        if (ranges.size() == 1)
        {
            return sstable.coveredKeysFlow(dfile, ranges.get(0))
                          .flatMap(this::partitions);
        }
        else
        {
            return Flow.fromIterable(ranges)
                       .flatMap(range -> sstable.coveredKeysFlow(dfile, range))
                       .flatMap(this::partitions);
        }
    }

    private Flow<FlowableUnfilteredPartition> partitions(IndexFileEntry entry)
    {
        if (dataRange == null)
        {
            return sstable.flow(entry, dfile, listener);
        }
        else
        {
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(entry.key);
            return sstable.flow(entry, dfile, filter.getSlices(sstable.metadata()), columns, filter.isReversed(), listener);
        }
    }

    public void onSubscribe(FlowSubscription source)
    {
        this.source = source;
    }

    public void requestFirst(FlowSubscriber<FlowableUnfilteredPartition> subscriber, FlowSubscriptionRecipient subscriptionRecipient)
    {
        listener.onScanningStarted(sstable);
        subscribe(subscriber, subscriptionRecipient);

        sourceFlow.requestFirst(subscriber, this);
    }

    public void requestNext()
    {
        source.requestNext();
    }

    public void close() throws Exception
    {
        Throwables.maybeFail(Throwables.closeNonNull(null, dfile, source));
    }

    public String toString()
    {
        return Flow.formatTrace(getClass().getSimpleName(), source);
    }
}
