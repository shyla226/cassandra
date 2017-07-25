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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowSubscriber;
import org.apache.cassandra.utils.flow.FlowSubscription;

/**
 * A filter selecting rows given their clustering value.
 */
public class ClusteringIndexNamesFilter extends AbstractClusteringIndexFilter
{
    static final InternalDeserializer deserializer = new NamesDeserializer();

    // This could be empty if selectedColumns only has static columns (in which case the filter still
    // selects the static row)
    private final NavigableSet<Clustering> clusterings;

    // clusterings is always in clustering order (because we need it that way in some methods), but we also
    // sometimes need those clustering in "query" order (i.e. in reverse clustering order if the query is
    // reversed), so we keep that too for simplicity.
    private final NavigableSet<Clustering> clusteringsInQueryOrder;

    public ClusteringIndexNamesFilter(NavigableSet<Clustering> clusterings, boolean reversed)
    {
        super(reversed);
        assert !clusterings.contains(Clustering.STATIC_CLUSTERING);
        this.clusterings = clusterings;
        this.clusteringsInQueryOrder = reversed ? clusterings.descendingSet() : clusterings;
    }

    /**
     * The set of requested rows.
     *
     * Please note that this can be empty if only the static row is requested.
     *
     * @return the set of requested clustering in clustering order (note that
     * this is always in clustering order even if the query is reversed).
     */
    public NavigableSet<Clustering> requestedRows()
    {
        return clusterings;
    }

    public boolean selectsAllPartition()
    {
        return clusterings.isEmpty();
    }

    public boolean selects(Clustering clustering)
    {
        return clusterings.contains(clustering);
    }

    public ClusteringIndexNamesFilter forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean inclusive)
    {
        NavigableSet<Clustering> newClusterings = reversed ?
                                                  clusterings.headSet(lastReturned, inclusive) :
                                                  clusterings.tailSet(lastReturned, inclusive);

        return new ClusteringIndexNamesFilter(newClusterings, reversed);
    }

    public boolean isFullyCoveredBy(CachedPartition partition)
    {
        if (partition.isEmpty())
            return false;

        // 'partition' contains all columns, so it covers our filter if our last clusterings
        // is smaller than the last in the cache
        return clusterings.comparator().compare(clusterings.last(), partition.lastRow().clustering()) <= 0;
    }

    public boolean isHeadFilter()
    {
        return false;
    }

    private Row filterNotIndexedStaticRow(ColumnFilter columnFilter, TableMetadata metadata, Row row)
    {
        return columnFilter.fetchedColumns().statics.isEmpty() ? Rows.EMPTY_STATIC_ROW : row.filter(columnFilter, metadata);
    }

    private Unfiltered filterNotIndexedRow(ColumnFilter columnFilter, TableMetadata metadata, Unfiltered unfiltered)
    {
        if (unfiltered instanceof Row)
        {
            Row row = (Row)unfiltered;
            return clusterings.contains(row.clustering()) ? row.filter(columnFilter, metadata) : null;
        }

        return unfiltered;
    }

    public FlowableUnfilteredPartition filterNotIndexed(ColumnFilter columnFilter, FlowableUnfilteredPartition partition)
    {
        return new FlowableUnfilteredPartition(partition.header,
                                               filterNotIndexedStaticRow(columnFilter, partition.metadata(), partition.staticRow),
                                               partition.content.skippingMap(row -> filterNotIndexedRow(columnFilter, partition.metadata(), row)));
    }

    public Slices getSlices(TableMetadata metadata)
    {
        Slices.Builder builder = new Slices.Builder(metadata.comparator, clusteringsInQueryOrder.size());
        for (Clustering clustering : clusteringsInQueryOrder)
            builder.add(Slice.make(clustering));
        return builder.build();
    }

    public UnfilteredRowIterator getUnfilteredRowIterator(final ColumnFilter columnFilter, final Partition partition)
    {
        final Iterator<Clustering> clusteringIter = clusteringsInQueryOrder.iterator();
        final SearchIterator<Clustering, Row> searcher = partition.searchIterator(columnFilter, reversed);

        return new AbstractUnfilteredRowIterator(partition.metadata(),
                                                 partition.partitionKey(),
                                                 partition.partitionLevelDeletion(),
                                                 columnFilter.fetchedColumns(),
                                                 searcher.next(Clustering.STATIC_CLUSTERING),
                                                 reversed,
                                                 partition.stats())
        {
            protected Unfiltered computeNext()
            {
                while (clusteringIter.hasNext())
                {
                    Row row = searcher.next(clusteringIter.next());
                    if (row != null)
                        return row;
                }
                return endOfData();
            }
        };
    }

    public FlowableUnfilteredPartition getFlowableUnfilteredPartition(ColumnFilter columnFilter, Partition partition)
    {
        final SearchIterator<Clustering, Row> searcher = partition.searchIterator(columnFilter, reversed);
        return new FlowableUnfilteredPartition(new PartitionHeader(partition.metadata(),
                                                                   partition.partitionKey(),
                                                                   partition.partitionLevelDeletion(),
                                                                   columnFilter.fetchedColumns(),
                                                                   reversed,
                                                                   partition.stats()),
                                               searcher.next(Clustering.STATIC_CLUSTERING),
                                               new Flow<Unfiltered>() {
                                                   final Iterator<Clustering> clusteringIter = clusteringsInQueryOrder.iterator();
                                                   public FlowSubscription subscribe(FlowSubscriber<Unfiltered> subscriber) throws Exception
                                                   {
                                                       return new FlowSubscription()
                                                       {
                                                           public void request()
                                                           {
                                                               while (clusteringIter.hasNext())
                                                               {
                                                                   Row row = searcher.next(clusteringIter.next());
                                                                   if (row != null)
                                                                   {
                                                                       subscriber.onNext(row);
                                                                       return;
                                                                   }
                                                               }

                                                               subscriber.onComplete();
                                                           }

                                                           public void close() throws Exception
                                                           {

                                                           }

                                                           public Throwable addSubscriberChainFromSource(Throwable throwable)
                                                           {
                                                               return Flow.wrapException(throwable, this);
                                                           }
                                                       };
                                                   }
                                               });
    }

    public boolean shouldInclude(SSTableReader sstable)
    {
        ClusteringComparator comparator = sstable.metadata().comparator;
        List<ByteBuffer> minClusteringValues = sstable.getSSTableMetadata().minClusteringValues;
        List<ByteBuffer> maxClusteringValues = sstable.getSSTableMetadata().maxClusteringValues;

        // If any of the requested clustering is within the bounds covered by the sstable, we need to include the sstable
        for (Clustering clustering : clusterings)
        {
            if (Slice.make(clustering).intersects(comparator, minClusteringValues, maxClusteringValues))
                return true;
        }
        return false;
    }

    public String toString(TableMetadata metadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("names(");
        int i = 0;
        for (Clustering clustering : clusterings)
            sb.append(i++ == 0 ? "" : ", ").append(clustering.toString(metadata));
        if (reversed)
            sb.append(", reversed");
        return sb.append(')').toString();
    }

    public String toCQLString(TableMetadata metadata)
    {
        if (metadata.clusteringColumns().isEmpty() || clusterings.size() <= 1)
            return "";

        StringBuilder sb = new StringBuilder();
        sb.append('(').append(ColumnMetadata.toCQLString(metadata.clusteringColumns())).append(')');
        sb.append(clusterings.size() == 1 ? " = " : " IN (");
        int i = 0;
        for (Clustering clustering : clusterings)
            sb.append(i++ == 0 ? "" : ", ").append("(").append(clustering.toCQLString(metadata)).append(")");
        sb.append(clusterings.size() == 1 ? "" : ")");

        appendOrderByToCQLString(metadata, sb);
        return sb.toString();
    }

    public Kind kind()
    {
        return Kind.NAMES;
    }

    protected void serializeInternal(DataOutputPlus out, ReadVersion version) throws IOException
    {
        ClusteringComparator comparator = (ClusteringComparator)clusterings.comparator();
        out.writeUnsignedVInt(clusterings.size());
        for (Clustering clustering : clusterings)
            Clustering.serializer.serialize(clustering, out, version.encodingVersion.clusteringVersion, comparator.subtypes());
    }

    protected long serializedSizeInternal(ReadVersion version)
    {
        ClusteringComparator comparator = (ClusteringComparator)clusterings.comparator();
        long size = TypeSizes.sizeofUnsignedVInt(clusterings.size());
        for (Clustering clustering : clusterings)
            size += Clustering.serializer.serializedSize(clustering, version.encodingVersion.clusteringVersion, comparator.subtypes());
        return size;
    }

    private static class NamesDeserializer implements InternalDeserializer
    {
        public ClusteringIndexFilter deserialize(DataInputPlus in, ReadVersion version, TableMetadata metadata, boolean reversed) throws IOException
        {
            ClusteringComparator comparator = metadata.comparator;
            BTreeSet.Builder<Clustering> clusterings = BTreeSet.builder(comparator);
            int size = (int)in.readUnsignedVInt();
            for (int i = 0; i < size; i++)
                clusterings.add(Clustering.serializer.deserialize(in, version.encodingVersion.clusteringVersion, comparator.subtypes()));

            return new ClusteringIndexNamesFilter(clusterings.build(), reversed);
        }
    }
}
