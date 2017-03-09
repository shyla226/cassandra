package org.apache.cassandra.db.rows;

import java.util.Iterator;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.UnfilteredRowIterators.MergeListener;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A header for flowable partition containers. Contains partition-level data that isn't expected to change with
 * transformations, to avoid having to copy/repeat it every time a transformation is applied to the partition content.
 */
public class PartitionHeader
{
    /**
     * The metadata for the table this iterator on.
     */
    public final TableMetadata metadata;

    /**
     * Whether or not the rows returned by this iterator are in reversed
     * clustering order.
     */
    public final boolean isReverseOrder;

    /**
     * A subset of the columns for the (static and regular) rows returned by this iterator.
     * Every row returned by this iterator must guarantee that it has only those columns.
     */
    public final RegularAndStaticColumns columns;

    /**
     * The partition key of the partition this in an iterator over.
     */
    public final DecoratedKey partitionKey;

    /**
     * The partition level deletion for the partition this iterate over.
     */
    public DeletionTime partitionLevelDeletion;

    /**
     * Return "statistics" about what is returned by this iterator. Those are used for
     * performance reasons (for delta-encoding for instance) and code should not
     * expect those to be exact.
     */
    public EncodingStats stats;

    public PartitionHeader(TableMetadata metadata, DecoratedKey partitionKey, DeletionTime partitionLevelDeletion,
                           RegularAndStaticColumns columns, boolean isReverseOrder, EncodingStats stats)
    {
        super();
        this.metadata = metadata;
        this.partitionKey = partitionKey;
        this.partitionLevelDeletion = partitionLevelDeletion;
        this.columns = columns;
        this.isReverseOrder = isReverseOrder;
        this.stats = stats;
    }

    @Override
    public String toString()
    {
        String cfs = String.format("table %s.%s", metadata.keyspace, metadata.name);
        return String.format("partition key %s deletion %s %s", partitionKey, partitionLevelDeletion, cfs);
    }

    public PartitionHeader mergeWith(Iterator<PartitionHeader> sources)
    {
        if (!sources.hasNext())
            return this;

        Merger merger = new Merger(0, metadata, partitionKey, isReverseOrder, null);
        merger.add(0, this);

        while (sources.hasNext())
            merger.add(0, sources.next());

        return merger.merge();
    }

    static class Merger
    {
        /**
         * The metadata for the table this iterator on.
         */
        public final TableMetadata metadata;

        /**
         * The partition key of the partition this in an iterator over.
         */
        public final DecoratedKey partitionKey;

        /**
         * Whether or not the rows returned by this iterator are in reversed
         * clustering order.
         */
        public final boolean isReverseOrder;

        /**
         * A subset of the columns for the (static and regular) rows returned by this iterator.
         * Every row returned by this iterator must guarantee that it has only those columns.
         */
        public Columns statics, regulars;

        /**
         * The partition level deletion for the partition this iterate over.
         */
        public DeletionTime delTime;
        public DeletionTime[] delTimeVersions;

        /**
         * Return "statistics" about what is returned by this iterator. Those are used for
         * performance reasons (for delta-encoding for instance) and code should not
         * expect those to be exact.
         */
        public EncodingStats.Merger statsMerger;

        final MergeListener listener;

        public Merger(int size, TableMetadata metadata, DecoratedKey partitionKey, boolean reversed, MergeListener listener)
        {
            this.metadata = metadata;
            this.partitionKey = partitionKey;
            this.isReverseOrder = reversed;
            this.listener = listener;
            statics = Columns.NONE;
            regulars = Columns.NONE;
            delTime = DeletionTime.LIVE;
            if (listener != null)
                delTimeVersions = new DeletionTime[size];
        }

        public Merger(int size, int idx, PartitionHeader header, MergeListener listener)
        {
            this(size, header.metadata, header.partitionKey, header.isReverseOrder, listener);
            add(idx, header);
        }


        public void add(int idx, PartitionHeader source)
        {
            DeletionTime currDelTime = source.partitionLevelDeletion;
            if (!delTime.supersedes(currDelTime))
                delTime = currDelTime;
            if (listener != null)
                delTimeVersions[idx] = currDelTime;

            statics = statics.mergeTo(source.columns.statics);
            regulars = regulars.mergeTo(source.columns.regulars);

            EncodingStats stats = source.stats;
            if (!stats.equals(EncodingStats.NO_STATS))
            {
                if (statsMerger == null)
                    statsMerger = new EncodingStats.Merger(stats);
                else
                    statsMerger.mergeWith(stats);
            }

        }


        public PartitionHeader merge()
        {
            if (listener != null)
            {
                listener.onMergedPartitionLevelDeletion(delTime, delTimeVersions);
            }

            return new PartitionHeader(metadata, partitionKey, delTime,
                                       new RegularAndStaticColumns(statics, regulars), isReverseOrder,
                                       statsMerger != null ? statsMerger.get() : EncodingStats.NO_STATS);
        }
    }
}
