package org.apache.cassandra.db.rows;

import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.UnfilteredRowIterators.MergeListener;

public class PartitionHeader implements Unfiltered
{
    /**
     * The metadata for the table this iterator on.
     */
    public final CFMetaData metadata;

    /**
     * Whether or not the rows returned by this iterator are in reversed
     * clustering order.
     */
    public final boolean isReverseOrder;

    /**
     * A subset of the columns for the (static and regular) rows returned by this iterator.
     * Every row returned by this iterator must guarantee that it has only those columns.
     */
    public final PartitionColumns columns;

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

    // static row is in stream

    public PartitionHeader(CFMetaData metadata, DecoratedKey partitionKey, DeletionTime partitionLevelDeletion,
            PartitionColumns columns, boolean isReverseOrder, EncodingStats stats)
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
    public ClusteringPrefix clustering()
    {
        return Clustering.HEADER_CLUSTERING;
    }

    @Override
    public Kind kind()
    {
        return Kind.HEADER;
    }

    @Override
    public void digest(MessageDigest digest)
    {
        // Not used in digest
    }

    @Override
    public void validateData(CFMetaData metadata)
    {
        // TODO: No validation fine?
    }

    @Override
    public String toString(CFMetaData metadata)
    {
        return toString(metadata, false, false);
    }

    @Override
    public String toString(CFMetaData metadata, boolean fullDetails)
    {
        return toString(metadata, false, fullDetails);
    }

    @Override
    public String toString(CFMetaData metadata, boolean includeClusterKeys, boolean fullDetails)
    {
        String cfs = "";
        if (fullDetails)
            cfs = String.format("table %s.%s", metadata.ksName, metadata.cfName);
        return String.format("partition %skey %s deletion %s", cfs, partitionKey, partitionLevelDeletion);
    }

    
    static class Merger
    {
        /**
         * The metadata for the table this iterator on.
         */
        public final CFMetaData metadata;

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

        public Merger(int size, CFMetaData metadata, DecoratedKey partitionKey, boolean reversed, MergeListener listener)
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
                                       new PartitionColumns(statics, regulars), isReverseOrder,
                                       statsMerger != null ? statsMerger.get() : EncodingStats.NO_STATS);
        }
    }
}
