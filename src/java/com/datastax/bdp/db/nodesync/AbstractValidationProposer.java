/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.units.Units;

/**
 * Simple helper class for common parts to all/most {@link ValidationProposer}'s implementation.
 * <p>
 * Note: {@link ValidationProposer} doesn't assume that a proposer is table-based, but all our current proposers are
 * so pulling that here.
 */
abstract class AbstractValidationProposer implements ValidationProposer
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractValidationProposer.class);

    protected final NodeSyncService service;
    protected final TableMetadata table;

    /** The 'depth' at which we spit segments for this table. See {@link Segments} and {@link Segments#depth} for details. */
    protected final int depth;

    AbstractValidationProposer(NodeSyncService service,
                               TableMetadata table,
                               int depth)
    {
        this.service = service;
        this.table = table;
        this.depth = depth;
    }

    static int computeDepth(ColumnFamilyStore store,
                            int localRangeCount,
                            long maxSegmentSize)
    {
        return Segments.depth(NodeSyncHelpers.estimatedSizeOf(store), localRangeCount, maxSegmentSize);
    }

    /**
     * The service for which this proposer was created.
     */
    public NodeSyncService service()
    {
        return service;
    }

    /**
     * The table for which this generates validation proposals.
     */
    public TableMetadata table()
    {
        return table;
    }

    protected Collection<Range<Token>> localRanges()
    {
        return NodeSyncHelpers.localRanges(table.keyspace);
    }

    // Function used for debug/trace logging, to display when was the last validation of a particular segment.
    static String timeSinceStr(long validationTime)
    {
        // Negative values for the validation time means we don't have a record for it
        return validationTime < 0
               ? "<not recorded>"
               : String.format("%s ago (%d)",
                               Units.toString(System.currentTimeMillis() - validationTime, TimeUnit.MILLISECONDS),
                               validationTime);
    }

    public ValidationProposer onTableRemoval(String keyspace, String table)
    {
        if (this.table.keyspace.equals(keyspace) && this.table.name.equals(table))
        {
            // Logging at debug because when you explicitly dropped a table, it doesn't feel like you'd care too much
            // about that confirmation. Further, when a keyspace is dropped, this is called for every table it has
            // and this would feel like log spamming if the keyspace has very many tables.
            logger.debug("Stopping NodeSync validations on table {} as the table has been dropped", table);
            return null;
        }
        return this;
    }
}
