/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.nodesync;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
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

    // For testing, we abstract how the size on disk of a table is computed, so this is the function we use in real code.
    protected static final ToLongFunction<ColumnFamilyStore> DEFAULT_TABLE_SIZE_PROVIDER = t -> t.getMemtablesLiveSize() + t.metric.liveDiskSpaceUsed.getCount();
    // For testing, we want to fake the local range of a keyspace so that's the function we use in real code.
    protected static final Function<String, Collection<Range<Token>>> DEFAULT_LOCAL_RANGES_PROVIDER = StorageService.instance::getLocalRanges;

    protected final NodeSyncService service;
    protected final TableMetadata table;

    /** The 'depth' at which we spit segments for this table. See {@link Segments} and {@link Segments#depth} for details. */
    protected final int depth;

    // Only exists for unit tests purposes, always DEFAULT_LOCAL_RANGES_PROVIDER otherwise.
    private final Function<String, Collection<Range<Token>>> localRangesProvider;
    // Only exists for unit tests purposes, always DEFAULT_TABLE_SIZE_PROVIDER otherwise.
    private final ToLongFunction<ColumnFamilyStore> tableSizeProvider;

    AbstractValidationProposer(NodeSyncService service,
                               TableMetadata table,
                               int depth,
                               Function<String, Collection<Range<Token>>> localRangesProvider,
                               ToLongFunction<ColumnFamilyStore> tableSizeProvider)
    {
        this.service = service;
        this.table = table;
        this.depth = depth;
        this.localRangesProvider = localRangesProvider;
        this.tableSizeProvider = tableSizeProvider;
    }

    static int computeDepth(ColumnFamilyStore store,
                            int localRangeCount,
                            ToLongFunction<ColumnFamilyStore> tableSizeProvider,
                            long maxSegmentSize)
    {
        return Segments.depth(tableSizeProvider.applyAsLong(store), localRangeCount, maxSegmentSize);
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
    TableMetadata table()
    {
        return table;
    }

    protected Collection<Range<Token>> localRanges()
    {
        return localRangesProvider.apply(table.keyspace);
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
