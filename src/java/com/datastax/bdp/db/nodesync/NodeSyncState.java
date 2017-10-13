/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Holds the state of NodeSync for the table on which NodeSync is enabled, and for the range replicated on this node.
 * <p>
 * This is largely an in-memory representation of the parts of the {@link SystemDistributedKeyspace#NODESYNC_STATUS}
 * table that this node is interested in. Of course, as said table is also updated by other nodes, this will not always
 * be an in-sync representation, and more precisely the information stored here can be stale temporarily.
 * <p>
 * The NodeSync process on this node uses that state to decide on which ({@link ValidationProposal}) to submit next,
 * but when such proposal is considered, the {@link SystemDistributedKeyspace#NODESYNC_STATUS} is checked for updates
 * made by other nodes: if there has been such update, it means the segment we're considering has been repaired by
 * another node recently and so we update this state and generates an updated proposal for that segment based on the
 * updated information. If there is no update by other node, the segment is validated by this node, after which
 * both the {@link SystemDistributedKeyspace#NODESYNC_STATUS} table and this state are updated.
 * <p>
 * Basically, the guarantee is that while the state might be pessimistic (indicates that some range hasn't been validated
 * for a longer time than in reality), it is never optimistic (never indicates that a range has been validated at a
 * particular time if it hadn't).
 * <p>
 * Note that this global state is mostly a map of tables to their respective {@link TableState}, and {@link TableState}
 * is where most of the fun happens.
 */
class NodeSyncState
{
    private static final Logger logger = LoggerFactory.getLogger(NodeSyncState.class);

    private final NodeSyncService service;

    /**
     * A loading "cache" of the table state. If a table is being validated by NodeSync, the {@link ValidationProposer}
     * will keep a reference to that table state. If it's not validated, we don't want to keep the state in memory,
     * hence the user of weak values.
     */
    private final LoadingCache<TableId, TableState> tableStates = CacheBuilder.newBuilder()
                                                                              .weakValues()
                                                                              .build(CacheLoader.from(this::load));

    NodeSyncState(NodeSyncService service)
    {
        this.service = service;
    }

    /**
     * Get a reference to the state for a particular table, or {@code null} if said state is not loaded (typically
     * because the table isn't NodeSync-enabled).
     *
     * @param table the table for which to get the state.
     * @return the state for {@code table} or {@code null} if that state is not in memory.
     */
    @Nullable
    public TableState get(TableMetadata table)
    {
        return tableStates.getIfPresent(table.id);
    }

    /**
     * Get the sate for a particular table. If said state is not loaded in memory yet, load it and return the loaded
     * state. Note that once the reference returned by this method becomes unreachable, the state may get unloaded,
     * so caller must keep that reference if they want to guarantee the table state stick in memory.
     *
     * @param table the table for which to get the state.
     * @return the state for {@code table}. Will never return {@code null} as this load the state if necessary, _even_
     * if the table is not NodeSync-enabled.
     *
     * @throws UnknownTableException if the state has been loaded, but cannot because the {@link ColumnFamilyStore} for
     * the table cannot be retrieved (which basically means the call has raced with a DROP of either the table or its
     * whole keyspace).
     */
    TableState getOrLoad(TableMetadata table)
    {
        try
        {
            return tableStates.get(table.id);
        }
        catch (ExecutionException e)
        {
            if (e.getCause() instanceof UnknownTableException)
                throw (UnknownTableException)e.getCause();
            throw Throwables.propagate(e.getCause());
        }
    }

    private TableState load(TableId id)
    {
        ColumnFamilyStore table = Schema.instance.getColumnFamilyStoreInstance(id);
        // We could be racing with a DROP
        if (table == null)
            throw new UnknownTableException(id);

        TableMetadata metadata = table.metadata();
        Collection<Range<Token>> localRanges = NodeSyncHelpers.localRanges(metadata.keyspace);
        int depth = Segments.depth(table, localRanges.size());
        return TableState.load(service, table.metadata(), localRanges, depth);
    }
}
