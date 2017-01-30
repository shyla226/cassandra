package org.apache.cassandra.index.internal;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * A custom index that uses a normal table to store index data.  This is based on the PartitionVertexTable index from DSE Graph.
 */
public class TableBackedCustomIndex implements Index
{
    private final String keyspace = "index_keyspace";
    private final String indexTable = "index_table";

    private final ColumnFamilyStore baseCfs;
    private IndexMetadata metadata;
    private ColumnFamilyStore indexCfs;
    private TableMetadata indexCfm;

    public TableBackedCustomIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef)
    {
        this.baseCfs = baseCfs;
        setMetadata(indexDef);
    }

    private void maybeInitializeIndexCfs()
    {
        if (indexCfs != null)
            return;

        indexCfs = Keyspace.open(keyspace).getColumnFamilyStore(indexTable);
    }

    public boolean shouldBuildBlocking()
    {
        return true;
    }

    public boolean dependsOn(ColumnMetadata column)
    {
        return false;
    }

    public void register(IndexRegistry registry)
    {
        registry.registerIndex(this);
    }

    public Callable<?> getInitializationTask()
    {
        return isBuilt() ? null : getBuildIndexTask();
    }

    public IndexMetadata getIndexMetadata()
    {
        return metadata;
    }

    public String getIndexName()
    {
        return metadata.name;
    }

    public Optional<ColumnFamilyStore> getBackingTable()
    {
        maybeInitializeIndexCfs();
        return indexCfs == null ? Optional.empty() : Optional.of(indexCfs);
    }

    public Callable<Void> getBlockingFlushTask()
    {
        maybeInitializeIndexCfs();
        return () -> {
            indexCfs.forceBlockingFlush();
            return null;
        };
    }

    public Callable<?> getInvalidateTask()
    {
        maybeInitializeIndexCfs();
        return () -> {
            indexCfs.invalidate();
            return null;
        };
    }

    public Callable<?> getMetadataReloadTask(IndexMetadata indexDef)
    {
        setMetadata(indexDef);
        maybeInitializeIndexCfs();
        return () -> {
            indexCfs.reload();
            return null;
        };
    }

    public AbstractType<?> customExpressionValueType()
    {
        return null;
    }

    private void setMetadata(IndexMetadata indexDef)
    {
        this.metadata = indexDef;
        this.indexCfm = buildIndexCFMetadata();
        MigrationManager.announceNewTable(this.indexCfm, true);
        indexCfs = Keyspace.openAndGetStore(this.indexCfm);
    }

    private TableMetadata buildIndexCFMetadata()
    {
        TableMetadata.Builder builder = TableMetadata.builder(keyspace, indexTable);

        for (ColumnMetadata partitionKeyColumn : baseCfs.metadata().partitionKeyColumns())
            builder.addPartitionKeyColumn(partitionKeyColumn.name, partitionKeyColumn.type);

        for (ColumnMetadata clusteringColumn : baseCfs.metadata().clusteringColumns())
            builder.addClusteringColumn(clusteringColumn.name, clusteringColumn.type);

        for (ColumnMetadata column : baseCfs.metadata().regularColumns())
            builder.addRegularColumn(column.name, column.type);

        return builder.build().updateIndexTableMetadata(baseCfs.metadata().params);
    }

    public Callable<?> getTruncateTask(final long truncatedAt) { return null; }

    public boolean indexes(RegularAndStaticColumns columns) { return true; }

    public boolean supportsExpression(ColumnMetadata column, Operator operator) { return false; }

    public long getEstimatedResultRows() { throw new UnsupportedOperationException(); }

    public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(ReadCommand command)
    {
        return (partitionIterator, rowFilter) -> partitionIterator;
    }

    public RowFilter getPostIndexQueryFilter(RowFilter filter) { return filter; }

    public Index.Searcher searcherFor(ReadCommand command) { throw new UnsupportedOperationException(); }

    public void validate(PartitionUpdate update) { }

    public Index.Indexer indexerFor(final DecoratedKey key,
                                    final RegularAndStaticColumns columns,
                                    final int nowInSec,
                                    final OpOrder.Group opGroup,
                                    final IndexTransaction.Type transactionType)
    {
        return new Index.Indexer()
        {
            public void begin()
            {
                maybeInitializeIndexCfs();
            }

            public void finish() { }

            public void insertRow(Row newBaseRow)
            {
                PartitionUpdate update = PartitionUpdate.singleRowUpdate(indexCfs.metadata(), key, newBaseRow);
                applyMutation(new Mutation(update));
            }

            public void partitionDelete(DeletionTime deletionTime) { }
            public void rangeTombstone(RangeTombstone tombstone) { }
            public void removeRow(Row row) { }
            public void updateRow(Row oldRow, Row newRow) { }

            private void applyMutation(Mutation mutation)
            {
                Keyspace ks = Keyspace.open(mutation.getKeyspaceName());
                // set writeCommitlog to false
                CompletableFuture<?> future = ks.applyFuture(mutation, false, true);

                if (future.isDone())
                    future.getNow(null);  // force exception to be thrown if this errored
            }
        };
    }

    private boolean isBuilt()
    {
        return SystemKeyspace.isIndexBuilt(baseCfs.keyspace.getName(), getIndexName());
    }

    private void markBuilt()
    {
        SystemKeyspace.setIndexBuilt(baseCfs.keyspace.getName(), getIndexName());
    }

    private Callable<?> getBuildIndexTask()
    {
        return () -> {
            maybeInitializeIndexCfs();
            baseCfs.forceBlockingFlush();
            markBuilt();
            return null;
        };
    }
}
