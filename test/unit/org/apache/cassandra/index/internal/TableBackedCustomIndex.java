package org.apache.cassandra.index.internal;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
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
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.MigrationManager;
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
    private CFMetaData indexCfm;

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

    public boolean dependsOn(ColumnDefinition column)
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
            indexCfs.metadata.reloadIndexMetadataProperties(baseCfs.metadata);
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
        MigrationManager.announceNewColumnFamily(this.indexCfm, true);
        indexCfs = Keyspace.openAndGetStore(this.indexCfm);
    }

    private CFMetaData buildIndexCFMetadata()
    {
        CFMetaData.Builder builder = CFMetaData.Builder.create(keyspace, indexTable);

        for (ColumnDefinition partitionKeyColumn : baseCfs.metadata.partitionKeyColumns())
                builder.addPartitionKey(partitionKeyColumn.name, partitionKeyColumn.type);

        for (ColumnDefinition clusteringColumn : baseCfs.metadata.clusteringColumns())
            builder.addClusteringColumn(clusteringColumn.name, clusteringColumn.type);

        for (ColumnDefinition column : baseCfs.metadata.allColumns())
        {
            if (column.isRegular())
                builder.addRegularColumn(column.name, column.type);
        }

        CFMetaData metadata = builder.build();
        metadata.reloadIndexMetadataProperties(baseCfs.metadata);
        return metadata;
    }

    public Callable<?> getTruncateTask(final long truncatedAt) { return null; }

    public boolean indexes(PartitionColumns columns) { return true; }

    public boolean supportsExpression(ColumnDefinition column, Operator operator) { return false; }

    public long getEstimatedResultRows() { throw new UnsupportedOperationException(); }

    public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(ReadCommand command)
    {
        return (partitionIterator, rowFilter) -> partitionIterator;
    }

    public RowFilter getPostIndexQueryFilter(RowFilter filter) { return filter; }

    public Index.Searcher searcherFor(ReadCommand command) { throw new UnsupportedOperationException(); }

    public void validate(PartitionUpdate update) { }

    public Index.Indexer indexerFor(final DecoratedKey key,
                                    final PartitionColumns columns,
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
                PartitionUpdate update = PartitionUpdate.singleRowUpdate(indexCfs.metadata, key, newBaseRow);
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
