package org.apache.cassandra.index.internal;

import io.reactivex.Completable;
import org.apache.cassandra.utils.concurrent.OpOrder;
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

import java.util.Optional;
import java.util.concurrent.Callable;
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
        MigrationManager.announceNewTable(this.indexCfm, true).blockingAwait();
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

            public Completable finish()
            {
                return Completable.complete();
            }

            public Completable insertRow(Row newBaseRow)
            {
                PartitionUpdate update = PartitionUpdate.singleRowUpdate(indexCfs.metadata(), key, newBaseRow);
                return applyMutation(new Mutation(update));
            }

            public Completable partitionDelete(DeletionTime deletionTime)
            {
                return Completable.complete();
            }

            public Completable rangeTombstone(RangeTombstone tombstone)
            {
                return Completable.complete();
            }

            public Completable removeRow(Row row)
            {
                return Completable.complete();
            }

            public Completable updateRow(Row oldRow, Row newRow)
            {
                return Completable.complete();
            }

            private Completable applyMutation(Mutation mutation)
            {
                Keyspace ks = Keyspace.open(mutation.getKeyspaceName());
                // set writeCommitlog to false
                return ks.apply(mutation, false);
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
