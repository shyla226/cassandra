/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 *
 */
package com.datastax.bdp.db.upgrade;

import java.util.Collections;
import java.util.List;

import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * {@link VersionDependentFeature Version dependent features} that require DDL changes
 * should use this helper class to perform schema updates.
 * <p>
 * Schema updates are restricted to the following:
 * <ul>
 * <li>create the keyspace (only if it does not exist),</li>
 * <li>create entire tables (if does not exist),</li>
 * <li>add non-existing columns.</li>
 * </ul>
 * The code does not remove any columns nor does it drop tables or even keyspaces.
 * All schema changes are applied with a single schema-mutation.
 */
public class SchemaUpgrade
{
    private final KeyspaceMetadata keyspaceMetadata;
    private final List<TableMetadata> tables;
    private final boolean schemaChangeUseCurrentTimestamp;

    /**
     * @param keyspaceMetadata                the keyspace to create if it does not already exist.
     *                                        Parameters of an existing keyspace will not be modifed.
     * @param tables                          the tables to create. If those tables already exist, missing
     *                                        columns will be added.
     * @param schemaChangeUseCurrentTimestamp whether to use the current timestamp for schema modifications
     *                                        or {@code 0}, which is used for distributed system keyspaces.
     */
    public SchemaUpgrade(KeyspaceMetadata keyspaceMetadata,
                         List<TableMetadata> tables,
                         boolean schemaChangeUseCurrentTimestamp)
    {
        this.keyspaceMetadata = keyspaceMetadata;
        this.tables = tables;
        this.schemaChangeUseCurrentTimestamp = schemaChangeUseCurrentTimestamp;
    }

    public boolean ddlChangeRequired()
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspaceMetadata.name);
        if (ksm == null)
            return true;

        Tables t = tablesWithUpdates(ksm);

        return t != ksm.tables; // object-reference comparison is fine
    }

    public void executeDDL()
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspaceMetadata.name);
        if (ksm == null)
            ksm = keyspaceMetadata;

        Tables t = tablesWithUpdates(ksm);

        if (t == ksm.tables) // object-reference comparison is fine
            // nothing has changed
            return;

        // Tables has been changed - get an updated KeyspaceMetadata
        ksm = ksm.withSwapped(t);

        TPCUtils.blockingAwait(StorageService.instance.maybeAddOrUpdateKeyspace(ksm,
                                                                                Collections.emptyList(),
                                                                                schemaModificationTimestamp()));
    }

    private long schemaModificationTimestamp()
    {
        return schemaChangeUseCurrentTimestamp ? FBUtilities.timestampMicros() : 0L;
    }

    /**
     * Get the {@link Tables} instance that <em>should</em> be in place.
     * If all tables passed to the constructor of this class have all specified columns,
     * it just returns the same {@link Tables} object-<em>reference</em> as in {@code ksm}
     * argument.
     * If any table needs additional columns, this method returns a <em>different</em>
     * {@link Tables} object with the updated tables.
     */
    private Tables tablesWithUpdates(KeyspaceMetadata ksm)
    {
        Tables t = ksm.tables;
        for (TableMetadata table : this.tables)
            t = withMissingColumns(t, table);
        return t;
    }

    private Tables withMissingColumns(Tables current, TableMetadata expected)
    {
        TableMetadata existing = current.getNullable(expected.name);
        if (existing == null)
            return current.with(expected);

        if (!existing.partitionKeyColumns().equals(expected.partitionKeyColumns()) ||
            !existing.clusteringColumns().equals(expected.clusteringColumns()))
        {
            throw new RuntimeException(String.format("The primary key columns of the existing table table definition and the " +
                                       "expected table definition for %s.%s do not match", existing.keyspace, existing.name));
        }

        TableMetadata.Builder builder = null;
        for (ColumnMetadata expectedColumn : expected.regularAndStaticColumns())
        {
            if (existing.getColumn(expectedColumn.name) == null)
            {
                if (builder == null)
                    builder = existing.unbuild();
                if (expectedColumn.isStatic())
                    builder.addStaticColumn(expectedColumn.name, expectedColumn.type);
                else
                    builder.addRegularColumn(expectedColumn.name, expectedColumn.type);
            }
        }

        return builder != null ? current.withSwapped(builder.build()) : current;
    }
}
