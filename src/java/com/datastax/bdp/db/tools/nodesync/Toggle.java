/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * {@link NodeSyncCommand} to enable/disable nodesync in a group of tables through {@code ALTER TABLE} statements.
 * <p>
 * This command uses an option to specify the name of the default keyspace and a list of table name arguments. These
 * table name arguments can be:
 * <p>
 * <li>Unqualifed table names such as {@code t1} or {@code "t1"}
 * <li>Qualifed table names such as {@code k1.t1} or {@code "k1"."t1"}
 * <li>Wildcards ({@code *}), meaning all the user-alterable tables either in the default keyspace, or in the whole
 * cluster if no default keyspace has been specified
 */
public abstract class Toggle extends NodeSyncCommand
{
    private static final String ALTER_QUERY_FORMAT = "ALTER TABLE %s WITH nodesync = {'enabled':'%s'}";

    @Option(name = { "-k", "--keyspace" }, description = "Default keyspace to be used with unqualified table names and wildcards")
    String defaultKeyspace = null;

    @Arguments(usage = "<table> [<table>...]", required = true,
               description = "One or many qualified table names such as \"my_keyspace.my_table\", " +
                             "or unqualifed table names such as \"my_table\" relying of the default keyspace, " +
                             "or wildcards (*) meaning all the user-alterable tables " +
                             "either in the default keyspace or in the whole cluster")
    List<String> tableSelectors = new ArrayList<>();

    @Override
    protected final void execute(Metadata metadata, Session session, NodeProbes probes)
    {
        List<Future<?>> futures = tableSelectors.stream()
                                                .map(selector -> tablesMetadata(metadata, selector))
                                                .flatMap(Collection::stream)
                                                .distinct()
                                                .map(m -> m.getKeyspace().getName() + '.' + m.getName())
                                                .map(t -> execute(session, t))
                                                .collect(toList());
        try
        {
            FBUtilities.waitOnFutures(futures);
        }
        catch (Exception e)
        {
            // The details should have been printed during the executions
            throw new NodeSyncException("Some operations have failed");
        }
    }

    /**
     * Executes the nodesync toggling in the specified table.
     *
     * @param session a CQL session
     * @param table the fully-qualified name of the table
     */
    private ListenableFuture<ResultSet> execute(Session session, String table)
    {
        ListenableFuture<ResultSet> future = session.executeAsync(alterQuery(table));
        final SettableFuture<ResultSet> result = SettableFuture.create();
        Futures.addCallback(future, new FutureCallback<ResultSet>()
        {
            public void onSuccess(@Nullable ResultSet rs)
            {
                printVerbose(successMessage(table));
                result.set(rs);
            }

            public void onFailure(Throwable cause)
            {
                System.err.println(failureMessage(table, cause));
                result.setException(cause);
            }
        });
        return result;
    }

    protected abstract String alterQuery(String table);

    protected abstract String successMessage(String table);

    protected abstract String failureMessage(String table, Throwable cause);

    /**
     * Returns the metadata of the tables referenced by the specified table selector.
     *
     * @param metadata a cluster metadata
     * @param tableSelector a table name selector, which can be a qualified or unqualified table name, or a wildcard
     * @return the metadata of the referenced tables
     */
    private Collection<TableMetadata> tablesMetadata(Metadata metadata, String tableSelector)
    {
        if (isWildcard(tableSelector))
        {
            return defaultKeyspace != null
                   ? checkUserAlterable(parseKeyspace(metadata, defaultKeyspace)).getTables()
                   : metadata.getKeyspaces()
                             .stream()
                             .filter(Toggle::isUserAlterable)
                             .map(KeyspaceMetadata::getTables)
                             .flatMap(Collection::stream)
                             .collect(toSet());
        }

        TableMetadata tableMetadata = parseTable(metadata, defaultKeyspace, tableSelector);
        checkUserAlterable(tableMetadata.getKeyspace());
        return Collections.singleton(tableMetadata);
    }

    /**
     * Returns if the specified table selector is a wildcard.
     *
     * @param tableSelector a table name selector, which can be a qualified or unqualified table name, or a wildcard
     * @return {@code true} if the table name selector is a wildcard, {code false} otherwise
     */
    private static boolean isWildcard(String tableSelector)
    {
        return tableSelector.equals("*");
    }

    private static KeyspaceMetadata checkUserAlterable(KeyspaceMetadata keyspace)
    {
        if (isUserAlterable(keyspace))
            return keyspace;
        else
            throw new NodeSyncException("Keyspace [" + keyspace.getName() + "] is not alterable.");
    }

    /**
     * Returns if the specified keyspace allows {@code ALTER TABLE} operations.
     *
     * @param keyspace the keyspace metadata
     * @return {@code true} if the keyspace is alterable, {code false} otherwise
     */
    private static boolean isUserAlterable(KeyspaceMetadata keyspace)
    {
        String name = keyspace.getName();
        return name.equals(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME) || SchemaConstants.isUserKeyspace(name);
    }

    /**
     * {@link Toggle} command to enable nodesync.
     */
    @Command(name = "enable", description = "Enable nodesync on the specified tables")
    public static class Enable extends Toggle
    {
        @Override
        public String alterQuery(String table)
        {
            return String.format(ALTER_QUERY_FORMAT, table, true);
        }

        @Override
        protected String successMessage(String table)
        {
            return "Nodesync enabled for " + table;
        }

        @Override
        protected String failureMessage(String table, Throwable cause)
        {
            return String.format("Error enabling nodesync for %s: %s", table, cause.getMessage());
        }
    }

    /**
     * {@link Toggle} command to disable nodesync.
     */
    @Command(name = "disable", description = "Disable nodesync on the specified tables")
    public static class Disable extends Toggle
    {
        @Override
        public String alterQuery(String table)
        {
            return String.format(ALTER_QUERY_FORMAT, table, false);
        }

        @Override
        protected String successMessage(String table)
        {
            return "Nodesync disabled for " + table;
        }

        @Override
        protected String failureMessage(String table, Throwable cause)
        {
            return String.format("Error disabling nodesync for %s: %s", table, cause.getMessage());
        }
    }
}
