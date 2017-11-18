/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import io.reactivex.Maybe;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.Resources;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.restrictions.AuthRestriction;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.Server.ChannelFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.Flow;

/**
 * Restricts visible data in the system keyspaces ({@code system_schema} + {@code system} keyspaces) as per
 * permissions granted to a role/user.
 * <p>
 * Internal requests and requests by superusers bypass any permission check.
 * </p>
 * <p>
 * Access to keyspaces {@value SchemaConstants#TRACE_KEYSPACE_NAME}, {@value SchemaConstants#AUTH_KEYSPACE_NAME},
 * {@value SchemaConstants#DISTRIBUTED_KEYSPACE_NAME} are not allowed at all.
 * </p>
 * <p>
 * Access to schema data of other keyspaces is granted if the user has {@link CorePermission#DESCRIBE} permission.
 * </p>
 * <p>
 * This functionality must be enabled by setting {@link org.apache.cassandra.config.Config#system_keyspaces_filtering}
 * to {@code true} and have authenticator and authorizer configured that require authentication.
 * </p>
 */
public final class SystemKeyspacesFilteringRestrictions
{
    static
    {
        if (enabled())
        {
            // Add a couple of system keyspace tables to org.apache.cassandra.service.ClientState.READABLE_SYSTEM_RESOURCES.
            // We can control what a user (or driver) can see in these tables.

            Resources.addReadableSystemResource(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.AVAILABLE_RANGES));
            Resources.addReadableSystemResource(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SIZE_ESTIMATES));

            Resources.addReadableSystemResource(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BUILT_INDEXES));
            Resources.addReadableSystemResource(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BUILT_VIEWS));
            Resources.addReadableSystemResource(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SSTABLE_ACTIVITY));
            Resources.addReadableSystemResource(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.VIEWS_BUILDS_IN_PROGRESS));
        }
    }

    private static boolean enabled()
    {
        return DatabaseDescriptor.isSystemKeyspaceFilteringEnabled();
    }

    public static void hasMandatoryPermissions(String keyspace, QueryState state)
    {
        if (enabled() && state.isOrdinaryUser() && SchemaConstants.isUserKeyspace(keyspace))
        {
            // The very minimum permission a user needs to have is system-keyspace-filtering is enabled.
            // Otherwise a user could just "peek" for schema information.
            state.checkPermission(DataResource.keyspace(keyspace), CorePermission.DESCRIBE);
        }
    }

    private static boolean checkDescribePermissionOnKeyspace(String keyspace, UserRolesAndPermissions userRolesAndPermissions)
    {
        // Check whether the current user has DESCRIBE permission on the keyspace.
        // Intentionally not checking permissions on individual schema objects like tables,
        // types or functions, because that would require checking the whole object _tree_.

        return userRolesAndPermissions.hasPermission(DataResource.keyspace(keyspace), CorePermission.DESCRIBE);
    }

    public static ChannelFilter getChannelFilter(Event.SchemaChange event)
    {
        String keyspace = event.keyspace;

        // just allow schema change notifications for all system keyspaces (local + distributed)
        if (!enabled()
                || SchemaConstants.isLocalSystemKeyspace(keyspace)
                || SchemaConstants.isReplicatedSystemKeyspace(keyspace))
        {
            return ChannelFilter.NOOP_FILTER;
        }

        return channel -> {
            ClientState clientState = channel.attr(Server.ATTR_KEY_CLIENT_STATE).get();
            AuthenticatedUser user = clientState.getUser();

            return DatabaseDescriptor.getAuthManager()
                    .getUserRolesAndPermissions(user)
                    .flatMapMaybe((userRolesAndPermissions) ->
                        {
                            if (checkDescribePermissionOnKeyspace(keyspace, userRolesAndPermissions))
                                return Maybe.just(channel);
                            return Maybe.never();
                        });
        };
    }

    public static AuthRestriction restrictionsForTable(TableMetadata tableMetadata)
    {
        if (!enabled())
            return null;

        switch (tableMetadata.keyspace)
        {
            case SchemaConstants.SYSTEM_KEYSPACE_NAME:
                switch (tableMetadata.name)
                {
                    case SystemKeyspace.LOCAL:
                    case SystemKeyspace.PEERS:
                        // allow
                        break;
                    case SystemKeyspace.SSTABLE_ACTIVITY:
                    case SystemKeyspace.SIZE_ESTIMATES:
                    case SystemKeyspace.BUILT_INDEXES: // note: column 'table_name' is the keyspace_name - duh!
                    case SystemKeyspace.BUILT_VIEWS:
                    case SystemKeyspace.AVAILABLE_RANGES:
                    case SystemKeyspace.VIEWS_BUILDS_IN_PROGRESS:
                        return new SystemKeyspacesRestriction(tableMetadata, false);
                    default:
                        return new DenyAllRestriction(tableMetadata.partitionKeyColumns());
                }
                break;
            case SchemaConstants.SCHEMA_KEYSPACE_NAME:
                switch (tableMetadata.name)
                {
                    case SchemaKeyspace.TABLES:
                    case SchemaKeyspace.COLUMNS:
                    case SchemaKeyspace.DROPPED_COLUMNS:
                    case SchemaKeyspace.VIEWS:
                        return new SystemKeyspacesRestriction(tableMetadata, true);
                    case SchemaKeyspace.FUNCTIONS:
                    case SchemaKeyspace.AGGREGATES:
                    default:
                        return new SystemKeyspacesRestriction(tableMetadata, false);
                }
        }
        return null;
    }

    /**
     * Implements restrictions to filter on keyspace_name / table_name depending on
     * the authenticated user's permissions.
     * <br/>
     * <em>NOTE:</em> this implementation can currently only be used on keyspaces
     * using {@code LocalStrategy}, maybe {@code EverywhereStrategy}.
     * We do not need this kind of filtering for other strategies yet.
     */
    private static final class SystemKeyspacesRestriction implements AuthRestriction
    {
        private final ColumnMetadata column;

        SystemKeyspacesRestriction(TableMetadata tableMetadata, boolean withTableInClustering)
        {
            column = withTableInClustering
                          ? tableMetadata.clusteringColumns().iterator().next()
                          : tableMetadata.partitionKeyColumns().iterator().next();
        }

        @Override
        public void addRowFilterTo(RowFilter filter,
                                   QueryState state)
        {
            if (state.isOrdinaryUser())
                filter.addUserExpression(new Expression(column, state));
        }

        private static class Expression extends RowFilter.UserExpression
        {
            private final boolean withTableInClustering;
            private final QueryState state;

            Expression(ColumnMetadata column, QueryState state)
            {
                super(column, Operator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
                this.state = state;
                this.withTableInClustering = column.isClusteringColumn();
            }

            protected void serialize(DataOutputPlus dataOutputPlus, ReadVerbs.ReadVersion version) throws IOException
            {
                throw new UnsupportedOperationException();
            }

            protected long serializedSize(ReadVerbs.ReadVersion version)
            {
                throw new UnsupportedOperationException();
            }

            public Flow<Boolean> isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row)
            {
                return Flow.just(isSatisfiedByInternal(metadata, partitionKey, row));
            }

            private boolean isSatisfiedByInternal(TableMetadata tableMetadata, DecoratedKey decoratedKey, Row row)
            {
                // note: does not support MVs (and we do not have those in system/system_schema keyspaces)

                ByteBuffer partitionKey = decoratedKey.getKey().duplicate();
                String keyspace = UTF8Serializer.instance.deserialize(partitionKey);

                // Handle SELECT against tables in 'system_schema' keyspace,
                // rows for 'system' and 'system_schema' keyspaces.
                // I.e. _describing_ system keyspaces.
                if (SchemaConstants.SCHEMA_KEYSPACE_NAME.equals(tableMetadata.keyspace))
                {
                    switch (keyspace)
                    {
                        // rows for schema tables in 'system_schema' keyspace
                        case SchemaConstants.SCHEMA_KEYSPACE_NAME:
                            // meaning: always allow reads of schema information of everything in the 'system_schema' keyspace
                            return true;
                        // rows for schema related tables in 'system' keyspace
                        case SchemaConstants.SYSTEM_KEYSPACE_NAME:
                            if (row.isEmpty())
                            {
                                // SELECT DISTINCT keyspace_name FROM ... (only selecting static olumns
                                return true;
                            }

                            if (!withTableInClustering)
                            {
                                return false;
                            }

                            String table = UTF8Serializer.instance.deserialize(row.clustering().clustering().getRawValues()[0]);

                            // meaning: allow reads of schema information of certain tables in the 'system' keyspace
                            switch (table)
                            {
                                case SystemKeyspace.LOCAL:
                                case SystemKeyspace.PEERS:
                                case SystemKeyspace.SSTABLE_ACTIVITY:
                                case SystemKeyspace.SIZE_ESTIMATES:
                                case SystemKeyspace.BUILT_INDEXES: // note: column 'table_name' is the keyspace_name - duh!
                                case SystemKeyspace.BUILT_VIEWS:
                                case SystemKeyspace.AVAILABLE_RANGES:
                                case SystemKeyspace.VIEWS_BUILDS_IN_PROGRESS:
                                    // meaning: always allow reads of schema information of the above tables in the 'system' keyspace
                                    return true;
                                default:
                                    // deny
                                    return false;
                            }
                    }
                }

                return checkDescribePermissionOnKeyspace(keyspace, state.getUserRolesAndPermissions());
            }
        }
    }

    private static final class DenyAllRestriction implements AuthRestriction
    {
        private final ColumnMetadata column;

        DenyAllRestriction(List<ColumnMetadata> columns)
        {
            this.column = columns.get(0);
        }

        @Override
        public void addRowFilterTo(RowFilter filter,
                                   QueryState state)
        {
            if (state.isOrdinaryUser())
                filter.addUserExpression(new Expression(column));
        }

        private static class Expression extends RowFilter.UserExpression
        {
            Expression(ColumnMetadata keyspaceColumn)
            {
                // A keyspace name cannot be empty - so this should be fine (EQ never yields true)
                super(keyspaceColumn, Operator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
            }

            protected void serialize(DataOutputPlus dataOutputPlus, ReadVerbs.ReadVersion version) throws IOException
            {
                throw new UnsupportedOperationException();
            }

            protected long serializedSize(ReadVerbs.ReadVersion version)
            {
                throw new UnsupportedOperationException();
            }

            public Flow<Boolean> isSatisfiedBy(TableMetadata cfMetaData, DecoratedKey decoratedKey, Row row)
            {
                return Flow.just(false);
            }
        }
    }
}
