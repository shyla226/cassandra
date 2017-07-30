/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.Restrictions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
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
    private static final Logger logger = LoggerFactory.getLogger(SystemKeyspacesFilteringRestrictions.class);

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
        if (enabled() && isOrdinaryUser(state) && !SchemaConstants.isSystemKeyspace(keyspace))
        {
            // The very minimum permission a user needs to have is system-keyspace-filtering is enabled.
            // Otherwise a user could just "peek" for schema information.
            state.checkPermission(DataResource.keyspace(keyspace), CorePermission.DESCRIBE);
        }
    }

    static CQLStatement maybeAdd(CQLStatement statement, QueryState state)
    {
        if (!enabled())
            return statement;

        if (statement instanceof SelectStatement && isOrdinaryUser(state))
        {
            SelectStatement selectStatement = (SelectStatement) statement;
            Restrictions restrictions = restrictionsForTable(selectStatement.table, state);
            if (restrictions != null)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Applying restrictions against {}.{} for user {}, using {}",
                                 selectStatement.keyspace(), selectStatement.columnFamily(),
                                 state.getUser().getName(), restrictions);
                return selectStatement.addIndexRestrictions(restrictions);
            }
        }
        return statement;
    }

    private static boolean isOrdinaryUser(QueryState state)
    {
        AuthenticatedUser user = state.getUser();
        return !state.getClientState().isInternal &&
               user != null &&
               !state.isSuper() &&
               !user.isSystem();
    }

    private static boolean checkDescribePermissionOnKeyspace(String keyspace, UserRolesAndPermissions userRolesAndPermissions)
    {
        // Check whether the current user has DESCRIBE permission on the keyspace.
        // Intentionally not checking permissions on individual schema objects like tables,
        // types or functions, because that would require checking the whole object _tree_.

        return userRolesAndPermissions.hasPermission(DataResource.keyspace(keyspace), CorePermission.DESCRIBE);
    }

    private static boolean schemaChangeFilter(Event event, ClientState clientState)
    {
        if (!(event instanceof Event.SchemaChange))
            return true;

        Event.SchemaChange schemaChange = (Event.SchemaChange) event;
        String keyspace = schemaChange.keyspace;

        // just allow schema change notifications for all system keyspaces (local + distributed)
        if (SchemaConstants.isSystemKeyspace(keyspace) || SchemaConstants.isReplicatedSystemKeyspace(keyspace))
            return true;

        // allow schema change notification, if
        // - no authentication is configured
        // - it's the system user
        // - it's a super user
        // - the user has DESCRIBE permission on the keyspace
        AuthenticatedUser user = clientState.getUser();
        if (user == null || user.isSystem())
            return true;
        UserRolesAndPermissions userRolesAndPermissions = DatabaseDescriptor.getAuthManager().getUserRolesAndPermissions(user).blockingGet();
        // TODO the blockingGet() is not good
        return userRolesAndPermissions.isSuper() || checkDescribePermissionOnKeyspace(keyspace, userRolesAndPermissions);
    }

    public static BiFunction<Event, ClientState, Boolean> getSchemaChangeFilter()
    {
        return !enabled() ?
               null :
               SystemKeyspacesFilteringRestrictions::schemaChangeFilter;
    }

    private static Restrictions restrictionsForTable(TableMetadata tableMetadata, QueryState state)
    {
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
                        return new SystemKeyspacesRestrictions(tableMetadata, state, false);
                    default:
                        return new DenyAllRestrictions(tableMetadata.partitionKeyColumns());
                }
                break;
            case SchemaConstants.SCHEMA_KEYSPACE_NAME:
                switch (tableMetadata.name)
                {
                    case SchemaKeyspace.TABLES:
                    case SchemaKeyspace.COLUMNS:
                    case SchemaKeyspace.DROPPED_COLUMNS:
                    case SchemaKeyspace.VIEWS:
                        return new SystemKeyspacesRestrictions(tableMetadata, state, true);
                    case SchemaKeyspace.FUNCTIONS:
                    case SchemaKeyspace.AGGREGATES:
                    default:
                        return new SystemKeyspacesRestrictions(tableMetadata, state, false);
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
    private static final class SystemKeyspacesRestrictions implements Restrictions
    {
        private final Expression expression;
        private final ColumnMetadata column;

        SystemKeyspacesRestrictions(TableMetadata tableMetadata, QueryState state, boolean withTableInClustering)
        {
            this.column = withTableInClustering
                          ? tableMetadata.clusteringColumns().iterator().next()
                          : tableMetadata.partitionKeyColumns().iterator().next();
            this.expression = new Expression(column, state, withTableInClustering);
        }

        public String toString()
        {
            return "SystemKeyspacesRestrictions[" + expression.state.getUser() + ']';
        }

        public Set<Restriction> getRestrictions(ColumnMetadata columnDef)
        {
            return Collections.emptySet();
        }

        public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options)
        throws InvalidRequestException
        {
            filter.addUserExpression(expression);
        }

        public List<ColumnMetadata> getColumnDefs()
        {
            return Collections.singletonList(column);
        }

        public ColumnMetadata getFirstColumn()
        {
            return column;
        }

        public ColumnMetadata getLastColumn()
        {
            return column;
        }

        public void addFunctionsTo(List<Function> list)
        {
        }

        public boolean hasIN()
        {
            return false;
        }

        public boolean hasOnlyEqualityRestrictions()
        {
            return false;
        }

        public boolean hasSupportingIndex(SecondaryIndexManager indexManager)
        {
            return false;
        }

        public boolean hasSlice()
        {
            return false;
        }

        public boolean hasContains()
        {
            return false;
        }

        public boolean isEmpty()
        {
            return false;
        }

        public int size()
        {
            return 1;
        }

        private static class Expression extends RowFilter.UserExpression
        {
            private final boolean withTableInClustering;
            private final QueryState state;

            Expression(ColumnMetadata column, QueryState state, boolean withTableInClustering)
            {
                super(column, Operator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
                this.state = state;
                this.withTableInClustering = withTableInClustering;
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

    private static final class DenyAllRestrictions implements Restrictions
    {
        private final List<ColumnMetadata> columnMetadata;

        public DenyAllRestrictions(List<ColumnMetadata> columnMetadata)
        {
            this.columnMetadata = columnMetadata;
        }

        public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options)
        throws InvalidRequestException
        {
            filter.addUserExpression(new Expression(columnMetadata));
        }

        public String toString()
        {
            return "DenyAllRestrictions";
        }

        public Set<Restriction> getRestrictions(ColumnMetadata columnDef)
        {
            return Collections.emptySet();
        }

        public List<ColumnMetadata> getColumnDefs()
        {
            return columnMetadata;
        }

        public ColumnMetadata getFirstColumn()
        {
            return columnMetadata.get(0);
        }

        public ColumnMetadata getLastColumn()
        {
            return columnMetadata.get(columnMetadata.size() - 1);
        }

        public void addFunctionsTo(List<Function> list)
        {
        }

        public boolean hasIN()
        {
            return false;
        }

        public boolean hasOnlyEqualityRestrictions()
        {
            return false;
        }

        public boolean hasSupportingIndex(SecondaryIndexManager indexManager)
        {
            return false;
        }

        public boolean hasSlice()
        {
            return false;
        }

        public boolean hasContains()
        {
            return false;
        }

        public boolean isEmpty()
        {
            return false;
        }

        public int size()
        {
            return 1;
        }

        private static class Expression extends RowFilter.UserExpression
        {
            Expression(List<ColumnMetadata> columnMetadata)
            {
                // A keyspace name cannot be empty - so this should be fine (EQ never yields true)
                super(columnMetadata.get(0), Operator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
            }

            protected void serialize(DataOutputPlus dataOutputPlus, ReadVerbs.ReadVersion version) throws IOException
            {
                throw new UnsupportedOperationException();
            }

            protected long serializedSize(ReadVerbs.ReadVersion version)
            {
                return 0;
            }

            public Flow<Boolean> isSatisfiedBy(TableMetadata cfMetaData, DecoratedKey decoratedKey, Row row)
            {
                return Flow.just(false);
            }
        }
    }
}
