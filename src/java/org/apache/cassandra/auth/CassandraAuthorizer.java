/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.auth;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.permission.Permissions;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.serializers.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * CassandraAuthorizer is an IAuthorizer implementation that keeps
 * user permissions internally in C* using the system_auth.role_permissions
 * table.
 */
public class CassandraAuthorizer implements IAuthorizer
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraAuthorizer.class);

    private static final String ROLE = "role";
    private static final String RESOURCE = "resource";
    private static final String PERMISSIONS = "permissions";
    private static final String GRANTABLES = "grantables";
    private static final String RESTRICTED = "restricted";

    private SelectStatement authorizeRoleStatement;

    public CassandraAuthorizer()
    {
    }

    @Override
    public PermissionSets allPermissionSets(AuthenticatedUser user, IResource resource)
    {
        if (user.isSuper())
            // superuser can do everything
            return PermissionSets.builder()
                                 .addGranted(applicablePermissions(resource))
                                 .addGrantables(applicablePermissions(resource))
                                 .build();

        try
        {
            return addPermissionsForUser(resource, user);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            logger.warn("CassandraAuthorizer failed to authorize {} for {}", user, resource);
            throw new RuntimeException(e);
        }
    }

    public void grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource grantee, GrantMode grantMode)
    throws RequestValidationException, RequestExecutionException
    {
        grantRevoke(permissions, resource, grantee, grantMode, "+");
    }

    public void revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource revokee, GrantMode grantMode)
    throws RequestValidationException, RequestExecutionException
    {
        grantRevoke(permissions, resource, revokee, grantMode, "-");
    }

    // Called when deleting a role with DROP ROLE query.
    // Internal hook, so no permission checks are needed here.
    public void revokeAllFrom(RoleResource revokee)
    {
        try
        {
            process(String.format("DELETE FROM %s.%s WHERE role = '%s'",
                                  SchemaConstants.AUTH_KEYSPACE_NAME,
                                  AuthKeyspace.ROLE_PERMISSIONS,
                                  escape(revokee.getRoleName())));
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            logger.warn("CassandraAuthorizer failed to revoke all permissions of {}: {}",
                        revokee.getRoleName(),
                        e.getMessage());
        }
    }

    // Called after a resource is removed (DROP KEYSPACE, DROP TABLE, etc.).
    // Execute a logged batch removing all the permissions for the resource.
    public void revokeAllOn(IResource droppedResource)
    {
        try
        {
            List<CQLStatement> statements = new ArrayList<>();

            Set<String> roles = new HashSet<>();

            UntypedResultSet rows = process(String.format("SELECT role FROM %s.%s WHERE resource = '%s' ALLOW FILTERING",
                                                          SchemaConstants.AUTH_KEYSPACE_NAME,
                                                          AuthKeyspace.ROLE_PERMISSIONS,
                                                          escape(droppedResource.getName())));
            for (UntypedResultSet.Row row : rows)
            {
                roles.add(row.getString("role"));
            }

            for (String role : roles)
            {
                String cql = String.format("DELETE FROM %s.%s WHERE role = '%s' AND resource = '%s'",
                                           SchemaConstants.AUTH_KEYSPACE_NAME,
                                           AuthKeyspace.ROLE_PERMISSIONS,
                                           escape(role),
                                           escape(droppedResource.getName()));
                statements.add(QueryProcessor.getStatement(cql, ClientState.forInternalCalls()).statement);
            }

            BatchStatement batch = new BatchStatement(0,
                                                      BatchStatement.Type.LOGGED,
                                                      Lists.newArrayList(Iterables.filter(statements, ModificationStatement.class)),
                                                      Attributes.none());
            TPCUtils.blockingGet(QueryProcessor.instance.processBatch(batch,
                                                                      QueryState.forInternalCalls(),
                                                                      BatchQueryOptions.withoutPerStatementVariables(QueryOptions.DEFAULT),
                                                                      System.nanoTime()));
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            logger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", droppedResource, e.getMessage());
        }
    }

    // Add every permission on the resource granted to the role
    private PermissionSets addPermissionsForUser(IResource resource, AuthenticatedUser user)
    throws RequestExecutionException, RequestValidationException
    {
        PermissionSets.Builder permissions = PermissionSets.builder();

        // Query looks like this:
        //      SELECT permissions, restricted, grantables
        //        FROM system_auth.role-permissions
        //       WHERE resource = ? AND role IN ?
        // Purpose is to fetch permissions for all role with one query and not one query per role.

        ByteBuffer roles = ListSerializer.getInstance(UTF8Serializer.instance)
                                         .serialize(user.getRoles()
                                                        .stream()
                                                        .map(RoleResource::getRoleName)
                                                        .collect(Collectors.toList()));

        QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE,
                                                             Lists.newArrayList(ByteBufferUtil.bytes(resource.getName()),
                                                                                roles));

        ResultMessage.Rows rows = TPCUtils.blockingGet(authorizeRoleStatement.execute(QueryState.forInternalCalls(), options, System.nanoTime()));
        UntypedResultSet result = UntypedResultSet.create(rows.result);

        for (UntypedResultSet.Row row : result)
            allPermissionsFromRow(row, permissions);

        return permissions.build();
    }

    private static void allPermissionsFromRow(UntypedResultSet.Row row, PermissionSets.Builder perms)
    {
        permissionsFromRow(row, PERMISSIONS, perms::addGranted);
        permissionsFromRow(row, RESTRICTED, perms::addRestricted);
        permissionsFromRow(row, GRANTABLES, perms::addGrantable);
    }

    private static void permissionsFromRow(UntypedResultSet.Row row, String column, Consumer<Permission> perms)
    {
        if (!row.has(column))
            return;
        row.getSet(column, UTF8Type.instance)
           .stream()
           .map(Permissions::permission)
           .forEach(perms);
    }

    private void grantRevoke(Set<Permission> permissions, IResource resource, RoleResource role, GrantMode grantMode,
                             String op)
    {
        // Construct a CQL command like the following. The updated columns are variable (depend on grantMode).
        //
        //   UPDATE system_auth.role_permissions
        //      SET permissions = permissions + {<permissions>}
        //    WHERE role = <role-name>
        //      AND resource = <resource-name>
        //

        String column = columnForGrantMode(grantMode);

        String perms = permissions.stream()
                                  .map(Permission::getFullName)
                                  .collect(Collectors.joining("','", "'", "'"));

        process(String.format("UPDATE %s.%s SET %s = %s %s { %s } WHERE role = '%s' AND resource = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.ROLE_PERMISSIONS,
                              column, column, op, perms,
                              escape(role.getRoleName()),
                              escape(resource.getName())));
    }

    private static String columnForGrantMode(GrantMode grantMode)
    {
        switch (grantMode)
        {
            case GRANT:
                return PERMISSIONS;
            case RESTRICT:
                return RESTRICTED;
            case GRANTABLE:
                return GRANTABLES;
            default:
                throw new AssertionError(); // make compiler happy
        }
    }

    public Set<PermissionDetails> list(Set<Permission> permissions,
                                       IResource resource,
                                       RoleResource grantee)
    throws RequestValidationException, RequestExecutionException
    {
        // 'grantee' can be null - in that case everyone's permissions have been requested. Otherwise only single user's.
        Set<RoleResource> roles = grantee != null
                                  ? DatabaseDescriptor.getRoleManager().getRoles(grantee, true)
                                  : null;

        Set<PermissionDetails> details = new HashSet<>();
        // If it exists, try the legacy user permissions table first. This is to handle the case
        // where the cluster is being upgraded and so is running with mixed versions of the perms table
        for (UntypedResultSet.Row row : process(buildListQuery(resource, roles)))
        {
            PermissionSets.Builder permsBuilder = PermissionSets.builder();
            allPermissionsFromRow(row, permsBuilder);
            PermissionSets perms = permsBuilder.build();

            String rowRole = row.getString(ROLE);
            IResource rowResource = Resources.fromName(row.getString(RESOURCE));

            for (Permission p : perms.allContainedPermissions())
            {
                if (permissions.contains(p))
                {
                    details.add(new PermissionDetails(rowRole,
                                                      rowResource,
                                                      p,
                                                      perms.grantModesFor(p)));
                }
            }
        }
        return details;
    }

    private String buildListQuery(IResource resource, Set<RoleResource> roles)
    {
        List<String> vars = new ArrayList<>();
        List<String> conditions = new ArrayList<>();

        if (resource != null)
        {
            conditions.add("resource = '%s'");
            vars.add(escape(resource.getName()));
        }

        boolean hasRoles = roles != null && !roles.isEmpty();
        if (hasRoles)
        {
            conditions.add("%s IN (" + StringUtils.repeat(", '%s'", roles.size()).substring(2) + ')');
            vars.add(ROLE);
            vars.addAll(roles.stream()
                             .map(r -> escape(r.getRoleName()))
                             .collect(Collectors.toList()));
        }

        String query = "SELECT " + ROLE + ", resource, " + PERMISSIONS + ", " + RESTRICTED + ", " + GRANTABLES
                       + " FROM " +  SchemaConstants.AUTH_KEYSPACE_NAME + '.' + AuthKeyspace.ROLE_PERMISSIONS;

        if (!conditions.isEmpty())
            query += " WHERE " + StringUtils.join(conditions, " AND ");

        if (resource != null && !hasRoles)
            query += " ALLOW FILTERING";

        return String.format(query, vars.toArray());
    }


    public Set<DataResource> protectedResources()
    {
        return ImmutableSet.of(DataResource.table(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS));
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
        String cql = String.format("SELECT %s FROM %s.%s WHERE resource = ? AND %s IN ?",
                                   PERMISSIONS + ", " + RESTRICTED + ", " + GRANTABLES,
                                   SchemaConstants.AUTH_KEYSPACE_NAME,
                                   AuthKeyspace.ROLE_PERMISSIONS,
                                   ROLE);
        authorizeRoleStatement = (SelectStatement) QueryProcessor.getStatement(cql, ClientState.forInternalCalls()).statement;
    }

    // We only worry about one character ('). Make sure it's properly escaped.
    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    private static UntypedResultSet process(String query) throws RequestExecutionException
    {
        return QueryProcessor.processBlocking(query, ConsistencyLevel.LOCAL_ONE);
    }
}
