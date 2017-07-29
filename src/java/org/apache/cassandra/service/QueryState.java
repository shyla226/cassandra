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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Connection;

/**
 * Represents the state related to a given query.
 */
public class QueryState
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraRoleManager.class);

    private final ClientState clientState;
    private final int streamId;
    private volatile UUID preparedTracingSession;

    // Bunch of authorization related information. Having this information in QueryState
    // prevents roundtrips to the database and escape from or even failure of a TPC thread.
    private AuthenticatedUser user;
    private boolean superuser;
    private Set<RoleResource> roles;
    // do NEVER modify the values of this map !
    private Map<RoleResource, Map<IResource, PermissionSets>> permissions;

    private Map<IResource, PermissionSets> additionalPermissions;

    private QueryState(QueryState queryState, ClientState clientState)
    {
        this(clientState, queryState.streamId);
        this.additionalPermissions = queryState.additionalPermissions;
        this.roles = queryState.roles;
        this.superuser = queryState.superuser;
        this.user = queryState.user;
        this.permissions = queryState.permissions;
        this.preparedTracingSession = queryState.preparedTracingSession;
    }

    public QueryState(ClientState clientState)
    {
        this(clientState, 0);
    }

    public QueryState(ClientState clientState, int streamId)
    {
        this.clientState = clientState;
        this.streamId = streamId;
    }

    /**
     * @return a QueryState object for internal C* calls (not limited by any kind of auth).
     */
    public static QueryState forInternalCalls()
    {
        return new QueryState(ClientState.forInternalCalls());
    }

    /**
     * @return a QueryState object for internal calls with a logged in role (not limited by any kind of auth).
     */
    public static QueryState forExternalCalls(AuthenticatedUser user)
    {
        return new QueryState(ClientState.forExternalCalls(user));
    }

    public ClientState getClientState()
    {
        return clientState;
    }

    public AuthenticatedUser getUser()
    {
        prepare();
        return user;
    }

    /**
     * Checks the user's superuser status.
     * Only a superuser is allowed to perform CREATE USER and DROP USER queries.
     * Im most cased, though not necessarily, a superuser will have Permission.ALL on every resource
     * (depends on IAuthorizer implementation).
     */
    public boolean isSuper()
    {
        prepare();
        return superuser;
    }

    /**
     * Get the roles that have been granted to the user via the IRoleManager
     *
     * @return a list of roles that have been granted to the user
     */
    public Set<RoleResource> getRoles()
    {
        prepare();
        return roles;
    }

    public boolean authorize(IResource resource, Permission permission)
    {
        prepare();

        if (user == null || isSuper() || !DatabaseDescriptor.getAuthorizer().requireAuthorization())
            return true;

        return resourceChainPermissions(resource).hasEffectivePermission(permission);
    }

    /**
     * Returns a cummulated view of all granted, restricted and grantable permissions on
     * the resource <em>chain</em> of the given resource for this user.
     */
    public PermissionSets resourceChainPermissions(IResource resource)
    {
        prepare();

        PermissionSets.Builder permissions = PermissionSets.builder();

        if (user == null || isSuper() || !DatabaseDescriptor.getAuthorizer().requireAuthorization())
        {
            permissions.addGranted(resource.applicablePermissions());
            permissions.addGrantables(resource.applicablePermissions());
        }
        else if (roles != null)
        {
            List<? extends IResource> chain = Resources.chain(resource);
            if (this.permissions != null)
            {
                for (RoleResource roleResource : roles)
                    permissions.addChainPermissions(chain, this.permissions.get(roleResource));
            }
            permissions.addChainPermissions(chain, additionalPermissions);
        }
        return permissions.buildSingleton();
    }

    /**
     * Add additional query permissions.
     * Used by Row-Level-Access-Control to "inject" additional permissions.
     */
    public void additionalQueryPermission(IResource resource, PermissionSets permissionSets)
    {
        if (additionalPermissions == null)
            additionalPermissions = new HashMap<>();
        PermissionSets previous = additionalPermissions.putIfAbsent(resource, permissionSets);
        assert previous == null;
    }

    /**
     * Retrieve the aggregated permissions for the role-resources accepted by the provided filter.
     *
     * @param applicablePermissions when there is no authenticated user, the user is a superuser or authorization
     *                              is not required, a permission set with granted and grantables set to the
     *                              permissions provided by this supplier will be used
     */
    public <R> R filterPermissions(java.util.function.Function<R, R> applicablePermissions,
                                   Supplier<R> initialState,
                                   RoleResourcePermissionFilter<R> aggregate)
    {
        prepare();

        R state = initialState.get();

        if (user == null || isSuper() || !DatabaseDescriptor.getAuthorizer().requireAuthorization())
        {
            state = applicablePermissions.apply(state);
        }
        else if (roles != null && permissions != null)
        {
            for (RoleResource roleResource : roles)
            {
                Map<IResource, PermissionSets> rolePerms = permissions.get(roleResource);
                if (rolePerms == null)
                    continue;
                for (Map.Entry<IResource, PermissionSets> resourcePerms : rolePerms.entrySet())
                {
                    state = aggregate.apply(state, roleResource, resourcePerms.getKey(), resourcePerms.getValue());
                }
            }
        }
        return state;
    }

    @FunctionalInterface
    public interface RoleResourcePermissionFilter<R>
    {
        R apply(R state, RoleResource role, IResource resource, PermissionSets permissionSets);
    }

    public QueryState cloneWithKeyspaceIfSet(String keyspace)
    {
        ClientState clState = clientState.cloneWithKeyspaceIfSet(keyspace);
        if (clState == clientState)
            // this will be trie, if either 'keyspace' is null or 'keyspace' is equals to the current keyspace
            return this;
        return new QueryState(this, clState);
    }

    public void hasAllKeyspacesAccess(Permission perm) throws UnauthorizedException
    {
        if (clientState.isInternal || isSuper())
            return;
        clientState.validateLogin();
        ensureHasPermission(perm, DataResource.root());
    }

    public void hasKeyspaceAccess(String keyspace, Permission perm) throws UnauthorizedException, InvalidRequestException
    {
        hasAccess(keyspace, perm, DataResource.keyspace(keyspace));
    }

    public void hasColumnFamilyAccess(String keyspace, String columnFamily, Permission perm)
    throws UnauthorizedException, InvalidRequestException
    {
        Schema.instance.validateTable(keyspace, columnFamily);
        hasAccess(keyspace, perm, DataResource.table(keyspace, columnFamily));
    }

    public void hasColumnFamilyAccess(TableMetadataRef tableRef, Permission perm)
    throws UnauthorizedException, InvalidRequestException
    {
        hasColumnFamilyAccess(tableRef.get(), perm);
    }

    public void hasColumnFamilyAccess(TableMetadata table, Permission perm)
    throws UnauthorizedException, InvalidRequestException
    {
        hasAccess(table.keyspace, perm, table.resource);
    }

    private void hasAccess(String keyspace, Permission perm, DataResource resource)
    throws UnauthorizedException, InvalidRequestException
    {
        validateKeyspace(keyspace);
        if (clientState.isInternal || isSuper())
            return;
        clientState.validateLogin();
        preventSystemKSSchemaModification(keyspace, resource, perm);
        if ((perm == CorePermission.SELECT) && ClientState.READABLE_SYSTEM_RESOURCES.contains(resource))
            return;
        if (ClientState.PROTECTED_AUTH_RESOURCES.contains(resource))
            if ((perm == CorePermission.CREATE) || (perm == CorePermission.ALTER) || (perm == CorePermission.DROP))
                throw new UnauthorizedException(String.format("%s schema is protected", resource));
        ensureHasPermission(perm, resource);
    }

    private void preventSystemKSSchemaModification(String keyspace, DataResource resource, Permission perm) throws UnauthorizedException
    {
        // we only care about schema modification.
        if (!((perm == CorePermission.ALTER) || (perm == CorePermission.DROP) || (perm == CorePermission.CREATE)))
            return;

        // prevent system keyspace modification (not only because this could be dangerous, but also because this just
        // wouldn't work with the way the schema of those system keyspace/table schema is hard-coded)
        if (SchemaConstants.isSystemKeyspace(keyspace))
            throw new UnauthorizedException(keyspace + " keyspace is not user-modifiable.");

        // Allow users with sufficient privileges to alter options on certain distributed system keyspaces/tables.
        // We only allow ALTER, not CREATE nor DROP, outside of a few specific tables that can be dropped  because they
        // are not used anymore but we prefer leaving user the responsibility to drop them. Note that even when altering
        // is allowed, only the table options can be altered but any change to the table schema (adding/removing columns
        // typically) is forbidden by AlterTableStatement.
        if (SchemaConstants.isReplicatedSystemKeyspace(keyspace))
        {
            if (perm != CorePermission.ALTER && !(perm == CorePermission.DROP && ClientState.DROPPABLE_SYSTEM_TABLES.contains(resource)))
                throw new UnauthorizedException(String.format("Cannot %s %s", perm, resource));
        }
    }

    public void ensureHasPermission(Permission perm, IResource resource) throws UnauthorizedException
    {
        if (!DatabaseDescriptor.getAuthorizer().requireAuthorization())
            return;

        // Access to built in functions is unrestricted
        if(resource instanceof FunctionResource && resource.hasParent())
            if (((FunctionResource)resource).getKeyspace().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME))
                return;

        checkPermissionOnResourceChain(perm, resource);
    }

    // Convenience method called from checkAccess method of CQLStatement
    // Also avoids needlessly creating lots of FunctionResource objects
    public void ensureHasPermission(Permission permission, Function function)
    {
        // Save creating a FunctionResource is we don't need to
        if (!DatabaseDescriptor.getAuthorizer().requireAuthorization())
            return;

        // built in functions are always available to all
        if (function.isNative())
            return;

        checkPermissionOnResourceChain(permission, FunctionResource.function(function.name().keyspace,
                                                                             function.name().name,
                                                                             function.argTypes()));
    }

    private void checkPermissionOnResourceChain(Permission perm, IResource resource)
    {
        PermissionSets chainPermissions = resourceChainPermissions(resource);
        if (!chainPermissions.granted.contains(perm))
            throw new UnauthorizedException(String.format("User %s has no %s permission on %s or any of its parents",
                                                          user.getName(),
                                                          perm,
                                                          resource));

        if (chainPermissions.restricted.contains(perm))
            throw new UnauthorizedException(String.format("Access for user %s on %s or any of its parents with %s permission is restricted",
                                                          user.getName(),
                                                          resource,
                                                          perm));
    }

    public boolean hasGrantOption(Permission perm, IResource resource)
    {
        return isSuper() || resourceChainPermissions(resource).grantables.contains(perm);
    }

    public void ensureNotAnonymous() throws UnauthorizedException
    {
        clientState.validateLogin();
        if (getUser().isAnonymous())
            throw new UnauthorizedException("You have to be logged in and not anonymous to perform this request");
    }

    public void ensureIsSuper(String message) throws UnauthorizedException
    {
        if (DatabaseDescriptor.getAuthenticator().requireAuthentication() && (getUser() == null || !isSuper()))
            throw new UnauthorizedException(message);
    }

    private static void validateKeyspace(String keyspace) throws InvalidRequestException
    {
        if (keyspace == null)
            throw new InvalidRequestException("You have not set a keyspace for this session");
    }

    /**
     * Setup per-query state of roles, superuser status and permissions. Will block is necessary information
     * needs to be fetched.
     *
     * Used to explicitly fill required fields using potentially blocking functionality (e.g. load data from
     * system_auth tables or LDAP). Also used implicitly by a couple of methods.
     *
     * @throws org.apache.cassandra.concurrent.TPCUtils.WouldBlockException if called from a TPC thread and
     *                                                                      data needs to be fetched.
     */
    public void prepare()
    {
        if (this.user != null)
            return;

        AuthenticatedUser user = clientState.getUser();
        try
        {
            if (user != null && !user.isSystem())
            {
                Map<RoleResource, Map<IResource, PermissionSets>> permissions = null;
                Set<RoleResource> roles;
                boolean superuser;

                roles = Auth.getRoles(user.getPrimaryRole());
                superuser = Auth.hasSuperuserStatus(user.getPrimaryRole());

                if (DatabaseDescriptor.getAuthorizer().requireAuthorization())
                {
                    permissions = new HashMap<>();
                    for (RoleResource role : roles)
                    {
                        Map<IResource, PermissionSets> rolePermissions = Auth.getPermissions(role);
                        permissions.put(role, rolePermissions);
                    }
                }

                this.permissions = permissions;
                this.roles = roles;
                this.superuser = superuser;
            }

            this.user = user;
        }
        catch (TPCUtils.WouldBlockException e)
        {
            throw e;
        }
        catch (Throwable t)
        {
            logger.error("Could not retrieve information for user " + user, t);
            if (t instanceof RuntimeException)
                throw (RuntimeException)t;
            throw new RuntimeException(t);
        }
    }

    /**
     * This clock guarantees that updates for the same QueryState will be ordered
     * in the sequence seen, even if multiple updates happen in the same millisecond.
     */
    public long getTimestamp()
    {
        return clientState.getTimestamp();
    }

    public boolean shouldTraceRequest(boolean tracingRequested)
    {
        if (tracingRequested)
            return true;

        // If no tracing is explicitly requested in the message, eventually trace the query according to configured trace probability.
        double traceProbability = StorageService.instance.getTraceProbability();
        return traceProbability != 0 && ThreadLocalRandom.current().nextDouble() < traceProbability;
    }

    public void prepareTracingSession(UUID sessionId)
    {
        this.preparedTracingSession = sessionId;
    }

    public void createTracingSession(Map<String,ByteBuffer> customPayload)
    {
        preparedTracingSession = Tracing.instance.newSession(customPayload);
    }

    public InetAddress getClientAddress()
    {
        return clientState.isInternal
             ? null
             : clientState.getRemoteAddress().getAddress();
    }

    public int getStreamId()
    {
        return streamId;
    }

    public Connection getConnection()
    {
        return clientState.connection;
    }

    public UUID getPreparedTracingSession()
    {
        return preparedTracingSession;
    }
}
