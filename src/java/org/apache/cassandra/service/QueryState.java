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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.transport.Connection;

/**
 * Represents the state related to a given query.
 */
public class QueryState
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraRoleManager.class);

    private final ClientState clientState;
    private final int streamId;

    private UserRolesAndPermissions userRolesAndPermissions;

    private QueryState(QueryState queryState,
                       ClientState clientState,
                       UserRolesAndPermissions userRolesAndPermissions)
    {
        this(clientState, queryState.streamId, userRolesAndPermissions);
    }

    public QueryState(ClientState clientState, UserRolesAndPermissions userRolesAndPermissions)
    {
        this(clientState, 0, userRolesAndPermissions);
    }

    public QueryState(ClientState clientState, int streamId, UserRolesAndPermissions userRolesAndPermissions)
    {
        this.clientState = clientState;
        this.streamId = streamId;
        this.userRolesAndPermissions = userRolesAndPermissions;
    }

    /**
     * @return a QueryState object for internal C* calls (not limited by any kind of auth).
     */
    public static QueryState forInternalCalls()
    {
        return new QueryState(ClientState.forInternalCalls(), UserRolesAndPermissions.SYSTEM);
    }

    public ClientState getClientState()
    {
        return clientState;
    }

    public String getUserName()
    {
        return userRolesAndPermissions.getName();
    }

    public AuthenticatedUser getUser()
    {
        return clientState.getUser();
    }

    public QueryState cloneWithKeyspaceIfSet(String keyspace)
    {
        ClientState clState = clientState.cloneWithKeyspaceIfSet(keyspace);
        if (clState == clientState)
            // this will be trie, if either 'keyspace' is null or 'keyspace' is equals to the current keyspace
            return this;
        return new QueryState(this, clState, userRolesAndPermissions);
    }

    /**
     * This clock guarantees that updates for the same QueryState will be ordered
     * in the sequence seen, even if multiple updates happen in the same millisecond.
     */
    public long getTimestamp()
    {
        return clientState.getTimestamp();
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

    /**
     * Checks if this user is a super user.
     * <p>Only a superuser is allowed to perform CREATE USER and DROP USER queries.
     * Im most cased, though not necessarily, a superuser will have Permission.ALL on every resource
     * (depends on IAuthorizer implementation).</p>
     */
    public boolean isSuper()
    {
        return userRolesAndPermissions.isSuper();
    }

    /**
     * Checks if this user is the system user (Apollo).
     * @return {@code true} if this user is the system user, {@code flase} otherwise.
     */
    public boolean isSystem()
    {
        return userRolesAndPermissions.isSystem();
    }

    /**
     * Validates that this user is not an anonymous one.
     */
    public void checkNotAnonymous()
    {
        userRolesAndPermissions.checkNotAnonymous();
    }

    /**
     * Checks if the user has the specified permission on the data resource.
     * @param resource the resource
     * @param perm the permission
     * @return {@code true} if the user has the permission on the data resource,
     * {@code false} otherwise.
     */
    public boolean hasDataPermission(DataResource resource, Permission perm)
    {
        return userRolesAndPermissions.hasDataPermission(resource, perm);
    }

    /**
     * Checks if the user has the specified permission on the function resource.
     * @param resource the resource
     * @param perm the permission
     * @return {@code true} if the user has the permission on the function resource,
     * {@code false} otherwise.
     */
    public final boolean hasFunctionPermission(FunctionResource resource, Permission perm)
    {
        return userRolesAndPermissions.hasFunctionPermission(resource, perm);
    }

    /**
     * Checks if the user has the specified permission on the resource.
     * @param resource the resource
     * @param perm the permission
     * @return {@code true} if the user has the permission on the resource,
     * {@code false} otherwise.
     */
    public final boolean hasPermission(IResource resource, Permission perm)
    {
        return userRolesAndPermissions.hasPermission(resource, perm);
    }

    /**
     * Checks if the user has the right to grant the permission on the resource.
     * @param resource the resource
     * @param perm the permission
     * @return {@code true} if the user has the right to grant the permission on the resource,
     * {@code false} otherwise.
     */
    public boolean hasGrantPermission(IResource resource, Permission perm)
    {
        return userRolesAndPermissions.hasGrantPermission(resource, perm);
    }

    /**
     * Validates that the user has the permission on all the keyspaces.
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on all the keyspaces
     */
    public final void checkAllKeyspacesPermission(Permission perm)
    {
        userRolesAndPermissions.checkAllKeyspacesPermission(perm);
    }

    /**
     * Validates that the user has the permission on the keyspace.
     * @param keyspace the keyspace
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the keyspace
     */
    public final void checkKeyspacePermission(String keyspace, Permission perm)
    {
        userRolesAndPermissions.checkKeyspacePermission(keyspace, perm);
    }

    /**
     * Validates that the user has the permission on the table.
     * @param keyspace the table keyspace
     * @param table the table
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the table.
     */
    public final void checkTablePermission(String keyspace, String table, Permission perm)
    {
        userRolesAndPermissions.checkTablePermission(keyspace, table, perm);
    }

    /**
     * Validates that the user has the permission on the table.
     * @param tableRef the table
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the table.
     */
    public final void checkTablePermission(TableMetadataRef tableRef, Permission perm)
    {
        userRolesAndPermissions.checkTablePermission(tableRef, perm);
    }

    /**
     * Validates that the user has the permission on the table.
     * @param table the table metadata
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the table.
     */
    public final void checkTablePermission(TableMetadata table, Permission perm)
    {
        userRolesAndPermissions.checkTablePermission(table, perm);
    }

    /**
     * Validates that the user has the permission on the function.
     * @param function the function
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the function.
     */
    public final void checkFunctionPermission(Function function, Permission permission)
    {
        userRolesAndPermissions.checkFunctionPermission(function, permission);
    }

    /**
     * Validates that the user has the permission on the function.
     * @param perm the permission
     * @param function the function resource
     * @throws UnauthorizedException if the user does not have the permission on the function.
     */
    public final void checkFunctionPermission(FunctionResource resource, Permission perm)
    {
        userRolesAndPermissions.checkFunctionPermission(resource, perm);
    }

    public final void checkPermission(IResource resource, Permission perm)
    {
        userRolesAndPermissions.checkPermission(resource, perm);
    }

    /**
     * Checks that this user has the specified role.
     * @param role the role
     * @return {@code true} if the user has the specified role, {@code false} otherwise.
     */
    public final boolean hasRole(RoleResource role)
    {
        return userRolesAndPermissions.hasRole(role);
    }

    /**
     * Checks if the user has the specified permission on the role resource.
     * @param resource the resource
     * @param perm the permission
     * @return {@code true} if the user has the permission on the role resource,
     * {@code false} otherwise.
     */
    public boolean hasRolePermission(RoleResource role, Permission perm)
    {
        return userRolesAndPermissions.hasRolePermission(role, perm);
    }

    public UserRolesAndPermissions getUserRolesAndPermissions()
    {
        return userRolesAndPermissions;
    }
}
