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
package org.apache.cassandra.cql3.statements;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.permission.Permissions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.QueryState;

public abstract class PermissionsManagementStatement extends AuthorizationStatement
{
    protected final Set<Permission> permissions;
    protected IResource resource;
    protected final RoleResource grantee;
    protected final GrantMode grantMode;

    protected PermissionsManagementStatement(Set<Permission> permissions, IResource resource, RoleName grantee, GrantMode grantMode)
    {
        this.permissions = permissions;
        this.resource = resource;
        this.grantee = RoleResource.role(grantee.getName());
        this.grantMode = grantMode;
    }

    public void validate(QueryState state) throws RequestValidationException
    {
    }

    public void checkAccess(QueryState state) throws UnauthorizedException
    {
        // validate login here before checkAccess to avoid leaking user existence to anonymous users.
        state.ensureNotAnonymous();

        if (!DatabaseDescriptor.getRoleManager().isExistingRole(grantee))
            throw new InvalidRequestException(String.format("Role %s doesn't exist", grantee.getRoleName()));

        // if a keyspace is omitted when GRANT/REVOKE ON TABLE <table>, we need to correct the resource.
        resource = maybeCorrectResource(resource, state.getClientState());

        // altering permissions on builtin functions is not supported
        if (resource instanceof FunctionResource
            && SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(((FunctionResource)resource).getKeyspace()))
        {
            throw new InvalidRequestException("Altering permissions on builtin functions is not supported");
        }

        if (!resource.exists())
            throw new InvalidRequestException(String.format("Resource %s doesn't exist", resource));

        if (state.isSuper())
            // Nobody can stop superman
            return;

        if (grantMode == GrantMode.RESTRICT)
            throw new UnauthorizedException("Only superusers are allowed to RESTRICT/UNRESTRICT");

        Set<Permission> missingPermissions = Permissions.setOf();
        try
        {
            // check that the user has AUTHORIZE permission on the resource or its parents, otherwise reject GRANT/REVOKE.
            state.ensureHasPermission(CorePermission.AUTHORIZE, resource);

            // check that the user has [a single permission or all in case of ALL] on the resource or its parents.
            for (Permission p : permissions)
                try
                {
                    state.ensureHasPermission(p, resource);
                }
                catch (UnauthorizedException noAuthorizePermission)
                {
                    missingPermissions.add(p);
                }
        }
        catch (UnauthorizedException noAuthorizePermission)
        {
            missingPermissions.add(CorePermission.AUTHORIZE);
        }

        if (!missingPermissions.isEmpty())
        {
            if (grantMode == GrantMode.GRANTABLE)
            {
                throw new UnauthorizedException(String.format("User %s must not grant AUTHORIZE FOR %s permission on %s",
                                                              state.getUser().getName(),
                                                              StringUtils.join(missingPermissions, ", "),
                                                              resource));
            }

            Set<Permission> missingGrantables = Permissions.setOf();

            // Check that the user has grant-option on all permissions to be
            // granted for the resource
            for (Permission p : permissions)
                if (!state.hasGrantOption(p, resource))
                {
                    missingGrantables.add(p);
                }

            if (!missingGrantables.isEmpty())
                throw new UnauthorizedException(String.format("User %s has no %s permission nor AUTHORIZE FOR %s permission on %s or any of its parents",
                                                              state.getUser().getName(),
                                                              StringUtils.join(missingPermissions, ", "),
                                                              StringUtils.join(missingGrantables, ", "),
                                                              resource));

            // This prevents a user to grant a permission to himself or
            // to any of the roles the user belongs to. This check
            // assumes that getRoles() returns a role for the user himself
            // (i.e. like "RoleResource(username)" as for AuthenticatedUser.role).
            if (state.getRoles().contains(grantee))
                throw new UnauthorizedException(String.format("User %s has grant privilege for %s permission(s) on %s but must not grant/revoke for him/herself",
                                                              state.getUser().getName(),
                                                              StringUtils.join(permissions, ", "),
                                                              resource));

        }
    }
}
