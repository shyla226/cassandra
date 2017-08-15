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

import io.reactivex.Single;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public class DropRoleStatement extends AuthenticationStatement
{
    private final RoleResource role;
    private final boolean ifExists;

    public DropRoleStatement(RoleName name, boolean ifExists)
    {
        this.role = RoleResource.role(name.getName());
        this.ifExists = ifExists;
    }

    @Override
    public AuditableEventType getAuditEventType()
    {
        return CoreAuditableEventType.DROP_ROLE;
    }

    @Override
    public void checkAccess(QueryState state)
    {
        // validate login first to avoid leaking user existence to anonymous users.
        state.checkNotAnonymous();

        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();

        if (!ifExists && !roleManager.isExistingRole(role))
            throw invalidRequest("%s doesn't exist", role.getRoleName());

        if (state != null && state.getUserName().equals(role.getRoleName()))
            throw invalidRequest("Cannot DROP primary role for current login");

        super.checkPermission(state, CorePermission.DROP, role);

        // We only check superuser status for existing roles to avoid
        // caching info about roles which don't exist (CASSANDRA-9189)
        if (roleManager.isExistingRole(role)
            && roleManager.hasSuperuserStatus(role)
            && !state.isSuper())
            throw new UnauthorizedException("Only superusers can drop a role with superuser status");
    }

    public void validate(QueryState state) throws RequestValidationException
    {
    }

    public Single<ResultMessage> execute(QueryState state) throws RequestValidationException, RequestExecutionException
    {
        return Single.fromCallable(() -> {

            // not rejected in validate()
            if (ifExists && !DatabaseDescriptor.getRoleManager().isExistingRole(role))
                return new ResultMessage.Void();

            DatabaseDescriptor.getRoleManager().dropRole(state.getUser(), role);

            return new ResultMessage.Void();
        });
    }
}
