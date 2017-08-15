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
import java.util.TreeSet;
import java.util.stream.Collectors;

import io.reactivex.Single;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;

import org.apache.cassandra.auth.GrantMode;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.enums.PartitionedEnum;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class RevokePermissionsStatement extends PermissionsManagementStatement
{
    public RevokePermissionsStatement(Set<Permission> permissions, IResource resource, RoleName grantee, GrantMode grantMode)
    {
        super(permissions, resource, grantee, grantMode);
    }

    @Override
    protected String operation()
    {
        return grantMode.revokeOperationName();
    }

    @Override
    public AuditableEventType getAuditEventType()
    {
        return CoreAuditableEventType.REVOKE;
    }

    @Override
    public Single<ResultMessage> execute(QueryState state)
    {
        return Single.fromCallable(() -> {

            IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
            Set<Permission> revoked = authorizer.revoke(state.getUser(), permissions, resource, grantee, grantMode);

            // We want to warn the client if all the specified permissions have not been revoked and the client did
            // not specified ALL in the query.
            if (!revoked.equals(permissions) && !permissions.equals(authorizer.applicablePermissions(resource)))
            {
                // We use a TreeSet to guarantee the order for testing
                String permissionsStr = new TreeSet<>(permissions).stream()
                                                                  .filter(permission -> !revoked.contains(permission))
                                                                  .map(PartitionedEnum::name)
                                                                  .collect(Collectors.joining(", "));

                ClientWarn.instance.warn(grantMode.revokeWarningMessage(grantee.getRoleName(),
                                                                        resource,
                                                                        permissionsStr));
            }

            return new ResultMessage.Void();
        });
    }
}
