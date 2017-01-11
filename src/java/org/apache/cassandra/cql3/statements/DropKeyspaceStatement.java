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

import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.Validation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.Event;

public class DropKeyspaceStatement extends SchemaAlteringStatement
{
    private final String keyspace;
    private final boolean ifExists;

    public DropKeyspaceStatement(String keyspace, boolean ifExists)
    {
        super();
        this.keyspace = keyspace;
        this.ifExists = ifExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasKeyspaceAccess(keyspace, CorePermission.DROP);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        Validation.validateKeyspaceNotSystem(keyspace);
    }

    @Override
    public String keyspace()
    {
        return keyspace;
    }

    public Single<Event.SchemaChange> announceMigration(boolean isLocalOnly) throws ConfigurationException
    {
        return MigrationManager.announceKeyspaceDrop(keyspace, isLocalOnly)
                .toSingle(() -> new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, keyspace()))
                .onErrorResumeNext(exc -> {
                    if (exc instanceof ConfigurationException && ifExists)
                        return Single.just(Event.SchemaChange.NONE);
                    else
                        return Single.error(exc);
                });
    }
}
