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

import io.reactivex.Maybe;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;

import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.QueryState;
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

    @Override
    public AuditableEventType getAuditEventType()
    {
        return CoreAuditableEventType.DROP_KS;
    }

    @Override
    public void checkAccess(QueryState state)
    {
        state.checkKeyspacePermission(keyspace, CorePermission.DROP);
    }

    public void validate(QueryState state) throws RequestValidationException
    {
        Schema.validateKeyspaceNotSystem(keyspace);
    }

    @Override
    public String keyspace()
    {
        return keyspace;
    }

    public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws ConfigurationException
    {
        return MigrationManager.announceKeyspaceDrop(keyspace, isLocalOnly)
                               .andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, keyspace())))
                               .onErrorResumeNext(e ->
                                                  {
                                                      if (e instanceof ConfigurationException && ifExists)
                                                          return Maybe.empty();

                                                      return Maybe.error(e);
                                                  });
    }
}
