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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Triggers;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public class DropTriggerStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(DropTriggerStatement.class);

    private final String triggerName;

    private final boolean ifExists;

    public DropTriggerStatement(CFName name, String triggerName, boolean ifExists)
    {
        super(name);
        this.triggerName = triggerName;
        this.ifExists = ifExists;
    }

    @Override
    public AuditableEventType getAuditEventType()
    {
        return CoreAuditableEventType.DROP_TRIGGER;
    }

    @Override
    public void checkAccess(QueryState state)
    {
        if (DatabaseDescriptor.getAuthenticator().requireAuthentication() && !state.isSuper())
            throw new UnauthorizedException("Only superusers are allowed to perform DROP TRIGGER queries");
    }

    public void validate(QueryState state) throws RequestValidationException
    {
        Schema.instance.validateTable(keyspace(), columnFamily());
    }

    public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws ConfigurationException, InvalidRequestException
    {
        TableMetadata current = Schema.instance.getTableMetadata(keyspace(), columnFamily());
        Triggers triggers = current.triggers;

        if (!triggers.get(triggerName).isPresent())
        {
            if (ifExists)
                return Maybe.empty();
            else
                return error(String.format("Trigger %s was not found", triggerName));
        }

        logger.info("Dropping trigger with name {}", triggerName);

        TableMetadata updated =
            current.unbuild()
                   .triggers(triggers.without(triggerName))
                   .build();

        return MigrationManager.announceTableUpdate(updated, isLocalOnly)
                .andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily())));
    }
}
