/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.audit.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.apollo.audit.AuditableEvent;
import com.datastax.apollo.audit.AuditableEventType;
import com.datastax.apollo.audit.BindVariablesFormatter;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.statements.AlterKeyspaceStatement;
import org.apache.cassandra.cql3.statements.AlterRoleStatement;
import org.apache.cassandra.cql3.statements.AlterTableStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.CreateIndexStatement;
import org.apache.cassandra.cql3.statements.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.CreateRoleStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.CreateTriggerStatement;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.DropIndexStatement;
import org.apache.cassandra.cql3.statements.DropKeyspaceStatement;
import org.apache.cassandra.cql3.statements.DropRoleStatement;
import org.apache.cassandra.cql3.statements.DropTableStatement;
import org.apache.cassandra.cql3.statements.DropTriggerStatement;
import org.apache.cassandra.cql3.statements.GrantPermissionsStatement;
import org.apache.cassandra.cql3.statements.GrantRoleStatement;
import org.apache.cassandra.cql3.statements.ListPermissionsStatement;
import org.apache.cassandra.cql3.statements.ListRolesStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.RevokePermissionsStatement;
import org.apache.cassandra.cql3.statements.RevokeRoleStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;

public class AuditableEventGenerator
{
    private static final Logger logger = LoggerFactory.getLogger(AuditableEventGenerator.class);

    private static final List<AuditableEvent> NO_EVENTS = Lists.newArrayList();

    private static final Map<Class<? extends CQLStatement>, AuditableEventType> STATEMENT_TYPES =
            new HashMap<Class<? extends CQLStatement>, AuditableEventType>()
            {
                {
                    put(UpdateStatement.class, AuditableEventType.CQL_UPDATE);
                    put(DeleteStatement.class, AuditableEventType.CQL_DELETE);
                    put(SelectStatement.class, AuditableEventType.CQL_SELECT);
                    put(AlterTableStatement.class, AuditableEventType.UPDATE_CF);
                    put(CreateTableStatement.class, AuditableEventType.ADD_CF);
                    put(DropTableStatement.class, AuditableEventType.DROP_CF);
                    put(CreateIndexStatement.class, AuditableEventType.CREATE_INDEX);
                    put(DropIndexStatement.class, AuditableEventType.DROP_INDEX);
                    put(CreateTriggerStatement.class, AuditableEventType.CREATE_TRIGGER);
                    put(DropTriggerStatement.class, AuditableEventType.DROP_TRIGGER);
                    put(CreateKeyspaceStatement.class, AuditableEventType.ADD_KS);
                    put(DropKeyspaceStatement.class, AuditableEventType.DROP_KS);
                    put(AlterKeyspaceStatement.class, AuditableEventType.UPDATE_KS);
                    put(CreateIndexStatement.class, AuditableEventType.CREATE_INDEX);
                    put(TruncateStatement.class, AuditableEventType.TRUNCATE);
                    put(UseStatement.class, AuditableEventType.SET_KS);
                    put(GrantPermissionsStatement.class, AuditableEventType.GRANT);
                    put(GrantRoleStatement.class, AuditableEventType.GRANT);
                    put(RevokePermissionsStatement.class, AuditableEventType.REVOKE);
                    put(RevokeRoleStatement.class, AuditableEventType.REVOKE);
                    put(CreateRoleStatement.class, AuditableEventType.CREATE_ROLE);
                    put(AlterRoleStatement.class, AuditableEventType.ALTER_ROLE);
                    put(DropRoleStatement.class, AuditableEventType.DROP_ROLE);
                    put(ListRolesStatement.class, AuditableEventType.LIST_ROLES);
                    put(ListPermissionsStatement.class, AuditableEventType.LIST_PERMISSIONS);
                }
            };

    public List<AuditableEvent> getEventsForPrepare(CQLStatement statement,
                                                    ClientState clientState,
                                                    String queryString)
    {
        AuditableEvent.Builder builder = AuditableEvent.Builder.fromClientState(clientState);
        builder.type(AuditableEventType.CQL_PREPARE_STATEMENT);
        if (statement instanceof BatchStatement)
        {
            UUID batchId = UUID.randomUUID();
            List<BatchStatementUtils.Meta> batchStatements = BatchStatementUtils.decomposeBatchStatement(queryString);
            int i = 0;
            List<AuditableEvent> events = Lists.newArrayList();
            for (ModificationStatement stmt : ((BatchStatement)statement).getStatements())
            {
                builder.batch(batchId);
                events.add(getEvent(builder,
                                    stmt,
                                    batchStatements.get(i++).query,
                                    Collections.<ByteBuffer>emptyList(),
                                    Collections.<ColumnSpecification>emptyList(),
                                    AuditableEvent.NO_CL));
            }
            return events;
        }
        else
        {
            return Lists.newArrayList(getEvent(builder,
                                               statement,
                                               queryString,
                                               Collections.<ByteBuffer>emptyList(),
                                               Collections.<ColumnSpecification>emptyList(),
                                               AuditableEvent.NO_CL));
        }
    }

    public List<AuditableEvent> getEvents(CQLStatement statement,
                                          QueryState queryState,
                                          String queryString,
                                          List<ByteBuffer> variables,
                                          List<ColumnSpecification> boundNames,
                                          UUID batchId,
                                          ConsistencyLevel consistencyLevel)
    {
        AuditableEvent.Builder builder = AuditableEvent.Builder.fromClientState(queryState.getClientState());
        builder.batch(batchId);
        AuditableEvent event = getEvent(builder, statement, queryString, variables, boundNames, consistencyLevel);
        return Lists.newArrayList(event);
    }

    public List<AuditableEvent> getEvents(CQLStatement statement,
                                          QueryState queryState,
                                          String queryString,
                                          List<ByteBuffer> variables,
                                          List<ColumnSpecification> boundNames,
                                          ConsistencyLevel consistencyLevel)
    {
        // this case handles old style batches, where the batch is submitted
        // either of the Thrift CQL3 methods or via a QueryMessage in the
        // native protocol. So, it has to jump through some hoops to decompose
        // the statement's CQL string into the individual statements.
        // Batches submitted via native protocol BatchMessage (in protocol v2)
        // are handled differently, with the batch uid assigned externally
        if (statement instanceof BatchStatement)
        {
            UUID batchId = UUID.randomUUID();
            List<BatchStatementUtils.Meta> batchStatements = BatchStatementUtils.decomposeBatchStatement(queryString);
            int i = 0;
            List<AuditableEvent> events = new ArrayList<>(batchStatements.size());
            for (ModificationStatement stmt : ((BatchStatement)statement).getStatements())
            {
                AuditableEvent.Builder builder = AuditableEvent.Builder.fromClientState(queryState.getClientState());
                builder.batch(batchId);
                BatchStatementUtils.Meta stmtMeta = batchStatements.get(i++);
                AuditableEvent event = getEvent(builder,
                                                stmt,
                                                stmtMeta.query,
                                                stmtMeta.getSubList(variables),
                                                stmtMeta.getSubList(boundNames),
                                                consistencyLevel);
                if (event != null)
                {
                    events.add(event);
                }
            }
            return events;
        }
        else
        {
            AuditableEvent.Builder builder = AuditableEvent.Builder.fromClientState(queryState.getClientState());
            AuditableEvent event = getEvent(builder, statement, queryString, variables, boundNames, consistencyLevel);
            return event != null ? com.google.common.collect.Lists.newArrayList(event) : NO_EVENTS;
        }
    }

    private AuditableEvent getEvent(AuditableEvent.Builder builder,
                                    CQLStatement stmt,
                                    String queryString,
                                    List<ByteBuffer> variables,
                                    List<ColumnSpecification> boundNames,
                                    ConsistencyLevel cl)
    {
        AuditableEventType type = STATEMENT_TYPES.get(stmt.getClass());
        if (null == type)
        {
            logger.info("Encountered a CQL statement I don't know how to log : "
                    + stmt.getClass().getName() + " ( " + queryString + ")");
            return null;
        }
        else if (! builder.isTypeSet())
        {
            builder.type(type);
        }

        // builder handles null values for us
        builder.keyspace(AuditUtils.getKeyspace(stmt));
        builder.columnFamily(AuditUtils.getColumnFamily(stmt));

        if (null != variables && ! variables.isEmpty())
        {
            queryString += " " + formatBindVariables(boundNames, variables);
        }

        // special case CREATE & ALTER USER statements to obfuscate passwords
        if (stmt instanceof CreateRoleStatement || stmt instanceof AlterRoleStatement)
        {
            queryString = queryString.replaceAll("(?i)PASSWORD\\s+'.*'", "PASSWORD '*****'");
            queryString = queryString.replaceAll("(?i)PASSWORD\\s+=\\s+'.*'", "PASSWORD = '*****'");
        }

        builder.operation(queryString);
        builder.consistencyLevel(cl);
        AuditableEvent event = builder.build();
        return event;
    }

    public String formatBindVariables(List<ColumnSpecification> boundNames, List<ByteBuffer> variables)
    {
        // If there are no variables in the list, or for some reason we don't
        // have the variable definitions just return an empty string
        if (null == variables || variables.isEmpty() || null == boundNames || boundNames.isEmpty() )
        {
            return "[bind variable values unavailable]";
        }
        else
        {
            // where we do know about the type and name of variables in the
            // query, construct a formatted string to represent them
            BindVariablesFormatter formatter = new BindVariablesFormatter();
            int idx = 0;
            ColumnSpecification spec;
            for (ByteBuffer var : variables)
            {
                spec = boundNames.get(idx++);
                formatter.collect(spec.name.toString(), var == null ? "NULL" : spec.type.getString(var));
            }
            return formatter.format();
        }
    }
}
