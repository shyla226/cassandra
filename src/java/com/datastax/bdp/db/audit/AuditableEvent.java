/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.UUID;

import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static com.google.common.base.Strings.isNullOrEmpty;

public class AuditableEvent
{
    private static final String HOST = "host:";
    private static final String SOURCE = "|source:";
    private static final String USER = "|user:";
    private static final String AUTHENTICATED = "|authenticated:";
    private static final String TIMESTAMP = "|timestamp:";
    private static final String CATEGORY = "|category:";
    private static final String TYPE = "|type:";
    private static final String BATCH = "|batch:";
    private static final String KS = "|ks:";
    private static final String CF = "|cf:";
    private static final String OPERATION = "|operation:";
    private static final String CONSISTENCY_LEVEL = "|consistency level:";

    private final AuditableEventType type;
    private final UserRolesAndPermissions user;
    private final String source;
    private final InetAddress host;
    private final UUID uid;
    private final long timestamp;
    private final String keyspace;
    private final String columnFamily;
    private final UUID batch;
    private final String operation;
    private final ConsistencyLevel consistencyLevel;

    public static final ConsistencyLevel NO_CL = null;

    /**
     * The string used for unknown sources
     */
    public static String UNKNOWN_SOURCE = "/0.0.0.0";

    public AuditableEvent(UserRolesAndPermissions user,
                          AuditableEventType type,
                          String source,
                          UUID uid,
                          UUID batchId,
                          String keyspace,
                          String table,
                          String operation,
                          ConsistencyLevel cl)
    {
        this.type = type;
        this.user = user;
        this.source = source;
        this.host = FBUtilities.getBroadcastAddress();
        this.uid = uid;
        this.timestamp = UUIDGen.unixTimestamp(uid);
        this.keyspace = keyspace;
        this.columnFamily = table;
        this.batch = batchId;
        this.operation = operation;
        this.consistencyLevel = cl;
    }

    public AuditableEvent(QueryState state, AuditableEventType type, String operation)
    {
        this(state, type, null, null, null, operation, null);
    }

    public AuditableEvent(UserRolesAndPermissions user, AuditableEventType type, String source, String operation)
    {
        this(user, type, source, UUIDGen.getTimeUUID(), null, null, null, operation, null);
    }

    public AuditableEvent(QueryState queryState,
                          AuditableEventType type,
                          UUID batchId,
                          String keyspace,
                          String table,
                          String operation,
                          ConsistencyLevel cl)
    {
        this(queryState.getUserRolesAndPermissions(),
             type,
             getEventSource(queryState.getClientState()),
             UUIDGen.getTimeUUID(),
             batchId,
             keyspace,
             table,
             operation,
             cl);
    }

    public AuditableEvent(AuditableEvent event, AuditableEventType type, String operation)
    {
        this.type = type;
        this.user = event.user;
        this.source = event.source;
        this.host = event.host;
        this.uid = UUIDGen.getTimeUUID();
        this.timestamp = event.timestamp;
        this.keyspace = event.keyspace;
        this.columnFamily = event.columnFamily;
        this.batch = event.batch;
        this.operation = operation;
        this.consistencyLevel = event.consistencyLevel;
    }

    public AuditableEventType getType()
    {
        return type;
    }

    public String getUser()
    {
        return user.getName();
    }

    public String getAuthenticated()
    {
        return user.getAuthenticatedName();
    }

    public String getSource()
    {
        return source;
    }

    public InetAddress getHost()
    {
        return host;
    }

    public UUID getUid()
    {
        return uid;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public String getOperation()
    {
        return operation;
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    public UUID getBatchId()
    {
        return batch;
    }

    public String getColumnFamily()
    {
        return columnFamily;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    /**
     * Checks if the user that performed the operation has the specified role.
     * @param role the role to check
     * @return {@code true} if the user has the specified role, {@code false} otherwise.
     */
    public boolean userHasRole(RoleResource role)
    {
        return user.hasRole(role);
    }

    public String toString()
    {
        StringBuilder builder =  new StringBuilder(HOST).append(host)
                                                        .append(SOURCE).append(source)
                                                        .append(USER).append(user.getName())
                                                        .append(AUTHENTICATED).append(user.getAuthenticatedName())
                                                        .append(TIMESTAMP).append(timestamp)
                                                        .append(CATEGORY).append(type.getCategory())
                                                        .append(TYPE).append(type);
        // optional fields
        if ( batch != null )
            builder.append(BATCH).append(batch);

        if ( ! isNullOrEmpty(keyspace) )
            builder.append(KS).append(keyspace);

        if ( ! isNullOrEmpty(columnFamily) )
            builder.append(CF).append(columnFamily);

        if ( ! isNullOrEmpty(operation) )
            builder.append(OPERATION).append(operation);

        if (consistencyLevel != null)
            builder.append(CONSISTENCY_LEVEL).append(consistencyLevel);

        return builder.toString();
    }

    private static String getEventSource(ClientState state)
    {
        SocketAddress sockAddress = state.getRemoteAddress();
        if (sockAddress instanceof InetSocketAddress)
        {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) sockAddress;
            if (!inetSocketAddress.isUnresolved())
                return inetSocketAddress.getAddress().toString();
        }

        return UNKNOWN_SOURCE;
    }
}
