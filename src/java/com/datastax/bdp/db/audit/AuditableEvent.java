/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static com.google.common.base.Strings.isNullOrEmpty;

public class AuditableEvent
{
    private static final Logger logger = LoggerFactory.getLogger(AuditableEvent.class);

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
    private final String user;
    private final String authenticated;
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
    private static InetAddress UNKNOWN_SOURCE;

    static
    {
        try
        {
            UNKNOWN_SOURCE = InetAddress.getByAddress(new byte[]{ 0, 0, 0, 0 });
        }
        catch (UnknownHostException e)
        {
            logger.error("Error creating default InetAddress for unknown event sources", e);
            // fail fast
            throw new RuntimeException("Unable to initialise constants for audit logging", e);
        }
    }

    private AuditableEvent(Builder builder)
    {
        this.type = builder.type;
        this.user = builder.user;
        this.authenticated = builder.authenticated;
        this.source = builder.source;
        this.host = builder.host;
        this.uid = builder.uid;
        this.timestamp = builder.timestamp;
        this.keyspace = builder.keyspace;
        this.columnFamily = builder.columnFamily;
        this.batch = builder.batch;
        this.operation = builder.operation;
        this.consistencyLevel = builder.consistencyLevel;
    }

    public AuditableEventType getType()
    {
        return type;
    }

    public String getUser()
    {
        return user;
    }

    public String getAuthenticated()
    {
        return authenticated;
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

    public String toString()
    {
        StringBuilder builder =  new StringBuilder(HOST).append(host)
                                                        .append(SOURCE).append(source)
                                                        .append(USER).append(user)
                                                        .append(AUTHENTICATED).append(authenticated)
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

    public static class Builder
    {
        private final String user;
        private final String authenticated;
        private final String source;
        private final InetAddress host;

        private AuditableEventType type;
        private long timestamp;
        private String keyspace = null;
        private String columnFamily = null;
        private UUID batch = null;
        private String operation = null;
        private ConsistencyLevel consistencyLevel;
        private UUID uid = null;

        // user -> authorization user
        public Builder(AuditableEventType type, String user, String authenticated, String source)
        {
            this.user = user;
            this.authenticated = authenticated;
            this.source = source;
            this.type = type;
            this.host = FBUtilities.getBroadcastAddress();
        }

        public Builder(String user, String source)
        {
            this(null, user, user, source);
        }

        public Builder(AuthenticatedUser user, String source)
        {
            this(null, user.getName(), user.getName(), source);
        }

        public Builder(AuditableEventType type, String user, String source)
        {
            this(type, user, user, source);
        }

        public Builder type(AuditableEventType type)
        {
            this.type = type;
            return this;
        }

        private static InetAddress getEventSource(ClientState state)
        {
            SocketAddress sockAddress = state.getRemoteAddress();
            if (sockAddress == null)
                return UNKNOWN_SOURCE;

            if (sockAddress instanceof InetSocketAddress && !(((InetSocketAddress) sockAddress).isUnresolved()))
            {
                return ((InetSocketAddress) sockAddress).getAddress();
            }
            else
            {
                return UNKNOWN_SOURCE;
            }
        }

        public static Builder fromClientState(ClientState clientState)
        {
            AuthenticatedUser user = (clientState == null || clientState.getUser() == null)
                                        ? AuthenticatedUser.SYSTEM_USER
                                        : clientState.getUser();
            return new Builder(user, getEventSource(clientState).toString());
        }

        public static Builder fromEvent(AuditableEvent event)
        {
            Builder builder = new Builder(event.type, event.user, event.authenticated, event.source);
            builder.timestamp = event.timestamp;
            builder.keyspace = event.keyspace;
            builder.columnFamily = event.columnFamily;
            builder.batch = event.batch;
            builder.operation = event.operation;
            builder.consistencyLevel = event.consistencyLevel;
            return builder;
        }

        public static Builder fromUnauthorizedException(AuditableEvent event, UnauthorizedException e)
        {
            AuditableEvent.Builder builder = fromEvent(event);
            builder.type(AuditableEventType.UNAUTHORIZED_ATTEMPT);
            String operation = event.getOperation();
            operation = e.getLocalizedMessage() + (operation != null ? " " + operation : "");
            builder.operation(operation);
            return builder;
        }

        public boolean isTypeSet()
        {
            return type != null;
        }

        public Builder keyspace(String keyspace)
        {
            this.keyspace = keyspace;
            return this;
        }

        public Builder columnFamily(String columnFamily)
        {
            this.columnFamily = columnFamily;
            return this;
        }

        public Builder batch(UUID batchId)
        {
            this.batch = batchId;
            return this;
        }

        public Builder batch(String batchId)
        {
            if (batchId == null || batchId.length() == 0)
            {
                this.batch = null;
                return this;
            }

            try
            {
                return batch(UUID.fromString(batchId));
            }
            catch (IllegalArgumentException e)
            {
                this.batch = null;
                return this;
            }
        }

        public Builder operation(String operation)
        {
            this.operation = operation;
            return this;
        }

        public Builder consistencyLevel(ConsistencyLevel consistencyLevel)
        {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        @VisibleForTesting
        Builder uid(UUID uid)
        {
            this.uid = uid;
            return this;
        }

        public AuditableEvent build()
        {
            if (null == type)
                throw new IllegalStateException("Cannot build event without a type");

            if (uid == null)
            {
                uid = UUIDGen.getTimeUUID();
            }
            timestamp = UUIDs.unixTimestamp(uid);
            return new AuditableEvent(this);
        }
    }
}
