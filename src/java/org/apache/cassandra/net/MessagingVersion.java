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
package org.apache.cassandra.net;

import java.util.EnumMap;

import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.WriteVerbs.WriteVersion;
import org.apache.cassandra.gms.GossipVerbs.GossipVersion;
import org.apache.cassandra.hints.HintsVerbs.HintsVersion;
import org.apache.cassandra.repair.messages.RepairVerbs.RepairVersion;
import org.apache.cassandra.schema.SchemaVerbs.SchemaVersion;
import org.apache.cassandra.service.OperationsVerbs.OperationsVersion;
import org.apache.cassandra.service.paxos.LWTVerbs.LWTVersion;
import org.apache.cassandra.utils.versioning.Version;

import static org.apache.cassandra.net.ProtocolVersion.dse;
import static org.apache.cassandra.net.ProtocolVersion.oss;

/**
 * Represents a version of the inter-node protocol.
 * <p>
 * The protocol version is mostly the product of all the messaging group versions.
 */
public enum MessagingVersion
{
    OSS_30 (oss(10), ReadVersion.OSS_30, WriteVersion.OSS_30, LWTVersion.OSS_30, HintsVersion.OSS_30, OperationsVersion.OSS_30, GossipVersion.OSS_30, RepairVersion.OSS_30, SchemaVersion.OSS_30),
    OSS_40 (oss(11), ReadVersion.OSS_40, WriteVersion.OSS_30, LWTVersion.OSS_30, HintsVersion.OSS_30, OperationsVersion.OSS_30, GossipVersion.OSS_30, RepairVersion.OSS_30, SchemaVersion.OSS_30),
    DSE_60 (dse(1), ReadVersion.DSE_60, WriteVersion.OSS_30, LWTVersion.OSS_30, HintsVersion.OSS_30, OperationsVersion.DSE_60, GossipVersion.OSS_30, RepairVersion.OSS_30, SchemaVersion.OSS_30);

    private final ProtocolVersion protocolVersion;
    private final EnumMap<Verbs.Group, Version<?>> groupVersions;

    MessagingVersion(ProtocolVersion version,
                     ReadVersion readVersion,
                     WriteVersion writeVersion,
                     LWTVersion lwtVersion,
                     HintsVersion hintsVersion,
                     OperationsVersion operationsVersion,
                     GossipVersion gossipVersion,
                     RepairVersion repairVersion,
                     SchemaVersion schemaVersion)
    {
        this.protocolVersion = version;
        this.groupVersions = new EnumMap<>(Verbs.Group.class);

        groupVersions.put(Verbs.Group.READS, readVersion);
        groupVersions.put(Verbs.Group.WRITES, writeVersion);
        groupVersions.put(Verbs.Group.LWT, lwtVersion);
        groupVersions.put(Verbs.Group.HINTS, hintsVersion);
        groupVersions.put(Verbs.Group.OPERATIONS, operationsVersion);
        groupVersions.put(Verbs.Group.GOSSIP, gossipVersion);
        groupVersions.put(Verbs.Group.REPAIR, repairVersion);
        groupVersions.put(Verbs.Group.SCHEMA, schemaVersion);
    }

    public boolean isDSE()
    {
        return protocolVersion.isDSE;
    }

    public ProtocolVersion protocolVersion()
    {
        return protocolVersion;
    }

    public static MessagingVersion min(MessagingVersion v1, MessagingVersion v2)
    {
        return v1.compareTo(v2) < 0 ? v1 : v2;
    }

    /**
     * Returns the biggest messaging version whose protocol version is less or equal to the provided version.
     * <p>
     * WARNING: note that this may not return the version exactly correspond to the provided protocol version if that
     * version is unknown to us.
     *
     * @param protocolVersion the protocol version.
     * @return the "best" known messaging version corresponding to {@code protocolVersion} or {@code null} if the version
     * cannot be supported (too old).
     */
    public static MessagingVersion from(ProtocolVersion protocolVersion)
    {
        MessagingVersion previous = null;
        for (MessagingVersion version : values())
        {
            int cmp = version.protocolVersion.compareTo(protocolVersion);
            // Either we have an exact match and return it, or we're on a version
            // greater than the one asked and we want to return the previous version
            // (or null if there was none), or we continue the search
            if (cmp == 0)
                return version;
            if (cmp > 0)
                return previous;

            previous = version;
        }
        // It's a version in the future, return our biggest version.
        return previous;
    }

    /**
     * A shortcut for {@code from(ProtocolVersion.fromHandshakeVersion(handshakeVersion))}.
     */
    public static MessagingVersion fromHandshakeVersion(int handshakeVersion)
    {
        return from(ProtocolVersion.fromHandshakeVersion(handshakeVersion));
    }

    @SuppressWarnings("unchecked")
    public <V extends Enum<V> & Version<V>> V groupVersion(Verbs.Group groupId)
    {
        return (V)groupVersions.get(groupId);
    }

    public <P, Q, V extends Enum<V> & Version<V>> VerbSerializer<P, Q> serializer(Verb<P, Q> verb)
    {
        VerbGroup<V> group = verb.group();
        V version = groupVersion(verb.group().id());
        return group.forVersion(version).getByVerb(verb);
    }

    public <P, Q, V extends Enum<V> & Version<V>> VerbSerializer<P, Q> serializerByVerbCode(VerbGroup<V> group, int code)
    {
        V version = groupVersion(group.id());
        VerbSerializer<P, Q> serializer = group.forVersion(version).getByCode(code);
        if (serializer == null)
            throw new IllegalArgumentException(String.format("Invalid verb code %d for group %s at version %s", code, group, this));
        return serializer;
    }
}
