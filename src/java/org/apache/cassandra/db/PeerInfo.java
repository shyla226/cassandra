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

package org.apache.cassandra.db;

import java.net.InetAddress;
import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.utils.CassandraVersion;

/**
 * A class for storing properties from System.PEERS, see {@link SystemKeyspace#peers}.
 */
class PeerInfo
{
    @Nullable String rack;
    @Nullable String dc;
    @Nullable CassandraVersion version;
    @Nullable UUID schemaVersion;
    @Nullable UUID hostId;
    @Nullable InetAddress preferredIp;

    PeerInfo()
    {
        this(null);
    }

    PeerInfo(UntypedResultSet.Row row)
    {
        if (row == null)
            return; // all properties will be null

        this.rack = row.has("rack") ? row.getString("rack") : null;
        this.dc = row.has("data_center") ? row.getString("data_center") : null;
        this.version = row.has("release_version") ? new CassandraVersion(row.getString("release_version")) : null;
        this.schemaVersion = row.has("schema_version") ? row.getUUID("schema_version") : null;
        this.hostId = row.has("host_id") ? row.getUUID("host_id") : null;
        this.preferredIp = row.has("preferred_ip") ? row.getInetAddress("preferred_ip") : null;
    }

    PeerInfo setRack(String rack)
    {
        this.rack = rack;
        return this;
    }

    PeerInfo setDc(String dc)
    {
        this.dc = dc;
        return this;
    }

    PeerInfo setVersion(CassandraVersion version)
    {
        this.version = version;
        return this;
    }

    PeerInfo setSchemaVersion(UUID schemaVersion)
    {
        this.schemaVersion = schemaVersion;
        return this;
    }

    PeerInfo setHostId(UUID hostId)
    {
        this.hostId = hostId;
        return this;
    }

    PeerInfo setPreferredIp(InetAddress preferredIp)
    {
        this.preferredIp = preferredIp;
        return this;
    }

    PeerInfo setValue(String name, Object value)
    {
        if (name.equals("rack"))
            return setRack((String)value);
        else if (name.equals("data_center"))
            return setDc((String)value);
        if (name.equals("release_version"))
            return setVersion(new CassandraVersion((String)value));
        if (name.equals("schema_version"))
            return setSchemaVersion((UUID) value);
        if (name.equals("host_id"))
            return setHostId((UUID) value);
        if (name.equals("preferred_ip"))
            return setPreferredIp((InetAddress) value);

        return this;
    }
}
