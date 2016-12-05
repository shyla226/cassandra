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
package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.NodePair;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.messages.RepairVerbs.RepairVersion;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 *
 * @since 2.0
 */
public class SyncComplete extends RepairMessage<SyncComplete>
{
    public static final Versioned<RepairVersion, MessageSerializer<SyncComplete>> serializers = RepairVersion.versioned(v -> new MessageSerializer<SyncComplete>(v)
    {
        public void serialize(SyncComplete message, DataOutputPlus out) throws IOException
        {
            RepairJobDesc.serializers.get(version).serialize(message.desc, out);
            NodePair.serializer.serialize(message.nodes, out);
            out.writeBoolean(message.success);
        }

        public SyncComplete deserialize(DataInputPlus in) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializers.get(version).deserialize(in);
            NodePair nodes = NodePair.serializer.deserialize(in);
            return new SyncComplete(desc, nodes, in.readBoolean());
        }

        public long serializedSize(SyncComplete message)
        {
            long size = RepairJobDesc.serializers.get(version).serializedSize(message.desc);
            size += NodePair.serializer.serializedSize(message.nodes);
            size += TypeSizes.sizeof(message.success);
            return size;
        }
    });

    /** nodes that involved in this sync */
    public final NodePair nodes;
    /** true if sync success, false otherwise */
    public final boolean success;

    public SyncComplete(RepairJobDesc desc, NodePair nodes, boolean success)
    {
        super(desc);
        this.nodes = nodes;
        this.success = success;
    }

    public SyncComplete(RepairJobDesc desc, InetAddress endpoint1, InetAddress endpoint2, boolean success)
    {
        super(desc);
        this.nodes = new NodePair(endpoint1, endpoint2);
        this.success = success;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof SyncComplete))
            return false;
        SyncComplete other = (SyncComplete)o;
        return desc.equals(other.desc) &&
               success == other.success &&
               nodes.equals(other.nodes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(desc, success, nodes);
    }

    public MessageSerializer<SyncComplete> serializer(RepairVersion version)
    {
        return serializers.get(version);
    }

    public Verb<SyncComplete, ?> verb()
    {
        return Verbs.REPAIR.SYNC_COMPLETE;
    }
}
