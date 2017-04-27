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
import java.util.ArrayList;
import java.util.List;
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
import org.apache.cassandra.streaming.SessionSummary;

/**
 *
 * @since 2.0
 */
public class SyncComplete extends RepairMessage<SyncComplete>
{

    /** nodes that involved in this sync */
    public final NodePair nodes;
    /** true if sync success, false otherwise */
    public final boolean success;

    public final List<SessionSummary> summaries;

    public SyncComplete(RepairJobDesc desc, NodePair nodes, boolean success, List<SessionSummary> summaries)
    {
        super(desc);
        this.nodes = nodes;
        this.success = success;
        this.summaries = summaries;
    }

    public SyncComplete(RepairJobDesc desc, InetAddress endpoint1, InetAddress endpoint2, boolean success, List<SessionSummary> summaries)
    {
        super(desc);
        this.nodes = new NodePair(endpoint1, endpoint2);
        this.success = success;
        this.summaries = summaries;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof SyncComplete))
            return false;
        SyncComplete other = (SyncComplete)o;
        return desc.equals(other.desc) &&
               success == other.success &&
               nodes.equals(other.nodes) &&
               summaries.equals(other.summaries);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(desc, success, nodes, summaries);
    }

    public MessageSerializer<SyncComplete> serializer(RepairVersion version)
    {
        return serializers.get(version);
    }

    public Verb<SyncComplete, ?> verb()
    {
        return Verbs.REPAIR.SYNC_COMPLETE;
    }

    public static final Versioned<RepairVersion, MessageSerializer<SyncComplete>> serializers = RepairVersion.versioned(v -> new MessageSerializer<SyncComplete>(v)
    {
        public void serialize(SyncComplete message, DataOutputPlus out) throws IOException
        {
            RepairJobDesc.serializers.get(version).serialize(message.desc, out);
            NodePair.serializer.serialize(message.nodes, out);
            out.writeBoolean(message.success);
            out.writeInt(message.summaries.size());
            for (SessionSummary summary: message.summaries)
            {
                SessionSummary.serializers.get(version).serialize(summary, out);
            }
        }

        public SyncComplete deserialize(DataInputPlus in) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializers.get(version).deserialize(in);
            NodePair nodes = NodePair.serializer.deserialize(in);
            boolean success = in.readBoolean();
            int numSummaries = in.readInt();
            List<SessionSummary> summaries = new ArrayList<>(numSummaries);
            for (int i=0; i<numSummaries; i++)
            {
                summaries.add(SessionSummary.serializers.get(version).deserialize(in));
            }

            return new SyncComplete(desc, nodes, success, summaries);
        }

        public long serializedSize(SyncComplete message)
        {
            long size = RepairJobDesc.serializers.get(version).serializedSize(message.desc);
            size += NodePair.serializer.serializedSize(message.nodes);
            size += TypeSizes.sizeof(message.success);

            size += TypeSizes.sizeof(message.summaries.size());
            for (SessionSummary summary: message.summaries)
            {
                size += SessionSummary.serializers.get(version).serializedSize(summary);
            }

            return size;
        }
    });
}
