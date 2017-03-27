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
import java.util.Objects;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.messages.RepairVerbs.RepairVersion;
import org.apache.cassandra.utils.versioning.Versioned;

public class SnapshotMessage extends RepairMessage<SnapshotMessage>
{
    public final static Versioned<RepairVersion, MessageSerializer<SnapshotMessage>> serializers = RepairVersion.versioned(v -> new MessageSerializer<SnapshotMessage>(v)
    {
        public void serialize(SnapshotMessage message, DataOutputPlus out) throws IOException
        {
            RepairJobDesc.serializers.get(v).serialize(message.desc, out);
        }

        public SnapshotMessage deserialize(DataInputPlus in) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializers.get(v).deserialize(in);
            return new SnapshotMessage(desc);
        }

        public long serializedSize(SnapshotMessage message)
        {
            return RepairJobDesc.serializers.get(v).serializedSize(message.desc);
        }
    });

    public SnapshotMessage(RepairJobDesc desc)
    {
        super(desc);
    }

    public MessageSerializer<SnapshotMessage> serializer(RepairVersion version)
    {
        return serializers.get(version);
    }

    public Verb<SnapshotMessage, ?> verb()
    {
        return Verbs.REPAIR.SNAPSHOT;
    }

    @Override
    public String toString()
    {
        return desc.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof SnapshotMessage))
            return false;
        SnapshotMessage other = (SnapshotMessage) o;
        return desc.equals(other.desc);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(desc);
    }
}
