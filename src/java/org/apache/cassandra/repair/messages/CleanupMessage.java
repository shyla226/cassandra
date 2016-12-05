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
import java.util.UUID;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.RepairVerbs.RepairVersion;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * Message to cleanup repair resources on replica nodes.
 *
 * @since 2.1.6
 */
public class CleanupMessage extends RepairMessage<CleanupMessage>
{
    public static Versioned<RepairVersion, MessageSerializer<CleanupMessage>> serializers = RepairVersion.versioned(v -> new MessageSerializer<CleanupMessage>(v)
    {
        public void serialize(CleanupMessage message, DataOutputPlus out) throws IOException
        {
            UUIDSerializer.serializer.serialize(message.parentRepairSession, out);
        }

        public CleanupMessage deserialize(DataInputPlus in) throws IOException
        {
            UUID parentRepairSession = UUIDSerializer.serializer.deserialize(in);
            return new CleanupMessage(parentRepairSession);
        }

        public long serializedSize(CleanupMessage message)
        {
            return UUIDSerializer.serializer.serializedSize(message.parentRepairSession);
        }
    });
    
    public final UUID parentRepairSession;

    public CleanupMessage(UUID parentRepairSession)
    {
        super(null);
        this.parentRepairSession = parentRepairSession;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof CleanupMessage))
            return false;
        CleanupMessage other = (CleanupMessage) o;
        return parentRepairSession.equals(other.parentRepairSession);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(parentRepairSession);
    }

    public MessageSerializer<CleanupMessage> serializer(RepairVersion version)
    {
        return serializers.get(version);
    }

    public Verb<CleanupMessage, ?> verb()
    {
        return Verbs.REPAIR.CLEANUP;
    }
}
