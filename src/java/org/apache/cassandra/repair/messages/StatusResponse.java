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
import java.util.Optional;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.consistent.ConsistentSession;
import org.apache.cassandra.repair.messages.RepairVerbs.RepairVersion;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class StatusResponse extends ConsistentRepairMessage<StatusResponse>
{
    public static final Versioned<RepairVersion, MessageSerializer<StatusResponse>> serializers = RepairVersion.versioned(v -> new MessageSerializer<StatusResponse>(v)
    {
        public void serialize(StatusResponse msg, DataOutputPlus out) throws IOException
        {
            UUIDSerializer.serializer.serialize(msg.sessionID, out);
            out.writeInt(msg.state.ordinal());
            out.writeBoolean(msg.running);
        }

        public StatusResponse deserialize(DataInputPlus in) throws IOException
        {
            return new StatusResponse(UUIDSerializer.serializer.deserialize(in),
                                      ConsistentSession.State.valueOf(in.readInt()),
                                      in.readBoolean());
        }

        public long serializedSize(StatusResponse msg)
        {
            return UUIDSerializer.serializer.serializedSize(msg.sessionID)
                   + TypeSizes.sizeof(msg.state.ordinal())
                   + TypeSizes.sizeof(msg.running);
        }
    });

    public final ConsistentSession.State state;
    public final boolean running;

    public StatusResponse(UUID sessionID, ConsistentSession.State state)
    {
        this(sessionID, state, false);
    }

    public StatusResponse(UUID sessionID, ConsistentSession.State state, boolean running)
    {
        super(sessionID);
        assert state != null;
        this.state = state;
        this.running = running;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatusResponse that = (StatusResponse) o;

        return sessionID.equals(that.sessionID) && state == that.state && running == that.running;
    }

    public int hashCode()
    {
        int result = sessionID.hashCode();
        result = 31 * result + state.hashCode();
        return running ? result * 31 : result;
    }

    public String toString()
    {
        return "StatusResponse{" +
               "sessionID=" + sessionID +
               ", state=" + state +
               ", running=" + running +
               '}';
    }

    public MessageSerializer<StatusResponse> serializer(RepairVersion version)
    {
        return serializers.get(version);
    }

    public Optional<Verb<StatusResponse, ?>> verb()
    {
        return Optional.empty();
    }
}
