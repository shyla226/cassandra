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
package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.messages.StreamMessage.StreamVersion;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * StreamInitMessage is first sent from the node where {@link org.apache.cassandra.streaming.StreamSession} is started,
 * to initiate corresponding {@link org.apache.cassandra.streaming.StreamSession} on the other side.
 */
public class StreamInitMessage
{
    public static Versioned<StreamVersion, Serializer<StreamInitMessage>> serializers = StreamVersion.<Serializer<StreamInitMessage>>versioned(v -> new Serializer<StreamInitMessage>()
    {
        public void serialize(StreamInitMessage message, DataOutputPlus out) throws IOException
        {
            CompactEndpointSerializationHelper.serialize(message.from, out);
            out.writeInt(message.sessionIndex);
            UUIDSerializer.serializer.serialize(message.planId, out);
            out.writeUTF(message.streamOperation.getDescription());
            out.writeBoolean(message.isForOutgoing);
            out.writeBoolean(message.keepSSTableLevel);

            out.writeBoolean(message.pendingRepair != null);
            if (message.pendingRepair != null)
                UUIDSerializer.serializer.serialize(message.pendingRepair, out);
            out.writeInt(message.previewKind.getSerializationVal());
        }

        public StreamInitMessage deserialize(DataInputPlus in) throws IOException
        {
            InetAddress from = CompactEndpointSerializationHelper.deserialize(in);
            int sessionIndex = in.readInt();
            UUID planId = UUIDSerializer.serializer.deserialize(in);
            String description = in.readUTF();
            boolean sentByInitiator = in.readBoolean();
            boolean keepSSTableLevel = in.readBoolean();

            UUID pendingRepair = in.readBoolean() ? UUIDSerializer.serializer.deserialize(in) : null;
            PreviewKind previewKind = PreviewKind.deserialize(in.readInt());
            return new StreamInitMessage(from, sessionIndex, planId, StreamOperation.fromString(description), sentByInitiator, keepSSTableLevel, pendingRepair, previewKind);
        }

        public long serializedSize(StreamInitMessage message)
        {
            long size = CompactEndpointSerializationHelper.serializedSize(message.from);
            size += TypeSizes.sizeof(message.sessionIndex);
            size += UUIDSerializer.serializer.serializedSize(message.planId);
            size += TypeSizes.sizeof(message.streamOperation.getDescription());
            size += TypeSizes.sizeof(message.isForOutgoing);
            size += TypeSizes.sizeof(message.keepSSTableLevel);
            size += TypeSizes.sizeof(message.pendingRepair != null);
            if (message.pendingRepair != null)
                size += UUIDSerializer.serializer.serializedSize(message.pendingRepair);
            size += TypeSizes.sizeof(message.previewKind.getSerializationVal());
            return size;
        }
    });

    public final InetAddress from;
    public final int sessionIndex;
    public final UUID planId;
    public final StreamOperation streamOperation;

    // true if this init message is to connect for outgoing message on receiving side
    public final boolean isForOutgoing;
    public final boolean keepSSTableLevel;
    public final UUID pendingRepair;
    public final PreviewKind previewKind;

    public StreamInitMessage(InetAddress from, int sessionIndex, UUID planId, StreamOperation streamOperation, boolean isForOutgoing, boolean keepSSTableLevel, UUID pendingRepair, PreviewKind previewKind)
    {
        this.from = from;
        this.sessionIndex = sessionIndex;
        this.planId = planId;
        this.streamOperation = streamOperation;
        this.isForOutgoing = isForOutgoing;
        this.keepSSTableLevel = keepSSTableLevel;
        this.pendingRepair = pendingRepair;
        this.previewKind = previewKind;
    }

    /**
     * Create serialized message.
     *
     * @param compress true if message is compressed
     * @param version Streaming protocol version
     * @return serialized message in ByteBuffer format
     */
    public ByteBuffer createMessage(boolean compress, StreamVersion version)
    {
        int header = version.protocolVersion.makeProtocolHeader(compress, true);

        byte[] bytes;
        try
        {
            int size = Math.toIntExact(StreamInitMessage.serializers.get(version).serializedSize(this));
            try (DataOutputBuffer buffer = new DataOutputBufferFixed(size))
            {
                StreamInitMessage.serializers.get(version).serialize(this, buffer);
                bytes = buffer.getData();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        assert bytes.length > 0;

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + bytes.length);
        buffer.putInt(MessagingService.PROTOCOL_MAGIC);
        buffer.putInt(header);
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }
}
