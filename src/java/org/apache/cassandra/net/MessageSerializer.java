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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tracing.Tracing;

/**
 * Message serializer for the DSE inter-node protocol.
 */
class MessageSerializer implements Message.Serializer
{
    static final String BASE_TIMESTAMP_KEY = "BASE_TIMESTAMP";

    private final boolean serializeTimestamps = DatabaseDescriptor.hasCrossNodeTimeout();

    private final MessagingVersion version;

    private final long timestampBaseMillis;

    // Flags common to all messages
    private static final int REQUEST_FLAG    = 0x01;
    private static final int PARAMETERS_FLAG = 0x02;
    private static final int TIMESTAMP_FLAG  = 0x04;
    private static final int TRACING_FLAG    = 0x08;

    // Flags for requests
    private static final int REQUEST_FORWARDS_FLAG  = 0x10;
    private static final int REQUEST_FORWARDED_FLAG = 0x20;

    // Flags for responses
    private static final int RESPONSE_FAILURE_FLAG = 0x10;

    MessageSerializer(MessagingVersion version, long timestampBaseMillis)
    {
        assert version.isDSE(); // LegacyMessageSerializer should be used instead
        this.version = version;
        this.timestampBaseMillis = timestampBaseMillis;
    }

    @SuppressWarnings("unchecked")
    public <P> void serialize(Message<P> message, DataOutputPlus out) throws IOException
    {
        VerbSerializer<?, ?> serializer = version.serializer(message.verb());

        out.writeByte(message.group().id().serializationCode());
        out.writeByte(serializer.code);
        out.writeByte(computeFlags(message));

        if (!message.verb().isOneWay())
            out.writeInt(message.id());

        if (!message.parameters().isEmpty())
            MessageParameters.serializer().serialize(message.parameters(), out);

        if (serializeTimestamps)
            out.writeVInt(message.operationStartMillis() - timestampBaseMillis);

        if (!message.verb().isOneWay())
            out.writeUnsignedVInt(message.timeoutMillis());

        if (message.isTraced())
            message.tracingInfo().serialize(out);

        if (message.isRequest())
        {
            Request<P, ?> request = (Request<P, ?>)message;
            if (!request.forwards().isEmpty())
            {
                out.writeVInt(request.forwards().size());
                for (Request.Forward forward : request.forwards())
                {
                    CompactEndpointSerializationHelper.serialize(forward.to, out);
                    if (!message.verb().isOneWay())
                        out.writeInt(forward.id);
                }
            }
            else if (request.isForwarded())
            {
                CompactEndpointSerializationHelper.serialize(((ForwardRequest)request).replyTo, out);
            }

            ((VerbSerializer<P, ?>)serializer).requestSerializer.serialize(request.payload(), out);
        }
        else
        {
            Response<P> response = (Response<P>)message;
            if (response.isFailure())
                out.writeInt(((FailureResponse) response).reason().codeForInternodeProtocol(version));
            else
                ((VerbSerializer<?, P>) serializer).responseSerializer.serialize(response.payload(), out);
        }
    }

    private int computeFlags(Message message)
    {
        int flags = 0;
        if (message.isRequest())
            flags |= REQUEST_FLAG;
        if (!message.parameters().isEmpty())
            flags |= PARAMETERS_FLAG;
        if (serializeTimestamps)
            flags |= TIMESTAMP_FLAG;
        if (message.isTraced())
            flags |= TRACING_FLAG;

        if (message.isRequest())
        {
            Request<?, ?> request = (Request)message;
            if (!request.forwards().isEmpty())
                flags |= REQUEST_FORWARDS_FLAG;
            else if (request.isForwarded())
                flags |= REQUEST_FORWARDED_FLAG;
        }
        else
        {
            Response<?> response = (Response)message;
            if (response.isFailure())
                flags |= RESPONSE_FAILURE_FLAG;
        }

        return flags;
    }

    public <P> long serializedSize(Message<P> message)
    {
        long size = 3; // group (1) + verb (1) + flags (1)

        if (!message.verb().isOneWay())
            size += 4; // message id

        if (!message.parameters().isEmpty())
            size += MessageParameters.serializer().serializedSize(message.parameters());

        if (serializeTimestamps)
            size += TypeSizes.sizeofVInt(message.operationStartMillis() - timestampBaseMillis);

        if (!message.verb().isOneWay())
            size += TypeSizes.sizeofUnsignedVInt(message.timeoutMillis());

        if (message.isTraced())
            size += message.tracingInfo().serializedSize();

        if (message.isRequest())
        {
            Request<P, ?> request = (Request<P, ?>)message;
            VerbSerializer<P, ?> serializer = version.serializer(request.verb());
            if (!request.forwards().isEmpty())
            {
                size += TypeSizes.sizeofVInt(request.forwards().size());
                for (Request.Forward forward : request.forwards())
                {
                    size += CompactEndpointSerializationHelper.serializedSize(forward.to);
                    if (!message.verb().isOneWay())
                        size += 4; // forward id
                }
            }
            else if (request.isForwarded())
            {
                size += CompactEndpointSerializationHelper.serializedSize(((ForwardRequest)request).replyTo);
            }
            size += serializer.requestSerializer.serializedSize(request.payload());
        }
        else
        {
            Response<P> response = (Response<P>)message;
            VerbSerializer<?, P> serializer = version.serializer(response.verb());
            if (response.isFailure())
                size += 4; // failure reason
            else
                size += serializer.responseSerializer.serializedSize(response.payload());
        }

        return size;
    }

    @SuppressWarnings("unchecked")
    public <P> Message<P> deserialize(DataInputPlus in, InetAddress from) throws IOException
    {
        int groupCode = in.readUnsignedByte();
        int verbCode = in.readUnsignedByte();
        int flags = in.readUnsignedByte();

        VerbGroup<?> group = Verbs.fromSerializationCode(groupCode);

        VerbSerializer<?, ?> serializer = version.serializerByVerbCode(group, verbCode);

        int messageId = serializer.verb.isOneWay() ? -1 : in.readInt();

        boolean hasParameters = (flags & PARAMETERS_FLAG) != 0;
        MessageParameters parameters = hasParameters
                                       ? MessageParameters.serializer().deserialize(in)
                                       : MessageParameters.EMPTY;

        long createdAtMillis = System.currentTimeMillis();
        if (serializeTimestamps)
        {
            long tstamp = timestampBaseMillis + in.readVInt();
            long elapsed = createdAtMillis - tstamp;
            if (elapsed > 0)
            {
                MessagingService.instance().metrics.addTimeTaken(from, createdAtMillis - tstamp);
                createdAtMillis = tstamp;
            }
        }

        long timeoutMillis = serializer.verb.isOneWay() ? -1 : in.readUnsignedVInt();

        Tracing.SessionInfo tracingInfo = null;
        if ((flags & TRACING_FLAG) != 0)
            tracingInfo = Tracing.SessionInfo.deserialize(in);

        if ((flags & REQUEST_FLAG) != 0)
        {
            // Request
            Verb<P, ?> verb = (Verb<P, ?>)serializer.verb;

            List<Request.Forward> forwards = Collections.emptyList();
            InetAddress replyTo = null;

            if ((flags & REQUEST_FORWARDS_FLAG) != 0)
            {
                int forwardCount = (int)in.readVInt();
                forwards = new ArrayList<>(forwardCount);
                for (int i = 0; i < forwardCount; i++)
                {
                    InetAddress addr = CompactEndpointSerializationHelper.deserialize(in);
                    int id = verb.isOneWay() ? -1 : in.readInt();
                    forwards.add(new Request.Forward(addr, id));
                }
            }
            else if ((flags & REQUEST_FORWARDED_FLAG) != 0)
            {
                replyTo = CompactEndpointSerializationHelper.deserialize(in);
            }

            P payload = ((VerbSerializer<P, ?>)serializer).requestSerializer.deserialize(in);

            Message.Data<P> data = new Message.Data<>(payload,
                                                      -1,
                                                      createdAtMillis,
                                                      timeoutMillis,
                                                      parameters,
                                                      tracingInfo);

            return replyTo == null
                   ? (verb.isOneWay()
                      ? new OneWayRequest<>(from, Request.local, (Verb.OneWay<P>) verb, data, forwards)
                      : new Request<>(from, Request.local, messageId, verb, data, forwards))
                   : new ForwardRequest<>(from, Request.local, replyTo, messageId, verb, data);
        }
        else
        {
            Verb<?, P> verb = (Verb<?, P>)serializer.verb;

            // Response
            if ((flags & RESPONSE_FAILURE_FLAG) != 0)
            {
                RequestFailureReason reason = RequestFailureReason.fromCode(in.readInt());
                Message.Data<P> data = new Message.Data<>(null,
                                                          -1,
                                                          createdAtMillis,
                                                          timeoutMillis,
                                                          parameters,
                                                          tracingInfo);

                return new FailureResponse<>(from, Request.local, messageId, verb, reason, data);
            }

            P payload = ((VerbSerializer<?, P>) serializer).responseSerializer.deserialize(in);
            Message.Data<P> data = new Message.Data<>(payload,
                                                      -1,
                                                      createdAtMillis,
                                                      timeoutMillis,
                                                      parameters,
                                                      tracingInfo);

            return new Response<>(from, Request.local, messageId, verb, data);
        }
    }
}
