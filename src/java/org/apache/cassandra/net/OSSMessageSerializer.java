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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.repair.messages.RepairVerbs;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.TimeoutSupplier;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.net.Verbs.*;

/**
 * Inter-node protocol serializer for the OSS protocol (that is, the protocol we'll default to if we talk to some pure OSS
 * nodes).
 */
public class OSSMessageSerializer implements Message.Serializer
{
    private enum OSSVerb
    {
        MUTATION(WRITES.WRITE),
        HINT(HINTS.HINT),
        READ_REPAIR(WRITES.READ_REPAIR),
        READ(READS.READ),
        REQUEST_RESPONSE(null),
        BATCH_STORE(WRITES.BATCH_STORE),
        BATCH_REMOVE(WRITES.BATCH_REMOVE),
        @Deprecated STREAM_REPLY(null),
        @Deprecated STREAM_REQUEST(null),
        RANGE_SLICE(READS.READ),
        @Deprecated BOOTSTRAP_TOKEN(null),
        @Deprecated TREE_REQUEST(null),
        @Deprecated TREE_RESPONSE(null),
        @Deprecated JOIN(null),
        GOSSIP_DIGEST_SYN(GOSSIP.SYN),
        GOSSIP_DIGEST_ACK(GOSSIP.ACK),
        GOSSIP_DIGEST_ACK2(GOSSIP.ACK2),
        @Deprecated DEFINITIONS_ANNOUNCE(null),
        DEFINITIONS_UPDATE(SCHEMA.PUSH),
        TRUNCATE(OPERATIONS.TRUNCATE),
        SCHEMA_CHECK(SCHEMA.VERSION),
        @Deprecated INDEX_SCAN(null),
        REPLICATION_FINISHED(OPERATIONS.REPLICATION_FINISHED),
        INTERNAL_RESPONSE(null), // responses to internal calls
        COUNTER_MUTATION(WRITES.COUNTER_FORWARDING),
        @Deprecated STREAMING_REPAIR_REQUEST(null),
        @Deprecated STREAMING_REPAIR_RESPONSE(null),
        SNAPSHOT(OPERATIONS.SNAPSHOT),
        MIGRATION_REQUEST(SCHEMA.PULL),
        GOSSIP_SHUTDOWN(GOSSIP.SHUTDOWN),
        _TRACE(null),
        ECHO(GOSSIP.ECHO),
        REPAIR_MESSAGE(null),
        PAXOS_PREPARE(LWT.PREPARE),
        PAXOS_PROPOSE(LWT.PROPOSE),
        PAXOS_COMMIT(LWT.COMMIT),
        @Deprecated PAGED_RANGE(null),
        UNUSED_1(null),
        UNUSED_2(null),
        UNUSED_3(null),
        UNUSED_4(null),
        UNUSED_5(null);

        /**
         * The corresponding definition for the verb if there is one. Verbs that have no such definition are:
         *   - Deprecated verbs (and _TRACE): we won't ever receive them and only have them in the enum so ordinal() returns the right answer.
         *   - REQUEST_RESPONSE and INTERNAL_RESPONSE: previous used for all responses so correspond to any number of definitions.
         *   - REPAIR_MESSAGE: we've split all repair messages into sub-definitions so we need some specific handling.
         */
        private final Verb<?, ?> verb;

        OSSVerb(Verb<?, ?> verb)
        {
            this.verb = verb;
        }
    }

    private static final OSSVerb[] LEGACY_VERB_VALUES = OSSVerb.values();

    /**
     * Gets the verb corresponding to a particular _request_ definition.
     * <p>
     * WARNING: this should not be relied on for READS.READ, because we need the actual payload to decide if we should send
     * OSSVerb.READ or OSSVerb.RANGE_SLICE.
     */
    private static final Map<Verb<?, ?>, OSSVerb> definitionToVerb;
    static
    {
        ImmutableMap.Builder<Verb<?, ?>, OSSVerb> builder = ImmutableMap.builder();
        for (OSSVerb ossVerb : OSSVerb.values())
        {
            if (ossVerb.verb != null && ossVerb.verb != Verbs.READS.READ)
                builder.put(ossVerb.verb, ossVerb);
        }

        for (Verb<?, ?> msg : Verbs.REPAIR)
            builder.put(msg, OSSVerb.REPAIR_MESSAGE);

        // We distinguish VIEW_WRITE because 1) it feels like a good idea and 2) it's handled by a different stage than
        // writes locally and having a special definition is the easier way to do this. That say, in legacy code it's just a
        // mutation.
        builder.put(Verbs.WRITES.VIEW_WRITE, OSSVerb.MUTATION);

        definitionToVerb = builder.build();
    }

    private static final BiMap<Verb<?, ?>, Integer> repairVerbToLegacyCode = HashBiMap.create();
    static
    {
        RepairVerbs rm = Verbs.REPAIR;
        repairVerbToLegacyCode.put(rm.VALIDATION_REQUEST, 0);
        repairVerbToLegacyCode.put(rm.VALIDATION_COMPLETE, 1);
        repairVerbToLegacyCode.put(rm.SYNC_REQUEST,2);
        repairVerbToLegacyCode.put(rm.SYNC_COMPLETE, 3);
        repairVerbToLegacyCode.put(rm.PREPARE, 5);
        repairVerbToLegacyCode.put(rm.SNAPSHOT, 6);
        repairVerbToLegacyCode.put(rm.CLEANUP, 7);
        repairVerbToLegacyCode.put(rm.CONSISTENT_REQUEST, 8);
        repairVerbToLegacyCode.put(rm.CONSISTENT_RESPONSE, 9);
        repairVerbToLegacyCode.put(rm.FINALIZE_PROPOSE, 10);
        repairVerbToLegacyCode.put(rm.FINALIZE_PROMISE, 11);
        repairVerbToLegacyCode.put(rm.FINALIZE_COMMIT, 12);
        repairVerbToLegacyCode.put(rm.FAILED_SESSION, 13);
        repairVerbToLegacyCode.put(rm.STATUS_REQUEST, 14);
        repairVerbToLegacyCode.put(rm.STATUS_RESPONSE, 15);
    }

    private static final String TRACE_HEADER = "TraceSession";
    private static final String TRACE_TYPE = "TraceType";

    private static final String FORWARD_FROM = "FWD_FRM";
    private static final String FORWARD_TO = "FWD_TO";

    private static final String FAILURE_CALLBACK_PARAM = "CAL_BAC";
    private static final byte[] ONE_BYTE = new byte[1];
    private static final String FAILURE_RESPONSE_PARAM = "FAIL";
    private static final String FAILURE_REASON_PARAM = "FAIL_REASON";

    private final MessagingVersion version;

    OSSMessageSerializer(MessagingVersion version)
    {
        this.version = version;
    }

    @Override
    public void writeSerializedSize(int serializedSize, DataOutputPlus out) throws IOException
    {
        // The legacy protocol doesn't write the serialized size upfront
    }

    @Override
    public int readSerializedSize(DataInputPlus in) throws IOException
    {
        // The legacy protocol doesn't write the serialized size upfront
        // TODO: returning a bogus value is definitively dodgy. This isn't really used until CASSANDRA-8457 and that
        // code will change then to accomodate this later. Keeping it simple right now.
        return -1;
    }

    @SuppressWarnings("unchecked")
    public void serialize(Message message, DataOutputPlus out) throws IOException
    {
        OSSVerb ossVerb = computeLegacyVerb(message);
        if (ossVerb == null)
            throw new IllegalStateException(String.format("Cannot write message %s to legacy node", message));

        out.writeInt(MessagingService.PROTOCOL_MAGIC);
        out.writeInt(message.id());

        // int cast cuts off the high-order half of the timestamp, which we can assume remains
        // the same between now and when the recipient reconstructs it.
        out.writeInt((int) message.operationStartMillis());

        CompactEndpointSerializationHelper.serialize(message.from(), out);

        out.writeInt(ossVerb.ordinal());

        Map<String, byte[]> parameters = computeLegacyParameters(message);
        out.writeInt(parameters.size());
        for (Map.Entry<String, byte[]> entry : parameters.entrySet())
        {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().length);
            out.write(entry.getValue());
        }

        if (message.payload() != null)
        {
            VerbSerializer<?, ?> verbSerializer = version.serializer(message.verb());
            Serializer serializer = message.isRequest()
                                            ? verbSerializer.requestSerializer
                                            : verbSerializer.responseSerializer;
            try (DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
            {
                // The exact message type used to be inside the payload
                if (message.group() == Verbs.REPAIR)
                    dob.write(repairVerbToLegacyCode.get(message.verb()));

                serializer.serialize(message.payload(), dob);

                int size = dob.getLength();
                out.writeInt(size);
                out.write(dob.getData(), 0, size);
            }
        }
        else
        {
            out.writeInt(0);
        }
    }

    @SuppressWarnings("unchecked")
    private OSSVerb computeLegacyVerb(Message message)
    {
        if (message.isResponse())
            return wasUsingAndInternalResponse((Response<?>)message)
                   ? OSSVerb.INTERNAL_RESPONSE
                   : OSSVerb.REQUEST_RESPONSE;

        if (message.verb() == Verbs.READS.READ)
            return ((Request<ReadCommand, ReadResponse>)message).payload() instanceof SinglePartitionReadCommand
                   ? OSSVerb.READ
                   : OSSVerb.RANGE_SLICE;

        return definitionToVerb.get(message.verb());
    }

    private boolean wasUsingAndInternalResponse(Response<?> response)
    {
        if (response.isFailure())
            return true;

        Verb<?, ?> def = response.verb();
        VerbGroup<?> group = def.group();

        if (group == Verbs.SCHEMA || group == Verbs.REPAIR)
            return true;

        if (group == Verbs.OPERATIONS)
            return def == Verbs.OPERATIONS.SNAPSHOT || def == Verbs.OPERATIONS.REPLICATION_FINISHED;

        return false;
    }

    private Map<String, byte[]> computeLegacyParameters(Message message)
    {
        Map<String, byte[]> params = new HashMap<>();
        Tracing.SessionInfo info = message.tracingInfo();
        if (info != null)
        {
            params.put(TRACE_HEADER, UUIDGen.decompose(Tracing.instance.getSessionId()));
            params.put(TRACE_TYPE, new byte[] { Tracing.TraceType.serialize(Tracing.instance.getTraceType()) });
        }

        if (message.isRequest())
        {
            Request<?, ?> request = (Request<?, ?>) message;

            if (!request.verb().isOneWay())
                params.put(FAILURE_CALLBACK_PARAM, ONE_BYTE);

            List<Request.Forward> forwards = request.forwards();
            if (!forwards.isEmpty())
            {
                try (DataOutputBuffer out = new DataOutputBuffer())
                {
                    out.writeInt(forwards.size());
                    for (Request.Forward forward : forwards)
                    {
                        CompactEndpointSerializationHelper.serialize(forward.to, out);
                        out.writeInt(forward.id);
                    }
                    params.put(FORWARD_TO, out.getData());
                }
                catch (IOException e)
                {
                    // DataOutputBuffer is in-memory, doesn't throw IOException
                    throw new AssertionError(e);
                }
            }
            if (request.isForwarded())
            {
                params.put(FORWARD_FROM, ((ForwardRequest)request).replyTo.getAddress());
            }
        }
        else
        {
            Response<?> response = (Response<?>) message;
            if (response.isFailure())
            {
                params.put(FAILURE_RESPONSE_PARAM, ONE_BYTE);
                RequestFailureReason reason = ((FailureResponse<?>)response).reason();
                params.put(FAILURE_REASON_PARAM, ByteBufferUtil.getArray(ByteBufferUtil.bytes((short)reason.codeForInternodeProtocol(version))));
            }
        }

        return params;
    }

    public long serializedSize(Message message)
    {
        OSSVerb ossVerb = computeLegacyVerb(message);
        if (ossVerb == null)
            throw new IllegalStateException(String.format("Cannot write message %s to legacy node", message));

        long size = 12; // protocol magic + id + timestamp;

        size += CompactEndpointSerializationHelper.serializedSize(message.from());

        size += 4; // Verb


        Map<String, byte[]> parameters = computeLegacyParameters(message);
        size += 4; // Parameters size
        for (Map.Entry<String, byte[]> entry : parameters.entrySet())
        {
            size += TypeSizes.sizeof(entry.getKey());
            size += 4; // value length
            size += entry.getValue().length;
        }

        if (message.group() == Verbs.REPAIR)
            size += 1;

        if (message.payload() != null)
            size += message.payloadSerializedSize(version);
        return size;
    }

    @SuppressWarnings("unchecked")
    public Message deserialize(DataInputPlus in, InetAddress from) throws IOException
    {
        MessagingService.validateMagic(in.readInt());
        int id = in.readInt();
        long timestamp = deserializeTimestampPre40(in, from);

        // From: it's serialized, but not really used since we know which node is talking to us.
        CompactEndpointSerializationHelper.deserialize(in);

        OSSVerb ossVerb = LEGACY_VERB_VALUES[in.readInt()];

        int parameterCount = in.readInt();
        // Creating an immutable map as we'll remove some below.
        Map<String, byte[]> rawParameters = new HashMap<>();
        for (int i = 0; i < parameterCount; i++)
        {
            String key = in.readUTF();
            byte[] value = new byte[in.readInt()];
            in.readFully(value);
            rawParameters.put(key, value);
        }

        Tracing.SessionInfo tracingInfo = extractAndRemoveTracingInfo(rawParameters);

        int payloadSize = in.readInt();

        boolean isResponse = ossVerb == OSSVerb.INTERNAL_RESPONSE || ossVerb == OSSVerb.REQUEST_RESPONSE;
        if (isResponse)
        {
            // We unfortunately have to consult the callback to check what serializer to use
            CallbackInfo<?> info = MessagingService.instance().getRegisteredCallback(id);
            if (info == null)
            {
                // reply for expired callback.  we'll have to skip it.
                in.skipBytesFully(payloadSize);
                return null;
            }

            Verb<?, ?> verb = info.verb;

            // We don't serialize the timeout on the OSS protocol. We could use the definition.timeoutSupplier() instead
            // but we don't have the request to pass as argument. We know most supplier (all except the one of reads)
            // ignore their argument so we could still use it passing 'null' but that's a bit dodgy and not future proof.
            // Now, thing is, timeout reponses on the receiver isn't terribly useful since we've shipped the response
            // already and if the callback has timeouted we'll simply discard the response shortly before we've returned
            // it from this method. So simply passing Long.MAX_VALUE, which mean "don't time out", is good enough.
            long timeoutMillis = Long.MAX_VALUE;

            if (rawParameters.containsKey(FAILURE_RESPONSE_PARAM))
            {
                rawParameters.remove(FAILURE_RESPONSE_PARAM);
                RequestFailureReason reason = rawParameters.containsKey(FAILURE_REASON_PARAM)
                                              ? RequestFailureReason.fromCode(ByteBufferUtil.toShort(ByteBuffer.wrap(rawParameters.remove(FAILURE_REASON_PARAM))))
                                              : RequestFailureReason.UNKNOWN;


                Message.Data data = new Message.Data(null,
                                                     -1,
                                                     timestamp,
                                                     timeoutMillis,
                                                     MessageParameters.from(rawParameters),
                                                     tracingInfo);

                return new FailureResponse(from,
                                           FBUtilities.getBroadcastAddress(),
                                           id,
                                           verb,
                                           reason,
                                           data);
            }

            Object payload = version.serializer(verb).responseSerializer.deserialize(in);

            Message.Data data = new Message.Data(payload,
                                                 payloadSize,
                                                 timestamp,
                                                 timeoutMillis,
                                                 MessageParameters.from(rawParameters),
                                                 tracingInfo);

            return new Response(from,
                                FBUtilities.getBroadcastAddress(),
                                id,
                                verb,
                                data);
        }
        else
        {
            // Old code use to ask for when it wanted to know about errors, but we do this by default now so ignore
            rawParameters.remove(FAILURE_REASON_PARAM);

            Verb<?, ?> verb = ossVerb == OSSVerb.REPAIR_MESSAGE
                              ? repairVerbToLegacyCode.inverse().get((int)in.readByte())
                              : ossVerb.verb;
            assert verb != null : "Unknown definition for verb " + ossVerb;

            Object payload = version.serializer(verb).requestSerializer.deserialize(in);

            long timeoutMillis = verb.isOneWay() ? -1 : ((TimeoutSupplier<Object>)verb.timeoutSupplier()).get(payload);

            if (rawParameters.containsKey(FORWARD_FROM))
            {
                InetAddress replyTo = InetAddress.getByAddress(rawParameters.remove(FORWARD_FROM));
                Message.Data data = new Message.Data(payload,
                                                     payloadSize,
                                                     timestamp,
                                                     timeoutMillis,
                                                     MessageParameters.from(rawParameters),
                                                     tracingInfo);
                return new ForwardRequest(from,
                                          FBUtilities.getBroadcastAddress(),
                                          replyTo,
                                          id,
                                          verb,
                                          data);
            }

            List<Request.Forward> forwards = extractAndRemoveForwards(rawParameters);

            Message.Data data = new Message.Data(payload,
                                                 payloadSize,
                                                 timestamp,
                                                 timeoutMillis,
                                                 MessageParameters.from(rawParameters),
                                                 tracingInfo);

            return verb.isOneWay()
                   ? new OneWayRequest<>(from, Request.local, (Verb.OneWay) verb, data, forwards)
                   : new Request(from, Request.local, id, verb, data, forwards);
        }
    }

    private Tracing.SessionInfo extractAndRemoveTracingInfo(Map<String, byte[]> parameters)
    {
        if (!parameters.containsKey(TRACE_HEADER))
            return null;

        UUID sessionId = UUIDGen.getUUID(ByteBuffer.wrap(parameters.remove(TRACE_HEADER)));

        Tracing.TraceType traceType = Tracing.TraceType.QUERY;
        if (parameters.containsKey(TRACE_TYPE))
            traceType = Tracing.TraceType.deserialize(parameters.remove(TRACE_TYPE)[0]);

        return new Tracing.SessionInfo(sessionId, traceType);
    }

    private List<Request.Forward> extractAndRemoveForwards(Map<String, byte[]> parameters)
    {
        if (!parameters.containsKey(FORWARD_TO))
            return Collections.emptyList();

        try (DataInputStream in = new DataInputStream(new FastByteArrayInputStream(parameters.remove(FORWARD_TO))))
        {
            int size = in.readInt();
            List<Request.Forward> forwards = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
            {
                InetAddress address = CompactEndpointSerializationHelper.deserialize(in);
                int id = in.readInt();
                forwards.add(new Request.Forward(address, id));
            }
            return forwards;
        }
        catch (IOException e)
        {
            throw new AssertionError();
        }
    }

    private long deserializeTimestampPre40(DataInputPlus in, InetAddress from) throws IOException
    {
        // Reconstruct the message construction time sent by the remote host (we sent only the lower 4 bytes, assuming the
        // higher 4 bytes wouldn't change between the sender and receiver)
        int partial = in.readInt(); // make sure to readInt, even if cross_node_to is not enabled
        long currentTime = ApproximateTime.currentTimeMillis();
        long sentConstructionTime = (currentTime & 0xFFFFFFFF00000000L) | (((partial & 0xFFFFFFFFL) << 2) >> 2);

        // Because nodes may not have their clock perfectly in sync, it's actually possible the sentConstructionTime is
        // later than the currentTime (the received time). If that's the case, as we definitively know there is a lack
        // of proper synchronziation of the clock, we ignore sentConstructionTime. We also ignore that
        // sentConstructionTime if we're told to.
        long elapsed = currentTime - sentConstructionTime;
        if (elapsed > 0)
            MessagingService.instance().metrics.addTimeTaken(from, elapsed);

        boolean useSentTime = DatabaseDescriptor.hasCrossNodeTimeout() && elapsed > 0;
        return useSentTime ? sentConstructionTime : currentTime;
    }
}
