/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.hints.HintsVerbs.HintsVersion;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * The message we use to dispatch and forward hints.
 *
 * Encodes the host id the hint is meant for and the hint itself.
 * We use the host id to determine whether we should store or apply the hint:
 * 1. If host id equals to the receiving node host id, then we apply the hint
 * 2. If host id is different from the receiving node's host id, then we store the hint
 *
 * Scenario (1) means that we are dealing with regular hint dispatch.
 * Scenario (2) means that we got a hint from a node that's going through decommissioning and is streaming its hints
 * elsewhere first.
 */
public abstract class HintMessage
{
    public static final Versioned<HintsVersion, Serializer<HintMessage>> serializers = HintsVersion.versioned(HintSerializer::new);

    final UUID hostId;

    HintMessage(UUID hostId)
    {
        this.hostId = hostId;
    }

    public static HintMessage create(UUID hostId, Hint hint)
    {
        return new Simple(hostId, hint);
    }

    static HintMessage createEncoded(UUID hostId, ByteBuffer hint, HintsVersion version)
    {
        return new Encoded(hostId, hint, version);
    }

    /**
     * The hint sent.
     *
     * @return the hint sent.
     * @throws UnknownTableException if the hint deserialization failed due ot an unknown table.
     */
    public abstract Hint hint() throws UnknownTableException;

    abstract long getHintCreationTime();

    protected abstract long serializedSize(HintsVersion version);
    protected abstract void serialize(DataOutputPlus out, HintsVersion version) throws IOException;

    /**
     * A vanilla version of an HintMessage that contains a non-encoded hint.
     */
    private static class Simple extends HintMessage
    {
        @Nullable // can be null if we fail do decode the hint because of an unknown table id in it
        private final Hint hint;

        @Nullable // will usually be null, unless a hint deserialization fails due to an unknown table id
        private final TableId unknownTableID;

        private Simple(UUID hostId, Hint hint)
        {
            super(hostId);
            this.hint = hint;
            this.unknownTableID = null;
        }

        private Simple(UUID hostId, TableId unknownTableID)
        {
            super(hostId);
            this.hint = null;
            this.unknownTableID = unknownTableID;
        }

        public Hint hint() throws UnknownTableException
        {
            if (unknownTableID != null)
                throw new UnknownTableException(unknownTableID);

            return hint;
        }

        long getHintCreationTime()
        {
            return hint.creationTime;
        }

        protected long serializedSize(HintsVersion version)
        {
            Objects.requireNonNull(hint); // we should never *send* a HintMessage with null hint

            long hintSize = Hint.serializers.get(version).serializedSize(hint);
            return TypeSizes.sizeofUnsignedVInt(hintSize) + hintSize;
        }

        protected void serialize(DataOutputPlus out, HintsVersion version) throws IOException
        {
            Objects.requireNonNull(hint); // we should never *send* a HintMessage with null hint
            /*
             * We are serializing the hint size so that the receiver of the message could gracefully handle
             * deserialize failure when a table had been dropped, by simply skipping the unread bytes.
             */
            out.writeUnsignedVInt(Hint.serializers.get(version).serializedSize(hint));

            Hint.serializers.get(version).serialize(hint, out);
        }

        /*
         * It's not an exceptional scenario to have a hints file streamed that have partition updates for tables
         * that don't exist anymore. We want to handle that case gracefully instead of dropping the connection for every
         * one of them.
         */
        static Simple deserialize(UUID hostId, DataInputPlus in, HintsVersion version) throws IOException
        {
            long hintSize = in.readUnsignedVInt();
            TrackedDataInputPlus countingIn = new TrackedDataInputPlus(in);
            try
            {
                return new Simple(hostId, Hint.serializers.get(version).deserialize(countingIn));
            }
            catch (UnknownTableException e)
            {
                in.skipBytes(Ints.checkedCast(hintSize - countingIn.getBytesRead()));
                return new Simple(hostId, e.id);
            }
        }
    }

    /**
     * A specialized version of {@link HintMessage} that takes an already encoded in a bytebuffer hint and sends it verbatim.
     *
     * An optimization for when dispatching a hint file of the current messaging version to a node of the same messaging version,
     * which is the most common case. Saves on extra ByteBuffer allocations one redundant hint deserialization-serialization cycle.
     *
     * We never deserialize as an Encoded.
     */
    private static class Encoded extends HintMessage
    {
        private final ByteBuffer hint;
        private final HintsVersion version;

        private Encoded(UUID hostId, ByteBuffer hint, HintsVersion version)
        {
            super(hostId);
            this.hint = hint;
            this.version = version;
        }

        public Hint hint()
        {
            // Encoded is just used during dispatch where we explicitely want to avoid decoding the hint, so calling
            // this is a misuse and we don't allow it.
            throw new UnsupportedOperationException();
        }

        long getHintCreationTime()
        {
            return Hint.serializers.get(version).getHintCreationTime(hint);
        }

        protected long serializedSize(HintsVersion version)
        {
            if (this.version != version)
                throw new IllegalArgumentException("serializedSize() called with non-matching version " + version);

            return TypeSizes.sizeofUnsignedVInt(hint.remaining())
                   + hint.remaining();
        }

        protected void serialize(DataOutputPlus out, HintsVersion version) throws IOException
        {
            if (this.version != version)
                throw new IllegalArgumentException("serialize() called with non-matching version " + version);

            out.writeUnsignedVInt(hint.remaining());
            out.write(hint);
        }
    }

    private static class HintSerializer extends VersionDependent<HintsVersion> implements Serializer<HintMessage>
    {
        private HintSerializer(HintsVersion version)
        {
            super(version);
        }

        public long serializedSize(HintMessage message)
        {
            return UUIDSerializer.serializer.serializedSize(message.hostId)
                   + message.serializedSize(version);
        }

        public void serialize(HintMessage message, DataOutputPlus out) throws IOException
        {
            UUIDSerializer.serializer.serialize(message.hostId, out);
            message.serialize(out, version);
        }

        public HintMessage deserialize(DataInputPlus in) throws IOException
        {
            UUID hostId = UUIDSerializer.serializer.deserialize(in);
            return Simple.deserialize(hostId, in, version);
        }
    }
}
