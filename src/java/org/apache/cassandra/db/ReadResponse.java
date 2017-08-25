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

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import io.reactivex.Single;
import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowSource;
import org.apache.cassandra.utils.flow.FlowSubscriber;
import org.apache.cassandra.utils.flow.FlowSubscription;
import org.apache.cassandra.utils.versioning.Versioned;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public abstract class ReadResponse
{
    public static final Versioned<ReadVersion, Serializer<ReadResponse>> serializers = ReadVersion.<Serializer<ReadResponse>>versioned(v -> new Serializer<ReadResponse>()
    {
        public void serialize(ReadResponse response, DataOutputPlus out) throws IOException
        {
            boolean isDigest = response instanceof DigestResponse;
            ByteBuffer digest = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;

            ByteBufferUtil.writeWithVIntLength(digest, out);
            if (!isDigest)
            {
                ByteBuffer data = ((DataResponse)response).data;
                ByteBufferUtil.writeWithVIntLength(data, out);
            }
        }

        public ReadResponse deserialize(DataInputPlus in) throws IOException
        {
            ByteBuffer digest = ByteBufferUtil.readWithVIntLength(in);
            if (digest.hasRemaining())
                return new DigestResponse(digest);

            ByteBuffer data = ByteBufferUtil.readWithVIntLength(in);
            return new RemoteDataResponse(data, v.encodingVersion);
        }

        public long serializedSize(ReadResponse response)
        {
            boolean isDigest = response instanceof DigestResponse;
            ByteBuffer digest = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;

            long size = ByteBufferUtil.serializedSizeWithVIntLength(digest);
            if (!isDigest)
            {
                ByteBuffer data = ((DataResponse)response).data;
                size += ByteBufferUtil.serializedSizeWithVIntLength(data);
            }
            return size;
        }
    });

    protected ReadResponse()
    {
    }

    public static Single<ReadResponse> createDataResponse(Flow<FlowableUnfilteredPartition> partitions, ReadCommand command, boolean forLocalDelivery)
    {
        return forLocalDelivery
               ? LocalResponse.build(partitions)
               : LocalDataResponse.build(partitions, EncodingVersion.last(), command);
    }

    @VisibleForTesting
    public static ReadResponse createRemoteDataResponse(Flow<FlowableUnfilteredPartition> partitions, ReadCommand command)
    {
        final EncodingVersion version = EncodingVersion.last();
        return UnfilteredPartitionsSerializer.serializerForIntraNode(version).serialize(partitions, command.columnFilter())
                                             .map(buffer -> new RemoteDataResponse(buffer, version))
                                             .blockingSingle();
    }

    public static Single<ReadResponse> createDigestResponse(Flow<FlowableUnfilteredPartition> partitions, ReadCommand command)
    {
        return makeDigest(partitions, command).map(digest -> new DigestResponse(digest));
    }

    public abstract Flow<FlowableUnfilteredPartition> data(ReadCommand command);
    public abstract Single<ByteBuffer> digest(ReadCommand command);

    public abstract boolean isDigestResponse();

    /**
     * Creates a string of the requested partition in this read response suitable for debugging.
     */
    public String toDebugString(ReadCommand command, DecoratedKey key)
    {
        if (isDigestResponse())
            return "Digest:0x" + ByteBufferUtil.bytesToHex(digest(command).blockingGet());

        try (UnfilteredPartitionIterator iter = FlowablePartitions.toPartitions(data(command), command.metadata()))
        {
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    if (partition.partitionKey().equals(key))
                        return ImmutableBTreePartition.create(partition).toString();
                }
            }
        }
        return "<key " + key + " not found>";
    }

    protected static Single<ByteBuffer> makeDigest(Flow<FlowableUnfilteredPartition> partitions, ReadCommand command)
    {
        MessageDigest digest = FBUtilities.newMessageDigest("MD5");
        return UnfilteredPartitionIterators.digest(partitions,
                                                   // TODO perf. - do we need a cache to replace threadLocalMD5Digest()?
                                                   digest,
                                                   command.digestVersion())
                                           .processToRxCompletable()
                                           .toSingle(() -> ByteBuffer.wrap(digest.digest()));
    }

    private static class DigestResponse extends ReadResponse
    {
        private final ByteBuffer digest;

        private DigestResponse(ByteBuffer digest)
        {
            super();
            assert digest.hasRemaining();
            this.digest = digest;
        }

        public Flow<FlowableUnfilteredPartition> data(ReadCommand command)
        {
            throw new UnsupportedOperationException();
        }

        public Single<ByteBuffer> digest(ReadCommand command)
        {
            // We assume that the digest is in the proper version, which bug excluded should be true since this is called with
            // ReadCommand.digestVersion() as argument and that's also what we use to produce the digest in the first place.
            // Validating it's the proper digest in this method would require sending back the digest version along with the
            // digest which would waste bandwidth for little gain.
            return Single.just(digest);
        }

        public boolean isDigestResponse()
        {
            return true;
        }
    }

    /**
     * A local response that is not meant to be serialized. Currently we use an in-memory list of
     * ImmutableBTreePartition, a possible optimization would be to use the iterator directly, provided
     * it is not closed and we don't need to iterate more than once (CL.ONE).
     */
    private static class LocalResponse extends ReadResponse
    {
        private final List<ImmutableBTreePartition> partitions;

        private LocalResponse(List<ImmutableBTreePartition> partitions)
        {
            super();
            this.partitions = partitions;
        }

        public static Single<ReadResponse> build(Flow<FlowableUnfilteredPartition> partitions)
        {
            return ImmutableBTreePartition.create(partitions)
                                          .toList()
                                          .mapToRxSingle(LocalResponse::new);
        }

        public Flow<FlowableUnfilteredPartition> data(ReadCommand command)
        {
            return new FlowSource<FlowableUnfilteredPartition>()
            {
                private int idx = 0;

                public void request()
                {
                    if (idx < partitions.size())
                        subscriber.onNext(partitions.get(idx++).unfilteredPartition(command.columnFilter(),
                                                                                    Slices.ALL,
                                                                                    command.isReversed()));
                    else
                        subscriber.onComplete();
                }

                public void close() throws Exception
                {
                    // no op
                }

                public String toString()
                {
                    return Flow.formatTrace("LocalResponse", subscriber);
                }
            };
        }

        public boolean isDigestResponse()
        {
            return false;
        }

        public Single<ByteBuffer> digest(ReadCommand command)
        {
            return makeDigest(data(command), command);
        }
    }

    /**
     * A local response that needs to be serialized, i.e. sent to another node. The iterator
     * is serialized by the build method and can be closed as soon as this response has been created.
     */
    private static class LocalDataResponse extends DataResponse
    {
        private LocalDataResponse(ByteBuffer data, EncodingVersion version)
        {
            super(data, version, SerializationHelper.Flag.LOCAL);
        }

        private static Single<ReadResponse> build(Flow<FlowableUnfilteredPartition> partitions, EncodingVersion version, ReadCommand command)
        {
            return UnfilteredPartitionsSerializer.serializerForIntraNode(version).serialize(partitions, command.columnFilter())
                                                 .mapToRxSingle(buffer -> new LocalDataResponse(buffer, version));
        }
    }

    /**
     * A reponse received from a remove node. We kee the response serialized in the byte buffer.
     */
    private static class RemoteDataResponse extends DataResponse
    {
        protected RemoteDataResponse(ByteBuffer data, EncodingVersion version)
        {
            super(data, version, SerializationHelper.Flag.FROM_REMOTE);
        }
    }

    /**
     * The command base class for local or remote responses that stay serialized in a byte buffer,
     * the data.
     */
    static abstract class DataResponse extends ReadResponse
    {
        // TODO: can the digest be calculated over the raw bytes now?
        // The response, serialized in the current messaging version
        private final ByteBuffer data;
        protected final EncodingVersion version;
        private final SerializationHelper.Flag flag;

        protected DataResponse(ByteBuffer data, EncodingVersion version, SerializationHelper.Flag flag)
        {
            super();
            this.data = data;
            this.version = version;
            this.flag = flag;
        }

        public Flow<FlowableUnfilteredPartition> data(ReadCommand command)
        {
            // Note that the command parameter shadows the 'command' field and this is intended because
            // the later can be null (for RemoteDataResponse as those are created in the serializers and
            // those don't have easy access to the command). This is also why we need the command as parameter here.
            return UnfilteredPartitionsSerializer.serializerForIntraNode(version)
                                                 .deserialize(data, command.metadata(), command.columnFilter(), flag);

        }

        public Single<ByteBuffer> digest(ReadCommand command)
        {
            return makeDigest(data(command), command);
        }

        public boolean isDigestResponse()
        {
            return false;
        }
    }
}
