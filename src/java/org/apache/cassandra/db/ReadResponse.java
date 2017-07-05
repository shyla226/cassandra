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
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import io.reactivex.Single;
import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.flow.CsFlow;
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

    public static Single<ReadResponse> createDataResponse(CsFlow<FlowableUnfilteredPartition> partitions, ReadCommand command, boolean forLocalDelivery)
    {
        return forLocalDelivery
               ? LocalResponse.build(partitions)
               : LocalDataResponse.build(partitions, EncodingVersion.last(), command);
    }

    @VisibleForTesting
    public static ReadResponse createRemoteDataResponse(CsFlow<FlowableUnfilteredPartition> partitions, ReadCommand command)
    {
        final EncodingVersion version = EncodingVersion.last();
        return UnfilteredPartitionIterators.serializerForIntraNode(version).serialize(partitions, command.columnFilter())
                                           .map(buffer -> new RemoteDataResponse(buffer, version))
                                           .blockingSingle();
    }

    public static Single<ReadResponse> createDigestResponse(CsFlow<FlowableUnfilteredPartition> partitions, ReadCommand command)
    {
        return makeDigest(partitions, command).map(digest -> new DigestResponse(digest));
    }

    public abstract UnfilteredPartitionIterator makeIterator(ReadCommand command);
    public abstract ByteBuffer digest(ReadCommand command);

    public abstract boolean isDigestResponse();

    protected static ByteBuffer makeDigest(UnfilteredPartitionIterator iterator, ReadCommand command)
    {
        MessageDigest digest = FBUtilities.threadLocalMD5Digest();
        UnfilteredPartitionIterators.digest(iterator, digest, command.digestVersion()).blockingAwait();
        return ByteBuffer.wrap(digest.digest());
    }

    protected static Single<ByteBuffer> makeDigest(CsFlow<FlowableUnfilteredPartition> partitions, ReadCommand command)
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

        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer digest(ReadCommand command)
        {
            // We assume that the digest is in the proper version, which bug excluded should be true since this is called with
            // ReadCommand.digestVersion() as argument and that's also what we use to produce the digest in the first place.
            // Validating it's the proper digest in this method would require sending back the digest version along with the
            // digest which would waste bandwith for little gain.
            return digest;
        }

        public boolean isDigestResponse()
        {
            return true;
        }
    }

    static class InMemoryPartitionsIterator implements UnfilteredPartitionIterator
    {
        private final List<ImmutableBTreePartition> partitions;
        private final ReadCommand command;
        private int idx;

        InMemoryPartitionsIterator(List<ImmutableBTreePartition> partitions, ReadCommand command)
        {
            this(command);
            this.partitions.addAll(partitions);
        }

        InMemoryPartitionsIterator(ReadCommand command)
        {
            this.partitions = new ArrayList<>();
            this.command = command;
            this.idx = 0;
        }

        public void add(ImmutableBTreePartition partition)
        {
            this.partitions.add(partition);
        }

        public TableMetadata metadata()
        {
            return command.metadata();
        }

        public boolean hasNext()
        {
            return idx < partitions.size();
        }

        public UnfilteredRowIterator next()
        {
            // TODO: we know rows don't require any filtering and that we return everything. We ought to be able to optimize this.
            return partitions.get(idx++).unfilteredIterator(command.columnFilter(), Slices.ALL, command.isReversed());
        }

        public void close()
        {

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

        public static Single<ReadResponse> build(CsFlow<FlowableUnfilteredPartition> partitions)
        {
            return ImmutableBTreePartition.create(partitions)
                                          .toList()
                                          .mapToRxSingle(LocalResponse::new);
        }

        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            return new InMemoryPartitionsIterator(partitions, command);
        }

        public boolean isDigestResponse()
        {
            return false;
        }

        public ByteBuffer digest(ReadCommand command)
        {
            return makeDigest(makeIterator(command), command);
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

        private static Single<ReadResponse> build(CsFlow<FlowableUnfilteredPartition> partitions, EncodingVersion version, ReadCommand command)
        {
            return UnfilteredPartitionIterators.serializerForIntraNode(version).serialize(partitions, command.columnFilter())
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

        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            try (DataInputBuffer in = new DataInputBuffer(data, true))
            {
                // Note that the command parameter shadows the 'command' field and this is intended because
                // the later can be null (for RemoteDataResponse as those are created in the serializers and
                // those don't have easy access to the command). This is also why we need the command as parameter here.
                return UnfilteredPartitionIterators.serializerForIntraNode(version)
                                                   .deserialize(in, command.metadata(), command.columnFilter(), flag);
            }
            catch (IOException e)
            {
                // We're deserializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        public ByteBuffer digest(ReadCommand command)
        {
            try (UnfilteredPartitionIterator iterator = makeIterator(command))
            {
                return makeDigest(iterator, command);
            }
        }

        public boolean isDigestResponse()
        {
            return false;
        }
    }
}
