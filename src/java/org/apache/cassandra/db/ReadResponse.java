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
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public abstract class ReadResponse
{
    // Serializer for single partition read response
    public static final IVersionedSerializer<ReadResponse> serializer = new Serializer();

    protected ReadResponse()
    {
    }

    public static ReadResponse createLocalResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new LocalResponse(data, command);
    }

    public static ReadResponse createDataResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new LocalDataResponse(data, command);
    }

    @VisibleForTesting
    public static ReadResponse createRemoteDataResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new RemoteDataResponse(LocalDataResponse.build(data, command.columnFilter()), MessagingService.current_version);
    }

    public static ReadResponse createDigestResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new DigestResponse(makeDigest(data, command));
    }

    public abstract UnfilteredPartitionIterator makeIterator(ReadCommand command);
    public abstract ByteBuffer digest(ReadCommand command);

    public abstract boolean isDigestResponse();

    protected static ByteBuffer makeDigest(UnfilteredPartitionIterator iterator, ReadCommand command)
    {
        MessageDigest digest = FBUtilities.threadLocalMD5Digest();
        UnfilteredPartitionIterators.digest(iterator, digest, command.digestVersion());
        return ByteBuffer.wrap(digest.digest());
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

    /**
     * A local response that is not meant to be serialized. Currently we use an in-memory list of
     * ImmutableBTreePartition because if more than one replica was queries it may have to return
     * the iterator multiple times. We could wrap the incoming iterator and use it directly for
     * added performance but only if the local host host is the only host queried.
     */
    private static class LocalResponse extends ReadResponse
    {
        //private UnfilteredPartitionIterator iter;
        private final List<ImmutableBTreePartition> partitions;

        private LocalResponse(UnfilteredPartitionIterator iter, ReadCommand command)
        {
            super();
            //this.iter = iter; // this works only when the local host is the only host queried
            this.partitions = build(iter, command);
        }


        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            return new AbstractUnfilteredPartitionIterator()
            {
                private int idx;

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
            };

        }

        public boolean isDigestResponse()
        {
            return false;
        }

        private static List<ImmutableBTreePartition> build(UnfilteredPartitionIterator iterator, ReadCommand command)
        {
            if (!iterator.hasNext())
                return Collections.emptyList();

            try
            {
                if (command instanceof SinglePartitionReadCommand)
                {
                    try (UnfilteredRowIterator partition = iterator.next())
                    {
                        return Collections.singletonList(ImmutableBTreePartition.create(partition));
                    }
                }

                List<ImmutableBTreePartition> partitions = new ArrayList<>();
                while (iterator.hasNext())
                {
                    try(UnfilteredRowIterator partition = iterator.next())
                    {
                        partitions.add(ImmutableBTreePartition.create(partition));
                    }
                }

                return partitions;
            }
            finally
            {
                iterator.close();
            }
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
        private LocalDataResponse(UnfilteredPartitionIterator iter, ReadCommand command)
        {
            super(build(iter, command.columnFilter()), MessagingService.current_version, SerializationHelper.Flag.LOCAL);
        }

        private static ByteBuffer build(UnfilteredPartitionIterator iter, ColumnFilter selection)
        {
            try (DataOutputBuffer buffer = new DataOutputBuffer())
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iter, selection, buffer, MessagingService.current_version).blockingAwait();
                return buffer.buffer();
            }
            catch (IOException e)
            {
                // We're serializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * A reponse received from a remove node. We kee the response serialized in the byte buffer.
     */
    private static class RemoteDataResponse extends DataResponse
    {
        protected RemoteDataResponse(ByteBuffer data, int version)
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
        private final int dataSerializationVersion;
        private final SerializationHelper.Flag flag;

        protected DataResponse(ByteBuffer data, int dataSerializationVersion, SerializationHelper.Flag flag)
        {
            super();
            this.data = data;
            this.dataSerializationVersion = dataSerializationVersion;
            this.flag = flag;
        }

        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            try (DataInputBuffer in = new DataInputBuffer(data, true))
            {
                // Note that the command parameter shadows the 'command' field and this is intended because
                // the later can be null (for RemoteDataResponse as those are created in the serializers and
                // those don't have easy access to the command). This is also why we need the command as parameter here.
                return UnfilteredPartitionIterators.serializerForIntraNode().deserialize(in,
                                                                                         dataSerializationVersion,
                                                                                         command.metadata(),
                                                                                         command.columnFilter(),
                                                                                         flag);
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

    private static class Serializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
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

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            ByteBuffer digest = ByteBufferUtil.readWithVIntLength(in);
            if (digest.hasRemaining())
                return new DigestResponse(digest);

            ByteBuffer data = ByteBufferUtil.readWithVIntLength(in);
            return new RemoteDataResponse(data, version);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            boolean isDigest = response instanceof DigestResponse;
            ByteBuffer digest = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;

            long size = ByteBufferUtil.serializedSizeWithVIntLength(digest);
            if (!isDigest)
            {
                // In theory, we should deserialize/re-serialize if the version asked is different from the current
                // version as the content could have a different serialization format. So far though, we haven't made
                // change to partition iterators serialization since 3.0 so we skip this.
                assert version >= MessagingService.VERSION_30;
                ByteBuffer data = ((DataResponse)response).data;
                size += ByteBufferUtil.serializedSizeWithVIntLength(data);
            }
            return size;
        }
    }
}
