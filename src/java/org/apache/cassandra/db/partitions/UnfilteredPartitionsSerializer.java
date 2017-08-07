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

package org.apache.cassandra.db.partitions;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredPartitionSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowSource;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * Serialize each partition one after the other, with an initial byte that indicates whether
 * we're done or not.
 */
public class UnfilteredPartitionsSerializer
{
    private static final Versioned<EncodingVersion, Serializer> serializers = EncodingVersion.versioned(Serializer::new);

    public static Serializer serializerForIntraNode(EncodingVersion version)
    {
        return serializers.get(version);
    }

    public static class Serializer extends VersionDependent<EncodingVersion>
    {
        private Serializer(EncodingVersion version)
        {
            super(version);
        }

        @SuppressWarnings("resource") // DataOutputBuffer does not need closing.
        public Flow<ByteBuffer> serialize(Flow<FlowableUnfilteredPartition> partitions, ColumnFilter selection)
        {
            final DataOutputBuffer out = new DataOutputBuffer();
            // Previously, a boolean indicating if this was for a thrift query.
            // Unused since 4.0 but kept on wire for compatibility.
            try
            {
                out.writeBoolean(false);
            }
            catch (IOException e)
            {
                // Should never happen
                throw new AssertionError(e);
            }

            return partitions.flatProcess(partition ->
                                          {
                                              out.writeBoolean(true);
                                              return UnfilteredPartitionSerializer.serializers.get(version).serialize(partition, selection, out);
                                          })
                             .map(VOID ->
                                  {
                                      out.writeBoolean(false);
                                      return out.trimmedBuffer();
                                  });
        }

        private class DeserializePartitionsFlow extends FlowSource<FlowableUnfilteredPartition>
        {
            private final DataInputBuffer in;
            private final TableMetadata metadata;
            private final ColumnFilter selection;
            private final SerializationHelper.Flag flag;

            private volatile FlowableUnfilteredPartition current;


            private DeserializePartitionsFlow(ByteBuffer buffer,
                                              TableMetadata metadata,
                                              ColumnFilter selection,
                                              SerializationHelper.Flag flag)
            {
                // Skip now unused isForThrift boolean
                ByteBuffer buf = buffer.duplicate();
                buf.get();

                this.in = new DataInputBuffer(buf, false);
                this.metadata = metadata;
                this.selection = selection;
                this.flag = flag;
            }

            public void requestNext()
            {
                try
                {
                    if (current != null)
                        throw new IllegalStateException("Previous partition was not closed!");

                    boolean hasNext = in.readBoolean();
                    if (hasNext)
                    {
                        UnfilteredPartitionSerializer serializer = UnfilteredPartitionSerializer.serializers.get(version);
                        FlowableUnfilteredPartition fup = serializer.deserializeToFlow(in, metadata, selection, flag);
                        current = fup.withContent(fup.content().doOnClose(() -> current = null));
                        subscriber.onNext(current);
                    }
                    else
                    {
                        subscriber.onComplete();
                    }
                }
                catch (Throwable t)
                {
                    subscriber.onError(t);
                }
            }

            public void close() throws Exception
            {
                in.close();
            }

            @Override
            public String toString()
            {
                return Flow.formatTrace("deserialize-partitions", subscriber);
            }
        }

        public Flow<FlowableUnfilteredPartition> deserialize(final ByteBuffer buffer,
                                                             final TableMetadata metadata,
                                                             final ColumnFilter selection,
                                                             final SerializationHelper.Flag flag)
        {
            return new DeserializePartitionsFlow(buffer, metadata, selection, flag);
        }
    }
}
