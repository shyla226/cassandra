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
package org.apache.cassandra.streaming.compress;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.streaming.messages.StreamMessage.StreamVersion;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * Container that carries compression parameters and chunks to decompress data from stream.
 */
public class CompressionInfo
{
    public static final Versioned<StreamVersion, Serializer<CompressionInfo>> serializers = StreamVersion.<Serializer<CompressionInfo>>versioned(v -> new Serializer<CompressionInfo>()
    {
        public void serialize(CompressionInfo info, DataOutputPlus out) throws IOException
        {
            if (info == null)
            {
                out.writeInt(-1);
                return;
            }

            int chunkCount = info.chunks.length;
            out.writeInt(chunkCount);
            for (int i = 0; i < chunkCount; i++)
                CompressionMetadata.Chunk.serializer.serialize(info.chunks[i], out);
            // compression params
            CompressionParams.serializers.get(v).serialize(info.parameters, out);
        }

        public CompressionInfo deserialize(DataInputPlus in) throws IOException
        {
            // chunks
            int chunkCount = in.readInt();
            if (chunkCount < 0)
                return null;

            CompressionMetadata.Chunk[] chunks = new CompressionMetadata.Chunk[chunkCount];
            for (int i = 0; i < chunkCount; i++)
                chunks[i] = CompressionMetadata.Chunk.serializer.deserialize(in);

            // compression params
            CompressionParams parameters = CompressionParams.serializers.get(v).deserialize(in);
            return new CompressionInfo(chunks, parameters);
        }

        public long serializedSize(CompressionInfo info)
        {
            if (info == null)
                return TypeSizes.sizeof(-1);

            // chunks
            int chunkCount = info.chunks.length;
            long size = TypeSizes.sizeof(chunkCount);
            for (int i = 0; i < chunkCount; i++)
                size += CompressionMetadata.Chunk.serializer.serializedSize(info.chunks[i]);
            // compression params
            size += CompressionParams.serializers.get(v).serializedSize(info.parameters);
            return size;
        }
    });

    public final CompressionMetadata.Chunk[] chunks;
    public final CompressionParams parameters;

    public CompressionInfo(CompressionMetadata.Chunk[] chunks, CompressionParams parameters)
    {
        assert chunks != null && parameters != null;
        this.chunks = chunks;
        this.parameters = parameters;
    }
}
