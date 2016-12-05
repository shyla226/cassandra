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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Custom parameters for a message.
 * <p>
 * This is an immutable class, parameters must be built using the {@link Builder} and then added to a message using
 * {@link Message#addParameters}.
 */
public class MessageParameters
{
    public static final MessageParameters EMPTY = new MessageParameters(ImmutableMap.of());

    private static final Serializer serializer = new Serializer();

    private final ImmutableMap<String, byte[]> parameters;

    private MessageParameters(ImmutableMap<String, byte[]> parameters)
    {
        this.parameters = parameters;
    }

    public static MessageParameters from(Map<String, byte[]> map)
    {
        return map.isEmpty() ? EMPTY : new MessageParameters(ImmutableMap.copyOf(map));
    }

    MessageParameters unionWith(MessageParameters other)
    {
        Builder builder = builder();
        builder.builder.putAll(parameters);
        builder.builder.putAll(other.parameters);
        return builder.build();
    }

    public static Serializer serializer()
    {
        return serializer;
    }

    public boolean isEmpty()
    {
        return parameters.isEmpty();
    }

    public boolean has(String key)
    {
        return parameters.containsKey(key);
    }

    public String getString(String key)
    {
        try
        {
            byte[] value = get(key);
            return value == null ? null : ByteBufferUtil.string(ByteBuffer.wrap(value));
        }
        catch (CharacterCodingException e)
        {
            throw new IllegalStateException();
        }
    }

    public Integer getInt(String key)
    {
        byte[] value = get(key);
        return value == null ? null : ByteBufferUtil.toInt(ByteBuffer.wrap(value));
    }

    public Long getLong(String key)
    {
        byte[] value = get(key);
        return value == null ? null : ByteBufferUtil.toLong(ByteBuffer.wrap(value));
    }

    public byte[] get(String key)
    {
        return parameters.get(key);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();

        public Builder putString(String key, String value)
        {
            return put(key, ByteBufferUtil.getArray(ByteBufferUtil.bytes(value)));
        }

        public Builder putLong(String key, long value)
        {
            return put(key, ByteBufferUtil.getArray(ByteBufferUtil.bytes(value)));
        }

        public Builder putInt(String key, int value)
        {
            return put(key, ByteBufferUtil.getArray(ByteBufferUtil.bytes(value)));
        }

        public Builder put(String key, byte[] value)
        {
            builder.put(key, value);
            return this;
        }

        public MessageParameters build()
        {
            return new MessageParameters(builder.build());
        }
    }

    public static class Serializer
    {
        public void serialize(MessageParameters parameters, DataOutputPlus out) throws IOException
        {
            out.writeVInt(parameters.parameters.size());
            for (Map.Entry<String, byte[]> parameter : parameters.parameters.entrySet())
            {
                out.writeUTF(parameter.getKey());
                out.writeVInt(parameter.getValue().length);
                out.write(parameter.getValue());
            }
        }

        public MessageParameters deserialize(DataInputPlus in) throws IOException
        {
            int size = (int)in.readVInt();
            Builder builder = builder();
            for (int i = 0; i < size; i++)
            {
                String key = in.readUTF();
                byte[] value = new byte[(int)in.readVInt()];
                in.readFully(value);
                builder.put(key, value);
            }
            return builder.build();
        }

        public long serializedSize(MessageParameters parameters)
        {
            long size = TypeSizes.sizeofVInt(parameters.parameters.size());
            for (Map.Entry<String, byte[]> parameter : parameters.parameters.entrySet())
            {
                size += TypeSizes.sizeof(parameter.getKey());
                int length = parameter.getValue().length;
                size += TypeSizes.sizeofVInt(length)
                        + length;
            }
            return size;
        }
    }
}
