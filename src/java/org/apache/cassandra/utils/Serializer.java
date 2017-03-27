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
package org.apache.cassandra.utils;

import java.io.IOException;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Common interface for a serializer over a simple object.
 *
 * @param <T> the type of the object being serialized.
 */
public interface Serializer<T>
{
    /**
     * Serialize the provided object into the provided {@code DataOutputPlus} destination.
     *
     * @param t object to serialize
     * @param out destination into which to serialize {@code t}.
     *
     * @throws IOException if serialization fails.
     */
    public void serialize(T t, DataOutputPlus out) throws IOException;

    /**
     * Deserialize an object from the specified {@code DataInputPlus} source.
     *
     * @param in source from which to deserialize.
     * @return the object that was deserialized.
     *
     * @throws IOException if deserialization fails.
     */
    public T deserialize(DataInputPlus in) throws IOException;

    /**
     * Calculates the serialized size of the provided payload without actually serializing it.
     *
     * @param t object for which to calculate serialized size.
     * @return the serialized size of {@code t}.
     */
    public long serializedSize(T t);
}
