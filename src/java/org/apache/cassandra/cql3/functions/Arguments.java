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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;

import org.apache.cassandra.transport.ProtocolVersion;

/**
 * The input arguments to a function.
 * <p>The class can be reused for calling the same function multiple time.</p>
 */
public interface Arguments
{
    /**
     * Sets the specified value to the arguments
     * @param i the argument position
     * @param buffer the serialized argument
     */
    public void set(int i, ByteBuffer buffer);

    /**
     * Checks if at least one of the arguments is null.
     * @return {@code true} if at least one of the arguments is null, {@code false} otherwise.
     */
    public boolean containsNulls();

    /**
     * Returns the specified argument
     * @param i the argument index
     * @return the specified argument
     */
    public <T> T get(int i);

    /**
     * Returns the protocol version
     * @return the protocol version
     */
    public ProtocolVersion getProtocolVersion();

    /**
     * Returns the specified argument as a {@code boolean}.
     * @param i the argument index
     * @return the specified argument as a {@code boolean}
     */
    public default boolean getAsBoolean(int i)
    {
        return this.<Boolean>get(i).booleanValue();
    }

    /**
     * Returns the specified argument as a {@code byte}.
     * @param i the argument index
     * @return the specified argument as a {@code byte}
     */
    public default byte getAsByte(int i)
    {
        return this.<Number>get(i).byteValue();
    }

    /**
     * Returns the specified argument as a {@code short}.
     * @param i the argument index
     * @return the specified argument as a {@code short}
     */
    public default short getAsShort(int i)
    {
        return this.<Number>get(i).shortValue();
    }

    /**
     * Returns the specified argument as a {@code int}.
     * @param i the argument index
     * @return the specified argument as a {@code int}
     */
    public default int getAsInt(int i)
    {
        return this.<Number>get(i).intValue();
    }

    /**
     * Returns the specified argument as a {@code long}.
     * @param i the argument index
     * @return the specified argument as a {@code long}
     */
    public default long getAsLong(int i)
    {
        return this.<Number>get(i).longValue();
    }

    /**
     * Returns the specified argument as a {@code float}.
     * @param i the argument index
     * @return the specified argument as a {@code float}
     */
    public default float getAsFloat(int i)
    {
        return this.<Number>get(i).floatValue();
    }

    /**
     * Returns the specified argument as a {@code double}.
     * @param i the argument index
     * @return the specified argument as a {@code double}
     */
    public default double getAsDouble(int i)
    {
        return this.<Number>get(i).doubleValue();
    }

    /**
     * Returns the current number of arguments.
     * @return the current number of arguments.
     */
    public int size();
}