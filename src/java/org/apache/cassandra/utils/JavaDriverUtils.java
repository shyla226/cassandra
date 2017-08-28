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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Utility methods to convert from {@code AbstractType} to {@link DataType} or to {@link TypeCodec}.
 *
 */
public final class JavaDriverUtils
{
    private static final MethodHandle methodParseOne;
    static
    {
        try
        {
            Class<?> cls = Class.forName("com.datastax.driver.core.DataTypeClassNameParser");
            Method m = cls.getDeclaredMethod("parseOne", String.class, com.datastax.driver.core.ProtocolVersion.class, CodecRegistry.class);
            m.setAccessible(true);
            methodParseOne = MethodHandles.lookup().unreflect(m);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static TypeCodec<Object> codecFor(AbstractType<?> abstractType)
    {
        return codecFor(driverType(abstractType));
    }

    public static TypeCodec<Object> codecFor(DataType dataType)
    {
        return CodecRegistry.DEFAULT_INSTANCE.codecFor(dataType);
    }

    /**
     * Returns the Java Driver {@link com.datastax.driver.core.DataType} for the C* internal type.
     */
    public static DataType driverType(AbstractType<?> abstractType)
    {
        return driverType(abstractType.toString());
    }

    /**
     * Returns the Java Driver {@link com.datastax.driver.core.DataType} for the C* internal type.
     *
     * Used by DSE, please don't remove.
     */
    public static DataType driverType(String abstractType)
    {
        try
        {
            return (DataType) methodParseOne.invoke(abstractType,
                                                    LatestDriverSupportedVersion.protocolVersion,
                                                    CodecRegistry.DEFAULT_INSTANCE);
        }
        catch (RuntimeException | Error e)
        {
            // immediately rethrow these...
            throw e;
        }
        catch (Throwable e)
        {
            throw new RuntimeException("cannot parse driver type " + abstractType, e);
        }
    }

    /**
     * The class should never be instantiated as it contains only static methods.
     */
    private JavaDriverUtils()
    {
    }

    public static final class LatestDriverSupportedVersion
    {
        public static final com.datastax.driver.core.ProtocolVersion protocolVersion = newestSupportedProtocolVersion();

        private static com.datastax.driver.core.ProtocolVersion newestSupportedProtocolVersion()
        {
            List<ProtocolVersion> available = new ArrayList<>(org.apache.cassandra.transport.ProtocolVersion.SUPPORTED);
            for (int i = available.size() - 1; i >= 0; i--)
            {
                try
                {
                    ProtocolVersion avail = available.get(i);
                    return com.datastax.driver.core.ProtocolVersion.fromInt(avail.asInt());
                }
                catch (IllegalArgumentException e)
                {}
            }
            throw new AssertionError();
        }

        private LatestDriverSupportedVersion()
        {}
    }
}
