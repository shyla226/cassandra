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
package org.apache.cassandra.utils.versioning;

import java.util.function.Function;

public class Versioned<V extends Enum<V> & Version<V>, T>
{
    private final T[] serializers;
    private final Function<V, ? extends T> creator;

    public Versioned(Class<V> versionClass, Function<V, ? extends T> creator)
    {
        this.serializers = (T[]) new Object[versionClass.getEnumConstants().length];
        this.creator = creator;

        V last = versionClass.getEnumConstants()[versionClass.getEnumConstants().length - 1];
        serializers[last.ordinal()] =  creator.apply(last);
    }

    public T get(V version)
    {
        // this is racy and may result in double init of a serializer, but that is (apparently) harmless
        final int ordinal = version.ordinal();
        final T serializer = serializers[ordinal];
        if (serializer == null)
        {
            return serializers[ordinal] = creator.apply(version);
        }
        return serializer;
    }
}
