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

import java.util.EnumMap;
import java.util.function.Function;

public class Versioned<V extends Enum<V> & Version<V>, T>
{
    private final EnumMap<V, T> serializers;
    private final Function<V, ? extends T> creator;

    public Versioned(Class<V> versionClass, Function<V, ? extends T> creator)
    {
        this.serializers = new EnumMap<>(versionClass);
        this.creator = creator;

        V last = versionClass.getEnumConstants()[versionClass.getEnumConstants().length - 1];
        serializers.put(last, creator.apply(last));
    }

    public T get(V version)
    {
        // This is absolutely racy. However, serializer *must* be stateless, and we don't care much about having
        // a few duplicate instantiations for a short window the first time a particular version is used (in
        // practice, since we pre-instantiate the current version, this will only happen at startup and likely
        // for just one version). So we don't bother synchronizing anything.
        return serializers.computeIfAbsent(version, k -> creator.apply(version));
    }
}
