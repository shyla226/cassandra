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

import java.util.ArrayList;
import java.util.List;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Version;

/**
 * Holds information related to serializing the verbs of a particular group for a particular version.
 */
class VersionSerializers
{
    private final String groupName;
    private final Version<?> version;

    private final List<VerbSerializer> verbSerializers = new ArrayList<>();
    private final IntObjectOpenHashMap<VerbSerializer> codeToVerb = new IntObjectOpenHashMap<>();

    VersionSerializers(String groupName, Version<?> version)
    {
        this.groupName = groupName;
        this.version = version;
    }

    <P, Q> void add(Verb<P, Q> verb,
                    int code,
                    Serializer<P> requestSerializer,
                    Serializer<Q> responseSerializer)
    {
        VerbSerializer<P, Q> serializer = new VerbSerializer<>(verb, code, requestSerializer, responseSerializer);
        assert verb.groupIdx() == verbSerializers.size();
        verbSerializers.add(serializer);
        codeToVerb.put(code, serializer);
    }

    @SuppressWarnings("unchecked")
    <P, Q> VerbSerializer<P, Q> getByCode(int code)
    {
        VerbSerializer<P, Q> serializer = codeToVerb.get(code);
        if (serializer == null)
            throw new IllegalArgumentException(String.format("Invalid verb code %d for %s in version %s ", code, groupName, version));
        return serializer;
    }

    @SuppressWarnings("unchecked")
    <P, Q> VerbSerializer<P, Q> getByVerb(Verb<P, Q> verb)
    {
        return verbSerializers.get(verb.groupIdx());
    }
}
