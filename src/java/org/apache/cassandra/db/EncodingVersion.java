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

import java.util.function.Function;

import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * The encoding format/version for internal objects (rows, partitions and their iterators).
 */
public enum EncodingVersion implements Version<EncodingVersion>
{
    OSS_30(ClusteringVersion.OSS_30);

    public final ClusteringVersion clusteringVersion;

    private static EncodingVersion last = null;

    EncodingVersion(ClusteringVersion clusteringVersion)
    {
        this.clusteringVersion = clusteringVersion;
    }

    public static EncodingVersion last()
    {
        if (last == null)
            last = values()[values().length - 1];

        return last;
    }

    public static <T> Versioned<EncodingVersion, T> versioned(Function<EncodingVersion, ? extends T> function)
    {
        return new Versioned<>(EncodingVersion.class, function);
    }
}
