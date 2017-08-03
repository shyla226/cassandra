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

import java.net.InetAddress;

import org.apache.cassandra.utils.versioning.Version;

/**
 * Version used to serialize data digests.
 */
public enum DigestVersion implements Version<DigestVersion>
{
    // WARNING: make sure to update forReplicas below if a new version is added. It currently optimize based on the fact
    // we currently only support one version.
    OSS_30;

    /**
     * Returns the digest version suitable for querying the provided replicas.
     * <p>
     * This basically returns the minimum versions amongst node participating (new nodes will always know how to produce
     * old version digets, but the reverse is not true).
     *
     * @param replicas the replicas involved in the read for which we'll compute digests.
     * @return the digest version to use for a digest read involving {@code replicas}.
     */
    public static DigestVersion forReplicas(Iterable<InetAddress> replicas)
    {
        // In practice, what we should do is that's below. But as we have only one version suppported so far, not point
        // in getting fancy (this is on the read path, so not wasting cycles isn't a bad things, and it's easy to
        // uncomment the lines below next time we add a version).
        //MessagingVersion minVersion = MessagingService.current_version;
        //for (InetAddress replica : targetReplicas)
        //    minVersion = MessagingVersion.min(minVersion, MessagingService.instance().getVersion(replica));
        //return minVersion.<ReadVerbs.ReadVersion>groupVersion(Verbs.Group.READS).digestVersion;
        return OSS_30;
    }
}
