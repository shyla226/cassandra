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
package org.apache.cassandra.service.paxos;

import java.util.function.Function;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.net.*;
import org.apache.cassandra.net.Verb.AckedRequest;
import org.apache.cassandra.net.Verb.RequestResponse;
import org.apache.cassandra.utils.BooleanSerializer;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class LWTVerbs extends VerbGroup<LWTVerbs.LWTVersion>
{
    public enum LWTVersion implements Version<LWTVersion>
    {
        OSS_30(EncodingVersion.OSS_30);

        public final EncodingVersion encodingVersion;

        LWTVersion(EncodingVersion encodingVersion)
        {
            this.encodingVersion = encodingVersion;
        }

        public static <T> Versioned<LWTVersion, T> versioned(Function<LWTVersion, ? extends T> function)
        {
            return new Versioned<>(LWTVersion.class, function);
        }
    }

    public final RequestResponse<Commit, PrepareResponse> PREPARE;
    public final RequestResponse<Commit, Boolean> PROPOSE;
    public final AckedRequest<Commit> COMMIT;

    public LWTVerbs(Verbs.Group id)
    {
        super(id, false, LWTVersion.class);

        RegistrationHelper helper = helper().stage(Stage.MUTATION);

        PREPARE = helper.requestResponse("PREPARE", Commit.class, PrepareResponse.class)
                        .timeout(DatabaseDescriptor::getWriteRpcTimeout)
                        .syncHandler((from, commit) -> PaxosState.prepare(commit));
        PROPOSE = helper.requestResponse("PROPOSE", Commit.class, Boolean.class)
                        .timeout(DatabaseDescriptor::getWriteRpcTimeout)
                        .withResponseSerializer(BooleanSerializer.serializer)
                        .syncHandler((from, commit) -> PaxosState.propose(commit));
        COMMIT = helper.ackedRequest("COMMIT", Commit.class)
                       .timeout(DatabaseDescriptor::getWriteRpcTimeout)
                       .syncHandler((from, commit) -> PaxosState.commit(commit));
    }
}
