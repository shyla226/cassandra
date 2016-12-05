/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.service.paxos;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.paxos.LWTVerbs.LWTVersion;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class PrepareResponse
{
    public static final Versioned<LWTVersion, Serializer<PrepareResponse>> serializers = LWTVersion.versioned(PrepareSerializer::new);

    public final boolean promised;

    /*
     * To maintain backward compatibility (see #6023), the meaning of inProgressCommit is a bit tricky.
     * If promised is true, then that's the last accepted commit. If promise is false, that's just
     * the previously promised ballot that made us refuse this one.
     */
    public final Commit inProgressCommit;
    public final Commit mostRecentCommit;

    public PrepareResponse(boolean promised, Commit inProgressCommit, Commit mostRecentCommit)
    {
        assert inProgressCommit.update.partitionKey().equals(mostRecentCommit.update.partitionKey());
        assert inProgressCommit.update.metadata() == mostRecentCommit.update.metadata();

        this.promised = promised;
        this.mostRecentCommit = mostRecentCommit;
        this.inProgressCommit = inProgressCommit;
    }

    @Override
    public String toString()
    {
        return String.format("PrepareResponse(%s, %s, %s)", promised, mostRecentCommit, inProgressCommit);
    }

    public static class PrepareSerializer extends VersionDependent<LWTVersion> implements Serializer<PrepareResponse>
    {
        private final Commit.CommitSerializer commitSerializer;

        private PrepareSerializer(LWTVersion version)
        {
            super(version);
            this.commitSerializer = Commit.serializers.get(version);
        }

        public void serialize(PrepareResponse response, DataOutputPlus out) throws IOException
        {
            out.writeBoolean(response.promised);
            commitSerializer.serialize(response.inProgressCommit, out);
            commitSerializer.serialize(response.mostRecentCommit, out);
        }

        public PrepareResponse deserialize(DataInputPlus in) throws IOException
        {
            boolean success = in.readBoolean();
            Commit inProgress = commitSerializer.deserialize(in);
            Commit mostRecent = commitSerializer.deserialize(in);
            return new PrepareResponse(success, inProgress, mostRecent);
        }

        public long serializedSize(PrepareResponse response)
        {
            return TypeSizes.sizeof(response.promised)
                 + commitSerializer.serializedSize(response.inProgressCommit)
                 + commitSerializer.serializedSize(response.mostRecentCommit);
        }
    }
}
