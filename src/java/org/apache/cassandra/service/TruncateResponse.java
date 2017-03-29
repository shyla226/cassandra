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
package org.apache.cassandra.service;

import java.io.IOException;

import org.apache.cassandra.db.Truncation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.service.OperationsVerbs.OperationsVersion;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

/**
 * What is sent back from a truncate request ({@link Truncation}).
 * <p>
 * As far as DSE 6.0+ is concerned, this is an empty response as a truncation is essentially an acked request. However,
 * OSS side, we happen to send back the name of the table truncated and a "success" boolean. None of this is used
 * however, the table name is no useful since the coordinator of the truncation knows which table it sent the request for,
 * and the "sucess" boolean happens to be totally ignored (that is, it's actually counted as a success no matter what).
 * <p>
 * So, we preserve backward compatibility by serializing/deserializing what old/OSS nodes expect, but send nothing on
 * DSE 6.0+ (knowing that errors are properly handled through {@link FailureResponse}).
 */
public class TruncateResponse
{
    public static final Versioned<OperationsVersion, Serializer<TruncateResponse>> serializers = OperationsVersion.versioned(TruncateResponseSerializer::new);

    // Use on DSE 6.0 since we don't send back anything.
    private static final TruncateResponse EMPTY = new TruncateResponse(null, null);

    // Private as on DSE 6.0 they will always be null and are here only for OSS/backward compatibility.
    private final String keyspace;
    private final String columnFamily;

    TruncateResponse(String keyspace, String columnFamily)
    {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
    }

    public static class TruncateResponseSerializer extends VersionDependent<OperationsVersion> implements Serializer<TruncateResponse>
    {
        public TruncateResponseSerializer(OperationsVersion version)
        {
            super(version);
        }

        public void serialize(TruncateResponse tr, DataOutputPlus out) throws IOException
        {
            if (version.compareTo(OperationsVersion.DSE_60) >= 0)
                return;

            out.writeUTF(tr.keyspace);
            out.writeUTF(tr.columnFamily);
            out.writeBoolean(true);
        }

        public TruncateResponse deserialize(DataInputPlus in) throws IOException
        {
            if (version.compareTo(OperationsVersion.DSE_60) >= 0)
                return EMPTY;

            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();
            in.readBoolean();
            return new TruncateResponse(keyspace, columnFamily);
        }

        public long serializedSize(TruncateResponse tr)
        {
            if (version.compareTo(OperationsVersion.DSE_60) >= 0)
                return 0;

            return TypeSizes.sizeof(tr.keyspace)
                   + TypeSizes.sizeof(tr.columnFamily)
                   + TypeSizes.sizeof(true);
        }
    }
}
