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
package org.apache.cassandra.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.SchemaVerbs.SchemaVersion;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class SchemaMigration
{
    public static Versioned<SchemaVersion, Serializer<SchemaMigration>> serializers = SchemaVersion.<Serializer<SchemaMigration>>versioned(v -> new Serializer<SchemaMigration>()
    {
        public void serialize(SchemaMigration schema, DataOutputPlus out) throws IOException
        {
            out.writeInt(schema.mutations.size());
            for (Mutation mutation : schema.mutations)
                Mutation.rawSerializers.get(v.encodingVersion).serialize(mutation, out);
        }

        public SchemaMigration deserialize(DataInputPlus in) throws IOException
        {
            int count = in.readInt();
            Collection<Mutation> schema = new ArrayList<>(count);

            for (int i = 0; i < count; i++)
                schema.add(Mutation.rawSerializers.get(v.encodingVersion).deserialize(in));

            return new SchemaMigration(schema);
        }

        public long serializedSize(SchemaMigration schema)
        {
            long size = TypeSizes.sizeof(schema.mutations.size());
            for (Mutation mutation : schema.mutations)
                size += Mutation.rawSerializers.get(v.encodingVersion).serializedSize(mutation);
            return size;
        }
    });

    public final Collection<Mutation> mutations;

    SchemaMigration(Collection<Mutation> mutations)
    {
        this.mutations = mutations;
    }
}
