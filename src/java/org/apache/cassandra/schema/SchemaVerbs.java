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

import java.util.UUID;
import java.util.function.Function;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.Verb.OneWay;
import org.apache.cassandra.net.Verb.RequestResponse;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class SchemaVerbs extends VerbGroup<SchemaVerbs.SchemaVersion>
{
    public enum SchemaVersion implements Version<SchemaVersion>
    {
        OSS_30(EncodingVersion.OSS_30);

        public final EncodingVersion encodingVersion;

        SchemaVersion(EncodingVersion encodingVersion)
        {
            this.encodingVersion = encodingVersion;
        }

        public static <T> Versioned<SchemaVersion, T> versioned(Function<SchemaVersion, ? extends T> function)
        {
            return new Versioned<>(SchemaVersion.class, function);
        }
    }

    public final RequestResponse<EmptyPayload, UUID> VERSION;
    public final RequestResponse<EmptyPayload, SchemaMigration> PULL;
    public final OneWay<SchemaMigration> PUSH;

    public SchemaVerbs(Verbs.Group id)
    {
        super(id, true, SchemaVersion.class);

        RegistrationHelper helper = helper().stage(Stage.MIGRATION);

        VERSION = helper.requestResponse("VERSION", EmptyPayload.class, UUID.class)
                        .withResponseSerializer(UUIDSerializer.serializer)
                        .timeout(DatabaseDescriptor::getRpcTimeout)
                        .syncHandler((from, x) -> Schema.instance.getVersion());
        PULL = helper.requestResponse("PULL", EmptyPayload.class, SchemaMigration.class)
                        .timeout(DatabaseDescriptor::getRpcTimeout)
                        .syncHandler((from, x) -> SchemaKeyspace.convertSchemaToMutations());
        PUSH = helper.oneWay("PUSH", SchemaMigration.class)
                     .handler((from, migration) -> Schema.instance.mergeAndAnnounceVersion(migration));

    }
}
