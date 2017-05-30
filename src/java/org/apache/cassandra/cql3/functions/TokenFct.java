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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

public class TokenFct extends NativeScalarFunction
{
    private final TableMetadata metadata;

    public TokenFct(TableMetadata metadata)
    {
        super("token", metadata.partitioner.getTokenValidator(), getKeyTypes(metadata));
        this.metadata = metadata;
    }

    @Override
    public Arguments newArguments(ProtocolVersion version)
    {
        ArgumentDeserializer[] deserializers = new ArgumentDeserializer[argTypes.size()];
        Arrays.fill(deserializers, ArgumentDeserializer.NOOP_DESERIALIZER);
        return new FunctionArguments(version, deserializers);
    }

    private static AbstractType[] getKeyTypes(TableMetadata metadata)
    {
        AbstractType[] types = new AbstractType[metadata.partitionKeyColumns().size()];
        int i = 0;
        for (ColumnMetadata def : metadata.partitionKeyColumns())
            types[i++] = def.type;
        return types;
    }

    public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
    {
        CBuilder builder = CBuilder.create(metadata.partitionKeyAsClusteringComparator());
        for (int i = 0, m = arguments.size(); i < m; i++)
        {
            ByteBuffer bb = arguments.get(i);
            if (bb == null)
                return null;
            builder.add(bb);
        }
        return metadata.partitioner.getTokenFactory().toByteArray(metadata.partitioner.getToken(builder.build().serializeAsPartitionKey()));
    }
}
