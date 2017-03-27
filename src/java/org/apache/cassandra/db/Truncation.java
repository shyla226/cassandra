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

import java.io.IOException;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;

/**
 * A truncate operation descriptor
 */
public class Truncation
{
    public static final Serializer<Truncation> serializer = new TruncationSerializer();

    public final String keyspace;
    public final String columnFamily;

    public Truncation(String keyspace, String columnFamily)
    {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
    }

    public String toString()
    {
        return "Truncation(" + "keyspace='" + keyspace + '\'' + ", cf='" + columnFamily + "\')";
    }
}

class TruncationSerializer implements Serializer<Truncation>
{
    public void serialize(Truncation t, DataOutputPlus out) throws IOException
    {
        out.writeUTF(t.keyspace);
        out.writeUTF(t.columnFamily);
    }

    public Truncation deserialize(DataInputPlus in) throws IOException
    {
        String keyspace = in.readUTF();
        String columnFamily = in.readUTF();
        return new Truncation(keyspace, columnFamily);
    }

    public long serializedSize(Truncation truncation)
    {
        return TypeSizes.sizeof(truncation.keyspace) + TypeSizes.sizeof(truncation.columnFamily);
    }
}
