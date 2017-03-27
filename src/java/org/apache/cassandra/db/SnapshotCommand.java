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

public class SnapshotCommand
{
    public static final Serializer<SnapshotCommand> serializer = new Serializer<SnapshotCommand>()
    {
        public void serialize(SnapshotCommand command, DataOutputPlus out) throws IOException
        {
            out.writeUTF(command.keyspace);
            out.writeUTF(command.table);
            out.writeUTF(command.snapshotName);
            out.writeBoolean(command.clearSnapshot);
        }

        public SnapshotCommand deserialize(DataInputPlus in) throws IOException
        {
            String keyspace = in.readUTF();
            String table = in.readUTF();
            String snapshotName = in.readUTF();
            boolean clearSnapshot = in.readBoolean();
            return new SnapshotCommand(keyspace, table, snapshotName, clearSnapshot);
        }

        public long serializedSize(SnapshotCommand command)
        {
            return TypeSizes.sizeof(command.keyspace)
                   + TypeSizes.sizeof(command.table)
                   + TypeSizes.sizeof(command.snapshotName)
                   + TypeSizes.sizeof(command.clearSnapshot);
        }
    };

    public final String keyspace;
    public final String table;
    public final String snapshotName;
    public final boolean clearSnapshot;

    public SnapshotCommand(String keyspace, String columnFamily, String snapshotName, boolean clearSnapshot)
    {
        this.keyspace = keyspace;
        this.table = columnFamily;
        this.snapshotName = snapshotName;
        this.clearSnapshot = clearSnapshot;
    }

    @Override
    public String toString()
    {
        return "SnapshotCommand{" + "keyspace='" + keyspace + '\'' +
               ", table='" + table + '\'' +
               ", snapshotName=" + snapshotName +
               ", clearSnapshot=" + clearSnapshot + '}';
    }
}
