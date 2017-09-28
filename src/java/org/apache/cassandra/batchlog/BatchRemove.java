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

package org.apache.cassandra.batchlog;

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.concurrent.Schedulable;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.UUIDSerializer;

public class BatchRemove implements Schedulable
{
    public static final Serializer<BatchRemove> serializer = new BatchRemoveSerializer();

    public final UUID id;

    public BatchRemove(UUID id)
    {
        this.id = id;
    }

    static final class BatchRemoveSerializer implements Serializer<BatchRemove>
    {
        public void serialize(BatchRemove batchRemove, DataOutputPlus out) throws IOException
        {
            UUIDSerializer.serializer.serialize(batchRemove.id, out);
        }

        public BatchRemove deserialize(DataInputPlus in) throws IOException
        {
            return new BatchRemove(UUIDSerializer.serializer.deserialize(in));
        }

        public long serializedSize(BatchRemove batchRemove)
        {
            return UUIDSerializer.serializer.serializedSize(batchRemove.id);
        }
    }

    public StagedScheduler getScheduler()
    {
        return TPC.getForKey(Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME), SystemKeyspace.decorateBatchKey(id));
    }

    public TracingAwareExecutor getOperationExecutor()
    {
        return getScheduler().forTaskType(TPCTaskType.BATCH_REMOVE);
    }
}
