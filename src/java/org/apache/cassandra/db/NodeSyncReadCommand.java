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
import java.net.InetAddress;
import java.util.Collection;

import javax.annotation.Nullable;

import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

import com.datastax.bdp.db.nodesync.Segment;

/**
 * A read command use by NodeSync.
 * <p>
 * Note that for almost all intent and purposes this is just a {@link PartitionRangeReadCommand} and this class exists
 * mainly so we know the command is part of NodeSync. Concretely, the only difference is that this uses a specific
 * {@link Verb}, namely {@link ReadVerbs#NODESYNC}.
 */
public class NodeSyncReadCommand extends PartitionRangeReadCommand
{
    private static final SelectionDeserializer<NodeSyncReadCommand> selectionDeserializer = new Deserializer();
    public static final Versioned<ReadVersion, Serializer<NodeSyncReadCommand>> serializers = ReadVersion.versioned(v -> new ReadCommandSerializer<>(v, selectionDeserializer));

    @Nullable
    private final transient TracingAwareExecutor nodeSyncExecutor;

    private NodeSyncReadCommand(DigestVersion digestVersion,
                                TableMetadata table,
                                int nowInSec,
                                ColumnFilter columnFilter,
                                RowFilter rowFilter,
                                DataLimits limits,
                                DataRange range,
                                IndexMetadata index,
                                TracingAwareExecutor nodeSyncExecutor)
    {
        super(digestVersion,
              table,
              nowInSec,
              columnFilter,
              rowFilter,
              limits,
              range,
              index);
        this.nodeSyncExecutor = nodeSyncExecutor;
    }

    public NodeSyncReadCommand(Segment segment,
                               int nowInSec,
                               TracingAwareExecutor nodeSyncExecutor)
    {
        this(null,
             segment.table,
             nowInSec,
             ColumnFilter.all(segment.table),
             RowFilter.NONE,
             DataLimits.NONE,
             DataRange.forTokenRange(segment.range),
             null,
             nodeSyncExecutor);
    }

    protected PartitionRangeReadCommand copy(DigestVersion digestVersion,
                                             TableMetadata metadata,
                                             int nowInSec,
                                             ColumnFilter columnFilter,
                                             RowFilter rowFilter,
                                             DataLimits limits,
                                             DataRange dataRange,
                                             IndexMetadata index)
    {
        return new NodeSyncReadCommand(digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, dataRange, index, nodeSyncExecutor);
    }

    /**
     * The NodeSync executor that should be used to handle response to this request.
     *
     * @return the NodeSync executor, or {@code null} if this command has been serialized/deserialized. The later is
     * due to the fact that the executor value is lost through serialization, which is ok because we only need the
     * executor in practice on the NodeSync coordinator where this won't be {@code null}.
     */
    @Nullable
    TracingAwareExecutor nodeSyncExecutor()
    {
        return nodeSyncExecutor;
    }

    @Override
    public Request.Dispatcher<NodeSyncReadCommand, ReadResponse> dispatcherTo(Collection<InetAddress> endpoints)
    {
        return Verbs.READS.NODESYNC.newDispatcher(endpoints, this);
    }

    @Override
    public Request<NodeSyncReadCommand, ReadResponse> requestTo(InetAddress endpoint)
    {
        return Verbs.READS.NODESYNC.newRequest(endpoint, this);
    }

    private static class Deserializer extends SelectionDeserializer<NodeSyncReadCommand>
    {
        public NodeSyncReadCommand deserialize(DataInputPlus in,
                                               ReadVersion version,
                                               DigestVersion digestVersion,
                                               TableMetadata metadata,
                                               int nowInSec,
                                               ColumnFilter columnFilter,
                                               RowFilter rowFilter,
                                               DataLimits limits,
                                               IndexMetadata index)
        throws IOException
        {
            DataRange range = DataRange.serializers.get(version).deserialize(in, metadata);
            return new NodeSyncReadCommand(digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, range, null, null);
        }
    }
}
