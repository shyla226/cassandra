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

package org.apache.cassandra.transport.messages;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.reactivex.Single;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.cql3.continuous.paging.ContinuousPagingService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * A request to revise a previous request, currently used
 * by {@link ContinuousPagingService} to either cancel or update
 * the backpressure of a running continuous paging session.
 */
public class ReviseRequestMessage extends Message.Request
{
    private static final ColumnIdentifier RESULT_COLUMN = new ColumnIdentifier("[status]", false);
    private static final ColumnSpecification RESULT_COLUMN_SPEC = new ColumnSpecification("", "", RESULT_COLUMN, BooleanType.instance);
    private static final ResultSet.ResultMetadata RESULT_METADATA = new ResultSet.ResultMetadata(Collections.singletonList(RESULT_COLUMN_SPEC));

    public enum RevisionType
    {
        CONTINUOUS_PAGING_CANCEL(1),
        CONTINUOUS_PAGING_BACKPRESSURE(2);

        private final int id;
        RevisionType(int id)
        {
            this.id = id;
        }

        private static final RevisionType[] REVISION_TYPES;
        static
        {
            int maxId = Arrays.stream(RevisionType.values()).map(ot -> ot.id).reduce(0, Math::max);
            REVISION_TYPES = new RevisionType[maxId + 1];
            for (RevisionType ut : RevisionType.values())
            {
                if (REVISION_TYPES[ut.id] != null)
                    throw new IllegalStateException("Duplicate operation type");
                REVISION_TYPES[ut.id] = ut;
            }
        }

        static RevisionType decode(int id)
        {
            if (id >= REVISION_TYPES.length || REVISION_TYPES[id] == null)
                throw new ProtocolException(String.format("Unknown operation type %d", id));
            return REVISION_TYPES[id];
        }
    }

    public static final Message.Codec<ReviseRequestMessage> codec = new Message.Codec<ReviseRequestMessage>()
    {
        public ReviseRequestMessage decode(ByteBuf body, ProtocolVersion version)
        {
            RevisionType revisionType = RevisionType.decode(body.readInt());
            if (revisionType == RevisionType.CONTINUOUS_PAGING_CANCEL)
                return new ReviseRequestMessage(revisionType, body.readInt());
            else if (revisionType == RevisionType.CONTINUOUS_PAGING_BACKPRESSURE)
                return new ReviseRequestMessage(revisionType, body.readInt(), body.readInt());
            else
                throw new InvalidRequestException(String.format("Unknown update type: %s", revisionType));
        }

        public void encode(ReviseRequestMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            dest.writeInt(msg.revisionType.id);
            dest.writeInt(msg.id);

            if (msg.revisionType == RevisionType.CONTINUOUS_PAGING_BACKPRESSURE)
                dest.writeInt(msg.nextPages);
        }

        public int encodedSize(ReviseRequestMessage msg, ProtocolVersion version)
        {
            int ret = 8;
            if (msg.revisionType == RevisionType.CONTINUOUS_PAGING_BACKPRESSURE)
                ret += 4;
            return ret;
        }
    };

    /** The type of the update */
    private final RevisionType revisionType;

    /** The stream id of the operation to update */
    private final int id;

    /** The number of pages requested for {@link RevisionType#CONTINUOUS_PAGING_BACKPRESSURE} requests or zero. */
    private final int nextPages;

    private ReviseRequestMessage(RevisionType revisionType, int id)
    {
        this(revisionType, id, 0);
    }

    private ReviseRequestMessage(RevisionType revisionType, int id, int nextPages)
    {
        super(Type.REVISE_REQUEST);
        this.revisionType = revisionType;
        this.id = id;
        this.nextPages = nextPages;
    }

    public Single<Response> execute(QueryState queryState, long queryStartNanoTime)
    {
        Single<Boolean> ret;
        if (revisionType == RevisionType.CONTINUOUS_PAGING_CANCEL)
            ret = ContinuousPagingService.cancel(queryState, id);
        else if (revisionType == RevisionType.CONTINUOUS_PAGING_BACKPRESSURE)
            ret = Single.just(ContinuousPagingService.updateBackpressure(queryState, id, nextPages));
        else
            ret = Single.error(new InvalidRequestException(String.format("Unknown update type: %s", revisionType)));

        return ret.map(res -> {
            List<List<ByteBuffer>> rows = Collections.singletonList(Collections.singletonList(BooleanType.instance.decompose(res)));
            return (Response)(new ResultMessage.Rows(new ResultSet(RESULT_METADATA, rows)));
        }).onErrorReturn(err -> {
            JVMStabilityInspector.inspectThrowable(err);
            return ErrorMessage.fromException(err);
        });
    }

    @Override
    public String toString()
    {
        return String.format("REVISE REQUEST %d with revision %s (nextPages %s)", id, revisionType, nextPages);
    }
}
