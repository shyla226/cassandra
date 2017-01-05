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
 * A request to cancel a long running operation.
 */
public class CancelMessage extends Message.Request
{
    private static final ColumnIdentifier RESULT_COLUMN = new ColumnIdentifier("[status]", false);
    private static final ColumnSpecification RESULT_COLUMN_SPEC = new ColumnSpecification("", "", RESULT_COLUMN, BooleanType.instance);
    private static final ResultSet.ResultMetadata RESULT_METADATA = new ResultSet.ResultMetadata(Collections.singletonList(RESULT_COLUMN_SPEC));

    public enum OperationType
    {
        CONTINOUS_PAGING(1);

        private final int id;
        OperationType(int id)
        {
            this.id = id;
        }

        private static final OperationType[] operationTypes;
        static
        {
            int maxId = Arrays.stream(OperationType.values()).map(ot -> ot.id).reduce(0, Math::max);
            operationTypes = new OperationType[maxId + 1];
            for (OperationType ot : OperationType.values())
            {
                if (operationTypes[ot.id] != null)
                    throw new IllegalStateException("Duplicate operation type");
                operationTypes[ot.id] = ot;
            }
        }

        static OperationType decode(int id)
        {
            if (id >= operationTypes.length || operationTypes[id] == null)
                throw new ProtocolException(String.format("Unknown operation type %d", id));
            return operationTypes[id];
        }
    }

    public static final Message.Codec<CancelMessage> codec = new Message.Codec<CancelMessage>()
    {
        public CancelMessage decode(ByteBuf body, ProtocolVersion version)
        {
            return new CancelMessage(OperationType.decode(body.readInt()), body.readInt());
        }

        public void encode(CancelMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            dest.writeInt(msg.operationType.id);
            dest.writeInt(msg.id);
        }

        public int encodedSize(CancelMessage msg, ProtocolVersion version)
        {
            return 8;
        }
    };

    /** The type of the operation to cancel */
    private final OperationType operationType;

    /** The stream id of the operation to cancel */
    private final int id;

    private CancelMessage(OperationType operationType, int id)
    {
        super(Type.CANCEL);
        this.operationType = operationType;
        this.id = id;
    }

    public Response execute(QueryState queryState, long queryStartNanoTime)
    {
        if (operationType != OperationType.CONTINOUS_PAGING)
            throw new InvalidRequestException(String.format("Unknown operation type: %s", operationType));

        try
        {
            boolean res = ContinuousPagingService.cancel(queryState, id);
            List<List<ByteBuffer>> rows = Collections.singletonList(Collections.singletonList(BooleanType.instance.decompose(res)));
            return new ResultMessage.Rows(new ResultSet(RESULT_METADATA, rows));
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            return ErrorMessage.fromException(t);
        }
    }
}
