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
package org.apache.cassandra.service.pager;

import java.util.Optional;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Pages a PartitionRangeReadCommand.
 */
public class PartitionRangeQueryPager extends AbstractQueryPager<PartitionRangeReadCommand>
{
    private volatile DecoratedKey lastReturnedKey;
    private volatile PagingState.RowMark lastReturnedRow;

    public PartitionRangeQueryPager(PartitionRangeReadCommand command, PagingState state, ProtocolVersion protocolVersion)
    {
        super(command, protocolVersion);

        if (state != null)
        {
            lastReturnedKey = command.metadata().decorateKey(state.partitionKey);
            lastReturnedRow = state.rowMark;
            restoreState(lastReturnedKey, state.remaining, state.remainingInPartition, state.inclusive);
        }
    }

    private PartitionRangeQueryPager(PartitionRangeReadCommand command,
                                    ProtocolVersion protocolVersion,
                                    DecoratedKey lastReturnedKey,
                                    PagingState.RowMark lastReturnedRow,
                                    int remaining,
                                    int remainingInPartition)
    {
        super(command, protocolVersion);
        this.lastReturnedKey = lastReturnedKey;
        this.lastReturnedRow = lastReturnedRow;
        restoreState(lastReturnedKey, remaining, remainingInPartition, false);
    }

    public PartitionRangeQueryPager withUpdatedLimit(DataLimits newLimits)
    {
        return new PartitionRangeQueryPager(command.withUpdatedLimit(newLimits),
                                            protocolVersion,
                                            lastReturnedKey,
                                            lastReturnedRow,
                                            maxRemaining(),
                                            remainingInPartition());
    }

    protected PagingState makePagingState(DecoratedKey lastKey, Row lastRow, boolean inclusive)
    {
        return makePagingState(getLastReturnedKey(lastKey, lastRow), getLastReturnedRow(lastRow), inclusive);
    }

    protected PagingState makePagingState(boolean inclusive)
    {
        return makePagingState(lastReturnedKey, lastReturnedRow, inclusive);
    }

    private PagingState makePagingState(DecoratedKey lastKey, PagingState.RowMark lastRow, boolean inclusive)
    {
        return lastKey == null
               ? null
               : new PagingState(lastKey.getKey(), lastRow, maxRemaining(), remainingInPartition(), inclusive);
    }

    protected ReadCommand nextPageReadCommand(int pageSize)
    throws RequestExecutionException
    {
        DataLimits limits;
        DataRange fullRange = command.dataRange();
        DataRange pageRange;
        if (lastReturnedKey == null)
        {
            pageRange = fullRange;
            limits = command.limits().forPaging(pageSize);
        }
        else
        {
            // We want to include the last returned key only if we haven't achieved our per-partition limit, otherwise, don't bother.
            boolean includeLastKey = remainingInPartition() > 0 && lastReturnedRow != null;
            AbstractBounds<PartitionPosition> bounds = makeKeyBounds(lastReturnedKey, includeLastKey);
            if (includeLastKey)
            {
                pageRange = fullRange.forPaging(bounds, command.metadata().comparator, lastReturnedRow.clustering(command.metadata()), inclusive);
                limits = command.limits().forPaging(pageSize, lastReturnedKey.getKey(), remainingInPartition());
            }
            else
            {
                pageRange = fullRange.forSubRange(bounds);
                limits = command.limits().forPaging(pageSize);
            }
        }

        Index index = command.getIndex(Keyspace.openAndGetStore(command.metadata()));
        Optional<IndexMetadata> indexMetadata = index != null ? Optional.of(index.getIndexMetadata()) : Optional.empty();
        return command.withUpdatedParams(limits, pageRange, indexMetadata);
    }

    protected void recordLast(DecoratedKey key, Row last)
    {
        lastReturnedKey = getLastReturnedKey(key, last);
        lastReturnedRow = getLastReturnedRow(last);
    }

    private DecoratedKey getLastReturnedKey(DecoratedKey key, Row last)
    {
        if (last != null)
            return key;

        return lastReturnedKey; // no change
    }

    private PagingState.RowMark getLastReturnedRow(Row last)
    {
        if (last != null && last.clustering() != Clustering.STATIC_CLUSTERING)
            return PagingState.RowMark.create(command.metadata(), last, protocolVersion);

        return lastReturnedRow; //no change
    }

    protected boolean isPreviouslyReturnedPartition(DecoratedKey key)
    {
        // Note that lastReturnedKey can be null, but key cannot.
        return key.equals(lastReturnedKey);
    }

    private AbstractBounds<PartitionPosition> makeKeyBounds(PartitionPosition lastReturnedKey, boolean includeLastKey)
    {
        AbstractBounds<PartitionPosition> bounds = command.dataRange().keyRange();
        if (bounds instanceof Range || bounds instanceof Bounds)
        {
            return includeLastKey
                 ? new Bounds<>(lastReturnedKey, bounds.right)
                 : new Range<>(lastReturnedKey, bounds.right);
        }
        else
        {
            return includeLastKey
                 ? new IncludingExcludingBounds<>(lastReturnedKey, bounds.right)
                 : new ExcludingBounds<>(lastReturnedKey, bounds.right);
        }
    }
}
