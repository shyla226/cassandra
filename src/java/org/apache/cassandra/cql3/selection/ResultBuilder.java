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

package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * CQL row builder: abstract class for converting filtered rows and cells into CQL rows.
 *
 * Store a CQL row into a list of byte buffers (would be nice to get rid of this), which can
 * then be passed to {@link ResultBuilder#onRowCompleted(List, boolean)}, concrete implementations will convert this
 * row into the desired encoded representation, e.g. they will add it to a ResultSet or write it to
 * a Netty Byte Buffer.
 */
public abstract class ResultBuilder
{
    /**
     * The parent selection that created this row builder.
     */
    protected final Selection selection;
    private final ProtocolVersion protocolVersion;

    /**
     * As multiple thread can access a <code>Selection</code> instance each <code>ResultSetBuilder</code> will use
     * its own <code>Selectors</code> instance.
     */
    private final Selection.Selectors selectors;

    /**
     * The <code>GroupMaker</code> used to build the aggregates.
     */
    private final GroupMaker groupMaker;

    /*
     * We'll build CQL3 row one by one.
     * The currentRow is the values for the (CQL3) columns we've fetched.
     * We also collect timestamps and ttls for the case where the writetime and
     * ttl functions are used. Note that we might collect timestamp and/or ttls
     * we don't care about, but since the array below are allocated just once,
     * it doesn't matter performance wise.
     */
    List<ByteBuffer> current;
    final long[] timestamps;
    final int[] ttls;

    protected final boolean isJson;
    protected boolean completed;

    protected ResultBuilder(QueryOptions options, boolean isJson, GroupMaker groupMaker, Selection selection)
    {
        this.selection = selection;
        this.protocolVersion = options.getProtocolVersion();
        this.selectors = selection.newSelectors(options);
        this.groupMaker = groupMaker;
        this.timestamps = selection.collectTimestamps ? new long[selection.columns.size()] : null;
        this.ttls = selection.collectTTLs ? new int[selection.columns.size()] : null;
        this.isJson = isJson;

        // We use MIN_VALUE to indicate no timestamp and -1 for no ttl
        if (timestamps != null)
            Arrays.fill(timestamps, Long.MIN_VALUE);
        if (ttls != null)
            Arrays.fill(ttls, -1);
    }

    public void add(ByteBuffer v)
    {
        current.add(v);
    }

    public void add(Cell c, int nowInSec)
    {
        if (c == null)
        {
            current.add(null);
            return;
        }

        current.add(value(c));

        if (timestamps != null)
            timestamps[current.size() - 1] = c.timestamp();

        if (ttls != null)
            ttls[current.size() - 1] = remainingTTL(c, nowInSec);
    }

    private int remainingTTL(Cell c, int nowInSec)
    {
        if (!c.isExpiring())
            return -1;

        int remaining = c.localDeletionTime() - nowInSec;
        return remaining >= 0 ? remaining : -1;
    }

    private ByteBuffer value(Cell c)
    {
        return c.isCounterCell()
             ? ByteBufferUtil.bytes(CounterContext.instance().total(c.value()))
             : c.value();
    }

    /**
     * Notifies this <code>Builder</code> that a new row is being processed.
     *
     * @param partitionKey the partition key of the new row
     * @param clustering the clustering of the new row
     */
    public void newRow(DecoratedKey partitionKey, Clustering clustering)
    {
        // The groupMaker needs to be called for each row
        boolean isNewAggregate = groupMaker == null || groupMaker.isNewGroup(partitionKey, clustering);
        if (current != null)
        {
            selectors.addInputRow(protocolVersion, this);

            if (isNewAggregate)
            {
                boolean res = onRowCompleted(getOutputRow(), true);
                current = null;
                selectors.reset();
                if (!res)
                    complete();
            }
            else
            {
                current = null;
            }

        }
        current = new ArrayList<>(selection.columns.size());
    }

    /**
     * Complete the result because of an error.
     *
     * @param error - the exception that caused an early completion.
     */
    public void complete(Throwable error)
    {
        complete();
    }

    /**
     * Complete the result.
     */
    public void complete()
    {
        if (completed)
            return;

        if (current != null)
        {
            selectors.addInputRow(protocolVersion, this);
            onRowCompleted(getOutputRow(), false);
            selectors.reset();
            current = null;
        }

        // For aggregates we need to return a row even if no records have been found
        if (resultIsEmpty() && groupMaker != null && groupMaker.returnAtLeastOneRow())
            onRowCompleted(getOutputRow(), false);

        completed = true;
    }

    public boolean isCompleted()
    {
        return completed;
    }

    /**
     * Called when a complete row is available. Must return true in order to
     * continue processing more rows.
     *
     * @param row - the completed row
     * @param nextRowPending - true when there is a new row still to be processed
     * @return true if we should prcess more rows
     */
    public abstract boolean onRowCompleted(List<ByteBuffer> row, boolean nextRowPending);

    /**
     * @return - true if there is at least one row.
     */
    public abstract boolean resultIsEmpty();

    private List<ByteBuffer> getOutputRow()
    {
        List<ByteBuffer> outputRow = selectors.getOutputRow(protocolVersion);
        return isJson ? Selection.rowToJson(outputRow, protocolVersion, selection.metadata)
                      : outputRow;
    }
}
