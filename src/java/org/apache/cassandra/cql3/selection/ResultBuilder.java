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
import java.util.List;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.rows.Cell;

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
    private Selector.InputRow inputRow;

    protected boolean completed;

    protected ResultBuilder(Selection.Selectors selectors, GroupMaker groupMaker)
    {
        this.selectors = selectors;
        this.groupMaker = groupMaker;
    }

    public void add(ByteBuffer v)
    {
        inputRow.add(v);
    }

    public void add(Cell c, int nowInSec)
    {
        inputRow.add(c, nowInSec);
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
        if (inputRow != null)
        {
            selectors.addInputRow(inputRow);

            if (isNewAggregate)
            {
                boolean res = onRowCompleted(selectors.getOutputRow(), true);
                inputRow.reset(!selectors.hasProcessing());
                selectors.reset();
                if (!res)
                    complete();
            }
            else
            {
                inputRow.reset(!selectors.hasProcessing());
            }
        }
        else
        {
            inputRow = selectors.newInputRow();
        }
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

        if (inputRow != null)
        {
            selectors.addInputRow(inputRow);
            onRowCompleted(selectors.getOutputRow(), false);
            inputRow.reset(!selectors.hasProcessing());
            selectors.reset();
        }

        // For aggregates we need to return a row even if no records have been found
        if (resultIsEmpty() && groupMaker != null && groupMaker.returnAtLeastOneRow())
            onRowCompleted(selectors.getOutputRow(), false);

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
     * @return true if we should process more rows
     */
    public abstract boolean onRowCompleted(List<ByteBuffer> row, boolean nextRowPending);

    /**
     * @return - true if there is at least one row.
     */
    public abstract boolean resultIsEmpty();
}
