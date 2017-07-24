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

import org.apache.cassandra.transport.ProtocolVersion;

import java.util.Arrays;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.flow.Flow;

/**
 * Pager over a list of ReadCommand.
 *
 * Note that this is not easy to make efficient. Indeed, we need to page the first command fully before
 * returning results from the next one, but if the result returned by each command is small (compared to pageSize),
 * paging the commands one at a time under-performs compared to parallelizing. On the other, if we parallelize
 * and each command raised pageSize results, we'll end up with commands.size() * pageSize results in memory, which
 * defeats the purpose of paging.
 *
 * For now, we keep it simple (somewhat) and just do one command at a time. Provided that we make sure to not
 * create a pager unless we need to, this is probably fine. Though if we later want to get fancy, we could use the
 * cfs meanPartitionSize to decide if parallelizing some of the command might be worth it while being confident we don't
 * blow out memory.
 */
public class MultiPartitionPager implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(MultiPartitionPager.class);

    private final SinglePartitionPager[] pagers;
    private final DataLimits limits;

    private final int nowInSec;

    private int remaining;
    private int current;

    public MultiPartitionPager(SinglePartitionReadCommand.Group group, PagingState state, ProtocolVersion protocolVersion)
    {
        this.limits = group.limits();
        this.nowInSec = group.nowInSec();

        int i = 0;
        // If it's not the beginning (state != null), we need to find where we were and skip previous commands
        // since they are done.
        if (state != null)
            for (; i < group.commands.size(); i++)
                if (group.commands.get(i).partitionKey().getKey().equals(state.partitionKey))
                    break;

        if (i >= group.commands.size())
        {
            pagers = null;
            return;
        }

        pagers = new SinglePartitionPager[group.commands.size() - i];
        // 'i' is on the first non exhausted pager for the previous page (or the first one)
        SinglePartitionReadCommand command = group.commands.get(i);
        // the propagated state will allow the command to properly set the "remaining" value
        // see isExhausted() to see how such value is propagated to later commands
        pagers[0] = command.getPager(state, protocolVersion);

        // Following ones haven't been started yet
        for (int j = i + 1; j < group.commands.size(); j++)
            pagers[j - i] = group.commands.get(j).getPager(null, protocolVersion);

        remaining = state == null ? limits.count() : state.remaining;
    }

    private MultiPartitionPager(SinglePartitionPager[] pagers,
                                DataLimits limits,
                                int nowInSec,
                                int remaining,
                                int current)
    {
        this.pagers = pagers;
        this.limits = limits;
        this.nowInSec = nowInSec;
        this.remaining = remaining;
        this.current = current;
    }

    public QueryPager withUpdatedLimit(DataLimits newLimits)
    {
        SinglePartitionPager[] newPagers = Arrays.copyOf(pagers, pagers.length);
        newPagers[current] = newPagers[current].withUpdatedLimit(newLimits);

        return new MultiPartitionPager(newPagers,
                                       newLimits,
                                       nowInSec,
                                       remaining,
                                       current);
    }

    public PagingState state(boolean inclusive)
    {
        // Sets current to the first non-exhausted pager
        if (isExhausted())
            return null;

        PagingState state = pagers[current].state(inclusive);
        if (logger.isTraceEnabled())
            logger.trace("{} - state: {}, current: {}", hashCode(), state, current);

        // inclusive means that the next search command should include the row that has already been counted by the pager
        int remaining = inclusive ? FBUtilities.add(this.remaining, 1) : this.remaining;
        int remainingInPartition = inclusive ? FBUtilities.add(pagers[current].remainingInPartition(), 1) : pagers[current].remainingInPartition();

        return new PagingState(pagers[current].key(),
                               state == null ? null : state.rowMark,
                               remaining,
                               remainingInPartition,
                               inclusive);
    }

    public boolean isExhausted()
    {
        if (remaining <= 0 || pagers == null)
            return true;

        while (current < pagers.length)
        {
            if (!pagers[current].isExhausted())
                return false;

            if (logger.isTraceEnabled())
                logger.trace("{}, current: {} -> {}", hashCode(), current, current + 1);
            
            current++;

            if (current < pagers.length)
            {
                // update the next pager with the latest "remaining" value as the new limit count
                DataLimits limits = pagers[current].limits().withCount(remaining);
                pagers[current] = pagers[current].withUpdatedLimit(limits);
            }
        }
        return true;
    }

    public Flow<FlowablePartition> fetchPage(PageSize pageSize, ReadContext ctx)
    throws RequestValidationException, RequestExecutionException
    {
        return new MultiPartitions(pageSize, ctx).partitions();
    }

    public Flow<FlowablePartition> fetchPageInternal(PageSize pageSize)
    throws RequestValidationException, RequestExecutionException
    {
        return new MultiPartitions(pageSize, null).partitions();
    }

    private class MultiPartitions
    {
        private final PageSize pageSize;
        private PageSize toQuery;
        
        // For distributed queries, will be null for internal ones
        @Nullable
        private final ReadContext ctx;

        private int counted;

        private MultiPartitions(PageSize pageSize,
                                ReadContext ctx)
        {
            this.pageSize = pageSize;
            this.ctx = ctx;
        }

        private Flow<FlowablePartition> partitions()
        {
            return fetchSubPage(null).concatWith(this::moreContents)
                                         .doOnClose(this::close);
        }

        protected Flow<FlowablePartition> moreContents()
        {
            DataLimits.Counter currentCounter = pagers[current].lastCounter;
            
            counted += currentCounter.counted();
            remaining -= currentCounter.counted();
            
            // We are done if we have reached the page size or in the case of GROUP BY if the current pager
            // is not exhausted.
            boolean isDone = toQuery.isComplete(currentCounter.counted(), currentCounter.bytesCounted())
                             || (limits.isGroupByLimit() && !pagers[current].isExhausted());
            
            if (logger.isTraceEnabled())
                logger.trace("{} - moreContents, current: {}, counted: {}, isDone: {}", MultiPartitionPager.this.hashCode(), current, counted, isDone);

            // isExhausted() will sets us on the first non-exhausted pager
            if (isDone || isExhausted())
                return null;

            return fetchSubPage(currentCounter);
        }

        private Flow<FlowablePartition> fetchSubPage(DataLimits.Counter currentCounter)
        {
            // update the page size based on the current counter
            if (currentCounter != null)
            {
                if (pageSize.isInBytes())
                    toQuery = PageSize.bytesSize(toQuery.rawSize() - currentCounter.bytesCounted());
                else
                    toQuery = PageSize.rowsSize(toQuery.rawSize() - currentCounter.counted());
            }
            else
                toQuery = pageSize;
            
            if (logger.isTraceEnabled())
                logger.trace("{} - fetchSubPage, subPager: [{}], pageSize: {} {}", 
                             MultiPartitionPager.this.hashCode(), pagers[current].command, 
                             toQuery.rawSize(), toQuery.isInRows() ? PageSize.PageUnit.ROWS : PageSize.PageUnit.BYTES);

            // the current pager will now have an up-to-date value for both the remaining counter and the page size
            return ctx == null
                   ? pagers[current].fetchPageInternal(toQuery)
                   : pagers[current].fetchPage(toQuery, ctx);
        }

        private void close()
        {
            if (logger.isTraceEnabled())
                logger.trace("{} - closed, counted: {}, remaining: {}", MultiPartitionPager.this.hashCode(), counted, remaining);
        }
    }

    public int maxRemaining()
    {
        return remaining;
    }
}
