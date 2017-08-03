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

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
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
    private final DataLimits limit;

    private final int nowInSec;

    private int remaining;
    private int current;

    public MultiPartitionPager(SinglePartitionReadCommand.Group group, PagingState state, ProtocolVersion protocolVersion)
    {
        this.limit = group.limits();
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
        pagers[0] = command.getPager(state, protocolVersion);

        // Following ones haven't been started yet
        for (int j = i + 1; j < group.commands.size(); j++)
            pagers[j - i] = group.commands.get(j).getPager(null, protocolVersion);

        remaining = state == null ? limit.count() : state.remaining;
    }

    private MultiPartitionPager(SinglePartitionPager[] pagers,
                                DataLimits limit,
                                int nowInSec,
                                int remaining,
                                int current)
    {
        this.pagers = pagers;
        this.limit = limit;
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

        return new PagingState(pagers[current].key(),
                               state == null ? null : state.rowMark,
                               remaining,
                               pagers[current].remainingInPartition(),
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
                logger.trace("{}, current: {} -> {}", hashCode(), current, current+1);
            current++;
        }
        return true;
    }

    public Flow<FlowablePartition> fetchPage(int pageSize, ReadContext ctx)
    throws RequestValidationException, RequestExecutionException
    {
        int toQuery = Math.min(remaining, pageSize);
        return new MultiPartitions(toQuery, ctx).partitions();
    }

    public Flow<FlowablePartition> fetchPageInternal(int pageSize)
    throws RequestValidationException, RequestExecutionException
    {
        int toQuery = Math.min(remaining, pageSize);
        return new MultiPartitions(toQuery, null).partitions();
    }

    private class MultiPartitions
    {
        private final int pageSize;
        // For distributed queries, will be null for internal ones
        @Nullable
        private final ReadContext ctx;

        private int pagerMaxRemaining;
        private int counted;

        private MultiPartitions(int pageSize,
                                ReadContext ctx)
        {
            this.pageSize = pageSize;
            this.ctx = ctx;
            this.pagerMaxRemaining = pagers[current].maxRemaining();
        }

        private Flow<FlowablePartition> partitions()
        {
            return fetchSubPage(pageSize).concatWith(this::moreContents)
                                         .doOnClose(this::close);
        }

        protected Flow<FlowablePartition> moreContents()
        {
            counted += pagerMaxRemaining - pagers[current].maxRemaining();

            // We are done if we have reached the page size or in the case of GROUP BY if the current pager
            // is not exhausted.
            boolean isDone = counted >= pageSize
                             || (limit.isGroupByLimit() && !pagers[current].isExhausted());

            if (logger.isTraceEnabled())
                logger.trace("{} - moreContents, current: {}, counted: {}, isDone: {}", MultiPartitionPager.this.hashCode(), current, counted, isDone);

            // isExhausted() will sets us on the first non-exhausted pager
            if (isDone || isExhausted())
                return null;

            pagerMaxRemaining = pagers[current].maxRemaining();
            int toQuery = pageSize - counted;
            return fetchSubPage(toQuery);

        }

        private Flow<FlowablePartition> fetchSubPage(int toQuery)
        {
            return ctx == null
                   ? pagers[current].fetchPageInternal(toQuery)
                   : pagers[current].fetchPage(toQuery, ctx);
        }

        private void close()
        {
            remaining -= counted;

            if (logger.isTraceEnabled())
                logger.trace("{} - closed, counted: {}, remaining: {}", MultiPartitionPager.this.hashCode(), counted, remaining);
        }
    }

    public int maxRemaining()
    {
        return remaining;
    }
}
