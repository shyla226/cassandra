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

package org.apache.cassandra.cql3;

import java.util.Objects;

import org.apache.cassandra.service.pager.PagingState;

/**
 * The metadata related to paging, associated to a page of results.
 * <p>
 * This is sent along the result set metadata.
 */
public final class PagingResult
{
    public static final PagingResult NONE = new PagingResult(null);

    /** The paging state contains information opaque to the client that is needed by the
     * server in order to resume the query from the last row in this page. */
    public final PagingState state;

    /** A unique sequential number to identify the page being sent. This is only required when we
     * are delivering continuous pages, otherwise it is -1. */
    public final int seqNo;

    /** True when this is the last page, this relates to the delivery of pages, so the last page
     * is not necessarily the last result page that is available. For example the client may request
     * only the first 3 pages, in which case last would be true for the third page, even though
     * there is more data to be retrieved for this query. */
    public final boolean last;

    public PagingResult(PagingState pagingState)
    {
        this(pagingState, -1, true);
    }

    public PagingResult(PagingState state, int seqNo, boolean last)
    {
        this.state = state;
        this.seqNo = seqNo;
        this.last = last;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;

        if (!(other instanceof PagingResult))
            return false;

        PagingResult that = (PagingResult) other;
        return Objects.equals(state, that.state) && seqNo == that.seqNo && last == that.last;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(state, seqNo, last);
    }

    @Override
    public String toString()
    {
        return String.format("[Page seq. no. %d%s - state %s]", seqNo, last ? " final" : "", state);
    }
}
