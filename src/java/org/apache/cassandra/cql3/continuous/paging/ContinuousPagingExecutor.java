/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.cql3.continuous.paging;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cql3.selection.ResultBuilder;
import org.apache.cassandra.service.pager.PagingState;

/**
 * Interface for executing continuous paging queries, and carrying information related to such execution.
 */
public interface ContinuousPagingExecutor
{
    long localStartTimeInMillis();

    long queryStartTimeInNanos();

    boolean isLocalQuery();

    PagingState state(boolean inclusive);

    default void schedule(Runnable runnable)
    {
        schedule(runnable, 0, TimeUnit.NANOSECONDS);
    }

    void schedule(Runnable runnable, long delay, TimeUnit unit);

    void schedule(PagingState pagingState, ResultBuilder builder);

    int coreId();
    void setCoreId(int coreId);
}
