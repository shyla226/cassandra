package org.apache.cassandra.cql3.continuous.paging;

import io.reactivex.Scheduler;
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

    Scheduler getScheduler();

    void schedule(PagingState pagingState, ResultBuilder builder);
}
