package org.apache.cassandra.concurrent;

import java.util.concurrent.TimeUnit;

import io.reactivex.disposables.Disposable;

/**
 * Allows to run tasks at given timeouts, relying on the given TPC scheduler under the hood for actual execution.
 * <p>
 * There is no guarantee about the actual precision for the given timeout execution, so implementations are intended
 * to be used for non time sensitive tasks (i.e. network timeouts).
 */
public interface TPCTimer
{
    /**
     * The TPC scheduler used to actually execute the tasks.
     */
    TPCScheduler getScheduler();

    /**
     * Runs the given task at the given timeout.
     * @return The disposable handle to use for cancelling the expiration task.
     */
    Disposable onTimeout(Runnable task, long timeout, TimeUnit unit);
}
