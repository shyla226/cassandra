package org.apache.cassandra.concurrent;

import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import io.netty.util.Timeout;
import io.reactivex.disposables.Disposable;
import org.apache.cassandra.utils.ApproximateTimeSource;
import org.apache.cassandra.utils.HashedWheelTimer;
import org.apache.cassandra.utils.TimeSource;

/**
 * TPC timer based on the hashed wheel algorithm, with 100ms precision.
 */
public class TPCHashedWheelTimer implements TPCTimer
{
    private final TPCScheduler scheduler;
    private final HashedWheelTimer timer;

    public TPCHashedWheelTimer(TPCScheduler scheduler)
    {
        this(new ApproximateTimeSource(), scheduler);
    }

    @VisibleForTesting
    public TPCHashedWheelTimer(TimeSource timeSource, TPCScheduler scheduler)
    {
        this.scheduler = scheduler;
        this.timer = new HashedWheelTimer(timeSource, scheduler.forTaskType(TPCTaskType.TIMED_TIMEOUT), 100, TimeUnit.MILLISECONDS, 512, false);
        this.timer.start();
    }

    @Override
    public TPCScheduler getScheduler()
    {
        return scheduler;
    }

    @Override
    public Disposable onTimeout(Runnable task, long timeout, TimeUnit unit)
    {
        Timeout handle = timer.newTimeout(ignored -> task.run(), timeout, unit);
        return new Disposable()
        {
            @Override
            public void dispose()
            {
                handle.cancel();
            }

            @Override
            public boolean isDisposed()
            {
                return handle.isCancelled() || handle.isExpired();
            }
        };
    }
}
