package org.apache.cassandra.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.BeforeClass;

import org.apache.cassandra.utils.TimeSource;

public class WithParkedThreadsMonitorSupport
{
    private static final AtomicLong counter = new AtomicLong();

    @BeforeClass
    public static void beforeClass()
    {
        ParkedThreadsMonitor.instance.get().addAction(() -> counter.incrementAndGet());
    }

    protected void sleepFor(TimeSource timeSource, long millis) throws InterruptedException
    {
        timeSource.sleep(millis, TimeUnit.MILLISECONDS);
        long current = counter.get();
        while (counter.get() == current)
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MICROSECONDS);
    }
}
