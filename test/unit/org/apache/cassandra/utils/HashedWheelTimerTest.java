package org.apache.cassandra.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import io.netty.util.Timeout;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.WithParkedThreadsMonitorSupport;

public class HashedWheelTimerTest extends WithParkedThreadsMonitorSupport
{
    @Test
    public void testTimeout() throws InterruptedException
    {
        TimeSource timeSource = new TestTimeSource();
        HashedWheelTimer timer = new HashedWheelTimer(timeSource, new DebuggableThreadPoolExecutor("test", 1), 100, TimeUnit.MILLISECONDS, 2, true);
        timer.start();
        try
        {
            CountDownLatch run = new CountDownLatch(1);
            timer.newTimeout(ignored -> run.countDown(), 10, TimeUnit.MILLISECONDS);

            sleepFor(timeSource, 100);
            Assert.assertTrue(run.await(100, TimeUnit.MILLISECONDS));
        }
        finally
        {
            Assert.assertTrue(timer.stop().isEmpty());
        }
    }

    @Test
    public void testTimeoutLargerThanWheel() throws InterruptedException
    {
        TimeSource timeSource = new TestTimeSource();
        HashedWheelTimer timer = new HashedWheelTimer(timeSource, new DebuggableThreadPoolExecutor("test", 1), 100, TimeUnit.MILLISECONDS, 2, true);
        timer.start();
        try
        {
            CountDownLatch run = new CountDownLatch(1);
            timer.newTimeout(ignored -> run.countDown(), 400, TimeUnit.MILLISECONDS);

            sleepFor(timeSource, 200);
            Assert.assertFalse(run.await(100, TimeUnit.MILLISECONDS));

            sleepFor(timeSource, 300);
            Assert.assertTrue(run.await(100, TimeUnit.MILLISECONDS));
        }
        finally
        {
            Assert.assertTrue(timer.stop().isEmpty());
        }
    }

    @Test
    public void testCancellation() throws InterruptedException
    {
        TimeSource timeSource = new TestTimeSource();
        HashedWheelTimer timer = new HashedWheelTimer(timeSource, new DebuggableThreadPoolExecutor("test", 1), 100, TimeUnit.MILLISECONDS, 2, true);
        timer.start();
        try
        {
            CountDownLatch run = new CountDownLatch(1);
            Timeout timeout = timer.newTimeout(ignored -> run.countDown(), 10, TimeUnit.MILLISECONDS);

            timeout.cancel();

            sleepFor(timeSource, 100);
            Assert.assertFalse(run.await(100, TimeUnit.MILLISECONDS));
        }
        finally
        {
            Assert.assertTrue(timer.stop().isEmpty());
        }
    }

    @Test
    public void testStopWithPendingTimeout() throws InterruptedException
    {
        TimeSource timeSource = new TestTimeSource();
        HashedWheelTimer timer = new HashedWheelTimer(timeSource, new DebuggableThreadPoolExecutor("test", 1), 100, TimeUnit.MILLISECONDS, 2, true);
        timer.start();

        CountDownLatch run = new CountDownLatch(1);
        timer.newTimeout(ignored -> run.countDown(), 10, TimeUnit.MILLISECONDS);

        Assert.assertEquals(1, timer.stop().size());
        Assert.assertFalse(run.await(100, TimeUnit.MILLISECONDS));
    }
}
