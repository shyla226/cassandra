package org.apache.cassandra.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

public class EpollTPCEventLoopGroupTest extends WithParkedThreadsMonitorSupport
{
    private static EpollTPCEventLoopGroup group;

    @BeforeClass
    public static void setupClass()
    {
        WithParkedThreadsMonitorSupport.beforeClass();
        DatabaseDescriptor.daemonInitialization();

        DatabaseDescriptor.getRawConfig().tpc_cores = 2;
        DatabaseDescriptor.getRawConfig().tpc_concurrent_requests_limit = 1;
        DatabaseDescriptor.getRawConfig().tpc_pending_requests_limit = 1;

        if (TPC.eventLoopGroup() instanceof EpollTPCEventLoopGroup)
            group = (EpollTPCEventLoopGroup) TPC.eventLoopGroup();
    }

    @AfterClass
    public static void tearDownClass()
    {
        if (group == null)
            return;

        group.shutdown();
    }

    @Test
    public void testLocalBackpressure() throws InterruptedException
    {
        if (group == null)
            return;

        TPCEventLoop loop = group.eventLoops().get(0);

        CountDownLatch firstTaskProcessed = new CountDownLatch(1);
        CountDownLatch firstTaskLatch = new CountDownLatch(1);
        CountDownLatch lastTaskLatch = new CountDownLatch(1);

        // The first task gets executed and pauses:
        loop.execute(TPCRunnable.wrap(() -> {
            firstTaskProcessed.countDown();
            Uninterruptibles.awaitUninterruptibly(firstTaskLatch, 1, TimeUnit.MINUTES);
        }, TPCTaskType.WRITE_LOCAL, 0));
        Uninterruptibles.awaitUninterruptibly(firstTaskProcessed, 1, TimeUnit.MINUTES);
        Assert.assertFalse(loop.shouldBackpressure(false));

        // The second task is stored in the processing queue:
        loop.execute(TPCRunnable.wrap(() -> {}, TPCTaskType.WRITE_LOCAL, 0));
        Assert.assertFalse(loop.shouldBackpressure(false));

        // The third task is stored in the pending queue (due to tpc_concurrent_requests_limit=1)
        // and causes local backpressure (due to tpc_pending_requests_limit=1):
        loop.execute(TPCRunnable.wrap(() -> lastTaskLatch.countDown(), TPCTaskType.WRITE_LOCAL, 0));
        Assert.assertTrue(loop.shouldBackpressure(false));

        // Assert no remote backpressure:
        Assert.assertFalse(loop.shouldBackpressure(true));

        // Unblock tasks:
        firstTaskLatch.countDown();
        Assert.assertTrue(lastTaskLatch.await(1, TimeUnit.MINUTES));

        // Verify there's no more backpressure after tasks have been processed:
        Assert.assertFalse(loop.shouldBackpressure(false));
    }

    @Test
    public void testRemoteBackpressure() throws InterruptedException
    {
        if (group == null)
            return;

        TPCEventLoop loop = group.eventLoops().get(0);

        CountDownLatch firstTaskProcessed = new CountDownLatch(1);
        CountDownLatch firstTaskLatch = new CountDownLatch(1);
        CountDownLatch lastTaskLatch = new CountDownLatch(1);

        // The first task gets executed and pauses:
        loop.execute(TPCRunnable.wrap(() -> {
            firstTaskProcessed.countDown();
            Uninterruptibles.awaitUninterruptibly(firstTaskLatch, 1, TimeUnit.MINUTES);
        }, TPCTaskType.WRITE_LOCAL, 0));
        Uninterruptibles.awaitUninterruptibly(firstTaskProcessed, 1, TimeUnit.MINUTES);
        Assert.assertFalse(loop.shouldBackpressure(true));

        // The second task is stored in the processing queue:
        loop.execute(TPCRunnable.wrap(() -> {}, TPCTaskType.WRITE_REMOTE, 0));
        Assert.assertFalse(loop.shouldBackpressure(true));

        // Add 5 tasks which will cause remote backpressure (due to tpc_pending_requests_limit=1, times the default
        // multiplier of 5):
        for (int i = 0; i < 4; i++)
        {
            loop.execute(TPCRunnable.wrap(() -> {}, TPCTaskType.WRITE_REMOTE, 0));
            Assert.assertFalse(loop.shouldBackpressure(true));
        }
        loop.execute(TPCRunnable.wrap(() -> lastTaskLatch.countDown(), TPCTaskType.WRITE_REMOTE, 0));
        Assert.assertTrue(loop.shouldBackpressure(true));

        // Assert no local backpressure:
        Assert.assertFalse(loop.shouldBackpressure(false));

        // Unblock tasks:
        firstTaskLatch.countDown();
        Assert.assertTrue(lastTaskLatch.await(1, TimeUnit.MINUTES));

        // Verify there's no more backpressure after tasks have been processed:
        Assert.assertFalse(loop.shouldBackpressure(true));
    }

    @Test
    public void testGlobalBackpressure() throws InterruptedException
    {
        if (group == null)
            return;

        TPCEventLoop loop1 = group.eventLoops().get(0);
        TPCEventLoop loop2 = group.eventLoops().get(1);

        CountDownLatch firstTaskProcessed = new CountDownLatch(1);
        CountDownLatch firstTaskLatch = new CountDownLatch(1);
        CountDownLatch firstPendingTaskProcessed = new CountDownLatch(1);
        CountDownLatch firstPendingTaskLatch = new CountDownLatch(1);
        CountDownLatch lastTaskLatch = new CountDownLatch(1);

        // The first task gets executed and pauses:
        loop1.execute(TPCRunnable.wrap(() -> {
            firstTaskProcessed.countDown();
            Uninterruptibles.awaitUninterruptibly(firstTaskLatch, 1, TimeUnit.MINUTES);
        }, TPCTaskType.WRITE_LOCAL, 0));
        Uninterruptibles.awaitUninterruptibly(firstTaskProcessed, 1, TimeUnit.MINUTES);
        Assert.assertFalse(loop2.shouldBackpressure(false));
        Assert.assertFalse(loop2.shouldBackpressure(true));

        // The second task is stored in the processing queue:
        loop1.execute(TPCRunnable.wrap(() -> {}, TPCTaskType.WRITE_LOCAL, 0));
        Assert.assertFalse(loop2.shouldBackpressure(false));
        Assert.assertFalse(loop2.shouldBackpressure(true));

        // Add 11 tasks which will cause global backpressure (due to tpc_pending_requests_limit=1, times the default
        // multiplier of max(10, tpc cores)); we need one more task (so 11, not 10) because when the first task is
        // processed we need to test for backpressure:
        loop1.execute(TPCRunnable.wrap(() -> {
            firstPendingTaskProcessed.countDown();
            Uninterruptibles.awaitUninterruptibly(firstPendingTaskLatch, 1, TimeUnit.MINUTES);
        }, TPCTaskType.WRITE_LOCAL, 0));
        for (int i = 0; i < 9; i++)
        {
            loop1.execute(TPCRunnable.wrap(() -> {}, TPCTaskType.WRITE_LOCAL, 0));
            Assert.assertFalse(loop2.shouldBackpressure(false));
            Assert.assertFalse(loop2.shouldBackpressure(true));
        }
        loop1.execute(TPCRunnable.wrap(() -> lastTaskLatch.countDown(), TPCTaskType.WRITE_LOCAL, 0));

        // Unblock first task:
        firstTaskLatch.countDown();

        // Wait for the first pending task to be processed so we can check the global backpressure which is activated on
        // pending tasks transfer:
        Uninterruptibles.awaitUninterruptibly(firstPendingTaskProcessed, 1, TimeUnit.MINUTES);
        // Assert the second loop sees both local and remote backpressure as active as that's how global backpressure works:
        Assert.assertTrue(loop2.shouldBackpressure(false));
        Assert.assertTrue(loop2.shouldBackpressure(true));

        // Unblock the pending task and wait for all:
        firstPendingTaskLatch.countDown();
        Assert.assertTrue(lastTaskLatch.await(1, TimeUnit.MINUTES));

        // Verify there's no more backpressure after tasks have been processed:
        Assert.assertFalse(loop2.shouldBackpressure(false));
        Assert.assertFalse(loop2.shouldBackpressure(true));
    }
}
