package org.apache.cassandra.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCHashedWheelTimer;
import org.apache.cassandra.concurrent.WithParkedThreadsMonitorSupport;
import org.apache.cassandra.config.DatabaseDescriptor;

public class ExpiringMapTest extends WithParkedThreadsMonitorSupport
{
    @BeforeClass
    public static void beforeClass()
    {
        WithParkedThreadsMonitorSupport.beforeClass();
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testExpiration() throws InterruptedException
    {
        TimeSource timeSource = new TestTimeSource();
        TPCHashedWheelTimer timer = new TPCHashedWheelTimer(timeSource, TPC.bestTPCScheduler());

        CountDownLatch run = new CountDownLatch(2);
        ExpiringMap<String, String> map = new ExpiringMap<String, String>(100, ignored -> run.countDown());

        map.put("key1", "value1", 100, timer);
        map.put("key2", "value2", 500, timer);

        ExpiringMap.ExpiringObject<String> obj1 = map.get("key1");
        Assert.assertNotNull(obj1);
        ExpiringMap.ExpiringObject<String> obj2 = map.get("key2");
        Assert.assertNotNull(obj2);

        sleepFor(timeSource, 200);
        Assert.assertFalse(run.await(100, TimeUnit.MILLISECONDS));
        Assert.assertNull(map.get("key1"));
        Assert.assertNotNull(map.get("key2"));
        sleepFor(timeSource, 400);
        Assert.assertTrue(run.await(100, TimeUnit.MILLISECONDS));
        Assert.assertNull(map.get("key2"));
    }

    @Test
    public void testManualCancellation() throws InterruptedException
    {
        TimeSource timeSource = new TestTimeSource();
        TPCHashedWheelTimer timer = new TPCHashedWheelTimer(timeSource, TPC.bestTPCScheduler());

        CountDownLatch run = new CountDownLatch(1);
        ExpiringMap<String, String> map = new ExpiringMap<String, String>(100, ignored -> run.countDown());

        map.put("key", "value", 100, timer);

        ExpiringMap.ExpiringObject<String> obj = map.get("key");
        obj.cancel();

        sleepFor(timeSource, 200);
        Assert.assertFalse(run.await(100, TimeUnit.MILLISECONDS));
        Assert.assertNotNull(map.get("key"));
    }

    @Test
    public void testCancellationViaRemove() throws InterruptedException
    {
        TimeSource timeSource = new TestTimeSource();
        TPCHashedWheelTimer timer = new TPCHashedWheelTimer(timeSource, TPC.bestTPCScheduler());

        CountDownLatch run = new CountDownLatch(1);
        ExpiringMap<String, String> map = new ExpiringMap<String, String>(100, ignored -> run.countDown());

        String previous = map.put("key", "value", 100, timer);
        Assert.assertNull(previous);

        ExpiringMap.ExpiringObject<String> obj = map.remove("key");

        sleepFor(timeSource, 200);
        Assert.assertFalse(run.await(100, TimeUnit.MILLISECONDS));
        Assert.assertNull(map.get("key"));
    }

    @Test
    public void testCancellationViaReplace() throws InterruptedException
    {
        TimeSource timeSource = new TestTimeSource();
        TPCHashedWheelTimer timer = new TPCHashedWheelTimer(timeSource, TPC.bestTPCScheduler());

        CountDownLatch run = new CountDownLatch(1);
        ExpiringMap<String, String> map = new ExpiringMap<String, String>(100, ignored -> run.countDown());

        String previous = map.put("key", "value1", 100, timer);
        Assert.assertNull(previous);
        previous = map.put("key", "value2", 500, timer);
        Assert.assertNotNull(previous);

        ExpiringMap.ExpiringObject<String> obj = map.get("key");
        Assert.assertEquals("value2", obj.get());

        sleepFor(timeSource, 200);
        Assert.assertFalse(run.await(100, TimeUnit.MILLISECONDS));
        Assert.assertNotNull(map.get("key"));
    }
}
