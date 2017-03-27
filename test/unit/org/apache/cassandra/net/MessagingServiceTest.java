/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.apache.cassandra.metrics.Timer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.monitoring.ApproximateTime;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class MessagingServiceTest
{
    private final static long ONE_SECOND = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);
    private final MessagingService messagingService = MessagingService.test();
    private static int defaultMetricsHistogramUpdateInterval;

    private static Verb<?, ?> withBackPressure;
    private static Verb<?, ?> withoutBackPressure;

    @BeforeClass
    public static void beforeClass() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        NettyRxScheduler.register();

        DatabaseDescriptor.setBackPressureStrategy(new MockBackPressureStrategy(Collections.emptyMap()));
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.0.0.1"));

        defaultMetricsHistogramUpdateInterval = DatabaseDescriptor.getMetricsHistogramUpdateTimeMillis();
        DatabaseDescriptor.setMetricsHistogramUpdateTimeMillis(0); // this guarantees metrics histograms are updated on read

        withBackPressure = Verbs.WRITES.WRITE;
        withoutBackPressure = Verbs.GOSSIP.ACK; // one-way message, so can't suppor back-pressure;
    }

    @AfterClass
    public static void afterClass()
    {
        DatabaseDescriptor.setMetricsHistogramUpdateTimeMillis(defaultMetricsHistogramUpdateInterval);
    }

    private static int metricScopeId = 0;

    @Before
    public void before() throws UnknownHostException
    {
        messagingService.resetDroppedMessagesMap(Integer.toString(metricScopeId++));
        MockBackPressureStrategy.applied = false;
        messagingService.destroyConnectionPool(InetAddress.getByName("127.0.0.2"));
        messagingService.destroyConnectionPool(InetAddress.getByName("127.0.0.3"));
    }

    @Test
    public void testDroppedMessages()
    {
        Verb<?, ?> def = Verbs.READS.READ;

        for (int i = 1; i <= 5000; i++)
            messagingService.incrementDroppedMessages(def, i, i % 2 == 0);

        List<String> logs = messagingService.getDroppedMessagesLogs();
        assertEquals(1, logs.size());
        assertEquals("READS.READ messages were dropped in last 5000 ms: 2500 internal and 2500 cross node. Mean internal dropped latency: 2468 ms and Mean cross-node dropped latency: 2469 ms", logs.get(0));
        assertEquals(5000, (int) messagingService.getDroppedMessages().get(def.toString()));

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            messagingService.incrementDroppedMessages(def, i, i % 2 == 0);

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals("READS.READ messages were dropped in last 5000 ms: 1250 internal and 1250 cross node. Mean internal dropped latency: 2057 ms and Mean cross-node dropped latency: 2057 ms", logs.get(0));
        assertEquals(7500, (int) messagingService.getDroppedMessages().get(def.toString()));
    }

    @Test
    public void testDCLatency() throws Exception
    {
        int latency = 100;
        ConcurrentHashMap<String, Timer> dcLatency = MessagingService.instance().metrics.dcLatency;
        dcLatency.clear();

        long now = ApproximateTime.currentTimeMillis();
        long sentAt = now - latency;
        assertNull(dcLatency.get("datacenter1"));
        addDCLatency(sentAt, now);
        assertNotNull(dcLatency.get("datacenter1"));
        assertEquals(1, dcLatency.get("datacenter1").getCount());

        long latencyNanos = TimeUnit.MILLISECONDS.toNanos(latency);
        Snapshot snapshot = dcLatency.get("datacenter1").getSnapshot();
        long max = snapshot.getMax();
        long min = snapshot.getMin();
        assertTrue(min <= latencyNanos);
        assertTrue(max >= latencyNanos);

        long[] bucketOffsets = dcLatency.get("datacenter1").getHistogram().getOffsets();
        long expectedBucket = bucketOffsets[Math.abs(Arrays.binarySearch(bucketOffsets, latencyNanos)) - 1];
        assertEquals(expectedBucket, snapshot.getMax() + 1);
    }

    @Test
    public void testQueueWaitLatency() throws Exception
    {
        int latency = 100;
        String verb = Verbs.WRITES.WRITE.toString();

        ConcurrentHashMap<String, Timer> queueWaitLatency = MessagingService.instance().metrics.queueWaitLatency;
        queueWaitLatency.clear();

        assertNull(queueWaitLatency.get(verb));
        MessagingService.instance().metrics.addQueueWaitTime(verb, latency);
        assertNotNull(queueWaitLatency.get(verb));
        assertEquals(1, queueWaitLatency.get(verb).getCount());

        long latencyNanos = TimeUnit.MILLISECONDS.toNanos(latency);
        Snapshot snapshot = queueWaitLatency.get(verb).getSnapshot();
        long max = snapshot.getMax();
        long min = snapshot.getMin();
        assertTrue(min <= latencyNanos);
        assertTrue(max >= latencyNanos);

        long[] bucketOffsets = queueWaitLatency.get(verb).getHistogram().getOffsets();
        long expectedBucket = bucketOffsets[Math.abs(Arrays.binarySearch(bucketOffsets, latencyNanos)) - 1];
        assertEquals(expectedBucket, snapshot.getMax() + 1);
    }

    @Test
    public void testNegativeQueueWaitLatency() throws Exception
    {
        int latency = -100;
        String verb = Verbs.WRITES.WRITE.toString();

        ConcurrentHashMap<String, Timer> queueWaitLatency = MessagingService.instance().metrics.queueWaitLatency;
        queueWaitLatency.clear();

        assertNull(queueWaitLatency.get(verb));
        MessagingService.instance().metrics.addQueueWaitTime(verb, latency);
        assertNull(queueWaitLatency.get(verb));
    }

    @Test
    public void testUpdatesBackPressureOnSendWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getConnectionPool(InetAddress.getByName("127.0.0.2")).getBackPressureState();

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnSend(Request.fakeTestRequest(InetAddress.getByName("127.0.0.2"), -1, withoutBackPressure, null));
        assertFalse(backPressureState.onSend);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnSend(Request.fakeTestRequest(InetAddress.getByName("127.0.0.2"), -1, withBackPressure, null));
        assertFalse(backPressureState.onSend);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnSend(Request.fakeTestRequest(InetAddress.getByName("127.0.0.2"), -1, withBackPressure, null));
        assertTrue(backPressureState.onSend);
    }

    @Test
    public void testUpdatesBackPressureOnReceiveWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getConnectionPool(InetAddress.getByName("127.0.0.2")).getBackPressureState();
        boolean timeout = false;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), withoutBackPressure, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), withBackPressure, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), withBackPressure, timeout);
        assertTrue(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);
    }

    @Test
    public void testUpdatesBackPressureOnTimeoutWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getConnectionPool(InetAddress.getByName("127.0.0.2")).getBackPressureState();
        boolean timeout = true;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), withoutBackPressure, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), withBackPressure, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), withBackPressure, timeout);
        assertFalse(backPressureState.onReceive);
        assertTrue(backPressureState.onTimeout);
    }

    @Test
    public void testAppliesBackPressureWhenEnabled() throws UnknownHostException
    {
        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.2")), ONE_SECOND);
        assertFalse(MockBackPressureStrategy.applied);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.2")), ONE_SECOND);
        assertTrue(MockBackPressureStrategy.applied);
    }

    @Test
    public void testDoesntApplyBackPressureToBroadcastAddress() throws UnknownHostException
    {
        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.1")), ONE_SECOND);
        assertFalse(MockBackPressureStrategy.applied);
    }

    private static void addDCLatency(long sentAt, long nowTime) throws IOException
    {
        MessagingService.instance().metrics.addTimeTaken(InetAddress.getLocalHost(), nowTime - sentAt);
    }

    public static class MockBackPressureStrategy implements BackPressureStrategy<MockBackPressureStrategy.MockBackPressureState>
    {
        public static volatile boolean applied = false;

        public MockBackPressureStrategy(Map<String, Object> args)
        {
        }

        @Override
        public void apply(Set<MockBackPressureState> states, long timeout, TimeUnit unit)
        {
            if (!Iterables.isEmpty(states))
                applied = true;
        }

        @Override
        public MockBackPressureState newState(InetAddress host)
        {
            return new MockBackPressureState(host);
        }

        public static class MockBackPressureState implements BackPressureState
        {
            private final InetAddress host;
            public volatile boolean onSend = false;
            public volatile boolean onReceive = false;
            public volatile boolean onTimeout = false;

            private MockBackPressureState(InetAddress host)
            {
                this.host = host;
            }

            @Override
            public void onRequestSent(Request<?, ?> request)
            {
                onSend = true;
            }

            @Override
            public void onResponseReceived()
            {
                onReceive = true;
            }

            @Override
            public void onResponseTimeout()
            {
                onTimeout = true;
            }

            @Override
            public double getBackPressureRateLimit()
            {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public InetAddress getHost()
            {
                return host;
            }
        }
    }
}
