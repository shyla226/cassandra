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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.metrics.Timer;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.cassandra.db.monitoring.ApproximateTime;

import org.apache.cassandra.exceptions.ConfigurationException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class MessagingServiceTest
{
    private final static long ONE_SECOND = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);
    private final static long TEN_SECONDS = TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
    public static final IInternodeAuthenticator ALLOW_NOTHING_AUTHENTICATOR = new IInternodeAuthenticator()
    {
        public boolean authenticate(InetAddress remoteAddress, int remotePort)
        {
            return false;
        }

        public void validateConfiguration() throws ConfigurationException
        {

        }
    };
    static final IInternodeAuthenticator originalAuthenticator = DatabaseDescriptor.getInternodeAuthenticator();

    private final MessagingService messagingService = MessagingService.test();
    private static int defaultMetricsHistogramUpdateInterval;

    private static Verb<?, ?> withBackPressure;
    private static Verb<?, ?> withoutBackPressure;

    @BeforeClass
    public static void beforeClass() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();

        DatabaseDescriptor.setBackPressureStrategy(new MockBackPressureStrategy(Collections.emptyMap()));
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.0.0.1"));

        defaultMetricsHistogramUpdateInterval = DatabaseDescriptor.getMetricsHistogramUpdateTimeMillis();
        DatabaseDescriptor.setMetricsHistogramUpdateTimeMillis(0); // this guarantees metrics histograms are updated on read

        withBackPressure = Verbs.WRITES.WRITE;
        withoutBackPressure = Verbs.GOSSIP.ACK; // one-way message, so can't suppor back-pressure;

        SystemKeyspace.finishStartupBlocking();
    }

    @AfterClass
    public static void afterClass()
    {
        DatabaseDescriptor.setMetricsHistogramUpdateTimeMillis(defaultMetricsHistogramUpdateInterval);
    }

    @Before
    public void before() throws UnknownHostException
    {
        MockBackPressureStrategy.applied = false;
        MockBackPressureStrategy.sleep = 0;
        messagingService.destroyConnectionPool(InetAddress.getByName("127.0.0.2"));
        messagingService.destroyConnectionPool(InetAddress.getByName("127.0.0.3"));
    }

    @Test
    public void testDCLatency() throws Exception
    {
        int latency = 100;
        ConcurrentHashMap<String, Timer> dcLatency = MessagingService.instance().metrics.dcLatency;
        dcLatency.clear();

        long now = ApproximateTime.millisTime();
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
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getConnectionPool(InetAddress.getByName("127.0.0.2")).join().getBackPressureState();

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnSend(Request.fakeTestRequest(InetAddress.getByName("127.0.0.2"), -1, withoutBackPressure, null)).join();
        assertFalse(backPressureState.onSend);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnSend(Request.fakeTestRequest(InetAddress.getByName("127.0.0.2"), -1, withBackPressure, null)).join();
        assertFalse(backPressureState.onSend);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnSend(Request.fakeTestRequest(InetAddress.getByName("127.0.0.2"), -1, withBackPressure, null)).join();
        assertTrue(backPressureState.onSend);
    }

    @Test
    public void testUpdatesBackPressureOnReceiveWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getConnectionPool(InetAddress.getByName("127.0.0.2")).join().getBackPressureState();
        boolean timeout = false;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), withoutBackPressure, timeout).join();
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), withBackPressure, timeout).join();
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), withBackPressure, timeout).join();
        assertTrue(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);
    }

    @Test
    public void testUpdatesBackPressureOnTimeoutWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getConnectionPool(InetAddress.getByName("127.0.0.2")).join().getBackPressureState();
        boolean timeout = true;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), withoutBackPressure, timeout).join();
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), withBackPressure, timeout).join();
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), withBackPressure, timeout).join();
        assertFalse(backPressureState.onReceive);
        assertTrue(backPressureState.onTimeout);
    }

    @Test
    public void testAppliesBackPressureWhenEnabled() throws UnknownHostException
    {
        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.2")), ONE_SECOND).join();
        assertFalse(MockBackPressureStrategy.applied);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.2")), ONE_SECOND).join();
        assertTrue(MockBackPressureStrategy.applied);
    }

    @Test
    public void testDoesntApplyBackPressureToBroadcastAddress() throws UnknownHostException
    {
        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.1")), ONE_SECOND).join();
        assertFalse(MockBackPressureStrategy.applied);
    }

    @Test
    public void testBackPressureIsWaitedFor() throws UnknownHostException
    {
        MockBackPressureStrategy.sleep = TimeUnit.SECONDS.convert(TEN_SECONDS, TimeUnit.NANOSECONDS);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.2")), TEN_SECONDS).join();
        assertTrue(MockBackPressureStrategy.applied);
    }

    private static void addDCLatency(long sentAt, long nowTime) throws IOException
    {
        MessagingService.instance().metrics.addTimeTaken(InetAddress.getLocalHost(), nowTime - sentAt);
    }

    public static class MockBackPressureStrategy implements BackPressureStrategy<MockBackPressureStrategy.MockBackPressureState>
    {
        public static volatile boolean applied = false;
        public static volatile long sleep = 0;

        public MockBackPressureStrategy(Map<String, Object> args)
        {
        }

        @Override
        public CompletableFuture<Void> apply(Set<MockBackPressureState> states, long timeout, TimeUnit unit)
        {
            return CompletableFuture.supplyAsync(() ->
            {
                Uninterruptibles.sleepUninterruptibly(sleep, TimeUnit.SECONDS);

                if (!Iterables.isEmpty(states))
                    applied = true;
            
                return null;
            });
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
