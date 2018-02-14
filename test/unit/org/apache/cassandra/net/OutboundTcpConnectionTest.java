/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.net;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * The tests check whether Queue expiration in the OutboundTcpConnection behaves properly for droppable and
 * non-droppable messages.
 */
public class OutboundTcpConnectionTest
{
    // A (obviously fake but good enough for this test) Verb that never expires
    private final static Verb<?, ?> NON_DROPPABLE = new Verb.OneWay<>(null, null);

    private final static long TIMEOUT_MS = 10_000L;
    // A Verb with a 10s timeout (we'll fake the current time to expire, so we use a relatively long timeout that guarantee
    // things won't timeout without us wanting to even on slow CI)
    private final static Verb<?, ?> DROPPABLE = new Verb.AckedRequest<>(null, p -> TIMEOUT_MS, null);

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }
    
    /**
     * Make sure our NON_DROPPABLE verb stays that way.
     */
    @BeforeClass
    public static void assertDroppability()
    {
        if (!NON_DROPPABLE.isOneWay())
            throw new AssertionError("Expected " + NON_DROPPABLE + " to be one-way");
    }

    private static long currentTime()
    {
        return ApproximateTime.millisTime();
    }

    private static long expiredTime()
    {
        return currentTime() + 2 * TIMEOUT_MS;
    }

    /**
     * Tests that non-droppable (one-way) messages are never expired
     */
    @Test
    public void testNonDroppable() throws Exception
    {
        OutboundTcpConnection otc = getOutboundTcpConnectionForLocalhost();

        assertFalse("Fresh OutboundTcpConnection contains expired messages",
                    otc.backlogContainsExpiredMessages(expiredTime()));

        fillToPurgeSize(otc, NON_DROPPABLE);
        otc.expireMessages(expiredTime());

        assertFalse("OutboundTcpConnection with non-droppable verbs should not expire",
                    otc.backlogContainsExpiredMessages(expiredTime()));
    }

    /**
     * Tests that droppable messages will be dropped after they expire, but not before.
     * 
     * @throws UnknownHostException
     */
    @Test
    public void testDroppable() throws UnknownHostException
    {
        OutboundTcpConnection otc = getOutboundTcpConnectionForLocalhost();
        
        assertFalse("Fresh OutboundTcpConnection contains expired messages",
                    otc.backlogContainsExpiredMessages(expiredTime()));

        initialFill(otc, DROPPABLE);
        assertFalse("OutboundTcpConnection with droppable verbs should not expire immediately",
                    otc.backlogContainsExpiredMessages(currentTime()));

        assertTrue("OutboundTcpConnection with droppable verbs should expire after the delay",
                    otc.backlogContainsExpiredMessages(expiredTime()));

        otc.expireMessages(currentTime());

        // Lets presume, expiration time have passed => At that time there shall be expired messages in the Queue
        assertTrue("OutboundTcpConnection with droppable verbs should have expired",
                   otc.backlogContainsExpiredMessages(expiredTime()));

        // Using the same timestamp, lets expire them and check whether they have gone
        otc.expireMessages(expiredTime());
        assertFalse("OutboundTcpConnection should have expired entries",
                    otc.backlogContainsExpiredMessages(expiredTime()));

        // Actually the previous test can be done in a harder way: As expireMessages() has run, we cannot have
        // ANY expired values, thus lets test also against currentTime()
        assertFalse("OutboundTcpConnection should not have any expired entries",
                    otc.backlogContainsExpiredMessages(currentTime()));

    }

    private static Message msg(Verb verb)
    {
        return Request.fakeTestRequest(FBUtilities.getBroadcastAddress(), 0, verb, null);
    }

    /**
     * Fills the given OutboundTcpConnection with (1 + BACKLOG_PURGE_SIZE), elements. The first
     * BACKLOG_PURGE_SIZE elements are non-droppable, the last one is a message with the given Verb and can be
     * droppable or non-droppable.
     */
    private void initialFill(OutboundTcpConnection otc, Verb verb)
    {
        assertFalse("Fresh OutboundTcpConnection contains expired messages",
                otc.backlogContainsExpiredMessages(System.nanoTime()));

        fillToPurgeSize(otc, NON_DROPPABLE);
        otc.enqueue(msg(verb));
        otc.expireMessages(currentTime());
    }

    /**
     * Adds BACKLOG_PURGE_SIZE messages to the queue. Hint: At BACKLOG_PURGE_SIZE expiration starts to work.
     * 
     * @param otc
     *            The OutboundTcpConnection
     * @param verb
     *            The verb that defines the message type
     */
    private void fillToPurgeSize(OutboundTcpConnection otc, Verb verb)
    {
        for (int i = 0; i < OutboundTcpConnection.BACKLOG_PURGE_SIZE; i++)
        {
            otc.enqueue(msg(verb));
        }
    }

    private OutboundTcpConnection getOutboundTcpConnectionForLocalhost() throws UnknownHostException
    {
        InetAddress lo = InetAddress.getByName("127.0.0.1");
        OutboundTcpConnectionPool otcPool = new OutboundTcpConnectionPool(lo, lo, null);
        OutboundTcpConnection otc = new OutboundTcpConnection(otcPool, "lo-OutboundTcpConnectionTest");
        return otc;
    }
}
