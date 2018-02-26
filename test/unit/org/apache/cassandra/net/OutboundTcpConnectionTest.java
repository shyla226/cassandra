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
import org.apache.cassandra.utils.WrappedBoolean;
import org.jctools.queues.MessagePassingQueue;

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
        return ApproximateTime.currentTimeMillis();
    }

    private static long expiredTime()
    {
        return currentTime() + 2 * TIMEOUT_MS;
    }

    private static void expireMessages(OutboundTcpConnection otc, long currentTimeMillis)
    {
        MessagePassingQueue<OutboundTcpConnection.QueuedMessage> backlog = otc.backlog();
        backlog.drain(entry -> {
            if (!entry.message.isTimedOut(currentTimeMillis))
                backlog.relaxedOffer(entry);
        }, backlog.size());
    }

    private static boolean backlogContainsExpiredMessages(OutboundTcpConnection otc, long currentTimeMillis)
    {
        MessagePassingQueue<OutboundTcpConnection.QueuedMessage> backlog = otc.backlog();
        WrappedBoolean hasExpired = new WrappedBoolean(false);
        backlog.drain(entry -> {
            if (entry.message.isTimedOut(currentTimeMillis))
                hasExpired.set(true);

            backlog.offer(entry);
        }, backlog.size());

        return hasExpired.get();
    }

    /**
     * Tests that non-droppable (one-way) messages are never expired
     */
    @Test
    public void testNonDroppable() throws Exception
    {
        OutboundTcpConnection otc = getOutboundTcpConnectionForLocalhost();

        assertFalse("Fresh OutboundTcpConnection contains expired messages",
                    backlogContainsExpiredMessages(otc, expiredTime()));

        fill(otc, NON_DROPPABLE);
        expireMessages(otc, expiredTime());

        assertFalse("OutboundTcpConnection with non-droppable verbs should not expire",
                    backlogContainsExpiredMessages(otc, expiredTime()));
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
                    backlogContainsExpiredMessages(otc, expiredTime()));

        initialFill(otc, DROPPABLE);
        assertFalse("OutboundTcpConnection with droppable verbs should not expire immediately",
                    backlogContainsExpiredMessages(otc, currentTime()));

        assertTrue("OutboundTcpConnection with droppable verbs should expire after the delay",
                   backlogContainsExpiredMessages(otc, expiredTime()));

        expireMessages(otc, currentTime());

        // Lets presume, expiration time have passed => At that time there shall be expired messages in the Queue
        assertTrue("OutboundTcpConnection with droppable verbs should have expired",
                   backlogContainsExpiredMessages(otc, expiredTime()));

        // Using the same timestamp, lets expire them and check whether they have gone
        expireMessages(otc, expiredTime());
        assertFalse("OutboundTcpConnection should have expired entries",
                    backlogContainsExpiredMessages(otc, expiredTime()));

        // Actually the previous test can be done in a harder way: As expireMessages() has run, we cannot have
        // ANY expired values, thus lets test also against currentTime()
        assertFalse("OutboundTcpConnection should not have any expired entries",
                    backlogContainsExpiredMessages(otc, currentTime()));

    }

    private static Message msg(Verb verb)
    {
        return Request.fakeTestRequest(FBUtilities.getBroadcastAddress(), 0, verb, null);
    }

    /**
     * Fills the given OutboundTcpConnection with (1 + 1024), elements. The first
     * 1024 elements are non-droppable, the last one is a message with the given Verb and can be
     * droppable or non-droppable.
     */
    private void initialFill(OutboundTcpConnection otc, Verb verb)
    {
        assertFalse("Fresh OutboundTcpConnection contains expired messages",
                    backlogContainsExpiredMessages(otc, System.nanoTime()));

        fill(otc, NON_DROPPABLE);
        otc.enqueue(msg(verb));
        expireMessages(otc, currentTime());
    }

    /**
     * Enqueues 1024 messages to the {@link OutboundTcpConnection}. Hint: 1024 was the value of the
     * old {@code OutboundTcpConnection.BACKLOG_PURGE_SIZE} constant.
     * 
     * @param otc
     *            The OutboundTcpConnection
     * @param verb
     *            The verb that defines the message type
     */
    private void fill(OutboundTcpConnection otc, Verb verb)
    {
        for (int i = 0; i < 1024; i++)
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
