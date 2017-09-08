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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.*;

public class DroppedMessagesTest
{
    private static final InetAddress peer1 = peer(1);
    private static final InetAddress peer2 = peer(2);

    private static int defaultMetricsHistogramUpdateInterval;

    private static InetAddress peer(int id)
    {
        try
        {
            return InetAddress.getByAddress(new byte[]{ 127, 0, 0, (byte) id });
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void beforeClass() throws UnknownHostException
    {
        // It appears anything that load metrics currently needs daemon initialization. Only reason for this (and a sad
        // one).
        DatabaseDescriptor.daemonInitialization();

        defaultMetricsHistogramUpdateInterval = DatabaseDescriptor.getMetricsHistogramUpdateTimeMillis();
        DatabaseDescriptor.setMetricsHistogramUpdateTimeMillis(0); // this guarantees metrics histograms are updated on read
    }

    @AfterClass
    public static void afterClass()
    {
        DatabaseDescriptor.setMetricsHistogramUpdateTimeMillis(defaultMetricsHistogramUpdateInterval);
    }

    @Test
    public void testDroppedMessages()
    {
        DroppedMessages dms = new DroppedMessages();

        for (int i = 1; i <= 5000; i++)
            dms.onDroppedMessage(fakeMessage(i, i % 2 == 0));

        List<String> logs = dms.getDroppedMessagesLogs();
        assertEquals(1, logs.size());
        Pattern regexp = Pattern.compile("READ messages were dropped in last 5000 ms: (\\d+) internal and (\\d+) cross node. Mean internal dropped latency: (\\d+) ms and Mean cross-node dropped latency: (\\d+) ms");
        Matcher matcher = regexp.matcher(logs.get(0));
        assertTrue(matcher.find());
        assertEquals(2500, Integer.parseInt(matcher.group(1)));
        assertEquals(2500, Integer.parseInt(matcher.group(2)));
        assertTrue(Integer.parseInt(matcher.group(3)) > 0);
        assertTrue(Integer.parseInt(matcher.group(4)) > 0);
        assertEquals(5000, (int) dms.getSnapshot().get(Verbs.READS.READ.droppedGroup().toString()));

        logs = dms.getDroppedMessagesLogs();
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            dms.onDroppedMessage(fakeMessage(i, i % 2 == 0));

        logs = dms.getDroppedMessagesLogs();
        assertEquals(1, logs.size());
        matcher = regexp.matcher(logs.get(0));
        assertTrue(matcher.find());
        assertEquals(1250, Integer.parseInt(matcher.group(1)));
        assertEquals(1250, Integer.parseInt(matcher.group(2)));
        assertTrue(Integer.parseInt(matcher.group(3)) > 0);
        assertTrue(Integer.parseInt(matcher.group(4)) > 0);
        assertEquals(7500, (int) dms.getSnapshot().get(Verbs.READS.READ.droppedGroup().toString()));
    }

    private Message<?> fakeMessage(long lifeTime, boolean isCrossNode)
    {
        return Request.fakeTestRequest(peer1,
                                       isCrossNode ? peer2 : peer1,
                                       0,
                                       Verbs.READS.READ,
                                       null,
                                       System.currentTimeMillis() - lifeTime);
    }
}
