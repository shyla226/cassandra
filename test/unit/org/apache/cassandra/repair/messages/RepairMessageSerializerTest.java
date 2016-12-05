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

package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.net.InetAddress;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.repair.messages.RepairVerbs.RepairVersion;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.versioning.Version;

/**
 * verifies repair message serializers are working as advertised
 */
public class RepairMessageSerializerTest
{
    private static final RepairVersion CURRENT_VERSION = Version.last(RepairVersion.class);

    static RepairMessage serdes(RepairMessage message)
    {
        RepairMessage.MessageSerializer<RepairMessage> serializer = message.serializer(CURRENT_VERSION);
        int expectedSize = Math.toIntExact(serializer.serializedSize(message));
        try (DataOutputBuffer out = new DataOutputBuffer(expectedSize))
        {
            serializer.serialize(message, out);
            Assert.assertEquals(expectedSize, out.buffer().limit());
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                return serializer.deserialize(in);
            }
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    @Test
    public void prepareConsistentRequest() throws Exception
    {
        InetAddress coordinator = InetAddress.getByName("10.0.0.1");
        InetAddress peer1 = InetAddress.getByName("10.0.0.2");
        InetAddress peer2 = InetAddress.getByName("10.0.0.3");
        InetAddress peer3 = InetAddress.getByName("10.0.0.4");
        RepairMessage expected = new PrepareConsistentRequest(UUIDGen.getTimeUUID(),
                                                              coordinator,
                                                              Sets.newHashSet(peer1, peer2, peer3));
        RepairMessage actual = serdes(expected);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void prepareConsistentResponse() throws Exception
    {
        RepairMessage expected = new PrepareConsistentResponse(UUIDGen.getTimeUUID(),
                                                               InetAddress.getByName("10.0.0.2"),
                                                               true);
        RepairMessage actual = serdes(expected);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void failSession() throws Exception
    {
        RepairMessage expected = new FailSession(UUIDGen.getTimeUUID());
        RepairMessage actual = serdes(expected);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void finalizeCommit() throws Exception
    {
        RepairMessage expected = new FinalizeCommit(UUIDGen.getTimeUUID());
        RepairMessage actual = serdes(expected);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void finalizePromise() throws Exception
    {
        RepairMessage expected = new FinalizePromise(UUIDGen.getTimeUUID(),
                                                     InetAddress.getByName("10.0.0.2"),
                                                     true);
        RepairMessage actual = serdes(expected);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void finalizePropose() throws Exception
    {
        RepairMessage expected = new FinalizePropose(UUIDGen.getTimeUUID());
        RepairMessage actual = serdes(expected);
        Assert.assertEquals(expected, actual);
    }
}
