/*
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
 */
package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.Util;
import org.apache.cassandra.Util.PartitionerSwitcher;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.NodePair;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.repair.messages.RepairVerbs.RepairVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.Serializer;

public class SerializationsTest extends AbstractSerializationsTester
{
    private static final InetAddress inet;
    static
    {
        try
        {
            inet = InetAddress.getByName("127.0.0.1");
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError();
        }
    }

    private static PartitionerSwitcher partitionerSwitcher;
    private static UUID RANDOM_UUID;
    private static Range<Token> FULL_RANGE;
    private static RepairJobDesc DESC;

    private static Message.Serializer serializer;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        partitionerSwitcher = Util.switchPartitioner(RandomPartitioner.instance);
        RANDOM_UUID = UUID.fromString("b5c3d033-75aa-4c2f-a819-947aac7a0c54");
        FULL_RANGE = new Range<>(Util.testPartitioner().getMinimumToken(), Util.testPartitioner().getMinimumToken());
        DESC = new RepairJobDesc(RANDOM_UUID, RANDOM_UUID, "Keyspace1", "Standard1", Arrays.asList(FULL_RANGE));
        serializer = Message.createSerializer(getVersion(), 0);
    }

    @AfterClass
    public static void tearDown()
    {
        partitionerSwitcher.close();
    }

    public RepairVersion getRepairVersion()
    {
        return getVersion().groupVersion(Verbs.Group.REPAIR);
    }

    @SuppressWarnings("unchecked")
    private void testRepairMessageWrite(String fileName, RepairMessage... messages) throws IOException
    {
        try (DataOutputStreamPlus out = getOutput(fileName))
        {
            for (RepairMessage message : messages)
            {
                Serializer<RepairMessage> serializer = message.serializer(getRepairVersion());
                testSerializedSize(message, serializer);
                serializer.serialize(message, out);
            }
            // also serialize MessageOut
            for (RepairMessage message : messages)
                serializer.serialize(message.verb().newRequest(inet, message), out);
        }
    }

    private void testValidationRequestWrite() throws IOException
    {
        ValidationRequest message = new ValidationRequest(DESC, 1234);
        testRepairMessageWrite("service.ValidationRequest.bin", message);
    }

    @Test
    public void testValidationRequestRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testValidationRequestWrite();

        try (DataInputStreamPlus in = getInput("service.ValidationRequest.bin"))
        {
            ValidationRequest message = ValidationRequest.serializers.get(getRepairVersion()).deserialize(in);
            assert DESC.equals(message.desc);
            assert message.gcBefore == 1234;

            assert serializer.deserialize(in, inet) != null;
        }
    }

    private void testValidationCompleteWrite() throws IOException
    {
        IPartitioner p = RandomPartitioner.instance;

        MerkleTrees mt = new MerkleTrees(p);

        // empty validation
        mt.addMerkleTree((int) Math.pow(2, 15), FULL_RANGE);
        Validator v0 = new Validator(DESC, FBUtilities.getBroadcastAddress(),  -1);
        ValidationComplete c0 = new ValidationComplete(DESC, mt);

        // validation with a tree
        mt = new MerkleTrees(p);
        mt.addMerkleTree(Integer.MAX_VALUE, FULL_RANGE);
        for (int i = 0; i < 10; i++)
            mt.split(p.getRandomToken());
        Validator v1 = new Validator(DESC, FBUtilities.getBroadcastAddress(), -1);
        ValidationComplete c1 = new ValidationComplete(DESC, mt);

        // validation failed
        ValidationComplete c3 = new ValidationComplete(DESC);

        testRepairMessageWrite("service.ValidationComplete.bin", c0, c1, c3);
    }

    @Test
    public void testValidationCompleteRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testValidationCompleteWrite();

        try (DataInputStreamPlus in = getInput("service.ValidationComplete.bin"))
        {
            // empty validation
            ValidationComplete message = ValidationComplete.serializers.get(getRepairVersion()).deserialize(in);
            assert DESC.equals(message.desc);

            assert message.success();
            assert message.trees != null;

            // validation with a tree
            message = ValidationComplete.serializers.get(getRepairVersion()).deserialize(in);
            assert DESC.equals(message.desc);

            assert message.success();
            assert message.trees != null;

            // failed validation
            message = ValidationComplete.serializers.get(getRepairVersion()).deserialize(in);
            assert DESC.equals(message.desc);

            assert !message.success();
            assert message.trees == null;

            // MessageOuts
            for (int i = 0; i < 3; i++)
                assert serializer.deserialize(in, inet) != null;
        }
    }

    private void testSyncRequestWrite() throws IOException
    {
        InetAddress local = InetAddress.getByAddress(new byte[]{127, 0, 0, 1});
        InetAddress src = InetAddress.getByAddress(new byte[]{127, 0, 0, 2});
        InetAddress dest = InetAddress.getByAddress(new byte[]{127, 0, 0, 3});
        SyncRequest message = new SyncRequest(DESC, local, src, dest, Collections.singleton(FULL_RANGE));

        testRepairMessageWrite("service.SyncRequest.bin", message);
    }

    @Test
    public void testSyncRequestRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testSyncRequestWrite();

        InetAddress local = InetAddress.getByAddress(new byte[]{127, 0, 0, 1});
        InetAddress src = InetAddress.getByAddress(new byte[]{127, 0, 0, 2});
        InetAddress dest = InetAddress.getByAddress(new byte[]{127, 0, 0, 3});

        try (DataInputStreamPlus in = getInput("service.SyncRequest.bin"))
        {
            SyncRequest message = SyncRequest.serializers.get(getRepairVersion()).deserialize(in);
            assert DESC.equals(message.desc);
            assert local.equals(message.initiator);
            assert src.equals(message.src);
            assert dest.equals(message.dst);
            assert message.ranges.size() == 1 && message.ranges.contains(FULL_RANGE);

            assert serializer.deserialize(in, inet) != null;
        }
    }

    private void testSyncCompleteWrite() throws IOException
    {
        InetAddress src = InetAddress.getByAddress(new byte[]{127, 0, 0, 2});
        InetAddress dest = InetAddress.getByAddress(new byte[]{127, 0, 0, 3});
        // sync success
        SyncComplete success = new SyncComplete(DESC, src, dest, true);
        // sync fail
        SyncComplete fail = new SyncComplete(DESC, src, dest, false);

        testRepairMessageWrite("service.SyncComplete.bin", success, fail);
    }

    @Test
    public void testSyncCompleteRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testSyncCompleteWrite();

        InetAddress src = InetAddress.getByAddress(new byte[]{127, 0, 0, 2});
        InetAddress dest = InetAddress.getByAddress(new byte[]{127, 0, 0, 3});
        NodePair nodes = new NodePair(src, dest);

        try (DataInputStreamPlus in = getInput("service.SyncComplete.bin"))
        {
            // success
            SyncComplete message = SyncComplete.serializers.get(getRepairVersion()).deserialize(in);
            assert DESC.equals(message.desc);

            assert nodes.equals(message.nodes);
            assert message.success;

            // fail
            message = SyncComplete.serializers.get(getRepairVersion()).deserialize(in);
            assert DESC.equals(message.desc);

            assert nodes.equals(message.nodes);
            assert !message.success;

            // MessageOuts
            for (int i = 0; i < 2; i++)
                assert serializer.deserialize(in, inet) != null;
        }
    }
}
