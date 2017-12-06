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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.repair.messages.CleanupMessage;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.repair.messages.RepairVerbs;
import org.apache.cassandra.repair.messages.SyncComplete;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.repair.messages.ValidationComplete;
import org.apache.cassandra.repair.messages.ValidationRequest;

import static org.junit.Assert.assertEquals;

public class MessagingVersionTest
{
    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testOSS_40IsSupportedByRepair()
    {
        RepairVerbs repairVerbs = new RepairVerbs(Verbs.Group.REPAIR);
        assertEquals(PrepareMessage.serializers.get(RepairVerbs.RepairVersion.OSS_40), MessagingVersion.OSS_40.serializer(repairVerbs.PREPARE).requestSerializer);
        assertEquals(ValidationRequest.serializers.get(RepairVerbs.RepairVersion.OSS_40), MessagingVersion.OSS_40.serializer(repairVerbs.VALIDATION_REQUEST).requestSerializer);
        assertEquals(ValidationComplete.serializers.get(RepairVerbs.RepairVersion.OSS_40), MessagingVersion.OSS_40.serializer(repairVerbs.VALIDATION_COMPLETE).requestSerializer);
        assertEquals(SyncRequest.serializers.get(RepairVerbs.RepairVersion.OSS_40), MessagingVersion.OSS_40.serializer(repairVerbs.SYNC_REQUEST).requestSerializer);
        assertEquals(SyncComplete.serializers.get(RepairVerbs.RepairVersion.OSS_40), MessagingVersion.OSS_40.serializer(repairVerbs.SYNC_COMPLETE).requestSerializer);
        assertEquals(CleanupMessage.serializers.get(RepairVerbs.RepairVersion.OSS_40), MessagingVersion.OSS_40.serializer(repairVerbs.CLEANUP).requestSerializer);
    }

    @Test
    public void testOSS_30VersionIsNotSupportedByRepair()
    {
        RepairVerbs repairVerbs = new RepairVerbs(Verbs.Group.REPAIR);
        assertThrowsVersionNotSupportedError(() -> MessagingVersion.OSS_30.serializer(repairVerbs.PREPARE));
        assertThrowsVersionNotSupportedError(() -> MessagingVersion.OSS_30.serializer(repairVerbs.VALIDATION_REQUEST));
        assertThrowsVersionNotSupportedError(() -> MessagingVersion.OSS_30.serializer(repairVerbs.VALIDATION_COMPLETE));
        assertThrowsVersionNotSupportedError(() -> MessagingVersion.OSS_30.serializer(repairVerbs.SYNC_REQUEST));
        assertThrowsVersionNotSupportedError(() -> MessagingVersion.OSS_30.serializer(repairVerbs.SYNC_COMPLETE));
        assertThrowsVersionNotSupportedError(() -> MessagingVersion.OSS_30.serializer(repairVerbs.CLEANUP));
    }

    private void assertThrowsVersionNotSupportedError(Runnable r)
    {
        try
        {
            r.run();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Some nodes involved in repair are on an incompatible major version. Repair is not supported in mixed major version clusters.", e.getMessage());
        }
    }

}
