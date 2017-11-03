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

import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.messages.RepairVerbs.RepairVersion;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.VersionDependent;

/**
 * Base class of all repair related request/response messages.
 *
 * @since 2.0
 */
public abstract class RepairMessage<T extends RepairMessage>
{
    public static final RepairVersion MIN_MESSAGING_VERSION = RepairVersion.OSS_40;

    private static final String MIXED_MODE_ERROR = "Some nodes involved in repair are on an incompatible major version. " +
                                                   "Repair is not supported in mixed major version clusters.";

    public abstract static class MessageSerializer<T extends RepairMessage> extends VersionDependent<RepairVersion> implements Serializer<T>
    {
        protected MessageSerializer(RepairVersion version)
        {
            super(version);
            Preconditions.checkArgument(version.compareTo(MIN_MESSAGING_VERSION) >= 0, MIXED_MODE_ERROR);
        }
    }

    public final RepairJobDesc desc;

    protected RepairMessage(RepairJobDesc desc)
    {
        this.desc = desc;
    }

    /**
     * Validate the message: if true, and no exception is thrown, the message will be handled.
     * @return
     */
    public boolean validate()
    {
        if (desc != null && desc.parentSessionId != null && !ActiveRepairService.instance.hasParentRepairSession(desc.parentSessionId))
            throw new IllegalStateException("No parent repair session: " + desc.parentSessionId);

        return true;
    }

    @VisibleForTesting
    public abstract MessageSerializer<T> serializer(RepairVersion version);
    @VisibleForTesting
    public abstract Optional<Verb<T, ?>> verb();
}
