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
package org.apache.cassandra.hints;

import java.net.InetAddress;
import java.util.UUID;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.net.DroppedMessages;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.Verb.AckedRequest;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.VerbHandlers;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class HintsVerbs extends VerbGroup<HintsVerbs.HintsVersion>
{
    private static final Logger logger = LoggerFactory.getLogger(HintsVerbs.class);

    public enum HintsVersion implements Version<HintsVersion>
    {
        OSS_30(EncodingVersion.OSS_30);

        public final EncodingVersion encodingVersion;

        HintsVersion(EncodingVersion encodingVersion)
        {
            this.encodingVersion = encodingVersion;
        }

        public int code()
        {
            // Adding +1 for backward compatibility.
            return ordinal() + 1;
        }

        public static HintsVersion fromCode(int code)
        {
            return values()[code - 1];
        }

        public static <T> Versioned<HintsVersion, T> versioned(Function<HintsVersion, ? extends T> function)
        {
            return new Versioned<>(HintsVersion.class, function);
        }
    }

    public final AckedRequest<HintMessage> HINT;

    private static final VerbHandlers.AckedRequest<HintMessage> HINT_HANDLER = (from, msg) ->
    {
        UUID hostId = msg.hostId;
        Hint hint;
        InetAddress address = StorageService.instance.getEndpointForHostId(hostId);
        try
        {
            hint = msg.hint();
        }
        catch (UnknownTableException e)
        {
            // If we see an unknown table id, it means the table, or one of the tables in the mutation, had been dropped.
            // In that case there is nothing we can really do, or should do, other than log it and go on.
            // This will *not* happen due to a not-yet-seen table, because we don't transfer hints unless there
            // is schema agreement between the sender and the receiver.
            logger.trace("Failed to decode and apply a hint for {} ({}) - table with id {} is unknown", address, hostId, e.id);
            return null;
        }

        // We must perform validation before applying the hint, and there is no other place to do it other than here.
        try
        {
            hint.mutation.getPartitionUpdates().forEach(PartitionUpdate::validate);
        }
        catch (MarshalException e)
        {
            logger.warn("Failed to validate a hint for {} ({}) - skipped", address, hostId);
            return null;
        }

        if (!hostId.equals(StorageService.instance.getLocalHostUUID()))
        {
            // the node is not the final destination of the hint (must have gotten it from a decommissioning node),
            // so just store it locally, to be delivered later.
            HintsService.instance.write(hostId, hint);
            return null;
        }
        else if (!StorageProxy.instance.appliesLocally(hint.mutation))
        {
            // the topology has changed, and we are no longer a replica of the mutation - since we don't know which node(s)
            // it has been handed over to, re-address the hint to all replicas; see CASSANDRA-5902.
            HintsService.instance.writeForAllReplicas(hint);
            return null;
        }
        else
        {
            // the common path - the node is both the destination and a valid replica for the hint.
            return hint.applyFuture();
        }
    };

    public HintsVerbs(Verbs.Group id)
    {
        super(id, true, HintsVersion.class);

        RegistrationHelper helper = helper().droppedGroup(DroppedMessages.Group.HINT);

        HINT = helper.ackedRequest("HINT", HintMessage.class)
                     .requestStage(Stage.HINTS)
                     .timeout(DatabaseDescriptor::getWriteRpcTimeout)
                     .withBackPressure()
                     .handler(HINT_HANDLER);
    }
}
