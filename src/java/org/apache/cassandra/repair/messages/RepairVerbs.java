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

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.BoundsVersion;
import org.apache.cassandra.net.DroppedMessages;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.Verb.AckedRequest;
import org.apache.cassandra.net.Verb.OneWay;
import org.apache.cassandra.net.Verb.RequestResponse;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.VerbHandlers;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class RepairVerbs extends VerbGroup<RepairVerbs.RepairVersion>
{
    private static final String MIXED_MODE_ERROR = "Some nodes involved in repair are on an incompatible major version. " +
                                                   "Repair is not supported in mixed major version clusters.";

    /**
     * Amount of time the coordinator will wait for all participants to ack a finalize commit message
     */
    static final int FINALIZE_COMMIT_TIMEOUT = Integer.getInteger(
        "cassandra.finalize_commit_timeout_seconds",
        Ints.checkedCast(TimeUnit.MINUTES.toSeconds(10)));

    public enum RepairVersion implements Version<RepairVersion>
    {
        /**
         * Currently only a single version of repair is supported at a time.
         * When adding support to multiple versions make sure to add the
         * previously supported version to {@link MessagingVersion}
         */
        OSS_40(BoundsVersion.OSS_30, StreamMessage.StreamVersion.OSS_40), // Adds session summary to repair messages
        DSE_60(BoundsVersion.OSS_30, StreamMessage.StreamVersion.DSE_60); // Adds session summary to repair messages

        public final BoundsVersion boundsVersion;
        public final StreamMessage.StreamVersion streamVersion;

        RepairVersion(BoundsVersion boundsVersion, StreamMessage.StreamVersion streamVersion)
        {
            this.boundsVersion = boundsVersion;
            this.streamVersion = streamVersion;
        }

        public static <T> Versioned<RepairVersion, T> versioned(Function<RepairVersion, ? extends T> function)
        {
            return new Versioned<>(RepairVersion.class, function);
        }
    }

    public final OneWay<ValidationRequest> VALIDATION_REQUEST;
    public final OneWay<ValidationComplete> VALIDATION_COMPLETE;
    public final OneWay<SyncRequest> SYNC_REQUEST;
    public final OneWay<SyncComplete> SYNC_COMPLETE;
    public final AckedRequest<PrepareMessage> PREPARE;
    public final AckedRequest<SnapshotMessage> SNAPSHOT;
    public final AckedRequest<CleanupMessage> CLEANUP;

    public final OneWay<PrepareConsistentRequest> CONSISTENT_REQUEST;
    public final OneWay<PrepareConsistentResponse> CONSISTENT_RESPONSE;

    public final AckedRequest<FinalizeCommit> FINALIZE_COMMIT;
    public final OneWay<FailSession> FAILED_SESSION;

    public final RequestResponse<StatusRequest, StatusResponse> STATUS_REQUEST;

    public RepairVerbs(Verbs.Group id)
    {
        super(id, true, RepairVersion.class);

        RegistrationHelper helper = helper().stage(Stage.ANTI_ENTROPY)
                                            .droppedGroup(DroppedMessages.Group.REPAIR);

        VALIDATION_REQUEST = helper.oneWay("VALIDATION_REQUEST", ValidationRequest.class)
                                   .handler(decoratedOneWay(ActiveRepairService.instance::handleValidationRequest));
        VALIDATION_COMPLETE = helper.oneWay("VALIDATION_COMPLETE", ValidationComplete.class)
                                   .handler(decoratedOneWay(ActiveRepairService.instance::handleValidationComplete));
        SYNC_REQUEST = helper.oneWay("SYNC_REQUEST", SyncRequest.class)
                             .handler(decoratedOneWay(ActiveRepairService.instance::handleSyncRequest));
        SYNC_COMPLETE = helper.oneWay("SYNC_COMPLETE", SyncComplete.class)
                              .handler(decoratedOneWay(ActiveRepairService.instance::handleSyncComplete));
        PREPARE = helper.ackedRequest("PREPARE", PrepareMessage.class)
                        .timeout(DatabaseDescriptor::getRpcTimeout)
                        .syncHandler(decoratedAck(ActiveRepairService.instance::handlePrepare));
        SNAPSHOT = helper.ackedRequest("SNAPSHOT", SnapshotMessage.class)
                         .timeout(1, TimeUnit.HOURS)
                         .syncHandler(decoratedAck(ActiveRepairService.instance::handleSnapshot));
        CLEANUP = helper.ackedRequest("CLEANUP", CleanupMessage.class)
                        .timeout(1, TimeUnit.HOURS)
                        .syncHandler((from, msg) -> ActiveRepairService.instance.removeParentRepairSession(msg.parentRepairSession));

        CONSISTENT_REQUEST = helper.oneWay("CONSISTENT_REQUEST", PrepareConsistentRequest.class)
                                   .handler(decoratedOneWay(ActiveRepairService.instance.consistent.local::handlePrepareMessage));
        CONSISTENT_RESPONSE = helper.oneWay("CONSISTENT_RESPONSE", PrepareConsistentResponse.class)
                                    .handler(ActiveRepairService.instance.consistent.coordinated::handlePrepareResponse);
        FINALIZE_COMMIT = helper.ackedRequest("FINALIZE_COMMIT", FinalizeCommit.class)
                                .timeout(FINALIZE_COMMIT_TIMEOUT, TimeUnit.SECONDS)
                                .syncHandler(decoratedAck((from, msg) -> {
                                    ActiveRepairService.instance.consistent.local.handleFinalizeCommitMessage(from, msg);
                                    maybeRemoveSession(from, msg);
                                }));
        FAILED_SESSION = helper.oneWay("FAILED_SESSION", FailSession.class)
                               .handler(decoratedOneWay((from, msg) -> {
                                   ActiveRepairService.instance.consistent.local.handleFailSessionMessage(from, msg);
                                   maybeRemoveSession(from, msg);
                               }));
        STATUS_REQUEST = helper.requestResponse("STATUS_REQUEST", StatusRequest.class, StatusResponse.class)
                               .timeout(DatabaseDescriptor::getRpcTimeout)
                               .syncHandler(ActiveRepairService.instance.consistent.local::handleStatusRequest);
    }

    public String getUnsupportedVersionMessage(MessagingVersion version)
    {
        return MIXED_MODE_ERROR;
    }

    private static <P extends RepairMessage> VerbHandlers.OneWay<P> decoratedOneWay(VerbHandlers.OneWay<P> handler)
    {
        return (from, message) ->
        {
            try
            {
                if (message.validate())
                    handler.handle(from, message);
            }
            catch (Exception e)
            {
                removeSessionAndRethrow(message, e);
            }
        };
    }

    private static <P extends RepairMessage> VerbHandlers.SyncAckedRequest<P> decoratedAck(VerbHandlers.SyncAckedRequest<P> handler)
    {
        return (from, message) ->
        {
            try
            {
                if (message.validate())
                    handler.handle2(from, message);
            }
            catch (Exception e)
            {
                removeSessionAndRethrow(message, e);
            }
        };
    }

    private static void maybeRemoveSession(InetAddress from, ConsistentRepairMessage msg)
    {
        if (!from.equals(FBUtilities.getBroadcastAddress()))
            ActiveRepairService.instance.removeParentRepairSession(msg.sessionID);
    }

    private static void removeSessionAndRethrow(RepairMessage msg, Exception e)
    {
        RepairJobDesc desc = msg.desc;
        if (desc != null && desc.parentSessionId != null)
            ActiveRepairService.instance.removeParentRepairSession(desc.parentSessionId);
        throw Throwables.propagate(e);
    }
}
