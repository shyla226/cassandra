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

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.BoundsVersion;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.Verb.AckedRequest;
import org.apache.cassandra.net.Verb.OneWay;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.VerbHandlers;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class RepairVerbs extends VerbGroup<RepairVerbs.RepairVersion>
{
    private static final Logger logger = LoggerFactory.getLogger(RepairVerbs.class);

    public enum RepairVersion implements Version<RepairVersion>
    {
        OSS_30(BoundsVersion.OSS_30);

        public final BoundsVersion boundsVersion;

        RepairVersion(BoundsVersion boundsVersion)
        {
            this.boundsVersion = boundsVersion;
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

    public final OneWay<FinalizePropose> FINALIZE_PROPOSE;
    public final OneWay<FinalizePromise> FINALIZE_PROMISE;
    public final OneWay<FinalizeCommit> FINALIZE_COMMIT;
    public final OneWay<FailSession> FAILED_SESSION;

    public final OneWay<StatusRequest> STATUS_REQUEST;
    public final OneWay<StatusResponse> STATUS_RESPONSE;

    public RepairVerbs(Verbs.Group id)
    {
        super(id, true, RepairVersion.class);

        RegistrationHelper helper = helper().stage(Stage.ANTI_ENTROPY);

        VALIDATION_REQUEST = helper.oneWay("VALIDATION_REQUEST", ValidationRequest.class)
                                   .handler(withCleanup(ActiveRepairService.instance::handleValidationRequest));
        VALIDATION_COMPLETE = helper.oneWay("VALIDATION_COMPLETE", ValidationComplete.class)
                                   .handler(withCleanup(ActiveRepairService.instance::handleValidationComplete));
        SYNC_REQUEST = helper.oneWay("SYNC_REQUEST", SyncRequest.class)
                             .handler(withCleanup(ActiveRepairService.instance::handleSyncRequest));
        SYNC_COMPLETE = helper.oneWay("SYNC_COMPLETE", SyncComplete.class)
                              .handler(withCleanup(ActiveRepairService.instance::handleSyncComplete));
        PREPARE = helper.ackedRequest("PREPARE", PrepareMessage.class)
                        .timeout(DatabaseDescriptor::getRpcTimeout)
                        .syncHandler(withCleanupSync(ActiveRepairService.instance::handlePrepare));
        SNAPSHOT = helper.ackedRequest("SNAPSHOT", SnapshotMessage.class)
                         .timeout(1, TimeUnit.HOURS)
                         .syncHandler(withCleanupSync(ActiveRepairService.instance::handleSnapshot));
        CLEANUP = helper.ackedRequest("CLEANUP", CleanupMessage.class)
                        .timeout(1, TimeUnit.HOURS)
                        .syncHandler((from, msg) -> ActiveRepairService.instance.removeParentRepairSession(msg.parentRepairSession));

        CONSISTENT_REQUEST = helper.oneWay("CONSISTENT_REQUEST", PrepareConsistentRequest.class)
                                   .handler(ActiveRepairService.instance.consistent.local::handlePrepareMessage);
        CONSISTENT_RESPONSE = helper.oneWay("CONSISTENT_RESPONSE", PrepareConsistentResponse.class)
                                    .handler(ActiveRepairService.instance.consistent.coordinated::handlePrepareResponse);
        FINALIZE_PROPOSE = helper.oneWay("FINALIZE_PROPOSE", FinalizePropose.class)
                                 .handler(ActiveRepairService.instance.consistent.local::handleFinalizeProposeMessage);
        FINALIZE_PROMISE = helper.oneWay("FINALIZE_PROMISE", FinalizePromise.class)
                                 .handler(ActiveRepairService.instance.consistent.coordinated::handleFinalizePromiseMessage);
        FINALIZE_COMMIT = helper.oneWay("FINALIZE_COMMIT", FinalizeCommit.class)
                                .handler(ActiveRepairService.instance.consistent.local::handleFinalizeCommitMessage);
        FAILED_SESSION = helper.oneWay("FAILED_SESSION", FailSession.class)
                               .handler((from, failure) -> {
                                   ActiveRepairService.instance.consistent.coordinated.handleFailSessionMessage(failure);
                                   ActiveRepairService.instance.consistent.local.handleFailSessionMessage(from, failure);
                               });
        STATUS_REQUEST = helper.oneWay("STATUS_REQUEST", StatusRequest.class)
                               .handler(ActiveRepairService.instance.consistent.local::handleStatusRequest);
        STATUS_RESPONSE = helper.oneWay("STATUS_RESPONSE", StatusResponse.class)
                                .handler(ActiveRepairService.instance.consistent.local::handleStatusResponse);
    }

    private static <P extends RepairMessage> VerbHandlers.OneWay<P> withCleanup(VerbHandlers.OneWay<P> handler)
    {
        return (from, message) ->
        {
            try
            {
                handler.handle(from, message);
            }
            catch (Exception e)
            {
                removeSessionAndRethrow(message, e);
            }
        };
    }

    private static <P extends RepairMessage> VerbHandlers.SyncAckedRequest<P> withCleanupSync(VerbHandlers.SyncAckedRequest<P> handler)
    {
        return (from, message) ->
        {
            try
            {
                handler.handle2(from, message);
            }
            catch (Exception e)
            {
                removeSessionAndRethrow(message, e);
            }
        };
    }

    private static void removeSessionAndRethrow(RepairMessage msg, Exception e)
    {
        logger.error("Got error, removing parent repair session");
        RepairJobDesc desc = msg.desc;
        if (desc != null && desc.parentSessionId != null)
            ActiveRepairService.instance.removeParentRepairSession(desc.parentSessionId);
        throw Throwables.propagate(e);
    }
}
