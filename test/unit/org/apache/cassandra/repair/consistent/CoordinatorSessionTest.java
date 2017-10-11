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

package org.apache.cassandra.repair.consistent;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.repair.AbstractRepairTest;
import org.apache.cassandra.net.OneWayRequest;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.repair.RepairSessionResult;
import org.apache.cassandra.repair.messages.FailSession;
import org.apache.cassandra.repair.messages.FinalizeCommit;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.repair.consistent.ConsistentSession.State.*;

public class CoordinatorSessionTest extends AbstractRepairTest
{

    static CoordinatorSession.Builder createBuilder()
    {
        CoordinatorSession.Builder builder = CoordinatorSession.builder();
        builder.withState(PREPARING);
        builder.withSessionID(UUIDGen.getTimeUUID());
        builder.withCoordinator(COORDINATOR);
        builder.withUUIDTableIds(Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()));
        builder.withRepairedAt(System.currentTimeMillis());
        builder.withRanges(Sets.newHashSet(RANGE1, RANGE2, RANGE3));
        builder.withParticipants(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3));
        return builder;
    }

    static CoordinatorSession createSession()
    {
        return createBuilder().build();
    }

    static InstrumentedCoordinatorSession createInstrumentedSession()
    {
        return new InstrumentedCoordinatorSession(createBuilder());
    }

    private static RepairSessionResult createResult(CoordinatorSession coordinator)
    {
        return new RepairSessionResult(coordinator.sessionID, "ks", null);
    }

    private static void assertMessageSent(InstrumentedCoordinatorSession coordinator, InetAddress participant, RepairMessage expected)
    {
        Assert.assertTrue(coordinator.sentMessages.containsKey(participant));
        Assert.assertEquals(1, coordinator.sentMessages.get(participant).size());
        Assert.assertEquals(expected, coordinator.sentMessages.get(participant).get(0));
    }

    private static void assertMessagesSent(InstrumentedCoordinatorSession coordinator, InetAddress participant, List<RepairMessage> expected)
    {
        Assert.assertTrue(coordinator.sentMessages.containsKey(participant));
        Assert.assertEquals(expected.size(), coordinator.sentMessages.get(participant).size());
        for (int i = 0; i < expected.size(); i++)
        {
            Assert.assertEquals(expected.get(i), coordinator.sentMessages.get(participant).get(i));
        }
    }

    private static class InstrumentedCoordinatorSession extends CoordinatorSession
    {
        public InstrumentedCoordinatorSession(Builder builder)
        {
            super(builder);
        }

        Map<InetAddress, List<RepairMessage>> sentMessages = new HashMap<>();
        Set<InetAddress> failingParticipants = ImmutableSet.of();
        protected void send(OneWayRequest<? extends RepairMessage<?>> request)
        {
            registerMessage(request.to(), request.payload());
        }
        protected void send(Request<? extends RepairMessage<?>, EmptyPayload> request, MessageCallback<EmptyPayload> callback)
        {
            registerMessage(request.to(), request.payload());
            if (failingParticipants.contains(request.to()))
                callback.onFailure(request.respondWithFailure(RequestFailureReason.UNKNOWN));
            else
                callback.onResponse(Response.testResponse(request.to(), request.from(), request.verb(), EmptyPayload.instance));
        }
        private void registerMessage(InetAddress to, RepairMessage payload)
        {
            if (!sentMessages.containsKey(to))
            {
                sentMessages.put(to, new ArrayList<>());
            }
            sentMessages.get(to).add(payload);
        }

        Runnable onSetRepairing = null;
        boolean setRepairingCalled = false;
        public synchronized void setRepairing()
        {
            setRepairingCalled = true;
            if (onSetRepairing != null)
            {
                onSetRepairing.run();
            }
            super.setRepairing();
        }

        Runnable onFinalizeCommit = null;
        boolean finalizeCommitCalled = false;
        public synchronized ListenableFuture<Boolean> finalizeCommit()
        {
            finalizeCommitCalled = true;
            if (onFinalizeCommit != null)
            {
                onFinalizeCommit.run();
            }
            return super.finalizeCommit();
        }

        Runnable onFail = null;
        boolean failCalled = false;
        public synchronized void fail()
        {
            failCalled = true;
            if (onFail != null)
            {
                onFail.run();
            }
            super.fail();
        }

        boolean successCalled = false;
        public void success()
        {
            successCalled = true;
            super.success();
        }
    }

    /**
     * Coordinator state should only switch after all participants are set
     */
    @Test
    public void setPeerState()
    {
        CoordinatorSession session = createSession();
        Assert.assertEquals(PREPARING, session.getState());

        session.setParticipantState(PARTICIPANT1, PREPARED);
        Assert.assertEquals(PREPARING, session.getState());

        session.setParticipantState(PARTICIPANT2, PREPARED);
        Assert.assertEquals(PREPARING, session.getState());

        session.setParticipantState(PARTICIPANT3, PREPARED);
        Assert.assertEquals(PREPARED, session.getState());
    }

    @Test
    public void hasFailed()
    {
        CoordinatorSession session;

        // participant failure
        session = createSession();
        Assert.assertFalse(session.hasFailed());
        session.setParticipantState(PARTICIPANT1, FAILED);
        Assert.assertTrue(session.hasFailed());

        // coordinator failure
        session = createSession();
        Assert.assertFalse(session.hasFailed());
        session.setState(FAILED);
        Assert.assertTrue(session.hasFailed());
    }

    /**
     * Coordinator should only send out failures messages once
     */
    @Test
    public void multipleFailures()
    {
        InstrumentedCoordinatorSession coordinator = createInstrumentedSession();

        Assert.assertEquals(PREPARING, coordinator.getState());
        Assert.assertTrue(coordinator.sentMessages.isEmpty());

        coordinator.fail();
        Assert.assertEquals(FAILED, coordinator.getState());
        for (InetAddress participant : PARTICIPANTS)
        {
            assertMessageSent(coordinator, participant, new FailSession(coordinator.sessionID));
        }

        coordinator.sentMessages.clear();
        coordinator.fail();
        Assert.assertEquals(FAILED, coordinator.getState());
        Assert.assertTrue(coordinator.sentMessages.isEmpty());
    }

    /**
     * Tests the complete coordinator side consistent repair cycle
     */
    @Test
    public void successCase()
    {
        InstrumentedCoordinatorSession coordinator = createInstrumentedSession();
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        SettableFuture<List<RepairSessionResult>> repairFuture = SettableFuture.create();
        Supplier<ListenableFuture<List<RepairSessionResult>>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        AtomicBoolean hasFailures = new AtomicBoolean(false);
        Assert.assertFalse(repairSubmitted.get());
        Assert.assertTrue(coordinator.sentMessages.isEmpty());
        ListenableFuture sessionResult = coordinator.execute(sessionSupplier, hasFailures);

        for (InetAddress participant : PARTICIPANTS)
        {
            RepairMessage expected = new PrepareConsistentRequest(coordinator.sessionID, COORDINATOR, new HashSet<>(PARTICIPANTS));
            assertMessageSent(coordinator, participant, expected);
        }

        // participants respond to coordinator, and repair begins once all participants have responded with success
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        coordinator.handlePrepareResponse(PARTICIPANT1, true);
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        coordinator.handlePrepareResponse(PARTICIPANT2, true);
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        // set the setRepairing callback to verify the correct state when it's called
        Assert.assertFalse(coordinator.setRepairingCalled);
        coordinator.onSetRepairing = () -> Assert.assertEquals(PREPARED, coordinator.getState());
        coordinator.handlePrepareResponse(PARTICIPANT3, true);
        Assert.assertTrue(coordinator.setRepairingCalled);
        Assert.assertTrue(repairSubmitted.get());

        Assert.assertEquals(ConsistentSession.State.REPAIRING, coordinator.getState());

        ArrayList<RepairSessionResult> results = Lists.newArrayList(createResult(coordinator),
                                                                    createResult(coordinator),
                                                                    createResult(coordinator));

        coordinator.sentMessages.clear();

        // set the finalizeCommit callback so we can verify the state when it's called
        Assert.assertFalse(coordinator.finalizeCommitCalled);
        coordinator.onFinalizeCommit = () -> Assert.assertEquals(REPAIRING, coordinator.getState());

        // set the repair future to unblock the commit
        repairFuture.set(results);
        Assert.assertTrue(coordinator.finalizeCommitCalled);
        Assert.assertTrue(coordinator.successCalled);
        Assert.assertEquals(ConsistentSession.State.FINALIZED, coordinator.getState());
        for (InetAddress participant : PARTICIPANTS)
        {
            RepairMessage expected = new FinalizeCommit(coordinator.sessionID);
            assertMessageSent(coordinator, participant, expected);
        }

        Assert.assertTrue(sessionResult.isDone());
        Assert.assertFalse(hasFailures.get());
    }

    @Test
    public void failedRepairs()
    {
        InstrumentedCoordinatorSession coordinator = createInstrumentedSession();
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        SettableFuture<List<RepairSessionResult>> repairFuture = SettableFuture.create();
        Supplier<ListenableFuture<List<RepairSessionResult>>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        AtomicBoolean hasFailures = new AtomicBoolean(false);
        Assert.assertFalse(repairSubmitted.get());
        Assert.assertTrue(coordinator.sentMessages.isEmpty());
        ListenableFuture sessionResult = coordinator.execute(sessionSupplier, hasFailures);
        for (InetAddress participant : PARTICIPANTS)
        {
            PrepareConsistentRequest expected = new PrepareConsistentRequest(coordinator.sessionID, COORDINATOR, new HashSet<>(PARTICIPANTS));
            assertMessageSent(coordinator, participant, expected);
        }

        // participants respond to coordinator, and repair begins once all participants have responded with success
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        coordinator.handlePrepareResponse(PARTICIPANT1, true);
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        coordinator.handlePrepareResponse(PARTICIPANT2, true);
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        // set the setRepairing callback to verify the correct state when it's called
        Assert.assertFalse(coordinator.setRepairingCalled);
        coordinator.onSetRepairing = () -> Assert.assertEquals(PREPARED, coordinator.getState());
        coordinator.handlePrepareResponse(PARTICIPANT3, true);
        Assert.assertTrue(coordinator.setRepairingCalled);
        Assert.assertTrue(repairSubmitted.get());
        Assert.assertEquals(ConsistentSession.State.REPAIRING, coordinator.getState());

        // set a null result to simulate a failure
        ArrayList<RepairSessionResult> results = Lists.newArrayList(createResult(coordinator),
                                                                    null,
                                                                    createResult(coordinator));

        coordinator.sentMessages.clear();
        Assert.assertFalse(coordinator.failCalled);
        coordinator.onFail = () -> Assert.assertEquals(REPAIRING, coordinator.getState());
        repairFuture.set(results);
        Assert.assertTrue(coordinator.failCalled);

        // all participants should have been notified of session failure
        for (InetAddress participant : PARTICIPANTS)
        {
            RepairMessage expected = new FailSession(coordinator.sessionID);
            assertMessageSent(coordinator, participant, expected);
        }

        Assert.assertTrue(sessionResult.isDone());
        Assert.assertTrue(hasFailures.get());
    }

    @Test
    public void failedPrepare()
    {
        InstrumentedCoordinatorSession coordinator = createInstrumentedSession();
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        SettableFuture<List<RepairSessionResult>> repairFuture = SettableFuture.create();
        Supplier<ListenableFuture<List<RepairSessionResult>>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        AtomicBoolean hasFailures = new AtomicBoolean(false);
        Assert.assertFalse(repairSubmitted.get());
        Assert.assertTrue(coordinator.sentMessages.isEmpty());
        ListenableFuture sessionResult = coordinator.execute(sessionSupplier, hasFailures);
        for (InetAddress participant : PARTICIPANTS)
        {
            PrepareConsistentRequest expected = new PrepareConsistentRequest(coordinator.sessionID, COORDINATOR, new HashSet<>(PARTICIPANTS));
            assertMessageSent(coordinator, participant, expected);
        }

        coordinator.sentMessages.clear();

        // participants respond to coordinator, and repair begins once all participants have responded with success
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        coordinator.handlePrepareResponse(PARTICIPANT1, true);
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        // participant 2 fails to prepare for consistent repair
        Assert.assertFalse(coordinator.failCalled);
        coordinator.handlePrepareResponse(PARTICIPANT2, false);
        Assert.assertEquals(ConsistentSession.State.FAILED, coordinator.getState());
        Assert.assertTrue(coordinator.failCalled);

        // additional success messages should be ignored
        Assert.assertFalse(coordinator.setRepairingCalled);
        coordinator.onSetRepairing = Assert::fail;
        coordinator.handlePrepareResponse(PARTICIPANT3, true);
        Assert.assertFalse(coordinator.setRepairingCalled);
        Assert.assertFalse(repairSubmitted.get());

        // all participants should have been notified of session failure
        for (InetAddress participant : PARTICIPANTS)
        {
            RepairMessage expected = new FailSession(coordinator.sessionID);
            assertMessageSent(coordinator, participant, expected);
        }

        Assert.assertTrue(sessionResult.isDone());
        Assert.assertTrue(hasFailures.get());
    }

    @Test
    public void failedPrepareDueToDeadNode()
    {
        InstrumentedCoordinatorSession coordinator = createInstrumentedSession();
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        SettableFuture<List<RepairSessionResult>> repairFuture = SettableFuture.create();
        Supplier<ListenableFuture<List<RepairSessionResult>>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        AtomicBoolean hasFailures = new AtomicBoolean(false);
        Assert.assertFalse(repairSubmitted.get());
        Assert.assertTrue(coordinator.sentMessages.isEmpty());
        ListenableFuture sessionResult = coordinator.execute(sessionSupplier, hasFailures);
        for (InetAddress participant : PARTICIPANTS)
        {
            PrepareConsistentRequest expected = new PrepareConsistentRequest(coordinator.sessionID, COORDINATOR, new HashSet<>(PARTICIPANTS));
            assertMessageSent(coordinator, participant, expected);
        }

        coordinator.sentMessages.clear();

        // participants respond to coordinator, and repair begins once all participants have responded with success
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        // participant 1 succeeds
        coordinator.handlePrepareResponse(PARTICIPANT1, true);
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        // participant 2 dies
        coordinator.convict(PARTICIPANT2, Double.MAX_VALUE);

        // verify the whole session is failed
        Assert.assertTrue(sessionResult.isDone());
        Assert.assertTrue(hasFailures.get());

        // verify the coordinator is failed, the fail method has been called and fail messages have been sent
        Assert.assertEquals(ConsistentSession.State.FAILED, coordinator.getState());
        Assert.assertTrue(coordinator.failCalled);
        for (InetAddress participant : PARTICIPANTS)
        {
            FailSession expected = new FailSession(coordinator.sessionID);
            assertMessageSent(coordinator, participant, expected);
        }

        // additional success messages should be ignored
        Assert.assertFalse(coordinator.setRepairingCalled);
        coordinator.onSetRepairing = Assert::fail;
        coordinator.handlePrepareResponse(PARTICIPANT3, true);
        Assert.assertFalse(coordinator.setRepairingCalled);
        Assert.assertFalse(repairSubmitted.get());
    }

    @Test
    public void failedCommit()
    {
        InstrumentedCoordinatorSession coordinator = createInstrumentedSession();
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        SettableFuture<List<RepairSessionResult>> repairFuture = SettableFuture.create();
        Supplier<ListenableFuture<List<RepairSessionResult>>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        AtomicBoolean hasFailures = new AtomicBoolean(false);
        Assert.assertFalse(repairSubmitted.get());
        Assert.assertTrue(coordinator.sentMessages.isEmpty());
        ListenableFuture sessionResult = coordinator.execute(sessionSupplier, hasFailures);

        for (InetAddress participant : PARTICIPANTS)
        {
            RepairMessage expected = new PrepareConsistentRequest(coordinator.sessionID, COORDINATOR, new HashSet<>(PARTICIPANTS));
            assertMessageSent(coordinator, participant, expected);
        }

        // participants respond to coordinator, and repair begins once all participants have responded with success
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        coordinator.handlePrepareResponse(PARTICIPANT1, true);
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        coordinator.handlePrepareResponse(PARTICIPANT2, true);
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        // set the setRepairing callback to verify the correct state when it's called
        Assert.assertFalse(coordinator.setRepairingCalled);
        coordinator.onSetRepairing = () -> Assert.assertEquals(PREPARED, coordinator.getState());
        coordinator.handlePrepareResponse(PARTICIPANT3, true);
        Assert.assertTrue(coordinator.setRepairingCalled);
        Assert.assertTrue(repairSubmitted.get());

        Assert.assertEquals(ConsistentSession.State.REPAIRING, coordinator.getState());

        ArrayList<RepairSessionResult> results = Lists.newArrayList(createResult(coordinator),
                                                                    createResult(coordinator),
                                                                    createResult(coordinator));

        coordinator.sentMessages.clear();

        // set the finalizeCommit callback so we can verify the state when it's called
        Assert.assertFalse(coordinator.finalizeCommitCalled);
        coordinator.onFinalizeCommit = () -> Assert.assertEquals(REPAIRING, coordinator.getState());

        // make the commit fail
        coordinator.failingParticipants = ImmutableSet.copyOf(PARTICIPANTS);

        // set the repair future to unblock the commit
        repairFuture.set(results);
        Assert.assertTrue(coordinator.finalizeCommitCalled);
        Assert.assertTrue(coordinator.failCalled);
        Assert.assertFalse(coordinator.successCalled);
        Assert.assertEquals(ConsistentSession.State.FAILED, coordinator.getState());
        for (InetAddress participant : PARTICIPANTS)
        {
            RepairMessage commit = new FinalizeCommit(coordinator.sessionID);
            RepairMessage fail = new FailSession(coordinator.sessionID);
            assertMessagesSent(coordinator, participant, ImmutableList.of(commit, fail));
        }

        Assert.assertTrue(sessionResult.isDone());
        Assert.assertTrue(hasFailures.get());
    }

    @Test
    public void partiallyFailedCommit()
    {
        InstrumentedCoordinatorSession coordinator = createInstrumentedSession();
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        SettableFuture<List<RepairSessionResult>> repairFuture = SettableFuture.create();
        Supplier<ListenableFuture<List<RepairSessionResult>>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        AtomicBoolean hasFailures = new AtomicBoolean(false);
        Assert.assertFalse(repairSubmitted.get());
        Assert.assertTrue(coordinator.sentMessages.isEmpty());
        ListenableFuture sessionResult = coordinator.execute(sessionSupplier, hasFailures);

        for (InetAddress participant : PARTICIPANTS)
        {
            RepairMessage expected = new PrepareConsistentRequest(coordinator.sessionID, COORDINATOR, new HashSet<>(PARTICIPANTS));
            assertMessageSent(coordinator, participant, expected);
        }

        // participants respond to coordinator, and repair begins once all participants have responded with success
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        coordinator.handlePrepareResponse(PARTICIPANT1, true);
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        coordinator.handlePrepareResponse(PARTICIPANT2, true);
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        // set the setRepairing callback to verify the correct state when it's called
        Assert.assertFalse(coordinator.setRepairingCalled);
        coordinator.onSetRepairing = () -> Assert.assertEquals(PREPARED, coordinator.getState());
        coordinator.handlePrepareResponse(PARTICIPANT3, true);
        Assert.assertTrue(coordinator.setRepairingCalled);
        Assert.assertTrue(repairSubmitted.get());

        Assert.assertEquals(ConsistentSession.State.REPAIRING, coordinator.getState());

        ArrayList<RepairSessionResult> results = Lists.newArrayList(createResult(coordinator),
                                                                    createResult(coordinator),
                                                                    createResult(coordinator));

        coordinator.sentMessages.clear();

        // set the finalizeCommit callback so we can verify the state when it's called
        Assert.assertFalse(coordinator.finalizeCommitCalled);
        coordinator.onFinalizeCommit = () -> Assert.assertEquals(REPAIRING, coordinator.getState());

        // make the commit fail for all participants but first
        coordinator.failingParticipants = ImmutableSet.of(PARTICIPANT2, PARTICIPANT3);

        // set the repair future to unblock the commit
        repairFuture.set(results);
        Assert.assertTrue(coordinator.finalizeCommitCalled);
        Assert.assertTrue(coordinator.failCalled);
        Assert.assertFalse(coordinator.successCalled);
        Assert.assertEquals(ConsistentSession.State.FAILED, coordinator.getState());
        Assert.assertTrue(sessionResult.isDone());
        Assert.assertTrue(hasFailures.get());

        // participants didn't get a fail message this time, because the session was finalized on the first
        // participant, which will allow for later resolution
        for (InetAddress participant : PARTICIPANTS)
        {
            RepairMessage commit = new FinalizeCommit(coordinator.sessionID);
            assertMessagesSent(coordinator, participant, ImmutableList.of(commit));
        }
    }
}
