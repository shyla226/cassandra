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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.commons.lang3.time.DurationFormatUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OneWayRequest;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.repair.RepairSessionResult;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.service.ActiveRepairService;

/**
 * Coordinator side logic and state of a consistent repair session. Like {@link ActiveRepairService.ParentRepairSession},
 * there is only one {@code CoordinatorSession} per user repair command, regardless of the number of tables and token
 * ranges involved.
 */
public class CoordinatorSession extends ConsistentSession implements IEndpointStateChangeSubscriber, IFailureDetectionEventListener
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorSession.class);

    private final Map<InetAddress, State> participantStates = new HashMap<>();
    private final SettableFuture<Boolean> prepareFuture = SettableFuture.create();

    private volatile long sessionStart = Long.MIN_VALUE;
    private volatile long repairStart = Long.MIN_VALUE;
    private volatile long finalizeStart = Long.MIN_VALUE;

    private volatile Consumer<CoordinatorSession> onCompleteCallback;

    public CoordinatorSession(Builder builder)
    {
        super(builder);
        for (InetAddress participant : participants)
        {
            participantStates.put(participant, State.PREPARING);
        }
    }

    public static class Builder extends AbstractBuilder<Builder>
    {
        public CoordinatorSession build()
        {
            validate();
            return new CoordinatorSession(this);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public void setOnCompleteCallback(Consumer<CoordinatorSession> onCompleteCallback)
    {
        this.onCompleteCallback = onCompleteCallback;
    }

    public void setState(State state)
    {
        logger.trace("Setting coordinator state to {} for repair {}", state, sessionID);
        super.setState(state);
    }

    public synchronized void setParticipantState(InetAddress participant, State state)
    {
        if (!state.equals(participantStates.get(participant)))
        {
            logger.trace("Setting participant {} to state {} for repair {}", participant, state, sessionID);
            Preconditions.checkArgument(participantStates.containsKey(participant),
                                        "Session %s doesn't include %s",
                                        sessionID, participant);
            Preconditions.checkArgument(participantStates.get(participant).canTransitionTo(state),
                                        "Invalid state transition %s -> %s",
                                        participantStates.get(participant), state);
            participantStates.put(participant, state);
        }

        // update coordinator state if all participants are at the value being set
        if (Iterables.all(participantStates.values(), s -> s == state))
        {
            setState(state);
        }
    }

    synchronized void setAll(State state)
    {
        for (InetAddress participant : participants)
        {
            setParticipantState(participant, state);
        }
    }

    synchronized boolean allStates(State state)
    {
        return getState() == state && Iterables.all(participantStates.values(), v -> v == state);
    }

    synchronized boolean anyState(State state)
    {
        return getState() == state || Iterables.any(participantStates.values(), v -> v == state);
    }

    synchronized boolean hasFailed()
    {
        return getState() == State.FAILED || Iterables.any(participantStates.values(), v -> v == State.FAILED);
    }

    // Overridden by tests to intercept messages
    // TODO: the test could probably use the messaging service mocking instead
    @VisibleForTesting
    protected void send(OneWayRequest<? extends RepairMessage<?>> request)
    {
        logger.trace("Sending {} to {}", request.payload(), request.to());
        MessagingService.instance().send(request);
    }

    @VisibleForTesting
    protected void send(Request<? extends RepairMessage<?>, EmptyPayload> request, MessageCallback<EmptyPayload> callback)
    {
        logger.trace("Sending {} to {}", request.payload(), request.to());
        MessagingService.instance().send(request, callback);
    }

    public ListenableFuture<Boolean> prepare()
    {
        Preconditions.checkArgument(allStates(State.PREPARING));
        logger.debug("Beginning prepare phase of incremental repair session {}", sessionID);
        PrepareConsistentRequest message = new PrepareConsistentRequest(sessionID, coordinator, participants);
        for (final InetAddress participant : participants)
            send(Verbs.REPAIR.CONSISTENT_REQUEST.newRequest(participant, message));
        return prepareFuture;
    }

    public synchronized void handlePrepareResponse(InetAddress participant, boolean success)
    {
        if (getState() == State.FAILED)
        {
            logger.trace("Incremental repair session {} has failed, ignoring prepare response from {}", sessionID, participant);
        }
        else if (!success)
        {
            logger.debug("{} failed the prepare phase for incremental repair session {}. Aborting session", participant, sessionID);
            prepareFuture.set(false);
        }
        else
        {
            logger.trace("Successful prepare response received from {} for repair session {}", participant, sessionID);
            setParticipantState(participant, State.PREPARED);
            if (getState() == State.PREPARED)
            {
                logger.debug("Incremental repair session {} successfully prepared.", sessionID);
                prepareFuture.set(true);
            }
        }
    }

    public synchronized void setRepairing()
    {
        Preconditions.checkArgument(allStates(State.PREPARED));
        setAll(State.REPAIRING);
    }

    public synchronized ListenableFuture<Boolean> finalizeCommit()
    {
        Preconditions.checkArgument(allStates(State.REPAIRING));
        logger.debug("Committing finalization of repair session {}", sessionID);
        FinalizeCommit message = new FinalizeCommit(sessionID);
        SettableFuture<Boolean> finalizeResult = SettableFuture.create();
        FinalizeCommitCallback callback = new FinalizeCommitCallback(
            participants.size(),
            (response) -> setParticipantState(response.from(), State.FINALIZED),
            finalizeResult);

        for (final InetAddress participant : participants)
            send(Verbs.REPAIR.FINALIZE_COMMIT.newRequest(participant, message), callback);

        return finalizeResult;
    }

    @VisibleForTesting
    protected synchronized void fail()
    {
        logger.info("Incremental repair session {} failed", sessionID);
        FailSession message = new FailSession(sessionID);
        if (!anyState(State.FINALIZED))
        {
            for (final InetAddress participant : participants)
            {
                if (participantStates.get(participant) != State.FAILED && participantStates.get(participant) != State.FINALIZED)
                {
                    send(Verbs.REPAIR.FAILED_SESSION.newRequest(participant, message));
                    setParticipantState(participant, State.FAILED);
                }
            }
        }
        else
            logger.info("Incremental repair session {} was finalized on some participants, "
                + "do not send fail messages in order to allow for later resolution.", sessionID);

        setState(State.FAILED);

        if (onCompleteCallback != null)
            onCompleteCallback.accept(this);
    }

    @VisibleForTesting
    protected void success()
    {
        logger.info("Incremental repair session {} completed", sessionID);
        setAll(State.FINALIZED);

        if (onCompleteCallback != null)
            onCompleteCallback.accept(this);
    }

    private static String formatDuration(long then, long now)
    {
        if (then == Long.MIN_VALUE || now == Long.MIN_VALUE)
        {
            // if neither of the times were initially set, don't return a non-sensical answer
            return "n/a";
        }
        return DurationFormatUtils.formatDurationWords(now - then, true, true);
    }

    /**
     * Runs the asynchronous consistent repair session. Actual repair sessions are scheduled via a submitter to make unit testing easier
     */
    public ListenableFuture execute(Supplier<ListenableFuture<List<RepairSessionResult>>> sessionSubmitter, AtomicBoolean hasFailure)
    {
        logger.info("Beginning coordination of incremental repair session {}", sessionID);

        sessionStart = System.currentTimeMillis();

        // prepare (runs anticompaction)
        ListenableFuture<Boolean> prepareResult = prepare();

        // run repair sessions normally
        ListenableFuture<List<RepairSessionResult>> repairResults = Futures.transform(prepareResult, new AsyncFunction<Boolean, List<RepairSessionResult>>()
        {
            public ListenableFuture<List<RepairSessionResult>> apply(Boolean success) throws Exception
            {
                repairStart = System.currentTimeMillis();
                if (success)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Incremental repair {} prepare phase completed in {}", sessionID, formatDuration(sessionStart, repairStart));

                    setRepairing();
                    return sessionSubmitter.get();
                }
                else
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Incremental repair {} prepare phase failed in {}", sessionID, formatDuration(sessionStart, repairStart));

                    return Futures.immediateFuture(null);
                }

            }
        });

        // commit repaired data
        ListenableFuture<Boolean> finalizeResult = Futures.transform(repairResults, new AsyncFunction<List<RepairSessionResult>, Boolean>()
        {
            public ListenableFuture<Boolean> apply(List<RepairSessionResult> result) throws Exception
            {
                finalizeStart = System.currentTimeMillis();
                if (result == null || result.isEmpty() || Iterables.any(result, r -> r == null))
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Incremental repair {} validation/stream phase failed in {}", sessionID, formatDuration(repairStart, finalizeStart));

                    return Futures.immediateFuture(false);
                }
                else
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Incremental repair {} validation/stream phase completed in {}", sessionID, formatDuration(repairStart, finalizeStart));

                    return finalizeCommit();
                }
            }
        });

        // finalize
        Futures.addCallback(finalizeResult, new FutureCallback<Boolean>()
        {
            public void onSuccess(Boolean success)
            {
                if (!success)
                {
                    onFailure(new RuntimeException("Incremental repair failed!"));
                }
                else
                {
                    success();

                    if (logger.isDebugEnabled())
                        logger.debug("Incremental repair {} completed in {}", sessionID, formatDuration(sessionStart, System.currentTimeMillis()));
                }
            }

            public void onFailure(Throwable t)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Incremental repair {} failed in {}", sessionID, formatDuration(repairStart, System.currentTimeMillis()));

                hasFailure.set(true);
                fail();
            }
        });

        return finalizeResult;
    }

    public void onJoin(InetAddress endpoint, EndpointState epState) {}
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {}
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {}
    public void onAlive(InetAddress endpoint, EndpointState state) {}
    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void onRestart(InetAddress endpoint, EndpointState epState)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public synchronized void convict(InetAddress endpoint, double phi)
    {
        if (!participantStates.keySet().contains(endpoint))
            return;

        // We want a higher confidence in the failure detection than usual because failing a repair wrongly has a high cost.
        if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold())
            return;

        String error = String.format("[repair #%s] Endpoint %s died, will fail incremental repair session.", sessionID, endpoint);
        if (getState() == State.PREPARING)
        {
            logger.warn(error);
            prepareFuture.set(false);
        }
    }

    private class FinalizeCommitCallback implements MessageCallback<EmptyPayload>
    {
        private int expectedResponses;
        private boolean hasError;
        private final Consumer<Response> onSuccessResponse;
        private final SettableFuture<Boolean> successFuture;

        public FinalizeCommitCallback(int expectedResponses, Consumer<Response> onSuccessResponse, SettableFuture<Boolean> successFuture)
        {
            this.expectedResponses = expectedResponses;
            this.onSuccessResponse = onSuccessResponse;
            this.successFuture = successFuture;
        }

        @Override
        public synchronized void onTimeout(InetAddress host)
        {
            hasError = true;
            maybeUnblock();
        }

        @Override
        public synchronized void onFailure(FailureResponse response)
        {
            hasError = true;
            maybeUnblock();
        }

        @Override
        public synchronized void onResponse(Response response)
        {
            try
            {
                onSuccessResponse.accept(response);
            }
            finally
            {
                maybeUnblock();
            }
        }

        private void maybeUnblock()
        {
            if (--expectedResponses == 0)
                successFuture.set(!hasError);
        }
    }
}
