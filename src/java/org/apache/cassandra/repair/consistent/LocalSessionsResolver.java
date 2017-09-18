package org.apache.cassandra.repair.consistent;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.messages.StatusRequest;
import org.apache.cassandra.repair.messages.StatusResponse;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * When a new {@link LocalSession} is initiated, this callable resolves the currently pending local sessions by either
 * completing them, or aborting the new session if resolution failed.
 * <p/>
 * For each pending non-completed session, the exact resolution algorithm works as follows:
 * <ol>
 * <li>The session status is read from the local node.</li>
 * <li>If the session exists and is running (that is, it has an active parent session), the new session is aborted.</li>
 * <li>If the session is pending and not running, the session status is read from remote participants.</li>
 * <li>If any participant answers the session is in progress, the new session is aborted.</li>
 * <li>If any participant answers the session is completed, the pending local session is resolved with the
 * returned completed status (either FAILED or FINALIZED).</li>
 * <li>If all participants answer the session is pending, the pending local session is resolved with FAILED status:
 * this corresponds to a coordinator failure, or the failure of all repairing nodes together.</li>
 * </ol>
 */
public class LocalSessionsResolver implements Callable<Boolean>
{
    private static final Logger logger = LoggerFactory.getLogger(LocalSessionsResolver.class);

    private final LocalSession newSession;
    private final Collection<LocalSession> localSessions;
    private final Consumer<Pair<LocalSession, ConsistentSession.State>> resolver;

    /**
     * Creates a new LocalSessionsResolver.
     *
     * @param newSession The new local session "asking" for the resolution of other sessions.
     * @param localSessions The stored local sessions.
     * @param resolver A strategy object implementing the actual resolution of a given session with the given state.
     */
    public LocalSessionsResolver(LocalSession newSession, Collection<LocalSession> localSessions, Consumer<Pair<LocalSession, ConsistentSession.State>> resolver)
    {
        this.newSession = newSession;
        this.localSessions = localSessions;
        this.resolver = resolver;
    }

    /**
     * Implements the session resolution algorithm.
     *
     * @return True if all pending local sessions have been resolved, false if any session couldn't be resolved.
     */
    @Override
    public Boolean call() throws Exception
    {
        Collection<LocalSession> sessionsToResolve = localSessions.stream()
            .filter(s -> !s.sessionID.equals(newSession.sessionID)
                && !s.isCompleted()
                && s.ranges.stream().anyMatch(candidate -> newSession.ranges.stream().anyMatch(parent -> parent.intersects(candidate))))
            .collect(Collectors.toList());

        logger.info("Found {} local sessions to resolve.", sessionsToResolve.size());

        for (LocalSession session : sessionsToResolve)
        {
            /**
             * We need to synchronize on the session to avoid running concurrently with {@link LocalSessions#cleanup()};
             * not the nicest, but it is what it is.
             */
            synchronized (session)
            {
                // We also need to double check the session has not been completed by the cleanup thread:
                if (session.isCompleted())
                    continue;

                logger.info("Trying to resolve local session {}.", session.sessionID);

                Optional<StatusResponse> localResponse = Optional.empty();
                if (session.participants.contains(getBroadcastAddress()))
                {
                    localResponse = getStatusFromLocalNode(session);
                    logger.debug("Got status {} from local node {} for session {}.", localResponse, getBroadcastAddress(), session.sessionID);
                }

                Resolution status = tryResolveFromLocalNode(localResponse);

                if (status == Resolution.RUNNING)
                {
                    logger.info("Cannot resolve local session {} coordinated by {} because still running on ranges intersecting with new session {} coordinated by {}. "
                        + "Only one incremental repair session can be running for any given range, so the new session will not be started.",
                                session.sessionID, session.coordinator, newSession.sessionID, newSession.coordinator);

                    return false;
                }

                if (status == Resolution.PENDING)
                {
                    StatusResponseCallback remoteResponses = new StatusResponseCallback(session);
                    int responses = 0;
                    for (InetAddress participant : session.participants)
                    {
                        if (!participant.equals(getBroadcastAddress()))
                        {
                            responses++;
                            send(Verbs.REPAIR.STATUS_REQUEST.newRequest(participant, new StatusRequest(session.sessionID)), remoteResponses);
                        }
                    }
                    status = tryResolveFromParticipants(localResponse, remoteResponses.await(responses));
                }

                if (status == Resolution.FINALIZED || status == Resolution.FAILED)
                {
                    ConsistentSession.State state = Resolution.from(status);
                    logger.info("Resolving local session {} with state {}.", session.sessionID, state);
                    long start = System.currentTimeMillis();
                    resolver.accept(Pair.create(session, state));
                    logger.info("Resolved local session {} with state {}, took: {}ms.", session.sessionID, state, System.currentTimeMillis() - start);
                }
                else
                {
                    logger.info("Cannot resolve local session {} based on participant responses.", session.sessionID);
                    return false;
                }
            }
        }
        return true;
    }

    @VisibleForTesting
    protected InetAddress getBroadcastAddress()
    {
        return FBUtilities.getBroadcastAddress();
    }

    @VisibleForTesting
    protected Optional<StatusResponse> getStatusFromLocalNode(LocalSession session)
    {
        return Optional.of(new StatusResponse(session.sessionID, session.getState(), isRunningLocally(session)));
    }

    @VisibleForTesting
    protected void send(Request<StatusRequest, StatusResponse> request, MessageCallback<StatusResponse> callback)
    {
        MessagingService.instance().send(request, callback);
    }

    private boolean isComplete(ConsistentSession.State state)
    {
        return state == ConsistentSession.State.FAILED || state == ConsistentSession.State.FINALIZED;
    }

    private boolean isRunningLocally(LocalSession session)
    {
        return ActiveRepairService.instance.hasParentRepairSession(session.sessionID);
    }

    private Resolution tryResolveFromLocalNode(Optional<StatusResponse> localResponse)
    {
        if (localResponse.isPresent())
        {
            if (!localResponse.get().running && isComplete(localResponse.get().state))
                return Resolution.from(localResponse.get().state);
            else if (localResponse.get().running)
                return Resolution.RUNNING;
        }
        return Resolution.PENDING;
    }

    private Resolution tryResolveFromParticipants(Optional<StatusResponse> localResponse, List<Optional<StatusResponse>> remoteResponses)
    {
        Optional<Optional<StatusResponse>> completedResponse = remoteResponses.stream().filter(p -> p.isPresent() && isComplete(p.get().state)).findFirst();
        if (!completedResponse.isPresent()) {
            List<Optional<StatusResponse>> all = new ArrayList<>(remoteResponses.size() + 1);
            all.add(localResponse);
            all.addAll(remoteResponses);
            Optional<Integer> pendingCounter = all.stream()
                .filter(p -> p.isPresent() && !p.get().running && !isComplete(p.get().state))
                .map(p -> 1).reduce((c1, c2) -> c1 + c2);

            if (pendingCounter.isPresent() && pendingCounter.get().equals(all.size()))
                return Resolution.from(ConsistentSession.State.FAILED);
            else
                return Resolution.PENDING;
        }
        return Resolution.from(completedResponse.get().get().state);
    }

    private enum Resolution
    {
        FINALIZED, FAILED, RUNNING, PENDING;

        public static ConsistentSession.State from(Resolution from)
        {
            switch (from)
            {
                case FINALIZED:
                    return ConsistentSession.State.FINALIZED;
                case FAILED:
                    return ConsistentSession.State.FAILED;
                default:
                    throw new IllegalStateException(String.format("Cannot convert from %s", from));
            }
        }

        public static Resolution from(ConsistentSession.State from)
        {
            switch (from)
            {
                case FINALIZED:
                    return FINALIZED;
                case FAILED:
                    return FAILED;
                default:
                    throw new IllegalStateException(String.format("Cannot convert from %s", from));
            }
        }
    }

    private class StatusResponseCallback implements MessageCallback<StatusResponse>
    {
        private final Object monitor = new Object();
        private List<Optional<StatusResponse>> responses = new LinkedList<>();
        private final LocalSession session;

        public StatusResponseCallback(LocalSession session)
        {
            this.session = session;
        }

        @Override
        public void onResponse(Response<StatusResponse> response)
        {
            synchronized (monitor)
            {
                StatusResponse status = response.payload();

                logger.debug("Got status {} from remote node {} for session {}.",
                             status,
                             response.from(),
                             status.sessionID);

                responses.add(Optional.of(status));

                monitor.notifyAll();
            }
        }

        @Override
        public void onFailure(FailureResponse<StatusResponse> failure)
        {
            synchronized (monitor)
            {
                logger.debug("Failed to get status from remote node {} for session {}, reason: {}.",
                             failure.from(),
                             session.sessionID,
                             failure.reason());

                responses.add(Optional.empty());

                monitor.notifyAll();
            }
        }

        @Override
        public void onTimeout(InetAddress host)
        {
            synchronized (monitor)
            {
                logger.debug("Failed to get status from remote node {} for session {} due to timeout.",
                             host,
                             session.sessionID);

                responses.add(Optional.empty());

                monitor.notifyAll();
            }
        }

        public List<Optional<StatusResponse>> await(int expectedResponses)
        {
            logger.debug("Awaiting {} remote responses for session {}.",
                             expectedResponses,
                             session.sessionID);

            synchronized (monitor)
            {
                while (expectedResponses > responses.size())
                {
                    try
                    {
                        monitor.wait();
                    }
                    catch (InterruptedException ex)
                    {
                        Thread.currentThread().interrupt();
                    }
                }

                logger.debug("Got {} remote responses for session {}.",
                             expectedResponses,
                             session.sessionID);

                return responses;
            }
        }
    }
}
