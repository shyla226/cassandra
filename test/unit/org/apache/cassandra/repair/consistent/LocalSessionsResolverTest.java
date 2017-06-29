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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.repair.AbstractRepairTest;
import org.apache.cassandra.repair.messages.StatusRequest;
import org.apache.cassandra.repair.messages.StatusResponse;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.repair.consistent.ConsistentSession.State.PREPARING;

public class LocalSessionsResolverTest extends AbstractRepairTest
{
    private LocalSession.Builder getDefaultSession()
    {
        LocalSession.Builder builder = LocalSession.builder();
        int now = FBUtilities.nowInSeconds();
        builder.withState(PREPARING);
        builder.withSessionID(UUIDGen.getTimeUUID());
        builder.withCoordinator(COORDINATOR);
        builder.withUUIDTableIds(Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()));
        builder.withRepairedAt(System.currentTimeMillis());
        builder.withRanges(Sets.newHashSet(RANGE1, RANGE2, RANGE3));
        builder.withParticipants(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3));
        builder.withStartedAt(now);
        builder.withLastUpdate(now);
        return builder;
    }

    @After
    public void cleanup()
    {
        InstrumentedLocalSessionsResolver.resolutions = new LinkedList<>();
    }

    @Test
    public void testWithFinalizedSessions() throws Exception
    {
        LocalSession newSession = getDefaultSession().build();
        LocalSession session1 = getDefaultSession().withState(ConsistentSession.State.FINALIZED).build();
        LocalSession session2 = getDefaultSession().withState(ConsistentSession.State.FINALIZED).build();

        InstrumentedLocalSessionsResolver resolver = new InstrumentedLocalSessionsResolver(newSession, ImmutableList.of(session1, session2));
        Assert.assertTrue(resolver.call());
        resolver.assertNoResolutions();
    }

    @Test
    public void testWithFailedSessions() throws Exception
    {
        LocalSession newSession = getDefaultSession().build();
        LocalSession session1 = getDefaultSession().withState(ConsistentSession.State.FAILED).build();
        LocalSession session2 = getDefaultSession().withState(ConsistentSession.State.FAILED).build();

        InstrumentedLocalSessionsResolver resolver = new InstrumentedLocalSessionsResolver(newSession, ImmutableList.of(session1, session2));
        Assert.assertTrue(resolver.call());
        resolver.assertNoResolutions();
    }

    @Test
    public void testCannotResolveWithLocallyRunningSession() throws Exception
    {
        LocalSession newSession = getDefaultSession().build();
        LocalSession session1 = getDefaultSession().withState(ConsistentSession.State.PREPARING).build();

        InstrumentedLocalSessionsResolver resolver = new InstrumentedLocalSessionsResolver(newSession, ImmutableList.of(session1));

        resolver.runningLocally = true;
        Assert.assertFalse(resolver.call());
        resolver.assertNoRequestSent();
        resolver.assertNoResolutions();
    }

    @Test
    public void testResolveAsFinalizedViaRemoteParticipant() throws Exception
    {
        testResolveViaRemoteParticipant(ConsistentSession.State.FINALIZED);
    }

    @Test
    public void testResolveAsFailedViaRemoteParticipant() throws Exception
    {
        testResolveViaRemoteParticipant(ConsistentSession.State.FAILED);
    }

    private void testResolveViaRemoteParticipant(ConsistentSession.State resolvedState) throws Exception
    {
        LocalSession newSession = getDefaultSession().build();
        LocalSession session1 = getDefaultSession().withState(ConsistentSession.State.REPAIRING).build();

        InstrumentedLocalSessionsResolver resolver = new InstrumentedLocalSessionsResolver(newSession, ImmutableList.of(session1));

        resolver.responses.put(Pair.create(session1.sessionID, PARTICIPANT2),
                               new StatusResponse(session1.sessionID, resolvedState));
        resolver.responses.put(Pair.create(session1.sessionID, PARTICIPANT3),
                               new StatusResponse(session1.sessionID, ConsistentSession.State.REPAIRING));

        Assert.assertTrue(resolver.call());

        resolver.assertRequest(PARTICIPANT2, new StatusRequest(session1.sessionID));
        resolver.assertRequest(PARTICIPANT3, new StatusRequest(session1.sessionID));
        resolver.assertResolution(session1, resolvedState);
    }

    @Test
    public void testResolveAsFailedIfNoRemoteParticipantHasCompletedSession() throws Exception
    {
        LocalSession newSession = getDefaultSession().build();
        LocalSession session1 = getDefaultSession().withState(ConsistentSession.State.REPAIRING).build();

        InstrumentedLocalSessionsResolver resolver = new InstrumentedLocalSessionsResolver(newSession, ImmutableList.of(session1));

        resolver.responses.put(Pair.create(session1.sessionID, PARTICIPANT2),
                               new StatusResponse(session1.sessionID, ConsistentSession.State.REPAIRING));
        resolver.responses.put(Pair.create(session1.sessionID, PARTICIPANT3),
                               new StatusResponse(session1.sessionID, ConsistentSession.State.REPAIRING));

        Assert.assertTrue(resolver.call());

        resolver.assertRequest(PARTICIPANT2, new StatusRequest(session1.sessionID));
        resolver.assertRequest(PARTICIPANT3, new StatusRequest(session1.sessionID));
        resolver.assertResolution(session1, ConsistentSession.State.FAILED);
    }

    @Test
    public void testCannotResolveIfAnySessionIsNotResolved() throws Exception
    {
        LocalSession newSession = getDefaultSession().build();
        LocalSession session1 = getDefaultSession().withState(ConsistentSession.State.REPAIRING).build();
        LocalSession session2 = getDefaultSession().withState(ConsistentSession.State.REPAIRING).build();

        InstrumentedLocalSessionsResolver resolver = new InstrumentedLocalSessionsResolver(newSession, ImmutableList.of(session1, session2));

        resolver.responses.put(Pair.create(session1.sessionID, PARTICIPANT2),
                               new StatusResponse(session1.sessionID, ConsistentSession.State.FINALIZED));
        resolver.responses.put(Pair.create(session1.sessionID, PARTICIPANT3),
                               new StatusResponse(session1.sessionID, ConsistentSession.State.REPAIRING));

        resolver.failedParticipants.add(Pair.create(session2.sessionID, PARTICIPANT2));
        resolver.responses.put(Pair.create(session2.sessionID, PARTICIPANT3),
                               new StatusResponse(session2.sessionID, ConsistentSession.State.REPAIRING));

        Assert.assertFalse(resolver.call());

        resolver.assertRequest(PARTICIPANT2, new StatusRequest(session1.sessionID));
        resolver.assertRequest(PARTICIPANT3, new StatusRequest(session1.sessionID));
        resolver.assertRequest(PARTICIPANT2, new StatusRequest(session2.sessionID));
        resolver.assertRequest(PARTICIPANT3, new StatusRequest(session2.sessionID));
        resolver.assertResolutions(1);
        resolver.assertResolution(session1, ConsistentSession.State.FINALIZED);
    }

    @Test
    public void testCannotResolveIfRemoteSessionIsRunning() throws Exception
    {
        LocalSession newSession = getDefaultSession().build();
        LocalSession session1 = getDefaultSession().withState(ConsistentSession.State.REPAIRING).build();

        InstrumentedLocalSessionsResolver resolver = new InstrumentedLocalSessionsResolver(newSession, ImmutableList.of(session1));

        resolver.responses.put(Pair.create(session1.sessionID, PARTICIPANT2),
                               new StatusResponse(session1.sessionID, ConsistentSession.State.REPAIRING, true));
        resolver.responses.put(Pair.create(session1.sessionID, PARTICIPANT3),
                               new StatusResponse(session1.sessionID, ConsistentSession.State.REPAIRING, true));

        Assert.assertFalse(resolver.call());

        resolver.assertRequest(PARTICIPANT2, new StatusRequest(session1.sessionID));
        resolver.assertRequest(PARTICIPANT3, new StatusRequest(session1.sessionID));
        resolver.assertNoResolutions();
    }

    private static class InstrumentedLocalSessionsResolver extends LocalSessionsResolver
    {
        static volatile List<Pair<LocalSession, ConsistentSession.State>> resolutions = new LinkedList<>();

        volatile boolean runningLocally = false;
        volatile Map<Pair<UUID, InetAddress>, StatusResponse> responses = Maps.newConcurrentMap();
        volatile Set<Pair<UUID, InetAddress>> failedParticipants = Sets.newConcurrentHashSet();

        private volatile Set<Pair<InetAddress, StatusRequest>> requests = Sets.newConcurrentHashSet();

        public InstrumentedLocalSessionsResolver(LocalSession newSession, Collection<LocalSession> localSessions)
        {
            super(newSession, localSessions, (Pair<LocalSession, ConsistentSession.State> p) ->
            {
                resolutions.add(p);
            });
        }

        public void assertNoResolutions()
        {
            Assert.assertTrue(resolutions.isEmpty());
        }

        public void assertResolutions(int total)
        {
            Assert.assertEquals(total, resolutions.size());
        }

        public void assertResolution(LocalSession session, ConsistentSession.State state)
        {
            Assert.assertTrue(resolutions.contains(Pair.create(session, state)));
        }

        public void assertNoRequestSent()
        {
            Assert.assertTrue(requests.isEmpty());
        }

        public void assertRequest(InetAddress to, StatusRequest request)
        {
            Assert.assertTrue(requests.contains(Pair.create(to, request)));
        }

        public void clearInstrumentedState()
        {
            runningLocally = false;
            requests = Sets.newConcurrentHashSet();
            responses = Maps.newConcurrentMap();
            failedParticipants = Sets.newConcurrentHashSet();
        }

        @Override
        protected InetAddress getBroadcastAddress()
        {
            return PARTICIPANT1;
        }

        @Override
        protected Optional<StatusResponse> getStatusFromLocalNode(LocalSession session)
        {
            return Optional.of(new StatusResponse(session.sessionID, session.getState(), runningLocally));
        }

        @Override
        protected void send(Request<StatusRequest, StatusResponse> request, MessageCallback<StatusResponse> callback)
        {
            requests.add(Pair.create(request.to(), request.payload()));
            if (failedParticipants.contains(Pair.create(request.payload().sessionID, request.to())))
                callback.onFailure(request.respondWithFailure(RequestFailureReason.UNKNOWN));
            else
                callback.onResponse(request.respond(responses.get(Pair.create(request.payload().sessionID, request.to()))));
        }
    }
}
