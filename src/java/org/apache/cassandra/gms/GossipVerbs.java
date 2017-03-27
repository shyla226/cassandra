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
package org.apache.cassandra.gms;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Verb.AckedRequest;
import org.apache.cassandra.net.Verb.OneWay;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.VerbHandlers;
import org.apache.cassandra.utils.versioning.Version;

public class GossipVerbs extends VerbGroup<GossipVerbs.GossipVersion>
{
    private static final Logger logger = LoggerFactory.getLogger(GossipVerbs.class);

    public enum GossipVersion implements Version<GossipVersion>
    {
        OSS_30
    }

    public final OneWay<GossipDigestSyn> SYN;
    public final OneWay<GossipDigestAck> ACK;
    public final OneWay<GossipDigestAck2> ACK2;
    public final OneWay<EmptyPayload> SHUTDOWN;
    public final AckedRequest<EmptyPayload> ECHO;

    public GossipVerbs(Verbs.Group id)
    {
        super(id, true, GossipVersion.class);

        RegistrationHelper helper = helper()
                                    .stage(Stage.GOSSIP);

        SYN = helper.oneWay("SYN", GossipDigestSyn.class)
                    .handler(new SynHandler());
        ACK = helper.oneWay("ACK", GossipDigestAck.class)
                    .handler(new AckHandler());
        ACK2 = helper.oneWay("ACK2", GossipDigestAck2.class)
                     .handler(new Ack2Handler());
        SHUTDOWN = helper.oneWay("SHUTDOWN", EmptyPayload.class)
                     .handler(new ShutdownHandler());
        ECHO = helper.ackedRequest("ECHO", EmptyPayload.class)
                     .timeout(DatabaseDescriptor::getRpcTimeout)
                     .syncHandler((from, x) -> {});
    }

    private static class SynHandler implements VerbHandlers.OneWay<GossipDigestSyn>
    {
        public void handle(InetAddress from, GossipDigestSyn message)
        {
            if (!Gossiper.instance.isEnabled() && !Gossiper.instance.isInShadowRound())
            {
                logger.trace("Ignoring GossipDigestSynMessage because gossip is disabled");
                return;
            }

            // If the message is from a different cluster throw it away.
            if (!message.clusterId.equals(DatabaseDescriptor.getClusterName()))
            {
                logger.warn("ClusterName mismatch from {} {}!={}", from, message.clusterId, DatabaseDescriptor.getClusterName());
                return;
            }

            if (message.partioner != null && !message.partioner.equals(DatabaseDescriptor.getPartitionerName()))
            {
                logger.warn("Partitioner mismatch from {} {}!={}", from, message.partioner, DatabaseDescriptor.getPartitionerName());
                return;
            }

            List<GossipDigest> gDigestList = message.getGossipDigests();

            // if the syn comes from a peer performing a shadow round and this node is
            // also currently in a shadow round, send back a minimal ack. This node must
            // be in the sender's seed list and doing this allows the sender to
            // differentiate between seeds from which it is partitioned and those which
            // are in their shadow round
            if (!Gossiper.instance.isEnabled() && Gossiper.instance.isInShadowRound())
            {
                // a genuine syn (as opposed to one from a node currently
                // doing a shadow round) will always contain > 0 digests
                if (gDigestList.size() > 0)
                {
                    logger.debug("Ignoring non-empty GossipDigestSynMessage because currently in gossip shadow round");
                    return;
                }

                logger.debug("Received a shadow round syn from {}. Gossip is disabled but " +
                             "currently also in shadow round, responding with a minimal ack", from);

                ack(from, new ArrayList<>(), new HashMap<>());
                return;
            }

            if (logger.isTraceEnabled())
            {
                StringBuilder sb = new StringBuilder();
                for (GossipDigest gDigest : gDigestList)
                {
                    sb.append(gDigest);
                    sb.append(' ');
                }
                logger.trace("Gossip syn digests are : {}", sb);
            }

            doSort(gDigestList);

            List<GossipDigest> deltaGossipDigestList = new ArrayList<>();
            Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap<>();
            Gossiper.instance.examineGossiper(gDigestList, deltaGossipDigestList, deltaEpStateMap);

            if (logger.isTraceEnabled())
            {
                logger.trace("sending {} digests and {} deltas", deltaGossipDigestList.size(), deltaEpStateMap.size());
                logger.trace("Sending a GossipDigestAckMessage to {}", from);
            }

            ack(from, deltaGossipDigestList, deltaEpStateMap);
        }

        private void ack(InetAddress to, List<GossipDigest> digestList, Map<InetAddress, EndpointState> epStateMap)
        {
            // Note that Gossip doesn't use our request-response mechanism, partly for historical reason but also
            // because it does SYN->ACK->ACK2 which doesn't exactly fit the model either.
            MessagingService.instance().send(Verbs.GOSSIP.ACK.newRequest(to, new GossipDigestAck(digestList, epStateMap)));
            Gossiper.instance.onNewMessageProcessed();
        }

        /*
         * First construct a map whose key is the endpoint in the GossipDigest and the value is the
         * GossipDigest itself. Then build a list of version differences i.e difference between the
         * version in the GossipDigest and the version in the local state for a given InetAddress.
         * Sort this list. Now loop through the sorted list and retrieve the GossipDigest corresponding
         * to the endpoint from the map that was initially constructed.
        */
        private void doSort(List<GossipDigest> gDigestList)
        {
            // Construct a map of endpoint to GossipDigest.
            Map<InetAddress, GossipDigest> epToDigestMap = new HashMap<>();
            for (GossipDigest gDigest : gDigestList)
                epToDigestMap.put(gDigest.getEndpoint(), gDigest);

            // These digests have their maxVersion set to the difference of the version
            // of the local EndpointState and the version found in the GossipDigest.
            List<GossipDigest> diffDigests = new ArrayList<>(gDigestList.size());
            for (GossipDigest gDigest : gDigestList)
            {
                InetAddress ep = gDigest.getEndpoint();
                EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
                int version = (epState != null) ? Gossiper.instance.getMaxEndpointStateVersion(epState) : 0;
                int diffVersion = Math.abs(version - gDigest.getMaxVersion());
                diffDigests.add(new GossipDigest(ep, gDigest.getGeneration(), diffVersion));
            }

            gDigestList.clear();
            Collections.sort(diffDigests);
            int size = diffDigests.size();
            // Report the digests in descending order. This takes care of the endpoints
            // that are far behind w.r.t this local endpoint
            for (int i = size - 1; i >= 0; --i)
                gDigestList.add(epToDigestMap.get(diffDigests.get(i).getEndpoint()));
        }
    }

    private static class AckHandler implements VerbHandlers.OneWay<GossipDigestAck>
    {
        public void handle(InetAddress from, GossipDigestAck message)
        {
            if (!Gossiper.instance.isEnabled() && !Gossiper.instance.isInShadowRound())
            {
                logger.trace("Ignoring GossipDigestAckMessage because gossip is disabled");
                return;
            }

            List<GossipDigest> gDigestList = message.getGossipDigestList();
            Map<InetAddress, EndpointState> epStateMap = message.getEndpointStateMap();
            logger.trace("Received ack with {} digests and {} states", gDigestList.size(), epStateMap.size());

            if (Gossiper.instance.isInShadowRound())
            {
                if (logger.isDebugEnabled())
                    logger.debug("Received an ack from {}, which may trigger exit from shadow round", from);

                // if the ack is completely empty, then we can infer that the respondent is also in a shadow round
                Gossiper.instance.maybeFinishShadowRound(from, gDigestList.isEmpty() && epStateMap.isEmpty(), epStateMap);
                return; // don't bother doing anything else, we have what we came for
            }

            if (epStateMap.size() > 0)
            {
                // Ignore any GossipDigestAck messages that we handle before a regular GossipDigestSyn has been send.
                // This will prevent Acks from leaking over from the shadow round that are not actual part of
                // the regular gossip conversation.
                if ((System.nanoTime() - Gossiper.instance.firstSynSendAt) < 0 || Gossiper.instance.firstSynSendAt == 0)
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Ignoring unrequested GossipDigestAck from {}", from);
                    return;
                }

                /* Notify the Failure Detector */
                Gossiper.instance.notifyFailureDetector(epStateMap);
                Gossiper.instance.applyStateLocally(epStateMap);
            }

            // Get the state required to send to this gossipee - construct GossipDigestAck2Message
            Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap<>();
            for (GossipDigest gDigest : gDigestList)
            {
                InetAddress addr = gDigest.getEndpoint();
                EndpointState localEpStatePtr = Gossiper.instance.getStateForVersionBiggerThan(addr, gDigest.getMaxVersion());
                if (localEpStatePtr != null)
                    deltaEpStateMap.put(addr, localEpStatePtr);
            }

            if (logger.isTraceEnabled())
                logger.trace("Sending a GossipDigestAck2Message to {}", from);

            MessagingService.instance().send(Verbs.GOSSIP.ACK2.newRequest(from, new GossipDigestAck2(deltaEpStateMap)));
            Gossiper.instance.onNewMessageProcessed();
        }
    }

    private static class Ack2Handler implements VerbHandlers.OneWay<GossipDigestAck2>
    {
        public void handle(InetAddress from, GossipDigestAck2 message)
        {
            if (!Gossiper.instance.isEnabled())
            {
                logger.trace("Ignoring GossipDigestAck2Message because gossip is disabled");
                return;
            }

            Map<InetAddress, EndpointState> remoteEpStateMap = message.getEndpointStateMap();
            // Notify the Failure Detector
            Gossiper.instance.notifyFailureDetector(remoteEpStateMap);
            Gossiper.instance.applyStateLocally(remoteEpStateMap);

            Gossiper.instance.onNewMessageProcessed();
        }
    }

    private static class ShutdownHandler implements VerbHandlers.OneWay<EmptyPayload>
    {
        public void handle(InetAddress from, EmptyPayload payload)
        {
            if (!Gossiper.instance.isEnabled())
                logger.debug("Ignoring shutdown message from {} because gossip is disabled", from);
            else
                Gossiper.instance.markAsShutdown(from);
        }
    }
}
