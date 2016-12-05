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

import java.net.InetAddress;
import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Groups multiple targets for a message, potentially using options like grouping targets per-DC so we can use request
 * forwarding for remote DCs.
 */
abstract class MessageTargets
{
    private static final InetAddress local = FBUtilities.getBroadcastAddress();

    private final boolean hasLocal;

    private MessageTargets(boolean hasLocal)
    {
        this.hasLocal = hasLocal;
    }

    static MessageTargets createSimple(Collection<InetAddress> endpoints)
    {
        if (endpoints instanceof List)
            return createSimple((List<InetAddress>)endpoints);

        List<InetAddress> remotes = new ArrayList<>(endpoints.size());
        boolean hasLocal = false;
        for (InetAddress endpoint : endpoints)
        {
            if (endpoint.equals(local))
                hasLocal = true;
            else
                remotes.add(endpoint);
        }
        return new Simple(hasLocal, remotes);
    }

    private static MessageTargets createSimple(List<InetAddress> endpoints)
    {
        for (int i = 0; i < endpoints.size(); i++)
        {
            if (endpoints.get(i).equals(local))
            {
                ArrayList<InetAddress> remotes = new ArrayList<>(endpoints.size() - 1);
                remotes.addAll(endpoints.subList(0, i));
                remotes.addAll(i, endpoints.subList(i + 1, endpoints.size()));
                return new Simple(true, remotes);
            }
        }
        return new Simple(false, endpoints);
    }

    static MessageTargets createWithFowardingForRemoteDCs(Collection<InetAddress> endpoints, String localDc)
    {
        boolean hasLocal = false;
        List<InetAddress> localDcRemotes = null;
        Map<String, WithForwards> remoteDcsRemotes = null;

        for (InetAddress endpoint : endpoints)
        {
            if (endpoint.equals(local))
            {
                hasLocal = true;
                continue;
            }

            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);

            if (localDc.equals(dc))
            {
                if (localDcRemotes == null)
                    // most DCs will have <= 5 replicas, and we can assume token-aware routing is used if performance is any concern
                    localDcRemotes = new ArrayList<>(4);

                localDcRemotes.add(endpoint);
            }
            else
            {
                WithForwards dcRemotes = remoteDcsRemotes == null ? null : remoteDcsRemotes.get(dc);
                if (dcRemotes == null)
                {
                    dcRemotes = new WithForwards(endpoint);

                    if (remoteDcsRemotes == null)
                        remoteDcsRemotes = new HashMap<>();

                    remoteDcsRemotes.put(dc, dcRemotes);
                }
                else
                {
                    dcRemotes.forwards.add(endpoint);
                }
            }
        }

        return new WithForwarding(hasLocal,
                                  localDcRemotes == null ? Collections.emptyList() : localDcRemotes,
                                  remoteDcsRemotes == null ? Collections.emptyList() : remoteDcsRemotes.values());
    }

    boolean hasLocal()
    {
        return hasLocal;
    }

    abstract boolean hasForwards();

    abstract Iterable<InetAddress> nonForwardingRemotes();

    abstract Iterable<WithForwards> remotesWithForwards();

    static class WithForwards
    {
        final InetAddress target;
        final List<InetAddress> forwards = new ArrayList<>(4); // Most DC will have RF <= 5 (so target + 4 forwards)

        private WithForwards(InetAddress target)
        {
            this.target = target;
        }

        @Override
        public String toString()
        {
            return String.format("%s (forwards to: %s)", target, forwards);
        }
    }

    private static class Simple extends MessageTargets
    {
        private final List<InetAddress> remotes;

        Simple(boolean hasLocal, List<InetAddress> remotes)
        {
            super(hasLocal);
            this.remotes = remotes;
        }

        boolean hasForwards()
        {
            return false;
        }

        Iterable<InetAddress> nonForwardingRemotes()
        {
            return remotes;
        }

        Iterable<WithForwards> remotesWithForwards()
        {
            return Collections.emptyList();
        }

        @Override
        public String toString()
        {
            return hasLocal() ? "local + " + remotes : remotes.toString();
        }
    }

    private static class WithForwarding extends MessageTargets
    {
        private final List<InetAddress> nonForwardingRemotes;
        private final Collection<WithForwards> remotesWithForwards;

        WithForwarding(boolean hasLocal,
                       List<InetAddress> nonForwardingRemotes,
                       Collection<WithForwards> remotesWithForwards)
        {
            super(hasLocal);
            this.nonForwardingRemotes = nonForwardingRemotes;
            this.remotesWithForwards = remotesWithForwards;
        }

        boolean hasForwards()
        {
            return true;
        }

        Iterable<InetAddress> nonForwardingRemotes()
        {
            return nonForwardingRemotes;
        }

        Iterable<WithForwards> remotesWithForwards()
        {
            return remotesWithForwards;
        }

        @Override
        public String toString()
        {
            return String.format("%slocalDc=%s + remoteDc=%s", hasLocal() ? "local + " : "", nonForwardingRemotes, remotesWithForwards);
        }
    }
}
