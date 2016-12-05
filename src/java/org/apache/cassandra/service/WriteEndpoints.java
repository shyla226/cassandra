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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.view.ViewUtils;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.FBUtilities;

/**
 * The endpoints to use for a particular write operation.
 * <p>
 * This groups both natural endpoints for the write, and any pending ones.
 */
public class WriteEndpoints implements Iterable<InetAddress>
{
    private final Keyspace keyspace;

    private final List<InetAddress> natural;
    private final Collection<InetAddress> pending;

    private final List<InetAddress> live;
    private final List<InetAddress> dead;

    private WriteEndpoints(Keyspace keyspace,
                           List<InetAddress> natural,
                           Collection<InetAddress> pending,
                           List<InetAddress> live,
                           List<InetAddress> dead)
    {
        this.keyspace = keyspace;
        this.natural = natural;
        this.pending = pending;
        this.live = live;
        this.dead = dead;
    }

    private WriteEndpoints(Keyspace keyspace, List<InetAddress> natural, Collection<InetAddress> pending)
    {
        this.keyspace = keyspace;
        this.natural = natural;
        this.pending = pending;

        // Most of the time, everyone or almost everyone will be live. If not,
        // we have other problems that over-allocation of this list.
        List<InetAddress> tmpLive = new ArrayList<>(natural.size() + pending.size());
        // The reverse is true, so we allocate a dead list only if we have to.
        // When we do, we assume we won't have more than 2 deads. Again, if
        // we're wrong, we have bigger problems that resizing the list.
        List<InetAddress> tmpDead = null;

        for (InetAddress endpoint : Iterables.concat(natural, pending))
        {
            if (FailureDetector.instance.isAlive(endpoint))
            {
                tmpLive.add(endpoint);
            }
            else
            {
                if (tmpDead == null)
                    tmpDead = new ArrayList<>(2);
                tmpDead.add(endpoint);
            }
        }

        this.live = Collections.unmodifiableList(tmpLive);
        this.dead = tmpDead == null ? Collections.emptyList() : Collections.unmodifiableList(tmpDead);
    }

    /**
     * Compute the write endpoints for a write on the provided partition.
     *
     * @param keyspace the name of the keyspace written.
     * @param partitionKey the key of the partition written to.
     * @return the endpoints to write for a mutation on {@code partitionKey} in
     * {@code keyspace}.
     */
    public static WriteEndpoints compute(String keyspace, DecoratedKey partitionKey)
    {
        Token tk = partitionKey.getToken();
        List<InetAddress> natural = StorageService.instance.getNaturalEndpoints(keyspace, tk);
        Collection<InetAddress> pending = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspace);
        return new WriteEndpoints(Keyspace.open(keyspace), natural, pending);
    }

    /**
     * Compute the write endpoints for the provided mutation.
     *
     * @param mutation the mutation to compute endpoints for.
     * @return the endpoints to write {@code mutation} on.
     */
    public static WriteEndpoints compute(IMutation mutation)
    {
        return compute(mutation.getKeyspaceName(), mutation.key());
    }

    /**
     * Compute the write endpoints for the provided Paxos Commit.
     *
     * @param commit the commit to compute endpoints for.
     * @return the endpoints to write {@code commit} on.
     */
    public static WriteEndpoints compute(Commit commit)
    {
        return compute(commit.update.metadata().keyspace, commit.update.partitionKey());
    }

    /**
     * Creates a {@code WriteEndpoints} object that has the provided endpoints
     * as natural and live endpoints, and no pending or dead endpoints.
     *
     * @param live the endpoints to use as natural for the created object. They
     * are assumed live.
     */
    public static WriteEndpoints withLive(Keyspace keyspace, Iterable<InetAddress> live)
    {
        List<InetAddress> copy = ImmutableList.copyOf(live);
        return new WriteEndpoints(keyspace, copy, Collections.emptyList(), copy, Collections.emptyList());
    }

    /**
     * Compute the write endpoints for the provided view mutation. Specifically, the natural endpoints
     * are either empty or only contain the paired endpoint, see
     * {@link ViewUtils#getViewNaturalEndpoint(String, Token, Token)}.
     *
     * @param baseToken - the token of the base table mutation partition key
     * @param mutation - the view mutation to be written
     *
     * @return the endpoints to write the mutation to
     */
    public static WriteEndpoints computeForView(Token baseToken, Mutation mutation)
    {
        String keyspace = mutation.getKeyspaceName();
        Token tk = mutation.key().getToken();
        Optional<InetAddress> pairedEndpoint = ViewUtils.getViewNaturalEndpoint(keyspace, baseToken, tk);
        List<InetAddress> natural = pairedEndpoint.isPresent()
                                 ? Collections.singletonList(pairedEndpoint.get())
                                 : Collections.emptyList();
        Collection<InetAddress> pending = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspace);
        return new WriteEndpoints(Keyspace.open(keyspace), natural, pending);
    }

    public Keyspace keyspace()
    {
        return keyspace;
    }

    /**
     * Return a copy of this object that only contains the nodes that are in the
     * local DC.
     *
     * @return the restriction of the endpoints of this object to the local DC.
     */
    public WriteEndpoints restrictToLocalDC()
    {
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        String localDc = snitch.getDatacenter(FBUtilities.getBroadcastAddress());
        Predicate<InetAddress> isLocalDc = host -> localDc.equals(snitch.getDatacenter(host));
        return new WriteEndpoints(keyspace,
                                  ImmutableList.copyOf(Iterables.filter(natural, isLocalDc)),
                                  pending.isEmpty() // pending empty is frequent enough
                                     ? Collections.emptyList()
                                     : ImmutableList.copyOf(Iterables.filter(pending, isLocalDc)));
    }

    /**
     * Returns a copy of this object that doesn't contains the localhost.
     * <p>
     * <b>WARNING:</b> this methods assumes that the localhost is part of the
     * <i>natural</i> endpoints (<i>not</i> the pending ones).
     */
    public WriteEndpoints withoutLocalhost()
    {
        InetAddress localhost = FBUtilities.getBroadcastAddress();
        if (natural.size() == 1 && pending.isEmpty())
        {
            assert natural.get(0).equals(localhost);
            return new WriteEndpoints(keyspace,
                                      Collections.emptyList(),
                                      Collections.emptyList(),
                                      Collections.emptyList(),
                                      Collections.emptyList());

        }

        Predicate<InetAddress> notLocalhost = host -> !host.equals(localhost);

        List<InetAddress> newNatural = new ArrayList<>(natural.size() - 1);
        Iterables.addAll(newNatural, Iterables.filter(natural, notLocalhost));
        List<InetAddress> newLive = new ArrayList<>(live.size() - 1);
        Iterables.addAll(newLive, Iterables.filter(live, notLocalhost));
        return new WriteEndpoints(keyspace, newNatural, pending, newLive, dead);
    }

    /**
     * Check that there are enough live endpoints to possibly fulfill the
     * provided consistency level.
     */
    public void checkAvailability(ConsistencyLevel consistency) throws UnavailableException
    {
        consistency.assureSufficientLiveNodes(keyspace, live);
    }

    public boolean isEmpty()
    {
        return count() == 0;
    }

    public int count()
    {
        return natural.size() + pending.size();
    }

    public int naturalCount()
    {
        return natural.size();
    }

    public int pendingCount()
    {
        return pending.size();
    }

    public int liveCount()
    {
        return live.size();
    }

    public Iterator<InetAddress> iterator()
    {
        return Iterators.concat(natural.iterator(), pending.iterator());
    }

    public List<InetAddress> natural()
    {
        return natural;
    }

    public Collection<InetAddress> pending()
    {
        return pending;
    }

    public List<InetAddress> live()
    {
        return live;
    }

    public List<InetAddress> dead()
    {
        return dead;
    }

    @Override
    public String toString()
    {
        return live.toString();
    }
}
