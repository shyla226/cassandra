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
package org.apache.cassandra.db;

import java.net.InetAddress;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ReadRepairDecision;

/**
 * Parameters and options to perform a (distributed) read.
 */
public class ReadContext
{
    public final Keyspace keyspace;
    public final ConsistencyLevel consistencyLevel;
    @Nullable
    public final ClientState clientState;
    public final long queryStartNanos;

    public final boolean withDigests;

    public final boolean forContinuousPaging;

    private final boolean blockForAllReplicas;

    @Nullable
    public final ReadReconciliationObserver readObserver;

    public final ReadRepairDecision readRepairDecision;

    // Caches the value of how much responses we need to satisfy the CL
    private final int consistencyBlockFor;

    private ReadContext(Keyspace keyspace,
                        ConsistencyLevel consistencyLevel,
                        ClientState clientState,
                        long queryStartNanos,
                        boolean withDigests,
                        boolean forContinuousPaging,
                        boolean blockForAllReplicas,
                        ReadReconciliationObserver readObserver,
                        ReadRepairDecision readRepairDecision)
    {
        this.keyspace = keyspace;
        this.consistencyLevel = consistencyLevel;
        this.clientState = clientState;
        this.queryStartNanos = queryStartNanos;
        this.withDigests = withDigests;
        this.forContinuousPaging = forContinuousPaging;
        this.blockForAllReplicas = blockForAllReplicas;
        this.readObserver = readObserver;
        this.readRepairDecision = readRepairDecision;
        this.consistencyBlockFor = consistencyLevel.blockFor(keyspace);
    }

    /**
     * A read parameters builder suitable for the provided query.
     * <p>
     * Note that all parameters (outside of the consistency level) are optional or have defaults so that it's legit to
     * call {@code build()} on the return builder. Some queries may require some of the option though, like Paxos
     * queries that rely on the client state to be set.
     *
     * @param query the query for which to build parameters.
     * @param consistencyLevel the consistency level for the query.
     * @return a {@code ReadContext} builder for {@code query} using {@code consistencyLevel}.
     */
    public static Builder builder(ReadQuery query, ConsistencyLevel consistencyLevel)
    {
        Keyspace keyspace = Keyspace.open(query.metadata().keyspace);
        return query.applyDefaults(new Builder(keyspace, consistencyLevel));
    }

    /**
     * Returns a copy of this {@code ReadContext} with the provided modified consistency level.
     */
    public ReadContext withConsistency(ConsistencyLevel newConsistencyLevel)
    {
        return new ReadContext(keyspace,
                               newConsistencyLevel,
                               clientState,
                               queryStartNanos,
                               withDigests,
                               forContinuousPaging,
                               blockForAllReplicas,
                               readObserver,
                               readRepairDecision);
    }

    /**
     * A copy of this {@code ReadContext} suitable for doing a new query.
     * <p>
     * This basically is the same context than {@code this} but with an updated query start time. This must be used when
     * paging internally so that each new page get a full timeout.
     *
     * @param newQueryStartNanos the start time for the new query in which to use the returned context.
     * @return a newly created context, equivalent to {@code this} except for the provided query start time.
     */
    public ReadContext forNewQuery(long newQueryStartNanos)
    {
        return new ReadContext(keyspace,
                               consistencyLevel,
                               clientState,
                               newQueryStartNanos,
                               withDigests,
                               forContinuousPaging,
                               blockForAllReplicas,
                               readObserver,
                               readRepairDecision);
    }

    /**
     * Given the live replicas for the read, return the subset that should actually be queried based on the consistency
     * level and other parameters.
     *
     * @param liveEndpoints the live replicas for the read.
     * @return the subset of {@code liveEndpoints} to which the query should be sent (this can be all endpoints).
     */
    public List<InetAddress> filterForQuery(List<InetAddress> liveEndpoints)
    {
        return consistencyLevel.filterForQuery(keyspace, liveEndpoints, readRepairDecision);
    }

    /**
     * The number of responses the read should block for given the list of targets to which the read is sent to.
     * <p>
     * Note that in most case this is the same than {@link #requiredResponses()} but can be greater than the latter if
     * we want to wait on more responses that we really need too to fulfill the consistency level.
     *
     * @param targets the node to which the read is sent to.
     * @return the number of nodes the read should block for (usually defined by the consistency level, unless the
     * option to wait on all targets is set).
     */
    public int blockFor(List<InetAddress> targets)
    {
        return blockForAllReplicas ? targets.size() : consistencyBlockFor;
    }

    /**
     * The number of responses required for the read to be successful, that is the number of responses required to
     * fulfill the consistency level.
     * <p>
     * Note that this is generally the same than the value returned by {@link #blockFor} as we usually have no reason
     * to block on the read on more responses than we required, but this can be strictly less than that
     * {@link #blockFor} value if we want to give a chance for more node than strictly required to respond, but want
     * to still succeed the read (potentially on the timeout) as long as we do have enough responses. That behavior is
     * used for NodeSync in particular, where we want to wait on responses from every (live) nodes since a
     * range is truly considered validated if all replica respond, but still want to proceed validating the range as long
     * as 2 nodes answer (we simply record the range is partially validated if not all replica reply).
     *
     * @return the number of responses required to fulfill the {@link #consistencyLevel}.
     */
    public int requiredResponses()
    {
        return consistencyBlockFor;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(consistencyLevel).append('{');
        sb.append(withDigests ? "digests" : "no digests");
        sb.append(", read repair=").append(readRepairDecision);
        if (forContinuousPaging)
            sb.append(", for continuous paging");
        if (blockForAllReplicas)
            sb.append(", block on all replicas");
        if (clientState != null)
            sb.append(", has ClientState");
        if (readObserver != null)
            sb.append(", has observer");
        return sb.append('}').toString();
    }

    public static class Builder
    {
        private final Keyspace keyspace;
        private final ConsistencyLevel consistencyLevel;

        private ClientState clientState;

        private boolean withDigests;
        private boolean forContinuousPaging;
        private boolean blockForAllReplicas;
        private ReadReconciliationObserver readObserver;
        private ReadRepairDecision readRepairDecision = ReadRepairDecision.NONE;

        private Builder(Keyspace keyspace, ConsistencyLevel consistencyLevel)
        {
            this.keyspace = keyspace;
            this.consistencyLevel = consistencyLevel;
        }

        /**
         * The client state for the query. This is currently only required for Paxos queries.
         *
         * @param clientState the client state to use. Default to none if this method isn't called.
         * @return this builder.
         */
        public Builder state(ClientState clientState)
        {
            this.clientState = clientState;
            return this;
        }

        /**
         * Force the use of digests when performing the read.
         * <p>
         * By default, digests are only used for single partition queries, so this allow to force their use for
         * partition range queries (and is used by NodeSync for that purpose).
         *
         * @return this builder.
         */
        public Builder useDigests()
        {
            return useDigests(true);
        }

        /**
         * Sets whether digests should be used or not when performing the read.
         *
         * @param withDigests should pass {@code true} if digests should be used, {@code false} otherwise.
         * @return this builder.
         */
        public Builder useDigests(boolean withDigests)
        {
            this.withDigests = withDigests;
            return this;
        }

        /**
         * Indicates that the query is for continuous paging.
         * <p>
         * When this is  provided, queries that are fully local (and are CL.ONE or CL.LOCAL_ONE) are optimized by
         * returning a direct iterator to the data (as from calling executeInternal() but with the additional bits
         * needed for a full user query), which 1) leave to the caller the responsibility of not holding the iterator
         * open too long (as it holds a {@code executionController} open) and 2) bypass the dynamic snitch, read repair
         * and speculative retries. This flag is also used to record different metrics than for non-continuous queries.
         *
         * @return this builder.
         */
        public Builder forContinuousPaging()
        {
            this.forContinuousPaging = true;
            return this;
        }

        /**
         * Enables the option of blocking on all replicas.
         * <p>
         * This method allows to override the query consistency level as far as waiting for queried targets goes (namely,
         * the read will read for all queried targets). Note that this only influence the wait on queried nodes, but which
         * nodes are actually queried is still determined by the consistency level (and read-repair decision). Similarly,
         * the consistency level still controls when an unavailable exception is thrown.
         *
         * @return this builder.
         */
        public Builder blockForAllTargets()
        {
            this.blockForAllReplicas = true;
            return this;
        }

        /**
         * Sets the provided observer for the query.
         *
         * @param observer the observer to set.
         * @return this builder.
         */
        public Builder observer(ReadReconciliationObserver observer)
        {
            this.readObserver = observer;
            return this;
        }

        /**
         * Forces a particular read repair decision for the query.
         * <p>
         * By default, the read repair decision is randomly picked according to the table parameters for single
         * partition queries (in {@link #builder(ReadQuery, ConsistencyLevel)}) and disabled
         * ({@code ReadRepairDecision.NONE}) for partition range queries.
         *
         * @param readRepairDecision the read repair decision to force.
         * @return this builder.
         */
        public Builder readRepairDecision(ReadRepairDecision readRepairDecision)
        {
            this.readRepairDecision = readRepairDecision;
            return this;
        }

        public ReadContext build(long queryStartNanos)
        {
            return new ReadContext(keyspace,
                                   consistencyLevel,
                                   clientState,
                                   queryStartNanos,
                                   withDigests,
                                   forContinuousPaging,
                                   blockForAllReplicas,
                                   readObserver,
                                   readRepairDecision);
        }
    }
}
