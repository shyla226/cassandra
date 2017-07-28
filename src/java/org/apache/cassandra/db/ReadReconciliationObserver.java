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
import java.util.Collection;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;

/**
 * An interface that allows to capture what happens while reconciling multiple replica response on a read.
 * <p>
 * An implementation of this interface can be passed to reads through its {@link ReadContext} in order to get feedback
 * on replica response reconciliation.
 * <p>
 * This is notably used by NodeSync to maintain its metrics.
 */
public interface ReadReconciliationObserver
{
    /**
     * Called just before any reconciliation happens with the nodes whose response was received.
     *
     * @param responded the nodes whose response has been received.
     */
    public void responsesReceived(Collection<InetAddress> responded);

    /**
     * Calls if the read uses digests and the digests match.
     * Note that if this is called, then none of the methods in the remaining of this interface will be called since
     * no merging/reconciliation happens.
     */
    public void onDigestMatch();

    /**
     * Calls if the read uses digests and a mismatch happens.
     * Note that the rest of the methods of this interface are only called
     */
    public void onDigestMismatch();

    /**
     * Called on every new reconciled partition.
     *
     * @param partitionKey the partition key.
     */
    public void onPartition(DecoratedKey partitionKey);

    /**
     * Called every time a partition deletion is read.
     *
     * @param deletion the merged deletion.
     * @param isConsistent {@code true} if all replicas that responded where consistent on that partition deletion.
     */
    public void onPartitionDeletion(DeletionTime deletion, boolean isConsistent);

    /**
     * Called on every row read.
     *
     * @param row the merged row.
     * @param isConsistent {@code true} if all replicas that responded where consistent on that row.
     */
    public void onRow(Row row, boolean isConsistent);

    /**
     * Called on every range tombstone marker read.
     *
     * @param marker the merged range tombstone marker. This can be {@code null} in the case where there was a marker
     *               on one replica but none in the merge output due to shadowing.
     * @param isConsistent {@code true} if all replicas that responded where consistent on that range tombstone marker.
     */
    public void onRangeTombstoneMarker(RangeTombstoneMarker marker, boolean isConsistent);

    /**
     * Called for every read-repair update sent to a replica.
     *
     * @param endpoint the endpoint to which the read-repair update is sent.
     * @param repair the update sent to {@code endpoint} to repair it.
     */
    public void onRepair(InetAddress endpoint, PartitionUpdate repair);
}
