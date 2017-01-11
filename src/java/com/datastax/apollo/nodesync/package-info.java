/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

/**
 * Implements the NodeSync service.
 * <p>
 * NodeSync is a service ({@link com.datastax.apollo.nodesync.NodeSyncService}) that continuously and iteratively reads
 * the content of the node data, compares it to that of other replicas, and repairs any inconsistency found. In other
 * words, it continuously validate that node are in sync and repair any inconsistency found.
 * <p>
 * To perform his work and for each table[1], NodeSync service divides up the ranges it is a replica for into reasonably
 * sized "segments" ({@link com.datastax.apollo.nodesync.Segment}). It then goes through segments (using
 * {@link com.datastax.apollo.nodesync.ValidationScheduler} to provide segments {@link com.datastax.apollo.nodesync.Validator}
 * and {@link com.datastax.apollo.nodesync.ValidationExecutor} to run those validator), fully reading the content of the
 * range represented by each segment from all alive replica and relying on the normal read-repair machinery to detect
 * and repair any inconsistencies. Once a segment is validated, the time and exact result of that validation is recorded
 * in the {@link org.apache.cassandra.repair.SystemDistributedKeyspace#NodeSyncStatus} table. That information is then
 * fed back to the scheduler to decide on a priority for segments to validate.
 *
 * [1]: NodeSync can be enabled/disabled on a per-table basis (though the CQL table options stored in
 * {@link org.apache.cassandra.schema.NodeSyncParams}). Any table on which NodeSync is disabled is simply ignored by it.
 */
package com.datastax.apollo.nodesync;

