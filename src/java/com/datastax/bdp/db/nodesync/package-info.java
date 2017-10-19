/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

/**
 * Implements the NodeSync service.
 * <p>
 * NodeSync is a service ({@link com.datastax.bdp.db.nodesync.NodeSyncService}) that continuously and iteratively reads
 * the content of the node data, compares it to that of other replicas, and repairs any inconsistency found. In other
 * words, it continuously validate that node are in sync and repair any inconsistency found.
 * <p>
 * To perform his work and for each table[1], NodeSync service divides up the ranges it is a replica for into reasonably
 * sized "segments" ({@link com.datastax.bdp.db.nodesync.Segment}). It then validate segments by fully reading the
 * content of the range it represents from all alive replica and relying on the normal read-repair machinery to detect
 * and repair any inconsistencies (the next segment to validate is determined by {@link com.datastax.bdp.db.nodesync.ValidationScheduler}
 * which creates {@link com.datastax.bdp.db.nodesync.Validator} and pass then to {@link com.datastax.bdp.db.nodesync.ValidationExecutor}
 * for execution). Once a segment is validated, the time and exact result of that validation is recorded in the
 * {@link org.apache.cassandra.repair.SystemDistributedKeyspace#NodeSyncStatus} table. That information is then fed back
 * to the scheduler to decide on a priority for segments to validate.
 *
 * [1]: NodeSync can be enabled/disabled on a per-table basis (though the CQL table options stored in
 * {@link org.apache.cassandra.schema.NodeSyncParams}). Any table on which NodeSync is disabled is simply ignored by it.
 */
package com.datastax.bdp.db.nodesync;

