/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

/**
 * Exception that can be thrown by validations when the state for a table changes in a way that invalidates in-flight
 * validations (mostly to avoid continuing validations that we are not going to save).
 * <p>
 * Such invalidation only happens on topology changes and decrease of the NodeSync depth, so it should be rare overall.
 */
class InvalidatedNodeSyncStateException extends RuntimeException
{
}
