/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import org.apache.cassandra.schema.TableMetadata;

/**
 * A {@link ValidationProposer} is an object in charge of generating segment validation proposal ({@link ValidationProposal})
 * for a particular table. The exact details of how those proposals are generated is proposer dependent.
 * <p>
 * We have current 2 type of proposers:
 *  - {@link ContinuousValidationProposer}: the main NodeSync proposer that ensure NodeSync-enabled tables are
 *    continuously validated.
 *  - {@link UserValidationProposer}: for user triggered NodeSync validations.
 */
abstract class ValidationProposer
{
    protected final TableState state;

    ValidationProposer(TableState state)
    {
        this.state = state;
    }

    /**
     * The service for which this proposer was created.
     */
    NodeSyncService service()
    {
        return state.service();
    }

    /**
     * The table for which this generates validation proposals.
     */
    public TableMetadata table()
    {
        return state.table();
    }

    /**
     * Whether the proposer has been cancelled before natural completion (through {@link #cancel()}).
     */
    abstract boolean isCancelled();

    /**
     * Cancel a proposer, forcing it to stop emitting new proposals even if it was not naturally finished.
     * <p>
     * This method <b>must</b> be idempotent: cancelling an already finished or already cancelled proposer should be
     * a no-op and should not, in particular, throw.
     *
     * @return {@code true} if the proposer is cancelled following the call to this method, {@code false} if the
     * proposer had already naturally finished prior to this call.
     */
    abstract boolean cancel();

}
