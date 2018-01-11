/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

/**
 * Filter used to determine which events should be logged.
 */
@FunctionalInterface
public interface IAuditFilter
{
    /**
     * Checks if the specified event must be logged.
     * @param event the event
     * @return {@code true} if the event must be logged, {@code false} otherwise.
     */
    boolean accept(AuditableEvent event);
}
