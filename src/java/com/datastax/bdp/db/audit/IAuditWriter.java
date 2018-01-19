/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import io.reactivex.Completable;

public interface IAuditWriter
{
    Completable recordEvent(AuditableEvent event);

    boolean isLoggingEnabled();

    /**
     * Checks if this writer is ready
     * @return {@code true} if this writer is ready, {@code false} otherwise.
     */
    default boolean isSetUpComplete()
    {
        return true;
    };

    /**
     * Prepares this writer.
     */
    default void setUp()
    {
    }
}
