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
     * Prepares this writer.
     */
    default void setUp()
    {
        
    }
}
