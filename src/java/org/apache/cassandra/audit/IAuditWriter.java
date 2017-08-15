/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.audit;

import io.reactivex.Completable;

public interface IAuditWriter
{
    public Completable recordEvent(AuditableEvent event);
    public boolean isLoggingEnabled();
}
