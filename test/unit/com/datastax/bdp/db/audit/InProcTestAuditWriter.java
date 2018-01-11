/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.util.Stack;

import io.reactivex.Completable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InProcTestAuditWriter implements IAuditWriter
{
    private static final Logger logger = LoggerFactory.getLogger(InProcTestAuditWriter.class);

    private static Stack<AuditableEvent> events = new Stack<AuditableEvent>();

    @Override
    public Completable recordEvent(AuditableEvent event)
    {
        logger.debug("Record event: {}", event);

        events.add(event);
        return Completable.complete();
    }

    public static Stack<AuditableEvent> getEvents()
    {
        return events;
    }

    public static void reset()
    {
        events.clear();
    }

    public static AuditableEvent lastEvent()
    {
        AuditableEvent lastEvent = events.peek();

        logger.debug("lastEvent: {}", lastEvent);

        return lastEvent;
    }

    public static boolean hasEvents()
    {
        return ! events.isEmpty();
    }
}
