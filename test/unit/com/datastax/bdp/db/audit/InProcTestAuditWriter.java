/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Completable;
import org.codehaus.jackson.map.ObjectMapper;

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

    @Override
    public boolean isLoggingEnabled()
    {
        return true;
    }

    public static String asString() throws IOException
    {
        return new ObjectMapper().writeValueAsString(events);
    }

    public static Stack<AuditableEvent> fromString(String s) throws IOException
    {
        Stack<AuditableEvent> events = new Stack<AuditableEvent>();
        List<Map<String, String>> rawEvents = new ObjectMapper().readValue(s, List.class);
        for (Map<String, String> values : rawEvents)
        {
            events.push(new AuditableEvent.Builder(values.get("user"), values.get("source"))
                            .batch(values.get("batchId"))
                            .columnFamily(values.get("columnFamily"))
                            .keyspace(values.get("keyspace"))
                            .operation(values.get("operation"))
                            .type(AuditableEventType.valueOf(values.get("type")))
                            .build());
        }
        return events;
    }
}
