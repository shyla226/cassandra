/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.audit;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.zip.Deflater;

import com.datastax.driver.core.Row;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class AuditLoggingTestSupport
{
    public static ByteBuffer compress(String input)
    {
        byte[] output = new byte[100];
        Deflater compresser = new Deflater();
        compresser.setInput(input.getBytes());
        compresser.finish();
        int len = compresser.deflate(output);
        byte[] compressed = new byte[len];
        System.arraycopy(output, 0, compressed, 0, len);
        return ByteBuffer.wrap(compressed);
    }

    public static void assertEventProperties(AuditableEvent event, AuditableEventType type,
                                             String keyspace, String columnFamily)
    {
        assertEquals(columnFamily, event.getColumnFamily());
        assertEquals(keyspace, event.getKeyspace());
        assertEquals(type, event.getType());
    }

    public static void assertEventProperties(AuditableEvent event, AuditableEventType type,
                                             String keyspace, String columnFamily, String operation)
    throws Exception
    {
        assertEquals(columnFamily, event.getColumnFamily());
        assertEquals(keyspace, event.getKeyspace());
        assertEquals(type, event.getType());
        // whitespace in batch cql strings are normalised by CQL handler impls, so this test
        // is sensitive. If the assertion fails, check the whitespace in the tested string
        // as what is logged may not be exactly the same as the statement in the case of
        // BATCH statements
        assertEquals(operation, event.getOperation());
    }

    public static void assertAllEventsInSameBatch(List<AuditableEvent> events)
    {
        UUID batchId = events.get(0).getBatchId();
        assertNotNull(batchId);
        for (AuditableEvent event : events )
        {
            assertEquals(batchId, event.getBatchId());
        }
    }

    public static void assertMatchingEventInList(List<AuditableEvent> events, AuditableEventType type,
                                                 String keyspace, String columnFamily, String operation)
    {
        for (AuditableEvent event : events)
        {
            if (event.getType() == type)
            {
                assertEquals(operation, event.getOperation());
                assertEquals(keyspace, event.getKeyspace());
                assertEquals(columnFamily, event.getColumnFamily());
                return;
            }
        }
        fail("Didn't find matching event");
    }

    public static Stack<AuditableEvent> deserializeLogEvents(String events)
    {
        try
        {
            return InProcTestAuditWriter.fromString(events);
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static class InitialiseTestLogging implements Callable<String>
    {
        @Override
        public String call() throws Exception
        {
            InProcTestAuditWriter.reset();
            return null;
        }
    }

    public static class GetLogEvents implements Callable<String>
    {
        @Override
        public String call() throws Exception
        {
            return InProcTestAuditWriter.asString();
        }
    }

    public static int entryExists(List<Row> rows, AuditableEventType eventType, String user)
    {
        return entryExists(rows, eventType, user, null);
    }

    public static int entryExists(List<Row> rows, AuditableEventType eventType, String user,
                                  String address)
    {
        int occurrences = 0;

        for (Row row: rows)
        {
            String rowUser = row.getString("username");
            AuditableEventType rowType = AuditableEventType.valueOf(row.getString("type"));
            String source = row.getString("source");

            if (rowUser.equals(user) && rowType.equals(eventType))
            {
                if( address != null)
                {
                    if( source.equals(address) )
                        occurrences++;
                }
                else
                {
                    occurrences++;
                }
            }
        }

        return occurrences;
    }
}
