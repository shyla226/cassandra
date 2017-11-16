/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.*;

public class MessageSerializerTest extends CQLTester
{
    private static final long baseTimestampMillis = System.currentTimeMillis();
    private static final MessagingVersion last = MessagingVersion.values()[MessagingVersion.values().length - 1];
    private static final MessageSerializer serializer = new MessageSerializer(last, baseTimestampMillis);
    private static InetAddress from = FBUtilities.getBroadcastAddress();
    private static InetAddress to = from;

    @Test
    public void testReadSerDe() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY)");
        TableMetadata table = currentTableMetadata();

        ReadCommand cmd = PartitionRangeReadCommand.allDataRead(table, FBUtilities.nowInSeconds());
        testMessage(cmd.requestTo(to));
    }

    private void testMessage(Message<?> message) throws Throwable
    {
        ByteBuffer serialized = null;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(message, out);
            serialized = out.asNewBuffer();
        }

        long serializedSize = serializer.serializedSize(message);
        // Check we did write 'serializedSize' bytes
        assertEquals(serializedSize, serialized.remaining());

        try (DataInputBuffer in = new DataInputBuffer(serialized, false))
        {
            TrackedDataInputPlus trackedIn = new TrackedDataInputPlus(in);
            Message<?> deserialized = serializer.deserialize(trackedIn, (int)serializedSize, from);
            // First check the deserialized message is the same the serialized one
            assertEquals(message, deserialized);
            // Second make sure we did read exactly 'serializedSize' bytes
            assertEquals(serializedSize, trackedIn.getBytesRead());
        }
    }
}