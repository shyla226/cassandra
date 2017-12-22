/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.ArrayBackedPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.versioning.Version;

import static org.junit.Assert.*;

public class ReadRequestSerializationTest extends CQLTester
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
        serDeser(cmd.requestTo(to));
    }

    private void serDeser(Message<?> message) throws Throwable
    {
        ByteBuffer serialized;
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

    @Test
    public void testSerializeStaleColumnFilterSSTable() throws Throwable
    {
        testSerializeStaleColumnFilter(true);
    }

    @Test
    public void testSerializeStaleColumnFilterMemTable() throws Throwable
    {
        testSerializeStaleColumnFilter(false);
    }

    private void testSerializeStaleColumnFilter(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int)");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?);", 1, 2, 3);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?);", 2, 3, 4);
        flush(forceFlush);

        ColumnFilter columnFilter = ColumnFilter.all(currentTableMetadata());
        ReadCommand cmd = PartitionRangeReadCommand.allDataRead(currentTableMetadata(), columnFilter, FBUtilities.nowInSeconds());
        assertEquals(2, serDeser(cmd).size());

        alterTable("ALTER TABLE %s ADD d int");
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?);", 3, 4, 5, 6);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?);", 4, 5, 6, 7);
        flush(forceFlush);

        cmd = PartitionRangeReadCommand.allDataRead(currentTableMetadata(), columnFilter, FBUtilities.nowInSeconds());
        assertEquals(4, serDeser(cmd).size());
    }

    private List<Partition> serDeser(ReadCommand cmd) throws Throwable
    {
        ReadResponse response = ReadResponse.createDataResponse(cmd.executeLocally(), cmd, false).blockingGet();
        ReadVerbs.ReadVersion version = Version.last(ReadVerbs.ReadVersion.class);

        DataOutputBuffer out = new DataOutputBuffer(Math.toIntExact(ReadResponse.serializers.get(version).serializedSize(response)));
        ReadResponse.serializers.get(version).serialize(response, out);
        DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
        ReadResponse dst = ReadResponse.serializers.get(version).deserialize(in);

        return ArrayBackedPartition.create(dst.data(cmd)).toList().blockingSingle();
    }
}