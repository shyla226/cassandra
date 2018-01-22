/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
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
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
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
        testSerializeStaleColumnFilter(true, true);
    }

    @Test
    public void testSerializeStaleColumnFilterMemTable() throws Throwable
    {
        testSerializeStaleColumnFilter(false, false);
    }

    @Test
    public void testSerializeStaleColumnFilterMixed() throws Throwable
    {
        testSerializeStaleColumnFilter(true, false);
    }

    private void testSerializeStaleColumnFilter(boolean firstFlush, boolean secondFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int)");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?);", 1, 2, 3);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?);", 2, 3, 4);
        flush(firstFlush);

        ColumnFilter allColumnsFilter = ColumnFilter.all(currentTableMetadata());
        ColumnMetadata column = currentTableMetadata().getColumn(ByteBufferUtil.bytes("b"));
        ColumnFilter selectionFilter = ColumnFilter.selection(currentTableMetadata().regularAndStaticColumns().without(column));

        ReadCommand cmd = PartitionRangeReadCommand.allDataRead(currentTableMetadata(), allColumnsFilter, FBUtilities.nowInSeconds());
        List<Partition> partitions = serDeser(cmd);
        assertEquals(2, partitions.size());
        assertColumns(partitions, allColumnsFilter);

        cmd = PartitionRangeReadCommand.allDataRead(currentTableMetadata(), selectionFilter, FBUtilities.nowInSeconds());
        partitions = serDeser(cmd);
        assertEquals(2, partitions.size());
        assertColumns(partitions, selectionFilter);

        alterTable("ALTER TABLE %s ADD d int");
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?);", 3, 4, 5, 6);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?);", 4, 5, 6, 7);
        flush(secondFlush);

        cmd = PartitionRangeReadCommand.allDataRead(currentTableMetadata(), allColumnsFilter, FBUtilities.nowInSeconds());
        partitions = serDeser(cmd);
        assertEquals(4, partitions.size());
        assertColumns(partitions, allColumnsFilter);

        cmd = PartitionRangeReadCommand.allDataRead(currentTableMetadata(), selectionFilter, FBUtilities.nowInSeconds());
        partitions = serDeser(cmd);
        assertEquals(4, partitions.size());
        assertColumns(partitions, selectionFilter);
    }

    @Test
    public void testSerializeStaleColumnFilterDropMemtable() throws Throwable
    {
        testSerializeStaleColumnFilterDrop(false, false);
    }

    @Test
    public void testSerializeStaleColumnFilterDropMixed() throws Throwable
    {
        testSerializeStaleColumnFilterDrop(true, false);
    }

    private void testSerializeStaleColumnFilterDrop(boolean firstFlush, boolean secondFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, d int)");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?);", 1, 2, 3, 4);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?);", 2, 3, 4, 5);
        flush(firstFlush);

        ColumnFilter allColumnsFilter = ColumnFilter.all(currentTableMetadata());
        ColumnMetadata column = currentTableMetadata().getColumn(ByteBufferUtil.bytes("b"));
        ColumnFilter selectionFilter = ColumnFilter.selection(currentTableMetadata().regularAndStaticColumns().without(column));

        ReadCommand cmd = PartitionRangeReadCommand.allDataRead(currentTableMetadata(), allColumnsFilter, FBUtilities.nowInSeconds());
        List<Partition> partitions = serDeser(cmd);
        assertEquals(2, partitions.size());
        assertColumns(partitions, allColumnsFilter);

        cmd = PartitionRangeReadCommand.allDataRead(currentTableMetadata(), selectionFilter, FBUtilities.nowInSeconds());
        partitions = serDeser(cmd);
        assertEquals(2, partitions.size());
        assertColumns(partitions, selectionFilter);

        alterTable("ALTER TABLE %s DROP d");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?);", 3, 4, 5);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?);", 4, 5, 6);
        flush(secondFlush);

        cmd = PartitionRangeReadCommand.allDataRead(currentTableMetadata(), allColumnsFilter, FBUtilities.nowInSeconds());
        partitions = serDeser(cmd);
        assertEquals(4, partitions.size());
        assertColumns(partitions, allColumnsFilter);

        cmd = PartitionRangeReadCommand.allDataRead(currentTableMetadata(), selectionFilter, FBUtilities.nowInSeconds());
        partitions = serDeser(cmd);
        assertEquals(4, partitions.size());
        assertColumns(partitions, selectionFilter);
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

    private void assertColumns(Collection<Partition> partition, ColumnFilter filter)
    {
        for (Partition rows : partition)
        {
            try (UnfilteredRowIterator it = rows.unfilteredIterator())
            {
                while (it.hasNext())
                {
                    Row row = (Row) it.next();
                    row.columns().equals(filter.fetchedColumns());
                }
            }
        }
    }
}
