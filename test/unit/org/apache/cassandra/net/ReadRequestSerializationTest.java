/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.net;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.Assert.assertEquals;


public class ReadRequestSerializationTest extends CQLTester
{
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
        ColumnDefinition column = currentTableMetadata().getColumnDefinition(ByteBufferUtil.bytes("b"));
        ColumnFilter selectionFilter = ColumnFilter.selection(currentTableMetadata().partitionColumns().without(column));

        ReadCommand cmd = PartitionRangeReadCommand.allDataRead(currentTableMetadata(), allColumnsFilter, FBUtilities.nowInSeconds());
        List<ImmutableBTreePartition> partitions = serDeser(cmd);
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

    private List<ImmutableBTreePartition> serDeser(ReadCommand cmd) throws Throwable
    {
        ReadResponse response = ReadResponse.createDataResponse(cmd.executeLocally(ReadExecutionController.empty()), cmd);
        int version = MessagingService.current_version;

        DataOutputBuffer out = new DataOutputBuffer(Math.toIntExact(ReadResponse.serializer.serializedSize(response, version)));
        ReadResponse.serializer.serialize(response, out, version);
        DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
        ReadResponse dst = ReadResponse.serializer.deserialize(in, version);

        List<ImmutableBTreePartition> partitions = new ArrayList<>();
        try (UnfilteredPartitionIterator iter = dst.makeIterator(cmd))
        {
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    partitions.add(ImmutableBTreePartition.create(partition));
                }
            }
        }
        return partitions;
    }

    private void assertColumns(Collection<ImmutableBTreePartition> partition, ColumnFilter filter)
    {
        for (Partition rows : partition)
        {
            try (UnfilteredRowIterator it = rows.unfilteredIterator())
            {
                while (it.hasNext())
                {
                    Row row = (Row) it.next();
                    assertEquals(PartitionColumns.builder().addAll(row.columns()).build(), filter.fetchedColumns());
                }
            }
        }
    }
}
