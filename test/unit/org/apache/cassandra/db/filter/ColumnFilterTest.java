/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.filter;

import com.google.common.collect.Iterables;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ColumnFilterTest
{
    final static ColumnFilter.Serializer serializer = new ColumnFilter.Serializer();

    @Test
    public void testColumnFilterSerialisationRoundTrip() throws Exception
    {
        CFMetaData metadata = CFMetaData.Builder.create("ks", "table")
                                                .withPartitioner(Murmur3Partitioner.instance)
                                                .addPartitionKey("pk", Int32Type.instance)
                                                .addClusteringColumn("ck", Int32Type.instance)
                                                .addRegularColumn("v1", Int32Type.instance)
                                                .addRegularColumn("v2", Int32Type.instance)
                                                .addRegularColumn("v3", Int32Type.instance)
                                                .build();

        ColumnDefinition v1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("v1"));

        testRoundTrip(ColumnFilter.all(metadata), metadata, MessagingService.VERSION_30);
        testRoundTrip(ColumnFilter.all(metadata), metadata, MessagingService.VERSION_3014);

        testRoundTrip(ColumnFilter.selection(metadata.partitionColumns().without(v1)), metadata, MessagingService.VERSION_30);
        testRoundTrip(ColumnFilter.selection(metadata.partitionColumns().without(v1)), metadata, MessagingService.VERSION_3014);

        testRoundTrip(ColumnFilter.selection(metadata, metadata.partitionColumns().without(v1)), metadata, MessagingService.VERSION_30);
        testRoundTrip(ColumnFilter.selection(metadata, metadata.partitionColumns().without(v1)), metadata, MessagingService.VERSION_3014);

        // Table with static column
        metadata = CFMetaData.Builder.create("ks", "table")
                                .withPartitioner(Murmur3Partitioner.instance)
                                .addPartitionKey("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addStaticColumn("s1", Int32Type.instance)
                                .addRegularColumn("v1", Int32Type.instance)
                                .addRegularColumn("v2", Int32Type.instance)
                                .addRegularColumn("v3", Int32Type.instance)
                                .build();

        v1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("v1"));
        ColumnDefinition s1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("s1"));

        ColumnFilter columnFilter = ColumnFilter.all(metadata);
        testRoundTrip(columnFilter, metadata, MessagingService.VERSION_30);
        testRoundTrip(columnFilter, metadata, MessagingService.VERSION_3014);


        testRoundTrip(ColumnFilter.selection(metadata.partitionColumns().without(v1)), metadata, MessagingService.VERSION_30);
        testRoundTrip(ColumnFilter.selection(metadata.partitionColumns().without(v1)), metadata, MessagingService.VERSION_3014);

        testRoundTrip(ColumnFilter.selection(metadata.partitionColumns().without(v1).without(s1)), metadata, MessagingService.VERSION_30);
        testRoundTrip(ColumnFilter.selection(metadata.partitionColumns().without(v1).without(s1)), metadata, MessagingService.VERSION_3014);

        columnFilter = ColumnFilter.selection(metadata, metadata.partitionColumns().without(v1));

        testRoundTrip(columnFilter, metadata, MessagingService.VERSION_30);
        testRoundTrip(columnFilter, metadata, MessagingService.VERSION_3014);

        columnFilter = ColumnFilter.selection(metadata, metadata.partitionColumns().without(v1).without(s1));
        testRoundTrip(columnFilter, metadata, MessagingService.VERSION_30);
        testRoundTrip(columnFilter, metadata, MessagingService.VERSION_3014);
    }

    @Test
    public void testColumnFilterConstruction()
    {
        // all regular column
        CFMetaData metadata = CFMetaData.Builder.create("ks", "table")
                                              .withPartitioner(Murmur3Partitioner.instance)
                                              .addPartitionKey("pk", Int32Type.instance)
                                              .addClusteringColumn("ck", Int32Type.instance)
                                              .addRegularColumn("v1", Int32Type.instance)
                                              .addRegularColumn("v2", Int32Type.instance)
                                              .addRegularColumn("v3", Int32Type.instance)
                                              .build();
        ColumnFilter columnFilter = ColumnFilter.all(metadata);
        assertTrue(columnFilter.fetchesAllColumns());
        assertEquals(metadata.partitionColumns(), columnFilter.fetchedColumns());
        assertEquals(metadata.partitionColumns(), columnFilter.queriedColumns());
        assertEquals("*", columnFilter.toString());

        PartitionColumns queried = new PartitionColumns.Builder()
                                   .add(metadata.getColumnDefinition(ByteBufferUtil.bytes("v1"))).build();
        columnFilter = ColumnFilter.selection(queried);
        assertFalse(columnFilter.fetchesAllColumns());
        assertEquals(queried, columnFilter.fetchedColumns());
        assertEquals(queried, columnFilter.queriedColumns());
        assertEquals("v1", columnFilter.toString());

        // with static column
        metadata = CFMetaData.Builder.create("ks", "table")
                                .withPartitioner(Murmur3Partitioner.instance)
                                .addPartitionKey("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addStaticColumn("sc1", Int32Type.instance)
                                .addStaticColumn("sc2", Int32Type.instance)
                                .addRegularColumn("v1", Int32Type.instance)
                                .build();

        columnFilter = ColumnFilter.all(metadata);
        assertTrue(columnFilter.fetchesAllColumns());
        assertEquals(metadata.partitionColumns(), columnFilter.fetchedColumns());
        assertEquals(metadata.partitionColumns(), columnFilter.queriedColumns());
        assertEquals("*", columnFilter.toString());

        queried = new PartitionColumns.Builder()
                  .add(metadata.getColumnDefinition(ByteBufferUtil.bytes("v1"))).build();
        columnFilter = ColumnFilter.selection(metadata, queried);
        assertEquals("*", columnFilter.toString());

        // only static
        metadata = CFMetaData.Builder.create("ks", "table")
                                .withPartitioner(Murmur3Partitioner.instance)
                                .addPartitionKey("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addStaticColumn("sc", Int32Type.instance)
                                .build();

        columnFilter = ColumnFilter.all(metadata);
        assertTrue(columnFilter.fetchesAllColumns());
        assertEquals(metadata.partitionColumns(), columnFilter.fetchedColumns());
        assertEquals(metadata.partitionColumns(), columnFilter.queriedColumns());
        assertEquals("*", columnFilter.toString());

        // with collection type
        metadata = CFMetaData.Builder.create("ks", "table")
                                .withPartitioner(Murmur3Partitioner.instance)
                                .addPartitionKey("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addRegularColumn("v1", Int32Type.instance)
                                .addRegularColumn("set", SetType.getInstance(Int32Type.instance, true))
                                .build();

        columnFilter = ColumnFilter.all(metadata);
        assertTrue(columnFilter.fetchesAllColumns());
        assertEquals(metadata.partitionColumns(), columnFilter.fetchedColumns());
        assertEquals(metadata.partitionColumns(), columnFilter.queriedColumns());
        assertEquals("*", columnFilter.toString());

        columnFilter = ColumnFilter.selectionBuilder().add(metadata.getColumnDefinition(ByteBufferUtil.bytes("v1")))
                                   .select(metadata.getColumnDefinition(ByteBufferUtil.bytes("set")), CellPath.create(ByteBufferUtil.bytes(1)))
                                   .build();
        assertEquals("set[1], v1", columnFilter.toString());
    }

    @Test
    public void testColumnFilterSelectionTypes()
    {
        CFMetaData metadata = CFMetaData.Builder.create("ks", "table")
                                              .withPartitioner(Murmur3Partitioner.instance)
                                              .addPartitionKey("pk", Int32Type.instance)
                                              .addStaticColumn("s1", Int32Type.instance)
                                              .addClusteringColumn("c1", Int32Type.instance)
                                              .addClusteringColumn("c2", Int32Type.instance)
                                              .addRegularColumn("complex1", SetType.getInstance(Int32Type.instance, true))
                                              .addRegularColumn("v1", Int32Type.instance)
                                              .addRegularColumn("v2", Int32Type.instance)
                                              .addRegularColumn("v3", Int32Type.instance)
                                              .build();

        ColumnDefinition v1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("v1"));
        ColumnDefinition s1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("s1"));
        ColumnDefinition complex1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("complex1"));

        // filter fetches all
        ColumnFilter filter = ColumnFilter.all(metadata);
        assertEquals(metadata.partitionColumns(), filter.fetchedColumns());
        assertEquals(metadata.partitionColumns(), filter.queriedColumns());
        assertEquals("*", filter.toString());
        assertTrue(filter.fetchesAllColumns());
        for (ColumnDefinition c : Iterables.concat(metadata.partitionColumns().regulars,
                                                 metadata.partitionColumns().statics))
        {
            assertTrue(filter.fetches(c));
            assertTrue(filter.fetchedColumnIsQueried(c));
        }

        // filter fetches a selection only
        PartitionColumns selectedColumns = PartitionColumns.of(v1);
        filter = ColumnFilter.selection(selectedColumns);
        assertEquals(selectedColumns, filter.fetchedColumns());
        assertEquals(selectedColumns, filter.queriedColumns());
        assertEquals("v1", filter.toString());
        assertFalse(filter.fetchesAllColumns());
        for (ColumnDefinition c : Iterables.concat(metadata.partitionColumns().regulars,
                                                 metadata.partitionColumns().statics))
        {
            if (c.equals(v1))
            {
                assertTrue(filter.fetches(c));
                assertTrue(filter.fetchedColumnIsQueried(c));
            }
            else
            {
                assertFalse(filter.fetches(c));
            }
        }

        // filter fetches all but queries a selection only
        filter = ColumnFilter.selection(metadata, selectedColumns);
        assertEquals(metadata.partitionColumns(), filter.fetchedColumns());
        assertEquals(selectedColumns, filter.queriedColumns());
        assertEquals("*", filter.toString());
        assertFalse(filter.allFetchedColumnsAreQueried());
        assertTrue(filter.fetchesAllColumns());
        assertTrue(filter.fetches(s1));
        for (ColumnDefinition c : metadata.partitionColumns().regulars)
        {
            assertTrue(filter.fetches(c));
            if (c.equals(v1))
            {
                assertTrue(filter.fetchedColumnIsQueried(c));
            }
            else
            {
                assertFalse(filter.fetchedColumnIsQueried(c));
            }
        }

        // filter queries static columns
        selectedColumns = PartitionColumns.of(s1);
        filter = ColumnFilter.selection(selectedColumns);
        assertEquals(selectedColumns, filter.fetchedColumns());
        assertEquals(selectedColumns, filter.queriedColumns());
        assertEquals("s1", filter.toString());
        assertTrue(filter.allFetchedColumnsAreQueried());
        assertFalse(filter.fetchesAllColumns());
        for (ColumnDefinition c : Iterables.concat(metadata.partitionColumns().regulars,
                                                 metadata.partitionColumns().statics))
        {
            if (c.equals(s1))
            {
                assertTrue(filter.fetches(c));
                assertTrue(filter.fetchedColumnIsQueried(c));
            }
            else
            {
                assertFalse(filter.fetches(c));
            }
        }

        // filter queries complex columns
        selectedColumns = PartitionColumns.of(complex1);
        filter = ColumnFilter.selection(selectedColumns);
        assertEquals(selectedColumns, filter.fetchedColumns());
        assertEquals(selectedColumns, filter.queriedColumns());
        assertEquals("complex1", filter.toString());
        assertTrue(filter.allFetchedColumnsAreQueried());
        assertFalse(filter.fetchesAllColumns());
        for (ColumnDefinition c : Iterables.concat(metadata.partitionColumns().regulars,
                                                 metadata.partitionColumns().statics))
        {
            if (c.equals(complex1))
            {
                assertTrue(filter.fetches(c));
                assertTrue(filter.fetchedColumnIsQueried(c));
                assertTrue(filter.fetchedCellIsQueried(c, CellPath.create(ByteBufferUtil.bytes("val1"))));
                assertNull(filter.newTester(c));
            }
            else
            {
                assertFalse(filter.fetches(c));
            }
        }

        // filter queries only a regular column and a sub-selection of a complex column
        selectedColumns = PartitionColumns.of(v1).mergeTo(PartitionColumns.of(complex1));
        filter = ColumnFilter.selectionBuilder()
                             .add(metadata.getColumnDefinition(ByteBufferUtil.bytes("v1")))
                             .select(metadata.getColumnDefinition(ByteBufferUtil.bytes("complex1")), CellPath.create(ByteBufferUtil.bytes(1)))
                             .build();
        assertEquals(selectedColumns, filter.fetchedColumns());
        assertEquals(selectedColumns, filter.queriedColumns());
        assertEquals("complex1[1], v1", filter.toString());
        assertTrue(filter.allFetchedColumnsAreQueried());
        assertFalse(filter.fetchesAllColumns());
        for (ColumnDefinition c : Iterables.concat(metadata.partitionColumns().regulars,
                                                 metadata.partitionColumns().statics))
        {
            if (c.equals(v1))
            {
                assertTrue(filter.fetches(c));
                assertTrue(filter.fetchedColumnIsQueried(c));
            }
            else if (c.equals(complex1))
            {
                CellPath b1 = CellPath.create(ByteBufferUtil.bytes(1)); // fetched and queried
                CellPath b2 = CellPath.create(ByteBufferUtil.bytes(2)); // not fetched, not queried

                assertTrue(filter.fetches(c));
                assertTrue(filter.fetchedColumnIsQueried(c));
                assertTrue(filter.fetchedCellIsQueried(c, b1));
                ColumnFilter.Tester tester = filter.newTester(c);
                assertNotNull(tester);
                assertTrue(tester.fetches(b1));
                assertTrue(tester.fetchedCellIsQueried(b1));
                assertFalse(tester.fetches(b2));
            }
            else
            {
                assertFalse(filter.fetches(c));
            }
        }

        // filter fetches all columns but queries only a regular column and a sub-selection of a complex column
        selectedColumns = PartitionColumns.of(v1).mergeTo(PartitionColumns.of(complex1));
        filter = ColumnFilter.allColumnsBuilder(metadata)
                             .add(metadata.getColumnDefinition(ByteBufferUtil.bytes("v1")))
                             .select(metadata.getColumnDefinition(ByteBufferUtil.bytes("complex1")), CellPath.create(ByteBufferUtil.bytes(1)))
                             .build();
        assertEquals(metadata.partitionColumns(), filter.fetchedColumns());
        assertEquals(selectedColumns, filter.queriedColumns());
        assertEquals("*", filter.toString());
        assertFalse(filter.allFetchedColumnsAreQueried());
        assertTrue(filter.fetchesAllColumns());
        for (ColumnDefinition c : Iterables.concat(metadata.partitionColumns().regulars,
                                                 metadata.partitionColumns().statics))
        {
            if (c.equals(v1))
            {
                assertTrue(filter.fetches(c));
                assertTrue(filter.fetchedColumnIsQueried(c));
            }
            else if (c.equals(complex1))
            {
                CellPath b1 = CellPath.create(ByteBufferUtil.bytes(1)); // fetched and queried
                CellPath b2 = CellPath.create(ByteBufferUtil.bytes(2)); // fetched but not queried

                assertTrue(filter.fetches(c));
                assertTrue(filter.fetchedColumnIsQueried(c));
                assertTrue(filter.fetchedCellIsQueried(c, b1));
                assertFalse(filter.fetchedCellIsQueried(c, b2));

                ColumnFilter.Tester tester = filter.newTester(c);
                assertNotNull(tester);
                assertTrue(tester.fetches(b1));
                assertTrue(tester.fetchedCellIsQueried(b1));
                assertTrue(tester.fetches(b2));
            }
            else
            {
                assertTrue(filter.fetches(c));
                assertFalse(filter.fetchedColumnIsQueried(c));
            }
        }
    }

    @Test
    public void testColumnFilterWithColumnsAdded()
    {
        CFMetaData.Builder builder = CFMetaData.Builder.create("ks", "table")
                          .withPartitioner(Murmur3Partitioner.instance)
                          .addPartitionKey("pk", Int32Type.instance)
                          .addStaticColumn("s1", Int32Type.instance)
                          .addClusteringColumn("c1", Int32Type.instance)
                          .addClusteringColumn("c2", Int32Type.instance)
                          .addRegularColumn("complex1", SetType.getInstance(Int32Type.instance, true))
                          .addRegularColumn("v1", Int32Type.instance);

        CFMetaData metadata = builder.build();

        ColumnDefinition v1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("v1"));
        ColumnDefinition s1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("s1"));
        ColumnDefinition complex1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("complex1"));

        CFMetaData augmentedMetadata = builder.addStaticColumn("s2", Int32Type.instance)
                                              .addRegularColumn("v2", Int32Type.instance)
                                              .addRegularColumn("complex2", SetType.getInstance(Int32Type.instance, true))
                                              .build();

        ColumnDefinition v2 = augmentedMetadata.getColumnDefinition(ByteBufferUtil.bytes("v2"));
        ColumnDefinition s2 = augmentedMetadata.getColumnDefinition(ByteBufferUtil.bytes("s2"));
        ColumnDefinition complex2 = augmentedMetadata.getColumnDefinition(ByteBufferUtil.bytes("complex2"));

        // filter fetches all
        ColumnFilter filter = ColumnFilter.all(metadata);
        assertTrue(filter.fetchesAllColumns());
        assertTrue(filter.fetches(v1));
        assertTrue(filter.fetches(s1));
        assertTrue(filter.fetches(complex1));
        assertTrue(filter.fetches(v2)); // poor filter doesn't know any better
        assertTrue(filter.fetches(s2));
        assertTrue(filter.fetches(complex2));

        // verify columns but using the same set of columns, shouldn't change anything
        filter = filter.withPartitionColumnsVerified(metadata.partitionColumns());
        assertTrue(filter.fetchesAllColumns());
        assertTrue(filter.fetches(v1));
        assertTrue(filter.fetches(s1));
        assertTrue(filter.fetches(complex1));
        assertTrue(filter.fetches(v2)); // poor filter doesn't know any better
        assertTrue(filter.fetches(s2));
        assertTrue(filter.fetches(complex2));

        // not verify with the augmented columns, filter should conclude that it must check the fetched set in order to exclude v3
        filter = filter.withPartitionColumnsVerified(augmentedMetadata.partitionColumns());
        assertFalse(filter.fetchesAllColumns()); // the filter cannot any longer be sure that all columns are fetched
        assertTrue(filter.fetches(v1));
        assertTrue(filter.fetches(s1));
        assertTrue(filter.fetches(complex1));
        assertFalse(filter.fetches(v2)); // the filter now gets it that these new columns should be excluded
        assertFalse(filter.fetches(s2));
        assertFalse(filter.fetches(complex2));
    }

    @Test
    public void testColumnFilterWithColumnsRemoved()
    {
        CFMetaData metadata = CFMetaData.Builder.create("ks", "table")
                                              .withPartitioner(Murmur3Partitioner.instance)
                                              .addPartitionKey("pk", Int32Type.instance)
                                              .addStaticColumn("s1", Int32Type.instance)
                                              .addStaticColumn("s2", Int32Type.instance)
                                              .addClusteringColumn("c1", Int32Type.instance)
                                              .addClusteringColumn("c2", Int32Type.instance)
                                              .addRegularColumn("complex1", SetType.getInstance(Int32Type.instance, true))
                                              .addRegularColumn("complex2", SetType.getInstance(Int32Type.instance, true))
                                              .addRegularColumn("v1", Int32Type.instance)
                                              .addRegularColumn("v2", Int32Type.instance)
                                              .build();

        ColumnDefinition v1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("v1"));
        ColumnDefinition s1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("s1"));
        ColumnDefinition complex1 = metadata.getColumnDefinition(ByteBufferUtil.bytes("complex1"));

        ColumnDefinition v2 = metadata.getColumnDefinition(ByteBufferUtil.bytes("v2"));
        ColumnDefinition s2 = metadata.getColumnDefinition(ByteBufferUtil.bytes("s2"));
        ColumnDefinition complex2 = metadata.getColumnDefinition(ByteBufferUtil.bytes("complex2"));

        PartitionColumns reducedColumns = metadata.partitionColumns()
                                                         .without(v2)
                                                         .without(s2)
                                                         .without(complex2);

        // filter fetches all
        ColumnFilter filter = ColumnFilter.all(metadata);
        assertTrue(filter.fetchesAllColumns());
        assertTrue(filter.fetches(v1));
        assertTrue(filter.fetches(s1));
        assertTrue(filter.fetches(complex1));
        assertTrue(filter.fetches(v2));
        assertTrue(filter.fetches(s2));
        assertTrue(filter.fetches(complex2));

        // verify with the reduced columns, filter should conclude that it can still safely fetch all columns because
        // reducing columns simply means that the iterators will not ask the filter for some columns that were dropped
        filter = filter.withPartitionColumnsVerified(reducedColumns);
        assertTrue(filter.fetchesAllColumns());
        assertTrue(filter.fetches(v1));
        assertTrue(filter.fetches(s1));
        assertTrue(filter.fetches(complex1));
        assertTrue(filter.fetches(v2)); // the filter should not fetch the columns that were removed
        assertTrue(filter.fetches(s2));
        assertTrue(filter.fetches(complex2));
    }

    static void testRoundTrip(ColumnFilter columnFilter, CFMetaData metadata, int version) throws Exception
    {
        DataOutputBuffer output = new DataOutputBuffer();
        serializer.serialize(columnFilter, output, version);
        Assert.assertEquals(serializer.serializedSize(columnFilter, version), output.position());
        DataInputPlus input = new DataInputBuffer(output.buffer(), false);
        Assert.assertEquals(serializer.deserialize(input, version, metadata), columnFilter);
    }
}