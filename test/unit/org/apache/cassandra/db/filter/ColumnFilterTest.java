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

import org.junit.Test;

import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ColumnFilterTest
{
    final static ColumnFilter.Serializer SERIALIZER_30 = ColumnFilter.serializers.get(ReadVerbs.ReadVersion.OSS_30);
    final static ColumnFilter.Serializer SERIALIZER_3014 = ColumnFilter.serializers.get(ReadVerbs.ReadVersion.OSS_3014);
    final static ColumnFilter.Serializer SERIALIZER_40 = ColumnFilter.serializers.get(ReadVerbs.ReadVersion.OSS_40);
    final static ColumnFilter.Serializer SERIALIZER_DSE60 = ColumnFilter.serializers.get(ReadVerbs.ReadVersion.DSE_60);

    @Test
    public void testColumnFilterSerialisationRoundTrip() throws Exception
    {
        TableMetadata metadata = TableMetadata.builder("ks", "table")
                                              .partitioner(Murmur3Partitioner.instance)
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addClusteringColumn("ck", Int32Type.instance)
                                              .addRegularColumn("v1", Int32Type.instance)
                                              .addRegularColumn("v2", Int32Type.instance)
                                              .addRegularColumn("v3", Int32Type.instance)
                                              .build();

        ColumnMetadata v1 = metadata.getColumn(ByteBufferUtil.bytes("v1"));

        ColumnFilter columnFilter;

        columnFilter = ColumnFilter.all(metadata);

        testRoundTrip(columnFilter, SERIALIZER_30.maybeUpdateForBackwardCompatility(columnFilter), metadata, SERIALIZER_30);
        testRoundTrip(columnFilter, SERIALIZER_3014.maybeUpdateForBackwardCompatility(columnFilter), metadata, SERIALIZER_3014);
        testRoundTrip(columnFilter, metadata, SERIALIZER_40);
        testRoundTrip(columnFilter, metadata, SERIALIZER_DSE60);

        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_30);
        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_3014);
        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_40);
        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_DSE60);

        columnFilter = ColumnFilter.selection(metadata, metadata.regularAndStaticColumns().without(v1));
        testRoundTrip(columnFilter, SERIALIZER_30.maybeUpdateForBackwardCompatility(columnFilter), metadata, SERIALIZER_30);
        testRoundTrip(columnFilter, SERIALIZER_3014.maybeUpdateForBackwardCompatility(columnFilter), metadata, SERIALIZER_3014);
        testRoundTrip(columnFilter, metadata, SERIALIZER_40);
        testRoundTrip(columnFilter, metadata, SERIALIZER_DSE60);

        // Table with static column
        metadata = TableMetadata.builder("ks", "table")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addStaticColumn("s1", Int32Type.instance)
                                .addRegularColumn("v1", Int32Type.instance)
                                .addRegularColumn("v2", Int32Type.instance)
                                .addRegularColumn("v3", Int32Type.instance)
                                .build();

        v1 = metadata.getColumn(ByteBufferUtil.bytes("v1"));
        ColumnMetadata s1 = metadata.getColumn(ByteBufferUtil.bytes("s1"));

        columnFilter = ColumnFilter.all(metadata);
        testRoundTrip(columnFilter, SERIALIZER_30.maybeUpdateForBackwardCompatility(columnFilter), metadata, SERIALIZER_30);
        testRoundTrip(columnFilter, SERIALIZER_3014.maybeUpdateForBackwardCompatility(columnFilter), metadata, SERIALIZER_3014);
        testRoundTrip(columnFilter, metadata, SERIALIZER_40);
        testRoundTrip(columnFilter, metadata, SERIALIZER_DSE60);


        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_30);
        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_3014);
        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_40);
        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_DSE60);

        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1).without(s1)), metadata, SERIALIZER_30);
        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1).without(s1)), metadata, SERIALIZER_3014);
        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1).without(s1)), metadata, SERIALIZER_40);
        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1).without(s1)), metadata, SERIALIZER_DSE60);

        columnFilter = ColumnFilter.selection(metadata, metadata.regularAndStaticColumns().without(v1));

        testRoundTrip(columnFilter, SERIALIZER_30.maybeUpdateForBackwardCompatility(columnFilter), metadata, SERIALIZER_30);
        testRoundTrip(columnFilter, SERIALIZER_3014.maybeUpdateForBackwardCompatility(columnFilter), metadata, SERIALIZER_3014);
        testRoundTrip(columnFilter, metadata, SERIALIZER_40);
        testRoundTrip(columnFilter, metadata, SERIALIZER_DSE60);

        columnFilter = ColumnFilter.selection(metadata, metadata.regularAndStaticColumns().without(v1).without(s1))
                                   .withColumnsVerified(metadata.regularAndStaticColumns());
        testRoundTrip(columnFilter, SERIALIZER_30.maybeUpdateForBackwardCompatility(columnFilter), metadata, SERIALIZER_30);
        testRoundTrip(columnFilter, SERIALIZER_3014.maybeUpdateForBackwardCompatility(columnFilter), metadata, SERIALIZER_3014);
        testRoundTrip(columnFilter, metadata, SERIALIZER_40);
        testRoundTrip(columnFilter, metadata, SERIALIZER_DSE60);
    }

    @Test
    public void testColumnFilterConstruction()
    {
        // all regular column
        TableMetadata metadata = TableMetadata.builder("ks", "table")
                                              .partitioner(Murmur3Partitioner.instance)
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addClusteringColumn("ck", Int32Type.instance)
                                              .addRegularColumn("v1", Int32Type.instance)
                                              .addRegularColumn("v2", Int32Type.instance)
                                              .addRegularColumn("v3", Int32Type.instance)
                                              .build();
        ColumnFilter columnFilter = ColumnFilter.all(metadata);
        assertTrue(columnFilter.fetchAllRegulars);
        assertEquals(metadata.regularAndStaticColumns(), columnFilter.fetched);
        assertNull(columnFilter.queried);
        assertEquals("*", columnFilter.toString());

        RegularAndStaticColumns queried = RegularAndStaticColumns.builder()
                                                                 .add(metadata.getColumn(ByteBufferUtil.bytes("v1"))).build();
        columnFilter = ColumnFilter.selection(queried);
        assertFalse(columnFilter.fetchAllRegulars);
        assertEquals(queried, columnFilter.fetched);
        assertEquals(queried, columnFilter.queried);
        assertEquals("v1", columnFilter.toString());

        // with static column
        metadata = TableMetadata.builder("ks", "table")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addStaticColumn("sc1", Int32Type.instance)
                                .addStaticColumn("sc2", Int32Type.instance)
                                .addRegularColumn("v1", Int32Type.instance)
                                .build();

        columnFilter = ColumnFilter.all(metadata);
        assertTrue(columnFilter.fetchAllRegulars);
        assertEquals(metadata.regularAndStaticColumns(), columnFilter.fetched);
        assertNull(columnFilter.queried);
        assertEquals("*", columnFilter.toString());

        queried = RegularAndStaticColumns.builder()
                                         .add(metadata.getColumn(ByteBufferUtil.bytes("v1"))).build();
        columnFilter = ColumnFilter.selection(metadata, queried);
        assertEquals("v1", columnFilter.toString());

        // only static
        metadata = TableMetadata.builder("ks", "table")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addStaticColumn("sc", Int32Type.instance)
                                .build();

        columnFilter = ColumnFilter.all(metadata);
        assertTrue(columnFilter.fetchAllRegulars);
        assertEquals(metadata.regularAndStaticColumns(), columnFilter.fetched);
        assertNull(columnFilter.queried);
        assertEquals("*", columnFilter.toString());

        // with collection type
        metadata = TableMetadata.builder("ks", "table")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addRegularColumn("v1", Int32Type.instance)
                                .addRegularColumn("set", SetType.getInstance(Int32Type.instance, true))
                                .build();

        columnFilter = ColumnFilter.all(metadata);
        assertTrue(columnFilter.fetchAllRegulars);
        assertEquals(metadata.regularAndStaticColumns(), columnFilter.fetched);
        assertNull(columnFilter.queried);
        assertEquals("*", columnFilter.toString());

        columnFilter = ColumnFilter.selectionBuilder().add(metadata.getColumn(ByteBufferUtil.bytes("v1")))
                                   .select(metadata.getColumn(ByteBufferUtil.bytes("set")), CellPath.create(ByteBufferUtil.bytes(1)))
                                   .build();
        assertEquals("set[1], v1", columnFilter.toString());
    }

    static void testRoundTrip(ColumnFilter columnFilter, TableMetadata metadata, ColumnFilter.Serializer serializer) throws Exception
    {
        testRoundTrip(columnFilter, columnFilter, metadata, serializer);
    }

    @SuppressWarnings("resource")
    static void testRoundTrip(ColumnFilter columnFilter, ColumnFilter expected, TableMetadata metadata, ColumnFilter.Serializer serializer) throws Exception
    {
        DataOutputBuffer output = new DataOutputBuffer();
        serializer.serialize(columnFilter, output);
        assertEquals(serializer.serializedSize(columnFilter), output.position());
        DataInputPlus input = new DataInputBuffer(output.buffer(), false);
        ColumnFilter deserialized = serializer.deserialize(input, metadata);
        assertEquals(deserialized, expected);
    }
}