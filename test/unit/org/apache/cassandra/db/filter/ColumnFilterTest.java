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

import junit.framework.Assert;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnFilterTest
{
    final static ColumnFilter.Serializer SERIALIZER_30 = ColumnFilter.serializers.get(ReadVerbs.ReadVersion.OSS_30);
    final static ColumnFilter.Serializer SERIALIZER_3014 = ColumnFilter.serializers.get(ReadVerbs.ReadVersion.OSS_3014);
    final static ColumnFilter.Serializer SERIALIZER_40 = ColumnFilter.serializers.get(ReadVerbs.ReadVersion.OSS_40);
    final static ColumnFilter.Serializer SERIALIZER_DSE60 = ColumnFilter.serializers.get(ReadVerbs.ReadVersion.DSE_60);

    @Test
    public void columnFilterSerialisationRoundTrip() throws Exception
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
        testRoundTrip(ColumnFilter.all(metadata), metadata, SERIALIZER_40);
        testRoundTrip(ColumnFilter.all(metadata), metadata, SERIALIZER_DSE60);

        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_30);
        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_3014);
        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_40);
        testRoundTrip(ColumnFilter.selection(metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_DSE60);

        columnFilter = ColumnFilter.selection(metadata, metadata.regularAndStaticColumns().without(v1));
        testRoundTrip(columnFilter, SERIALIZER_30.maybeUpdateForBackwardCompatility(columnFilter), metadata, SERIALIZER_30);
        testRoundTrip(columnFilter, SERIALIZER_3014.maybeUpdateForBackwardCompatility(columnFilter), metadata, SERIALIZER_3014);
        testRoundTrip(ColumnFilter.selection(metadata, metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_40);
        testRoundTrip(ColumnFilter.selection(metadata, metadata.regularAndStaticColumns().without(v1)), metadata, SERIALIZER_DSE60);
    }

    static void testRoundTrip(ColumnFilter columnFilter, TableMetadata metadata, ColumnFilter.Serializer serializer) throws Exception
    {
        testRoundTrip(columnFilter, columnFilter, metadata, serializer);
    }

    static void testRoundTrip(ColumnFilter columnFilter, ColumnFilter expected, TableMetadata metadata, ColumnFilter.Serializer serializer) throws Exception
    {
        DataOutputBuffer output = new DataOutputBuffer();
        serializer.serialize(columnFilter, output);
        Assert.assertEquals(serializer.serializedSize(columnFilter), output.position());
        DataInputPlus input = new DataInputBuffer(output.buffer(), false);
        ColumnFilter deserialized = serializer.deserialize(input, metadata);
        Assert.assertEquals(deserialized, expected);
    }
}