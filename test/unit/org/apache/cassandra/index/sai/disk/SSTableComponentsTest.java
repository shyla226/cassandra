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

package org.apache.cassandra.index.sai.disk;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.PrimaryKeyMap;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertTrue;

public class SSTableComponentsTest extends SAITester
{
    private Descriptor descriptor;

    @Before
    public void createDescriptor() throws Throwable
    {
        Path tmpDir = Files.createTempDirectory("SegmentFlushTest");
        descriptor = new Descriptor(tmpDir.toFile(), "test", "test", 1);
    }

    @Test
    public void test() throws Throwable
    {
        SSTableComponentsWriter writer = new SSTableComponentsWriter.OnDiskSSTableComponentsWriter(descriptor, null);

        TableMetadata tableMetadata = TableMetadata.builder("test", "test")
                                                   .partitioner(Murmur3Partitioner.instance)
                                                   .addPartitionKeyColumn("pk", Int32Type.instance)
                                                   .addClusteringColumn("a", UTF8Type.instance)
                                                   .addClusteringColumn("b", UTF8Type.instance)
                                                   .build();

        PrimaryKey.PrimaryKeyFactory factory = PrimaryKey.factory(tableMetadata);

        List<PrimaryKey> expected = new ArrayList<>();

        expected.add(factory.createKey(makeKey(tableMetadata, "0"), makeClustering(tableMetadata, "0000", "0001")));
        expected.add(factory.createKey(makeKey(tableMetadata, "0"), makeClustering(tableMetadata, "0000", "0002")));
        expected.add(factory.createKey(makeKey(tableMetadata, "0"), makeClustering(tableMetadata, "0001", "0001")));
        expected.add(factory.createKey(makeKey(tableMetadata, "0"), makeClustering(tableMetadata, "0001", "0002")));
        expected.add(factory.createKey(makeKey(tableMetadata, "1"), makeClustering(tableMetadata, "0000", "0001")));
        expected.add(factory.createKey(makeKey(tableMetadata, "2"), makeClustering(tableMetadata, "0000", "0002")));
        expected.add(factory.createKey(makeKey(tableMetadata, "3"), makeClustering(tableMetadata, "0001", "0001")));
        expected.add(factory.createKey(makeKey(tableMetadata, "4"), makeClustering(tableMetadata, "0001", "0002")));

        expected.sort(PrimaryKey::compareTo);

        int sstableRowId = 0;
        for (PrimaryKey key : expected)
            writer.nextRow(factory.createKey(key.partitionKey, key.clustering(), sstableRowId++));

        writer.complete();

        IndexComponents indexComponents = IndexComponents.perSSTable(descriptor, null);

        PrimaryKeyMap primaryKeyMap = new PrimaryKeyMap.DefaultPrimaryKeyMap(indexComponents, tableMetadata);

        for (int rowId = 0; rowId < expected.size(); rowId++)
        {
            assertTrue(primaryKeyMap.primaryKeyFromRowId(rowId).compareTo(expected.get(rowId)) == 0);
        }

        primaryKeyMap.close();
    }

    private DecoratedKey makeKey(TableMetadata table, String...partitionKeys)
    {
        ByteBuffer key;
        if (TypeUtil.isComposite(table.partitionKeyType))
            key = ((CompositeType)table.partitionKeyType).decompose(partitionKeys);
        else
            key = table.partitionKeyType.fromString(partitionKeys[0]);
        return table.partitioner.decorateKey(key);
    }

    private Clustering makeClustering(TableMetadata table, String...clusteringKeys)
    {
        Clustering clustering;
        if (table.comparator.size() == 0)
            clustering = Clustering.EMPTY;
        else
        {
            ByteBuffer[] values = new ByteBuffer[clusteringKeys.length];
            for (int index = 0; index < table.comparator.size(); index++)
                values[index] = table.comparator.subtype(index).fromString(clusteringKeys[index]);
            clustering = Clustering.make(values);
        }
        return clustering;
    }

}