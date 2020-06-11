/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sai.virtual;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.SchemaConstants;

import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.MAX_TERM;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.MIN_TERM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the virtual table exposing SSTable index segment metadata.
 */
public class SegmentsSystemViewTest extends SAITester
{
    private static final String SELECT = String.format("SELECT %s, %s, %s, %s " +
                                                       "FROM %s.%s WHERE %s = '%s' AND %s = ?",
                                                       SegmentsSystemView.SEGMENT_ROW_ID_OFFSET,
                                                       SegmentsSystemView.CELL_COUNT,
                                                       SegmentsSystemView.MIN_SSTABLE_ROW_ID,
                                                       SegmentsSystemView.MAX_SSTABLE_ROW_ID,
                                                       SchemaConstants.VIRTUAL_VIEWS,
                                                       SegmentsSystemView.NAME,
                                                       SegmentsSystemView.KEYSPACE_NAME,
                                                       KEYSPACE,
                                                       SegmentsSystemView.INDEX_NAME);


    private static final String SELECT_INDEX_METADATA = String.format("SELECT %s, %s, %s " +
                                                                      "FROM %s.%s WHERE %s = '%s'",
                                                                      SegmentsSystemView.COMPONENT_METADATA,
                                                                      MIN_TERM,
                                                                      MAX_TERM,
                                                                      SchemaConstants.VIRTUAL_VIEWS,
                                                                      SegmentsSystemView.NAME,
                                                                      SegmentsSystemView.KEYSPACE_NAME,
                                                                      KEYSPACE);


    @BeforeClass
    public static void setUpClass()
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(SchemaConstants.VIRTUAL_VIEWS, ImmutableList.of(new SegmentsSystemView(SchemaConstants.VIRTUAL_VIEWS))));

        CQLTester.setUpClass();
    }

    @Before
    public void setup()
    {
        requireNetwork();
    }

    @Test
    public void testSegmentsMetadata() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 text, PRIMARY KEY (k, c))");
        String v1IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        String v2IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        waitForIndexQueryable();

        int num = 100;

        String insert = "INSERT INTO %s(k, c, v1, v2) VALUES (?, ?, ?, ?)";

        // the virtual table should be empty before adding contents
        assertEmpty(execute(SELECT, v1IndexName));
        assertEmpty(execute(SELECT, v2IndexName));

        // insert rows and verify that the virtual table is empty before flushing
        for (int i = 0; i < num / 2; i++)
            execute(insert, i, 10, 100, Integer.toString(1000));
        assertEmpty(execute(SELECT, v1IndexName));
        assertEmpty(execute(SELECT, v2IndexName));

        // flush the memtable and verify the new record in the virtual table
        flush();
        Object[] row1 = segment(0L, num / 2, 0, num / 2 - 1);
        assertRows(execute(SELECT, v1IndexName), row1);
        assertRows(execute(SELECT, v2IndexName), row1);

        // flush a second memtable and verify both the old and the new record in the virtual table
        for (int i = num / 2; i < num; i++)
            execute(insert, i, 20, 200, Integer.toString(2000));
        flush();
        Object[] row2 = segment(0L, num / 2, 0, num / 2 - 1);
        assertRows(execute(SELECT, v1IndexName), row1, row2);
        assertRows(execute(SELECT, v2IndexName), row1, row2);

        // force compaction, there is only 1 sstable
        compact();
        Object[] row3 = segment(0L, num, 0, num - 1);
        assertRows(execute(SELECT, v1IndexName), row3);
        assertRows(execute(SELECT, v2IndexName), row3);

        for (int lastValidSegmentRowId : Arrays.asList(0, 1, 2, 3, 5, 9, 25, 49, 59, 99, 101))
        {
            SegmentBuilder.updateLastValidSegmentRowId(lastValidSegmentRowId);

            // compaction to rewrite segments
            upgradeSSTables();

            int segments = (int) Math.ceil(num * 1.0 / (lastValidSegmentRowId + 1));
            // maxSegmentRowId is the same as numRows, because we don't have tombstone or non-indexable row.
            int numRowsPerSegment = lastValidSegmentRowId + 1;
            Object[][] segmentRows = segments(num, segments, numRowsPerSegment);
            assertRows(execute(SELECT, v1IndexName), segmentRows);
            assertRows(execute(SELECT, v2IndexName), segmentRows);

            // verify index metadata length
            Map<String, Long> indexLengths = new HashMap<>();
            for (Row row : executeNet(SELECT_INDEX_METADATA).all())
            {
                int minTerm = Integer.parseInt((String)row.get(MIN_TERM, String.class));
                int maxTerm = Integer.parseInt((String)row.get(MAX_TERM, String.class));

                assertTrue(minTerm >= 100);
                assertTrue(maxTerm <= 2000);

                Map<String, Map<String, String>> indexMetadatas = row.get(0, TypeCodec.map(TypeCodec.ascii(), TypeCodec.map(TypeCodec.ascii(), TypeCodec.ascii())));
                for (Map.Entry<String, Map<String, String>> entry : indexMetadatas.entrySet())
                {
                    final String indexType = entry.getKey();
                    final String str = (String)entry.getValue().getOrDefault(SegmentMetadata.ComponentMetadata.LENGTH, "0");

                    if (indexType.equals(IndexComponents.NDIType.KD_TREE.toString()))
                    {
                        int maxPointsInLeafNode = Integer.parseInt((String)entry.getValue().get("max_points_in_leaf_node"));

                        assertEquals(1024, maxPointsInLeafNode);
                    }
                    else if (indexType.equals(IndexComponents.NDIType.KD_TREE_POSTING_LISTS.toString()))
                    {
                        int numLeafPostings = Integer.parseInt((String)entry.getValue().get("num_leaf_postings"));

                        assertTrue(numLeafPostings > 0);
                    }

                    final long length = Long.parseLong(str);

                    final long value = indexLengths.getOrDefault(indexType, 0L);
                    indexLengths.put(indexType, value + length);
                }
            }
            assertEquals(indexFileLengths(), indexLengths);
        }

        // drop the numeric index and verify that there are not entries for it in the table
        dropIndex("DROP INDEX %s." + v1IndexName);
        assertEmpty(execute(SELECT, v1IndexName));
        assertNotEquals(0, executeNet(SELECT, v2IndexName).all().size());

        // drop the string index and verify that there are not entries for it in the table
        dropIndex("DROP INDEX %s." + v2IndexName);
        assertEmpty(execute(SELECT, v1IndexName));
        assertEmpty(execute(SELECT, v2IndexName));
    }

    private Object[][] segments(int num, int numSegments, int numRowsPerSegment)
    {
        Object[][] segments = new Object[numSegments][5];

        long segmentRowIdOffset = 0;
        long minSSTableRowId = 0;
        for (int i = 0; i < numSegments; i++)
        {
            // last segment may not reach numRowsPerSegment
            int cellCount = i == numSegments - 1 && (num % numRowsPerSegment != 0) ? num % numRowsPerSegment : numRowsPerSegment;

            segments[i] = segment(segmentRowIdOffset, cellCount, minSSTableRowId, minSSTableRowId + cellCount - 1);
            segmentRowIdOffset += cellCount;
            minSSTableRowId += cellCount;
        }
        return segments;
    }

    private static Object[] segment(long segmentRowIdOffset, long cellCount, long minSSTableRowId, long maxSSTableRowId)
    {
        return new Object[]{segmentRowIdOffset, cellCount, minSSTableRowId, maxSSTableRowId};
    }
    
    private HashMap<String, Long> indexFileLengths()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());

        HashMap<String, Long> lengths = new HashMap<>();
        for (Index idx : cfs.indexManager.listIndexes())
        {
            StorageAttachedIndex index = (StorageAttachedIndex) idx;

            for (SSTableIndex sstableIndex : index.getContext().getView().getIndexes())
            {
                SSTableReader sstable = sstableIndex.getSSTable();

                IndexComponents components = IndexComponents.create(sstableIndex.getColumnContext().getColumnName(), sstable);

                if (sstableIndex.getColumnContext().isString())
                {
                    addComponentSizeToMap(lengths, components.termsData, components);
                    addComponentSizeToMap(lengths, components.postingLists, components);
                }
                else
                {
                    addComponentSizeToMap(lengths, components.kdTree, components);
                    addComponentSizeToMap(lengths, components.kdTreePostingLists, components);
                }
            }
        }

        return lengths;
    }

    private void addComponentSizeToMap(HashMap<String, Long> map, IndexComponents.IndexComponent key, IndexComponents indexComponents)
    {
        map.compute(key.ndiType.name, (typeName, acc) -> {
            final long size = indexComponents.sizeOf(Collections.singleton(key));
            return acc == null ? size : size + acc;
        });
    }
}
