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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.TermsReader;
import org.apache.cassandra.index.sai.metrics.QueryEventListeners;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.index.sai.utils.SAICodecUtils.FOOTER_POINTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SegmentFlushTest
{
    private static long segmentRowIdOffset;
    private static int posting1;
    private static int posting2;
    private static DecoratedKey minKey;
    private static DecoratedKey maxKey;
    private static ByteBuffer minTerm;
    private static ByteBuffer maxTerm;
    private static int numRows;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void reset()
    {
        SegmentBuilder.updateLastValidSegmentRowId(-1); // reset
    }

    @Test
    public void testIntOverflow() throws Exception
    {
        SegmentBuilder.updateLastValidSegmentRowId(5_000_000_000L); // 5B

        // segment row id must not be Integer.MAX_VALUE
        assertIllegalEndOfStream(0, (long) Integer.MAX_VALUE);
        assertIllegalEndOfStream(Integer.MAX_VALUE, (long) Integer.MAX_VALUE * 2);

        // segmentId exceeds Integer.MAX_VALUE
        assertOverflow(0, (long) Integer.MAX_VALUE + 1L);
        assertOverflow(Integer.MAX_VALUE, (long) Integer.MAX_VALUE * 2 + 1);
        assertOverflow(0, 5_000_000_000L);
    }

    @Test
    public void testNoIntOverflow() throws Exception
    {
        SegmentBuilder.updateLastValidSegmentRowId(Integer.MAX_VALUE - 1);

        // segmentId within Integer.MAX_VALUE - 1
        testFlushBetweenRowIds(0, (long) Integer.MAX_VALUE - 1, 1);
        testFlushBetweenRowIds(Integer.MAX_VALUE, (long) Integer.MAX_VALUE * 2 - 1, 1);
    }

    @Test
    public void testFlushBetweenRowIds() throws Exception
    {
        // exceeds max rowId per segment
        testFlushBetweenRowIds(0, Integer.MAX_VALUE, 2);
        testFlushBetweenRowIds(0, Long.MAX_VALUE, 2);
        testFlushBetweenRowIds(0, SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID + 1, 2);
        testFlushBetweenRowIds(Integer.MAX_VALUE - SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID -1, Integer.MAX_VALUE, 2);
        testFlushBetweenRowIds(Long.MAX_VALUE - SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID - 1, Long.MAX_VALUE, 2);
    }

    @Test
    public void testNoFlushBetweenRowIds() throws Exception
    {
        // not exceeds max rowId per segment
        testFlushBetweenRowIds(0, SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID, 1);
        testFlushBetweenRowIds(Long.MAX_VALUE - SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID, Long.MAX_VALUE, 1);
    }

    private void testFlushBetweenRowIds(long sstableRowId1, long sstableRowId2, int segments) throws Exception
    {
        Path tmpDir = Files.createTempDirectory("SegmentFlushTest");
        Descriptor descriptor = new Descriptor(tmpDir.toFile(), "ks", "cf", 1);

        ColumnMetadata column = ColumnMetadata.regularColumn("sai", "internal", "column", UTF8Type.instance);

        ColumnContext context = new ColumnContext("ks", "cf",
                                                  UTF8Type.instance, new ClusteringComparator(),
                                                  column, IndexWriterConfig.defaultConfig("test"));

        SSTableIndexWriter writer = new SSTableIndexWriter(descriptor, context, StorageAttachedIndex.SEGMENT_BUILD_MEMORY_LIMITER, () -> true);

        List<DecoratedKey> keys = Arrays.asList(dk("1"), dk("2"));
        Collections.sort(keys);

        DecoratedKey key1 = keys.get(0);
        ByteBuffer term1 = ByteBufferUtil.bytes("a");
        Row row1 = createRow(column, term1);
        writer.addRow(key1, sstableRowId1, row1);

        // expect a flush if exceed max rowId per segment
        DecoratedKey key2 = keys.get(1);
        ByteBuffer term2 = ByteBufferUtil.bytes("b");
        Row row2 = createRow(column, term2);
        writer.addRow(key2, sstableRowId2, row2);

        writer.flush();

        IndexComponents components = IndexComponents.create(column.toString(), descriptor);
        MetadataSource source = MetadataSource.loadColumnMetadata(components);

        // verify segment count
        List<SegmentMetadata> segmentMetadatas = SegmentMetadata.load(source);
        assertEquals(segments, segmentMetadatas.size());

        // verify segment metadata
        SegmentMetadata segmentMetadata = segmentMetadatas.get(0);
        segmentRowIdOffset = sstableRowId1;
        posting1 = 0;
        posting2 = segments == 1 ? (int) (sstableRowId2 - segmentRowIdOffset) : 0;
        minKey = key1;
        maxKey = segments == 1 ? key2 : key1;
        minTerm = term1;
        maxTerm = segments == 1 ? term2 : term1;
        numRows = segments == 1 ? 2 : 1;
        verifySegmentMetadata(segmentMetadata);
        verifyStringIndex(components, segmentMetadata);

        if (segments > 1)
        {
            segmentRowIdOffset = sstableRowId2;
            posting1 = 0;
            posting2 = 0;
            minKey = key2;
            maxKey = key2;
            minTerm = term2;
            maxTerm = term2;
            numRows = 1;

            segmentMetadata = segmentMetadatas.get(1);
            verifySegmentMetadata(segmentMetadata);
            verifyStringIndex(components, segmentMetadata);
        }
    }

    private void verifyStringIndex(IndexComponents components, SegmentMetadata segmentMetadata) throws IOException
    {
        FileHandle termsData = components.createFileHandle(components.termsData);
        FileHandle postingLists = components.createFileHandle(components.postingLists);

        long termsFooterPointer = Long.parseLong(segmentMetadata.componentMetadatas.get(IndexComponents.NDIType.TERMS_DATA).attributes.get(FOOTER_POINTER));

        try (TermsReader reader = new TermsReader(components, termsData, postingLists,
                                                  segmentMetadata.componentMetadatas.get(components.termsData.ndiType).root, termsFooterPointer))
        {
            TermsIterator iterator = reader.allTerms(QueryEventListeners.NO_OP_TRIE_LISTENER);
            assertEquals(minTerm, iterator.getMinTerm());
            assertEquals(maxTerm, iterator.getMaxTerm());

            verifyTermPostings(iterator, minTerm, posting1, posting1);

            if (numRows > 1)
            {
                verifyTermPostings(iterator, maxTerm, posting2, posting2);
            }

            assertFalse(iterator.hasNext());
        }
    }

    private void verifyTermPostings(TermsIterator iterator, ByteBuffer expectedTerm, int minSegmentRowId, int maxSegmentRowId) throws IOException
    {
        ByteBuffer term = iterator.next();
        PostingList postings = iterator.postings();

        assertEquals(expectedTerm, term);
        assertEquals(minSegmentRowId == maxSegmentRowId ? 1 : 2, postings.size());
    }

    private void verifySegmentMetadata(SegmentMetadata segmentMetadata)
    {
        assertEquals(segmentRowIdOffset, segmentMetadata.segmentRowIdOffset);
        assertEquals(minKey, segmentMetadata.minKey);
        assertEquals(maxKey, segmentMetadata.maxKey);
        assertEquals(minTerm, segmentMetadata.minTerm);
        assertEquals(maxTerm, segmentMetadata.maxTerm);
        assertEquals(numRows, segmentMetadata.numRows);
    }

    private Row createRow(ColumnMetadata column, ByteBuffer value)
    {
        Row.Builder builder1 = BTreeRow.sortedBuilder();
        builder1.newRow(Clustering.EMPTY);
        builder1.addCell(BufferCell.live(column, 0, value));
        return builder1.build();
    }

    private void assertOverflow(long sstableRowId1, long sstableRowId2) throws Exception
    {
        try
        {
            testFlushBetweenRowIds(sstableRowId1, sstableRowId2, 0);
            fail("Expect integer overflow, but didn't");
        }
        catch (ArithmeticException e)
        {
            assertTrue(e.getMessage().contains("integer overflow"));
        }
    }

    private void assertIllegalEndOfStream(long sstableRowId1, long sstableRowId2) throws Exception
    {
        try
        {
            testFlushBetweenRowIds(sstableRowId1, sstableRowId2, 0);
            fail("Expect integer overflow, but didn't");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage().contains("END_OF_STREAM"));
        }
    }
}
