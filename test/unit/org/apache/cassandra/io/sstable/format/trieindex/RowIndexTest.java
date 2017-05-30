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
package org.apache.cassandra.io.sstable.format.trieindex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.io.sstable.format.trieindex.RowIndexReader;
import org.apache.cassandra.io.sstable.format.trieindex.RowIndexReader.IndexInfo;
import org.apache.cassandra.io.sstable.format.trieindex.RowIndexReverseIterator;
import org.apache.cassandra.io.sstable.format.trieindex.RowIndexWriter;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.Pair;

public class RowIndexTest
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
        TPC.ensureInitialized();
    }

    static final ClusteringComparator comparator = new ClusteringComparator(UUIDType.instance);
    static final long END_MARKER = 1L << 40;
    static final int COUNT = 145256;

    @Test
    public void testSingletons() throws IOException
    {
        Pair<List<ClusteringPrefix>, RowIndexReader> random = generateRandomIndexSingletons(COUNT);
        RowIndexReader summary = random.right;
        List<ClusteringPrefix> keys = random.left;
        for (int i = 0; i < COUNT; i++)
        {
            assertEquals(i, summary.separatorFloor(comparator.asByteComparableSource(keys.get(i))).offset);
        }
        summary.close();
    }

    @Test
    public void testSpans() throws IOException
    {
        Pair<List<ClusteringPrefix>, RowIndexReader> random = generateRandomIndexQuads(COUNT);
        RowIndexReader summary = random.right;
        List<ClusteringPrefix> keys = random.left;
        int missCount = 0;
        IndexInfo ii;
        for (int i = 0; i < COUNT; i++)
        {
            // These need to all be within the span
            assertEquals(i, (ii = summary.separatorFloor(comparator.asByteComparableSource(keys.get(4 * i + 1)))).offset);
            assertEquals(i, summary.separatorFloor(comparator.asByteComparableSource(keys.get(4 * i + 2))).offset);
            assertEquals(i, summary.separatorFloor(comparator.asByteComparableSource(keys.get(4 * i + 3))).offset);

            // check other data
            assertEquals(i + 2, ii.openDeletion.markedForDeleteAt());
            assertEquals(i - 3, ii.openDeletion.localDeletionTime());

            // before entry. hopefully here, but could end up in prev if matches prevMax too well
            ii = summary.separatorFloor(comparator.asByteComparableSource(keys.get(4 * i)));
            if (ii.offset != i)
            {
                ++missCount;
                assertEquals(i - 1, ii.offset);
            }
        }
        ii = summary.separatorFloor(comparator.asByteComparableSource(keys.get(4 * COUNT)));
        if (ii.offset != END_MARKER)
        {
            ++missCount;
            assertEquals(COUNT - 1, ii.offset);
        }
        ii = summary.separatorFloor(comparator.asByteComparableSource(ClusteringBound.BOTTOM));
        assertEquals(0, ii.offset);

        ii = summary.separatorFloor(comparator.asByteComparableSource(ClusteringBound.TOP));
        assertEquals(END_MARKER, ii.offset);

        summary.close();
        if (missCount > COUNT / 5)
            System.err.format("Unexpectedly high miss count: %d/%d\n", missCount, COUNT);
    }

    File file;
    DataOutputStreamPlus dos;
    RowIndexWriter writer;
    FileHandle fh;
    long root;

    @After
    public void cleanUp()
    {
        FileUtils.closeQuietly(dos);
        FileUtils.closeQuietly(fh);
    }

    public RowIndexTest() throws IOException
    {
        file = File.createTempFile("ColumnTrieReaderTest", "");
        dos = new SequentialWriter(file, SequentialWriterOption.newBuilder().finishOnClose(true).build());

        // write some junk
        dos.writeUTF("JUNK");
        dos.writeUTF("JUNK");

        writer = new RowIndexWriter(comparator, dos);
    }

    public RowIndexReader completeAndRead() throws IOException
    {
        root = writer.complete(END_MARKER);
        dos.writeUTF("JUNK");
        dos.writeUTF("JUNK");
        dos.close();

        try (FileHandle.Builder builder = new FileHandle.Builder(file.getPath()))
        {
            fh = builder.complete();
            try (RandomAccessReader rdr = fh.createReader())
            {
                assertEquals("JUNK", rdr.readUTF());
                assertEquals("JUNK", rdr.readUTF());
            }
            return new RowIndexReader(fh, root, Rebufferer.ReaderConstraint.NONE);
        }
    }

    @Test
    public void testAddEmptyKey() throws Exception
    {
        ClusteringPrefix key = Clustering.EMPTY;
        writer.add(key,  key, new IndexInfo(42, DeletionTime.LIVE));
        try (RowIndexReader summary = completeAndRead())
        {
            IndexInfo i = summary.min();
            assertEquals(42, i.offset);

            i = summary.separatorFloor(comparator.asByteComparableSource(ClusteringBound.BOTTOM));
            assertEquals(42, i.offset);

            i = summary.separatorFloor(comparator.asByteComparableSource(ClusteringBound.TOP));
            assertEquals(END_MARKER, i.offset);

            i = summary.separatorFloor(comparator.asByteComparableSource(key));
            assertEquals(42, i.offset);

        }
    }

    @Test
    public void testConstrainedIteration() throws IOException
    {
        // This is not too relevant: due to the way we construct separators we can't be good enough on the left side.
        Pair<List<ClusteringPrefix>, RowIndexReader> random = generateRandomIndexSingletons(COUNT);
        List<ClusteringPrefix> keys = random.left;
        Random rand = new Random();
        
        for (int i = 0; i < 500; ++i)
        {
            boolean exactLeft = rand.nextBoolean();
            boolean exactRight = rand.nextBoolean();
            ClusteringPrefix left = exactLeft ? keys.get(rand.nextInt(keys.size())) : generateRandomKey();
            ClusteringPrefix right = exactRight ? keys.get(rand.nextInt(keys.size())) : generateRandomKey();
            if (comparator.compare(right, left) < 0)
            {
                ClusteringPrefix t = left; left = right; right = t;
                boolean b = exactLeft; exactLeft = exactRight; exactRight = b;
            }

            try (RowIndexReverseIterator iter = new RowIndexReverseIterator(fh, root, comparator.asByteComparableSource(left), comparator.asByteComparableSource(right), Rebufferer.ReaderConstraint.NONE))
            {
                IndexInfo indexInfo = iter.nextIndexInfo();
                if (indexInfo == null)
                {
                    int idx = Collections.binarySearch(keys, right, comparator);
                    if (idx < 0)
                        idx = -2 - idx; // less than or equal
                    if (idx <= 0)
                        continue;
                    assertTrue(comparator.asByteComparableSource(left) + " <= "
                             + comparator.asByteComparableSource(keys.get(idx)) + " <= "
                             + comparator.asByteComparableSource(right) + " but " + idx + " wasn't iterated.",
                             comparator.compare(left, keys.get(idx - 1)) > 0);
                    continue;
                }

                int idx = (int) indexInfo.offset;
                if (idx > 0)
                    assertTrue(comparator.compare(right, keys.get(idx - 1)) > 0);
                if (idx < keys.size() - 1)
                    assertTrue(comparator.compare(right, keys.get(idx + 1)) < 0);
                if (exactRight)      // must be precise on exact, otherwise could be in any relation
                    assertTrue(right == keys.get(idx));
                while (true)
                {
                    --idx;
                    IndexInfo ii = iter.nextIndexInfo();
                    if (ii == null)
                        break;
                    assertEquals(idx, (int) ii.offset);
                }
                ++idx; // seek at last returned
                if (idx < keys.size() - 1)
                    assertTrue(comparator.compare(left, keys.get(idx + 1)) < 0);
                // Because of the way we build the index (using non-prefix separator) we are usually going to miss the last item.
                if (idx >= 2)
                    assertTrue(comparator.compare(left, keys.get(idx - 2)) > 0);
            } catch (AssertionError e) {
                e.printStackTrace(System.out);
                ClusteringPrefix ll = left; ClusteringPrefix rr = right;
                System.out.println(keys.stream()
                                       .filter(x -> comparator.compare(ll, x) <= 0 && comparator.compare(x, rr) <= 0)
                                       .map(comparator::asByteComparableSource)
                                       .map(Object::toString)
                                       .collect(Collectors.joining(", ")));
                System.out.format("Left %s%s Right %s%s\n", comparator.asByteComparableSource(left), exactLeft ? "#" : "", comparator.asByteComparableSource(right), exactRight ? "#" : "");
                try (RowIndexReverseIterator iter2 = new RowIndexReverseIterator(fh, root, comparator.asByteComparableSource(left), comparator.asByteComparableSource(right), Rebufferer.ReaderConstraint.NONE))
                {
                    IndexInfo ii;
                    while ((ii = iter2.nextIndexInfo()) != null) {
                        System.out.println(comparator.asByteComparableSource(keys.get((int) ii.offset)));
                    }
                    System.out.format("Left %s%s Right %s%s\n", comparator.asByteComparableSource(left), exactLeft ? "#" : "", comparator.asByteComparableSource(right), exactRight ? "#" : "");
                }
                throw e;
            }
        }
    }

    @Test
    public void testReverseIteration() throws IOException
    {
        Pair<List<ClusteringPrefix>, RowIndexReader> random = generateRandomIndexSingletons(COUNT);
        List<ClusteringPrefix> keys = random.left;
        Random rand = new Random();

        for (int i = 0; i < 1000; ++i)
        {
            boolean exactRight = rand.nextBoolean();
            ClusteringPrefix right = exactRight ? keys.get(rand.nextInt(keys.size())) : generateRandomKey();

            try (RowIndexReverseIterator iter = new RowIndexReverseIterator(fh, root, ByteSource.empty(), comparator.asByteComparableSource(right), Rebufferer.ReaderConstraint.NONE))
            {
                IndexInfo indexInfo = iter.nextIndexInfo();
                if (indexInfo == null)
                {
                    int idx = Collections.binarySearch(keys, right, comparator);
                    if (idx < 0)
                        idx = -2 - idx; // less than or equal
                    assertTrue(comparator.asByteComparableSource(keys.get(idx)) + " <= "
                             + comparator.asByteComparableSource(right) + " but " + idx + " wasn't iterated.",
                             idx < 0);
                    continue;
                }

                int idx = (int) indexInfo.offset;
                if (idx > 0)
                    assertTrue(comparator.compare(right, keys.get(idx - 1)) > 0);
                if (idx < keys.size() - 1)
                    assertTrue(comparator.compare(right, keys.get(idx + 1)) < 0);
                if (exactRight)      // must be precise on exact, otherwise could be in any relation
                    assertTrue(right == keys.get(idx));
                while (true)
                {
                    --idx;
                    IndexInfo ii = iter.nextIndexInfo();
                    if (ii == null)
                        break;
                    assertEquals(idx, (int) ii.offset);
                }
                assertEquals(-1, idx);
            } catch (AssertionError e) {
                e.printStackTrace(System.out);
                ClusteringPrefix rr = right;
                System.out.println(keys.stream()
                                       .filter(x -> comparator.compare(x, rr) <= 0)
                                       .map(comparator::asByteComparableSource)
                                       .map(Object::toString)
                                       .collect(Collectors.joining(", ")));
                System.out.format("Right %s%s\n", comparator.asByteComparableSource(right), exactRight ? "#" : "");
                try (RowIndexReverseIterator iter2 = new RowIndexReverseIterator(fh, root, ByteSource.empty(), comparator.asByteComparableSource(right), Rebufferer.ReaderConstraint.NONE))
                {
                    IndexInfo ii;
                    while ((ii = iter2.nextIndexInfo()) != null) {
                        System.out.println(comparator.asByteComparableSource(keys.get((int) ii.offset)));
                    }
                }
                System.out.format("Right %s%s\n", comparator.asByteComparableSource(right), exactRight ? "#" : "");
                throw e;
            }
        }
    }

    private Pair<List<ClusteringPrefix>, RowIndexReader> generateRandomIndexSingletons(int size) throws IOException
    {
        List<ClusteringPrefix> list = generateList(size);
        for (int i = 0; i < size; i++)
        {
            assert i == 0 || comparator.compare(list.get(i - 1), list.get(i)) < 0;
            assert i == 0 || ByteSource.compare(comparator.asByteComparableSource(list.get(i - 1)), comparator.asByteComparableSource(list.get(i))) < 0 : 
                String.format("%s bs %s versus %s bs %s", list.get(i - 1).clustering().clusteringString(comparator.subtypes()), comparator.asByteComparableSource(list.get(i - 1)), list.get(i).clustering().clusteringString(comparator.subtypes()), comparator.asByteComparableSource(list.get(i)));
            writer.add(list.get(i), list.get(i), new IndexInfo(i, DeletionTime.LIVE));
        }

        RowIndexReader summary = completeAndRead();
        return Pair.create(list, summary);
    }

    List<ClusteringPrefix> generateList(int size)
    {
        List<ClusteringPrefix> list = Lists.newArrayList();

        Set<ClusteringPrefix> set = Sets.newHashSet();
        for (int i = 0; i < size; i++)
        {
            ClusteringPrefix key = generateRandomKey(); // keys must be unique
            while (!set.add(key))
                key = generateRandomKey();
            list.add(key);
        }
        Collections.sort(list, comparator);
        return list;
    }

    private Pair<List<ClusteringPrefix>, RowIndexReader> generateRandomIndexQuads(int size) throws IOException
    {
        List<ClusteringPrefix> list = generateList(4 * size + 1);
        for (int i = 0; i < size; i++)
            writer.add(list.get(i * 4 + 1), list.get(i * 4 + 3), new IndexInfo(i, new DeletionTime(i + 2, i - 3)));

        RowIndexReader summary = completeAndRead();
        return Pair.create(list, summary);
    }

    ClusteringPrefix generateRandomKey()
    {
        UUID uuid = UUID.randomUUID();
        ClusteringPrefix key = comparator.make(uuid);
        return key;
    }
}
