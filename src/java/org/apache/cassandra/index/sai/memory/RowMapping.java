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
package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

import com.carrotsearch.hppc.IntArrayList;
import org.agrona.collections.Object2IntHashMap;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sai.disk.SegmentBuilder;
import org.apache.cassandra.index.sai.utils.AbstractIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.utils.Pair;

/**
 * In memory representation of {@link PrimaryKey} to row ID mappings which only contains
 * {@link Row} regardless it's live or deleted. ({@link RangeTombstoneMarker} is not included.)
 *
 * For JBOD, we can make use of sstable min/max partition key to filter irrelevant {@link MemtableIndex} subranges.
 */
public class RowMapping
{
    private static final int MISSING_VALUE = -1;

    public static final RowMapping DUMMY = new RowMapping()
    {
        @Override
        public Iterator<Pair<ByteBuffer, IntArrayList>> merge(MemtableIndex index) { return Collections.emptyIterator(); }

        @Override
        public void complete() {}

        @Override
        public void add(DecoratedKey key, Unfiltered unfiltered, long sstableRowId) {}
    };

    private final Object2IntHashMap<PrimaryKey> rowMapping = new Object2IntHashMap<>(MISSING_VALUE);

    private volatile boolean complete = false;

    public DecoratedKey minKey;
    public DecoratedKey maxKey;

    public int maxSegmentRowId = -1;

    private RowMapping()
    {
    }

    /**
     * Create row mapping for FLUSH operation only.
     */
    public static RowMapping create(OperationType opType)
    {
        if (opType == OperationType.FLUSH)
            return new RowMapping();
        return DUMMY;
    }

    /**
     * Merge IndexMemtable(index term to PrimaryKeys mappings) with row mapping of a sstable
     * (PrimaryKey to RowId mappings).
     *
     * @param index a Memtable-attached column index
     *
     * @return iterator of index term to postings mapping exists in the sstable
     */
    public Iterator<Pair<ByteBuffer, IntArrayList>> merge(MemtableIndex index)
    {
        assert complete : "RowMapping is not built.";

        Iterator<Pair<ByteBuffer, PrimaryKeys>> iterator = index.iterator();
        return new AbstractIterator<Pair<ByteBuffer, IntArrayList>>()
        {
            @Override
            protected Pair<ByteBuffer, IntArrayList> computeNext()
            {
                while (iterator.hasNext())
                {
                    Pair<ByteBuffer, PrimaryKeys> pair = iterator.next();

                    IntArrayList postings = null;
                    Iterator<PrimaryKey> primaryKeys = pair.right.iterator();

                    while (primaryKeys.hasNext())
                    {
                        PrimaryKey primaryKey = primaryKeys.next();
                        Integer segmentRowId = rowMapping.getOrDefault(primaryKey, MISSING_VALUE);

                        if (segmentRowId != MISSING_VALUE)
                        {
                            postings = postings == null ? new IntArrayList() : postings;
                            postings.add(segmentRowId);
                        }
                    }
                    if (postings != null && !postings.isEmpty())
                        return Pair.create(pair.left, postings);
                }
                return endOfData();
            }
        };
    }

    /**
     * Complete building in memory RowMapping, mark it as immutable.
     */
    public void complete()
    {
        assert !complete : "RowMapping can only be built once.";
        this.complete = true;
    }

    /**
     * Include PrimaryKey to RowId mapping
     */
    public void add(DecoratedKey key, Unfiltered unfiltered, long sstableRowId)
    {
        assert !complete : "Cannot modify built RowMapping.";

        if (unfiltered.isRangeTombstoneMarker())
        {
            // currently we don't record range tombstones..
        }
        else
        {
            assert unfiltered.isRow();
            Row row = (Row) unfiltered;
            PrimaryKey pk = PrimaryKey.of(key, row.clustering());

            // we don't expect to have more than 2B rows flushed from memtable,
            // so we can use IntArrayList to reduce memory usage.
            int segmentRowId = SegmentBuilder.castToSegmentRowId(sstableRowId, 0);
            rowMapping.put(pk, segmentRowId);

            maxSegmentRowId = Math.max(maxSegmentRowId, segmentRowId);

            // data is written in token sorted order
            if (minKey == null)
                minKey = key;
            maxKey = key;
        }
    }

    public boolean hasRows()
    {
        return maxSegmentRowId >= 0;
    }
}
