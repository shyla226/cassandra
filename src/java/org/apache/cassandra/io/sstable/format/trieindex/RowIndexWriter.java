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

import java.io.IOException;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.sstable.format.trieindex.RowIndexReader.IndexInfo;
import org.apache.cassandra.io.tries.*;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteSource;

/**
 * Preparer / writer of row index tries.
 *
 * Uses IncrementalTrieWriter to build a trie of index section separators of shortest possible length such that
 * prevMax < separator <= nextMin.
 */
class RowIndexWriter
{
    ByteSource prevMax = null;
    ByteSource prevSep = null;
    final ClusteringComparator comparator;
    final IncrementalTrieWriter<IndexInfo> trie;

    public RowIndexWriter(ClusteringComparator comparator, DataOutputPlus out)
    {
        trie = IncrementalTrieWriter.construct(RowIndexReader.trieSerializer, out);
        this.comparator = comparator;
    }

    void add(ClusteringPrefix firstName, ClusteringPrefix lastName, IndexInfo info) throws IOException
    {
        assert info.openDeletion != null;
        ByteSource sep;
        if (prevMax == null)
            sep = ByteSource.empty();
        else
        {
            ByteSource currMin = comparator.asByteComparableSource(firstName);
            sep = ByteSource.separatorGt(prevMax, currMin);
        }
        trie.add(sep, info);
        prevSep = sep;
        prevMax = comparator.asByteComparableSource(lastName);
    }

    public long complete(long endPos) throws IOException
    {
        // Add a separator after the last section, so that greater inputs can be quickly rejected.
        // To maximize its efficiency we add it with the length of the last added separator.
        int i = 0;
        prevMax.reset();
        prevSep.reset();
        while (prevMax.next() == prevSep.next())
            ++i;

        trie.add(nudge(prevMax, i), new IndexInfo(endPos, DeletionTime.LIVE));

        return trie.complete();
    }

    /**
     * Produces a source that is slightly greater than argument with length at least nudgeAt.
     */
    private ByteSource nudge(ByteSource v, int nudgeAt)
    {
        v.reset();
        return new ByteSource.WithToString()
        {
            int cur = 0;

            @Override
            public int next()
            {
                int b = ByteSource.END_OF_STREAM;
                if (cur <= nudgeAt)
                {
                    b = v.next();
                    if (cur == nudgeAt)
                    {
                        if (b < 255)
                            ++b;
                        else
                            return b;  // can't nudge here, increase next instead (eventually will be -1)
                    }
                }
                ++cur;
                return b;
            }

            @Override
            public void reset()
            {
                cur = 0;
                v.reset();
            }
        };
    }
}
