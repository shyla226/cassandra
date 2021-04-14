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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.github.jamm.MemoryLayoutSpecification;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cassandra.index.sai.utils.SAICodecUtils.validate;

/**
 * Mapping between node ID and an offset to its auxiliary posting list (containing every row id from all leaves
 * reachable from that node. See {@link OneDimBKDPostingsWriter}).
 */
class BKDPostingsIndex
{
    private final int size;
    public final Map<Integer, OneDimBKDPostingsWriter.NodeEntry> index = new HashMap();

    @SuppressWarnings("resource")
    BKDPostingsIndex(FileHandle postingsFileHandle, long filePosition) throws IOException
    {
        try (final RandomAccessReader reader = postingsFileHandle.createReader())
        {
            final IndexInputReader input = IndexInputReader.create(reader);
            validate(input);
            input.seek(filePosition);

            size = input.readVInt();

            for (int x = 0; x < size; x++)
            {
                final int node = input.readVInt();
                final long filePointer = input.readVLong();
                final int numLeaves = input.readVInt();
                final List<Integer> leafNodes = new ArrayList<>();
                for (int l=0; l < numLeaves; l++)
                {
                    final int leafNodeID = input.readVInt();
                    if (node != leafNodeID)
                        leafNodes.add(leafNodeID);
                }

                index.put(node, new OneDimBKDPostingsWriter.NodeEntry(filePointer, leafNodes));
            }
        }
    }

    public SortedMap<Long,Integer> toFilePointers()
    {
        final TreeMap<Long,Integer> map = new TreeMap<>();
        for (Map.Entry<Integer, OneDimBKDPostingsWriter.NodeEntry> entry : index.entrySet())
        {
            final int nodeID = entry.getKey();
            final long filePointer = entry.getValue().postingsFilePointer;
            map.put(filePointer, nodeID);
        }
        return map;
    }

    public long memoryUsage()
    {
        // IntLongHashMap uses two arrays: one for keys, one for values.
        return MemoryLayoutSpecification.sizeOfArray(index.size(), 4L)
               + MemoryLayoutSpecification.sizeOfArray(index.size(), 8L);
    }

    /**
     * Returns <tt>true</tt> if given node ID has an auxiliary posting list.
     */
    boolean exists(int nodeID)
    {
        checkArgument(nodeID > 0);
        return index.containsKey(nodeID);
    }

    /**
     * Returns an offset within the bkd postings file to the begining of the blocks summary of given node's auxiliary
     * posting list.
     *
     * @throws IllegalArgumentException when given nodeID doesn't have an auxiliary posting list. Check first with
     * {@link #exists(int)}
     */
    long getPostingsFilePointer(int nodeID)
    {
        checkArgument(exists(nodeID), "nodeID="+nodeID+" availableNodeIDs="+index.keySet());
        return index.get(nodeID).postingsFilePointer;
    }

    int size()
    {
        return size;
    }
}
