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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.tries.MemtableTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.NumericValuesWriter;
import org.apache.cassandra.index.sai.disk.v1.PartitionKeysMeta;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

/**
 * Writes all SSTable-attached index token and offset structures.
 */
public interface SSTableComponentsWriter
{
    Logger logger = LoggerFactory.getLogger(SSTableComponentsWriter.class);

    int MAX_RECURSIVE_KEY_LENGTH = 128;

    SSTableComponentsWriter NONE = (key) -> {};

    void nextRow(PrimaryKey key) throws MemtableTrie.SpaceExhaustedException, IOException;

    default void complete() throws IOException
    {}

    default void abort(Throwable accumulator)
    {}

    class OnDiskSSTableComponentsWriter implements SSTableComponentsWriter
    {
        private final NumericValuesWriter tokenWriter;
        private final MetadataWriter metadataWriter;
        private final Descriptor descriptor;
        private final IndexComponents indexComponents;
        private final MemtableTrie<Long> rowMapping;
        private long minSSTableRowId = Long.MAX_VALUE;
        private long maxSSTableRowId;
        private long rowCount;

        public OnDiskSSTableComponentsWriter(Descriptor descriptor, CompressionParams compressionParams) throws IOException
        {
            this.descriptor = descriptor;
            indexComponents = IndexComponents.perSSTable(descriptor, compressionParams);
            this.rowMapping = new MemtableTrie<>(BufferType.OFF_HEAP);
            this.metadataWriter = new MetadataWriter(indexComponents.createOutput(IndexComponents.GROUP_META));
            this.tokenWriter = new NumericValuesWriter(IndexComponents.TOKEN_VALUES,
                                                       indexComponents.createOutput(IndexComponents.TOKEN_VALUES),
                                                       metadataWriter, false);
        }

        public void nextRow(PrimaryKey key) throws MemtableTrie.SpaceExhaustedException, IOException
        {
            System.out.println("nextRow(" + key + ", " + key.sstableRowId() + ")");
            tokenWriter.add(key.partitionKey.getToken().getLongValue());
            if (key.size() <= MAX_RECURSIVE_KEY_LENGTH)
                rowMapping.putRecursive(v -> key.asComparableBytes(v), key.sstableRowId(), (existing, neww) -> neww);
            else
                rowMapping.apply(Trie.singleton(v -> key.asComparableBytes(v), key.sstableRowId()), (existing, neww) -> neww);
            minSSTableRowId = Math.min(minSSTableRowId, key.sstableRowId());
            maxSSTableRowId = key.sstableRowId();
            rowCount++;
        }

        public void complete() throws IOException
        {
            try (NumericIndexWriter writer = new NumericIndexWriter(indexComponents, 128, maxSSTableRowId, rowCount, false);
                 IndexOutput bkdOutput = indexComponents.createOutput(IndexComponents.PRIMARY_KEYS);
                 IndexOutput metaOutput = metadataWriter.builder(IndexComponents.PRIMARY_KEYS.name))
            {
                PartitionKeysMeta meta = writer.writeIndex(bkdOutput, new OneDimPointValues(rowMapping, 128));
                meta.write(metaOutput);
            }
            IOUtils.close(tokenWriter, metadataWriter);
            indexComponents.createGroupCompletionMarker();
        }

        public void abort(Throwable accumulator)
        {
            logger.debug(indexComponents.logMessage("Aborting token/offset writer for {}..."), descriptor);
            IndexComponents.deletePerSSTableIndexComponents(descriptor);
        }
    }

    class OneDimPointValues extends MutableOneDimPointValues
    {
        private final MemtableTrie<Long> trie;
        private final byte[] buffer;

        OneDimPointValues(MemtableTrie<Long> trie, int maximumSize)
        {
            this.trie = trie;
            this.buffer = new byte[maximumSize];
        }

        @Override
        public void intersect(IntersectVisitor visitor) throws IOException
        {
            try
            {
                Iterator<Map.Entry<ByteComparable, Long>> iterator = trie.entrySet().iterator();
                while (iterator.hasNext())
                {
                    Map.Entry<ByteComparable, Long> entry = iterator.next();
                    ByteBufferUtil.toBytes(entry.getKey().asComparableBytes(ByteComparable.Version.OSS41), buffer);
                    long rowId = entry.getValue();
                    visitor.visit(rowId, buffer);
                }
            }
            catch (Exception e)
            {
                throw new IOException(e);
            }
        }
    }
}
