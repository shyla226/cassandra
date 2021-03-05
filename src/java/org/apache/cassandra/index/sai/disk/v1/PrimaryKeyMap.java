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

import java.io.Closeable;
import java.io.IOException;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;

public interface PrimaryKeyMap extends Closeable
{
    PrimaryKey primaryKeyFromRowId(long sstableRowId);

    default PrimaryKeyMap copyOf()
    { return IDENTITY; }

    default void close() throws IOException
    {}

    PrimaryKeyMap IDENTITY = sstableRowId -> PrimaryKey.factory().createKey(new Murmur3Partitioner.LongToken(sstableRowId));

    class DefaultPrimaryKeyMap implements PrimaryKeyMap
    {
        private final IndexComponents indexComponents;
        private final TableMetadata tableMetadata;
        private final FileHandle primaryKeys;
        private final BKDReader reader;
        private final BKDReader.IteratorState iterator;
        private final PrimaryKey.PrimaryKeyFactory keyFactory;

        public DefaultPrimaryKeyMap(IndexComponents indexComponents, TableMetadata tableMetadata) throws IOException
        {
            this.indexComponents = indexComponents;
            this.tableMetadata = tableMetadata;

            MetadataSource metadataSource = MetadataSource.loadGroupMetadata(indexComponents);

            PartitionKeysMeta partitionKeysMeta = new PartitionKeysMeta(metadataSource.get(indexComponents.PRIMARY_KEYS.name));

            this.primaryKeys = indexComponents.createFileHandle(IndexComponents.PRIMARY_KEYS);
            this.reader = new BKDReader(indexComponents, primaryKeys, partitionKeysMeta.bkdPosition);

            this.iterator = reader.iteratorState();

            this.keyFactory = PrimaryKey.factory(tableMetadata);
        }

        private DefaultPrimaryKeyMap(BKDReader reader, PrimaryKey.PrimaryKeyFactory keyFactory) throws IOException
        {
            this.indexComponents = null;
            this.tableMetadata = null;
            this.reader = null;
            this.primaryKeys = null;
            this.iterator = reader.iteratorState();
            this.keyFactory = keyFactory;
        }

        public PrimaryKeyMap copyOf()
        {
            try
            {
                return new DefaultPrimaryKeyMap(reader, keyFactory);
            }
            catch (Throwable e)
            {
                Throwables.unchecked(e);
            }
            return null;
        }

        public PrimaryKey primaryKeyFromRowId(long sstableRowId)
        {
            iterator.seekTo(sstableRowId);
            PrimaryKey key = keyFactory.createKey(iterator.asByteComparable(), sstableRowId);
            return key;
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(reader);
            Throwables.maybeFail(Throwables.close(null, primaryKeys));
        }
    }
}
