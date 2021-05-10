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

package org.apache.cassandra.index.sai.cql;

import java.io.File;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.IndexSearcher;
import org.apache.cassandra.index.sai.disk.KDTreeIndexSearcher;
import org.apache.cassandra.index.sai.disk.Segment;
import org.apache.cassandra.index.sai.disk.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.BKDReader;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.PrimaryKeyMap;
import org.apache.cassandra.index.sai.metrics.QueryEventListeners;

import static org.junit.Assert.assertTrue;

public class SortedPostingsCQLTest extends SAITester
{
    @Test
    public void test() throws Throwable
    {
        createTable("CREATE TABLE %s (key int PRIMARY KEY, a int, b int)");

        String indexName = "sai_index_a";
        String indexName2 = "sai_index_b";

        executeNet(String.format("CREATE CUSTOM INDEX %s ON %%s(%s) USING 'StorageAttachedIndex'", indexName, "a"));
        executeNet(String.format("CREATE CUSTOM INDEX %s ON %%s(%s) USING 'StorageAttachedIndex'", indexName2, "b"));

        execute("INSERT INTO %s (key, a, b) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (key, a, b) VALUES (?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (key, a, b) VALUES (?, ?, ?)", 1, 2, 2);
        flush();

        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore().indexManager;

        StorageAttachedIndex index = (StorageAttachedIndex) indexManager.getIndexByName(indexName);
        ColumnContext columnContext = index.getContext();
        for (SSTableIndex ssTableIndex : columnContext.viewManager().getView().getIndexes())
        {
            IndexComponents indexComponents = ssTableIndex.indexComponents();

            MetadataSource metadataSource = MetadataSource.loadColumnMetadata(indexComponents);
            SegmentMetadata segmentMetadata = SegmentMetadata.load(metadataSource, columnContext.keyFactory(), null);

            BKDReader bkdReader = null;

            try (SSTableIndex.PerIndexFiles indexFiles = new SSTableIndex.PerIndexFiles(indexComponents, false))
            {
                Segment segment = new Segment(PrimaryKeyMap.IDENTITY, indexFiles, segmentMetadata, Int32Type.instance);
                KDTreeIndexSearcher searcher = IndexSearcher.open(segment, QueryEventListeners.NO_OP_BKD_LISTENER);

                bkdReader = searcher.bkdReader();
                System.out.println("bkdReader.postingIndexMap="+bkdReader.postingIndexMap.keySet());
            }

            File file = indexComponents.fileFor(indexComponents.kdTreeRowOrdinals);
            assertTrue(file.exists());

            System.out.println("baseFilename="+indexComponents.descriptor.baseFilename());
            System.out.println("kdTreeRowOrdinals.name="+indexComponents.kdTreeRowOrdinals.name);

            for (File subfile : file.getParentFile().listFiles())
            {
                System.out.println("filez="+subfile.getName());
            }

            String suffix = "sorted_"+columnContext.getIndexName()+"_asc";

//            IndexComponents.IndexComponent kdTreeOrderMapComp = indexComponents.kdTreeOrderMaps.ndiType.newComponent(suffix);
//            IndexComponents.IndexComponent kdTreePostingsComp = indexComponents.kdTreePostingLists.ndiType.newComponent(suffix);
//
//            file = indexComponents.fileFor(kdTreeOrderMapComp);
//            assertTrue(file.exists());
//
//            file = indexComponents.fileFor(kdTreePostingsComp);
//            assertTrue(file.exists());
//
//            IndexComponents.IndexComponent metaComp = indexComponents.meta.ndiType.newComponent(suffix);
//            MetadataSource metadataSource = MetadataSource.load(indexComponents.openBlockingInput(metaComp));

           // MetadataSource.load(components.openBlockingInput(components.meta));

        }
    }
}
