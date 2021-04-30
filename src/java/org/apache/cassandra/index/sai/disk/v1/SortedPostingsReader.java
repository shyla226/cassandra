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

import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexInput;

public class SortedPostingsReader
{
    public static SortedPostingsIndex open(String suffix, IndexComponents components) throws IOException
    {
        IndexComponents.IndexComponent metaComp = components.meta.ndiType.newComponent(suffix);
        IndexComponents.IndexComponent sortedPostingsComp = components.kdTreePostingLists.ndiType.newComponent(suffix);

        IndexInput metaInput = components.openBlockingInput(metaComp);
        StorageAttachedIndexWriter.SortedPostingsMeta sortedPostingsMeta = new StorageAttachedIndexWriter.SortedPostingsMeta(metaInput);
        metaInput.close();

        FileHandle file = components.createFileHandle(sortedPostingsComp);
        IndexInput input = components.openInput(file);
        SortedPostingsIndex sortedPostingsIndex = new SortedPostingsIndex(input, sortedPostingsMeta.postingsIndexFilePointer);
        file.close();
        return sortedPostingsIndex;
    }
}
