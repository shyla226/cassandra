/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.schema;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexFormatUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MockSchema
{
    private static final AtomicInteger id = new AtomicInteger();
    public static final Keyspace ks = Keyspace.mockKS(KeyspaceMetadata.create("mockks", KeyspaceParams.simpleTransient(1)));

    private static final FileHandle RANDOM_ACCESS_READER_FACTORY = new FileHandle.Builder(temp("mocksegmentedfile").getAbsolutePath()).complete();

    public static Memtable memtable(ColumnFamilyStore cfs)
    {
        return new Memtable(cfs.metadata());
    }

    public static SSTableReader sstable(int generation, ColumnFamilyStore cfs)
    {
        return sstable(generation, false, cfs);
    }

    public static SSTableReader sstable(int generation, boolean keepRef, ColumnFamilyStore cfs)
    {
        return sstable(generation, 0, keepRef, cfs);
    }

    public static SSTableReader sstable(int generation, int size, ColumnFamilyStore cfs)
    {
        return sstable(generation, size, false, cfs);
    }

    public static SSTableReader sstable(int generation, int size, boolean keepRef, ColumnFamilyStore cfs)
    {
        Descriptor descriptor = new Descriptor(cfs.getDirectories().getDirectoryForNewSSTables(),
                                               cfs.keyspace.getName(),
                                               cfs.getTableName(),
                                               generation, SSTableFormat.Type.TRIE_INDEX);
        Set<Component> components = ImmutableSet.of(Component.DATA, Component.PARTITION_INDEX, Component.TOC);
        for (Component component : components)
        {
            File file = new File(descriptor.filenameFor(component));
            try
            {
                file.createNewFile();
            }
            catch (IOException e)
            {
            }
        }
        if (size > 0)
        {
            try
            {
                File file = new File(descriptor.filenameFor(Component.DATA));
                try (RandomAccessFile raf = new RandomAccessFile(file, "rw"))
                {
                    raf.setLength(size);
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        SSTableReader reader = TrieIndexFormatUtil.emptyReader(descriptor, components, cfs.metadata,
                                                               RANDOM_ACCESS_READER_FACTORY.sharedCopy(), RANDOM_ACCESS_READER_FACTORY.sharedCopy());
        reader.first = reader.last = readerBounds(generation);
        if (!keepRef)
            reader.selfRef().release();
        return reader;
    }

    public static ColumnFamilyStore newCFS()
    {
        return newCFS(ks.getName());
    }

    public static ColumnFamilyStore newCFS(String ksname)
    {
        String cfname = "mockcf" + (id.incrementAndGet());
        TableMetadata metadata = newTableMetadata(ksname, cfname);
        return new ColumnFamilyStore(ks, cfname, 0, new TableMetadataRef(metadata), new Directories(metadata), false, false, false);
    }

    public static TableMetadata newTableMetadata(String ksname, String cfname)
    {
        return TableMetadata.builder(ksname, cfname)
                            .partitioner(Murmur3Partitioner.instance)
                            .addPartitionKeyColumn("key", UTF8Type.instance)
                            .addClusteringColumn("col", UTF8Type.instance)
                            .addRegularColumn("value", UTF8Type.instance)
                            .caching(CachingParams.CACHE_NOTHING)
                            .build();
    }

    public static BufferDecoratedKey readerBounds(int generation)
    {
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(generation), ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    private static File temp(String id)
    {
        try
        {
            File file = File.createTempFile(id, "tmp");
            file.deleteOnExit();
            return file;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void cleanup()
    {
        // clean up data directory which are stored as data directory/keyspace/data files
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations())
        {
            File dir = new File(dirName);
            if (!dir.exists())
                continue;
            String[] children = dir.list();
            for (String child : children)
                FileUtils.deleteRecursive(new File(dir, child));
        }
    }
}