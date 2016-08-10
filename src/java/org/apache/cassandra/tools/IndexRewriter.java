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
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.commons.cli.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.trieindex.PartitionIndex;
import org.apache.cassandra.io.sstable.format.trieindex.PartitionIndexBuilder;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexEntry;
import org.apache.cassandra.io.sstable.format.trieindex.PartitionIndex.IndexPosIterator;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * Rewrites table's partition index.
 */
public class IndexRewriter
{

    private static final String KEY_RENAME = "r";
    private static final Options options = new Options();
    private static CommandLine cmd;

    static
    {
        DatabaseDescriptor.toolInitialization();

        Option optKey = new Option(KEY_RENAME, false, "Rename files after completion so that rewritten one becomes active.");
        options.addOption(optKey);
    }

    /**
     * Given arguments specifying an sstable, recreate the table's partition index.
     *
     * @param args
     *            command lines arguments
     * @throws ConfigurationException
     *             on configuration failure (wrong params given)
     */
    public static void main(String[] args) throws ConfigurationException
    {
        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            printUsage();
            System.exit(1);
        }

        if (cmd.getArgs().length != 1)
        {
            System.err.println("You must supply exactly one sstable");
            printUsage();
            System.exit(1);
        }

        String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();

        File srcFile = new File(ssTableFileName);
        if (!srcFile.exists())
        {
            System.err.println("Cannot find file " + ssTableFileName);
            System.exit(1);
        }
        try
        {
            if (srcFile.isFile())
                rewriteIndex(ssTableFileName);
            else
                for (Descriptor desc : Arrays.stream(srcFile.listFiles()).filter(File::isFile).map(File::getPath).map(Descriptor::fromFilename).collect(Collectors.toSet()))
                    rewriteIndex(desc.filenameFor(Component.PARTITION_INDEX));
        }
        catch (IOException e)
        {
            // throwing exception outside main with broken pipe causes windows cmd to hang
            e.printStackTrace(System.err);
        }

        System.exit(0);
    }

    static class KeyIterator extends IndexPosIterator implements CloseableIterator<DecoratedKey>
    {
        private PartitionIndex index;
        private FileHandle dFile;
        private FileHandle riFile;
        private final IPartitioner partitioner;

        private long indexPosition;
        private long dataPosition;

        @SuppressWarnings("resource")   // KeyIterator closes resources
        public static KeyIterator create(Descriptor desc, TableMetadata metadata)
        {
            IPartitioner partitioner = metadata.partitioner;
            boolean compressedData = new File(desc.filenameFor(Component.COMPRESSION_INFO)).exists();
            try (FileHandle.Builder piBuilder = SSTableReader.indexFileHandleBuilder(desc, Component.PARTITION_INDEX);
                 FileHandle.Builder riBuilder = SSTableReader.indexFileHandleBuilder(desc, Component.ROW_INDEX);
                 FileHandle.Builder dBuilder = SSTableReader.dataFileHandleBuilder(desc, compressedData))
            {
                PartitionIndex index = PartitionIndex.load(piBuilder, partitioner, false);
                FileHandle dFile = dBuilder.complete();
                FileHandle riFile = riBuilder.complete();
                return new KeyIterator(index, dFile, riFile, partitioner);
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        private KeyIterator(PartitionIndex index, FileHandle dFile, FileHandle riFile, IPartitioner partitioner)
        {
            super(index);
            this.partitioner = partitioner;
            this.dFile = dFile;
            this.riFile = riFile;
        }

        @Override
        public boolean hasNext()
        {
            try
            {
                indexPosition = nextIndexPos();
                return indexPosition != PartitionIndex.NOT_FOUND;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }


        @Override
        public DecoratedKey next()
        {
            assert indexPosition != PartitionIndex.NOT_FOUND;
            try
            {
                try (FileDataInput in = indexPosition >= 0 ? riFile.createReader(indexPosition) : dFile.createReader(~indexPosition))
                {
                    DecoratedKey key = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
                    if (indexPosition >= 0)
                        dataPosition = TrieIndexEntry.deserialize(in, in.getFilePointer()).position;
                    else
                        dataPosition = ~indexPosition;
                    return key;
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(index);
            FileUtils.closeQuietly(dFile);
            FileUtils.closeQuietly(riFile);
        }

        public long getBytesRead()
        {
            return dataPosition;
        }

        public long getTotalBytes()
        {
            return dFile.dataLength();
        }

        public long getIndexPosition()
        {
            return indexPosition;
        }

        public long getDataPosition()
        {
            return dataPosition;
        }
    }

    static void rewriteIndex(String ssTableFileName) throws IOException
    {
        System.out.format("Rewriting %s\n", ssTableFileName);
        Descriptor desc = Descriptor.fromFilename(ssTableFileName);
        TableMetadata metadata = SSTableExport.metadataFromSSTable(desc);
        File destFile = new File(desc.filenameFor(Component.PARTITION_INDEX) + ".rewrite");
        try (SequentialWriter writer = new SequentialWriter(destFile);
             PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, null);
             KeyIterator iter = KeyIterator.create(desc, metadata))
        {
            int nextPrint = 1;
            while (iter.hasNext())
            {
                DecoratedKey key = iter.next();
                long pos = iter.getIndexPosition();
                builder.addEntry(key, pos);

                if (iter.getBytesRead() * 100 / iter.getTotalBytes() >= nextPrint)
                {
                    System.out.print('.');
                    if (nextPrint % 10 == 0)
                        System.out.print(nextPrint + "%\n");
                    ++nextPrint;
                }
            }
            builder.complete();
            if (cmd.hasOption(KEY_RENAME))
            {
                File srcFile = new File(desc.filenameFor(Component.PARTITION_INDEX));
                srcFile.renameTo(new File(srcFile.getPath() + ".original"));
                destFile.renameTo(srcFile);
                System.out.print(".Renamed");
            }
            System.out.println(".Done");
        }
    }

    private static void printUsage()
    {
        String usage = String.format("IndexRewriter <options> <sstable file or directory>%n");
        String header = "Recreate a partition index trie.";
        new HelpFormatter().printHelp(usage, header, options, "");
    }
}
