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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.primitives.Ints;

import org.apache.commons.cli.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.trieindex.PartitionIndex;
import org.apache.cassandra.io.tries.TrieNode;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.PageAware;

/**
 * Analyzes sstable's partition index.
 */
public class IndexAnalyzer
{

    private static final Options options = new Options();
    private static CommandLine cmd;

    static
    {
        DatabaseDescriptor.toolInitialization();
    }

    /**
     * Given arguments specifying an SSTable, analyze the partition index trie and output some information about its
     * layout.
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
            System.err.println("You must supply exactly one sstable index");
            printUsage();
            System.exit(1);
        }

        String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();

        if (!new File(ssTableFileName).exists())
        {
            System.err.println("Cannot find file " + ssTableFileName);
            System.exit(1);
        }
        Descriptor desc = Descriptor.fromFilename(ssTableFileName);
        String fname = ssTableFileName.contains("Partitions") ? ssTableFileName : desc.filenameFor(Component.PARTITION_INDEX);
        try (FileHandle.Builder fhBuilder = new FileHandle.Builder(fname).bufferSize(PageAware.PAGE_SIZE);
             PartitionIndex index = PartitionIndex.load(fhBuilder, null, false);
             Analyzer analyzer = new Analyzer(index, Rebufferer.ReaderConstraint.NONE))
        {
            analyzer.run();
            analyzer.printResults();
        }
        catch (IOException e)
        {
            // throwing exception outside main with broken pipe causes windows cmd to hang
            e.printStackTrace(System.err);
        }

        System.exit(0);
    }

    private static void printUsage()
    {
        String usage = String.format("analyzeindex <options> <sstable file path>%n");
        String header = "Print an analysis of the sstable index.";
        new HelpFormatter().printHelp(usage, header, options, "");
    }
    
    static class LevelStats
    {
        BitSet pages = new BitSet();
        long[] countPerType = new long[16];
        long[] bytesPerType = new long[16];
        long bytesInPayload = 0;
        long payloadCount = 0;
        List<AtomicLong> enterDepthHistogram = new ArrayList<>();

        public void join(LevelStats level)
        {
            pages.or(level.pages);
            for (int l = 0; l < countPerType.length; ++l)
            {
                countPerType[l] += level.countPerType[l];
                bytesPerType[l] += level.bytesPerType[l];
            }
            bytesInPayload += level.bytesInPayload;
            payloadCount += level.payloadCount;
        }

        public static String humanReadable(long bytes, boolean si) {
            int unit = si ? 1000 : 1024;
            int exp = (int) (Math.log(bytes) / Math.log(unit));
            exp = Math.max(0, exp);
            String pre = (si ? " kMGTPE" : " KMGTPE").charAt(exp) + (si ? "" : exp > 0 ? "i" : " ");
            return String.format("%.1f %s", bytes / Math.pow(unit, exp), pre);
        }

        public void print(String string, LevelStats combined, int combinedPages)
        {
            System.out.println("Node statistics " + string);
            int pc = pages.cardinality();
            System.out.format("Page count %,d (%sb, %.1f%%)\n", pc, humanReadable(pc * 4096L, false), pc * 100.0 / combinedPages);
            long allBytes = combinedPages * 4096L;
            long levelBytes = pc * 4096L;
            long nodeBytes = Arrays.stream(bytesPerType).sum();
            long nodeCount = Arrays.stream(countPerType).sum();
            for (int i = 0; i < countPerType.length; ++i)
            {
                if (countPerType[i] == 0)
                    continue;
                System.out.format("%15s count %,12d, bytes %s\n",
                        TrieNode.nodeTypeString(i),
                        countPerType[i],
                        bytePrint(bytesPerType[i], levelBytes, allBytes));
            }
            System.out.format("%15s count %,12d, bytes %s\n",
                    "All nodes",
                    nodeCount,
                    bytePrint(nodeBytes, levelBytes, allBytes));
            System.out.format("%15s count %,12d, bytes %s\n",
                    "Payload",
                    payloadCount,
                    bytePrint(bytesInPayload, levelBytes, allBytes));
            System.out.format("%15s count %,12d, bytes %s\n",
                    "Altogether",
                    nodeCount,
                    bytePrint(bytesInPayload + nodeBytes, levelBytes, allBytes));
            if (!enterDepthHistogram.isEmpty())
                printHistogram(enterDepthHistogram, "Enter depths");
            System.out.println();
        }

        private String bytePrint(long l, long levelBytes, long allBytes)
        {
            return String.format("%8sb (%5.2f%% level, %5.2f%% all)", humanReadable(l, false), l * 100.0 / levelBytes, l * 100.0 / allBytes);
        }
    }

    static class Analyzer extends PartitionIndex.Reader
    {
        List<LevelStats> levelStats = new ArrayList<>();
        List<LevelStats> bottomUpStats = new ArrayList<>();
        List<AtomicLong> countPerDepth = new ArrayList<>();
        List<AtomicLong> countPerPageDepth = new ArrayList<>();

        Analyzer(PartitionIndex index, Rebufferer.ReaderConstraint rc)
        {
            super(index, rc);
        }
        
        void run()
        {
            run(root, -1, -1, -1);
        }
        
        int run(long node, long prevNode, int depth, int pageDepth)
        {
            go(node);
            ++depth;
            int pageId = Ints.checkedCast(node >> 12);
            boolean newPage = pageId != prevNode >> 12;
            if (newPage)
                ++pageDepth;

            LevelStats ls = levelStats(levelStats, pageDepth);
            updateLevelStats(ls, depth, pageId, newPage);
            if (payloadFlags() != 0)
            {
                incCount(countPerDepth, depth);
                incCount(countPerPageDepth, pageDepth);
            }

            int tr = transitionRange();
            int bottomUpLevel = 0;
            for (int i = 0; i < tr; ++i)
            {
                long child = transition(i);
                if (child == -1)
                    continue;
                int childblevel = run(child, node, depth, pageDepth);
                bottomUpLevel = Math.max(bottomUpLevel, childblevel);
                go(node);
            }
            LevelStats bup = levelStats(bottomUpStats, bottomUpLevel);
            updateLevelStats(bup, depth, pageId, newPage);
            if (newPage)
                ++bottomUpLevel;
            return bottomUpLevel;
        }

        void updateLevelStats(LevelStats ls, int depth, int pageId, boolean newPage)
        {
            if (newPage)
                incCount(ls.enterDepthHistogram, depth);
            ls.pages.set(pageId);
            ++ls.countPerType[nodeTypeOrdinal()];
            ls.bytesPerType[nodeTypeOrdinal()] += nodeSize();
            if (payloadFlags() != 0)
            {
                ++ls.payloadCount;
                ls.bytesInPayload += payloadSize();
            }
        }

        private void incCount(List<AtomicLong> list, int index)
        {
            while (index >= list.size())
                list.add(new AtomicLong());
            list.get(index).incrementAndGet();
        }

        private LevelStats levelStats(List<LevelStats> levelStats, int pageDepth)
        {
            while (pageDepth >= levelStats.size())
                levelStats.add(new LevelStats());

            return levelStats.get(pageDepth);
        }

        void printResults()
        {
            LevelStats combined = new LevelStats();
            for (LevelStats level : levelStats)
                combined.join(level);
            int lvl = 0;
            int combinedPages = combined.pages.cardinality();
            for (LevelStats level : levelStats)
            {
                level.print("at page level " + (++lvl), combined, combinedPages);
            }
            lvl = 0;
            for (LevelStats level : bottomUpStats)
            {
                level.print("at bottom-up level " + (lvl++), combined, combinedPages);
            }
            combined.print("for all levels", combined, combinedPages);

            printHistogram(countPerDepth, "Depth");
            printHistogram(countPerPageDepth, "Page depth");
        }
    }


    private static void printHistogram(List<AtomicLong> list, String string)
    {
        System.out.println(string);
        long totalCount = list.stream().mapToLong(AtomicLong::get).sum();
        long sum = 0;
        for (int i = 0; i < list.size(); ++i)
        {
            long count = list.get(i).get();
            if (count == 0)
                continue;
            System.out.format("%s %3d: %,12d (%5.2f%%)\n", string, i + 1, count, count * 100.0 / totalCount);
            sum += count * (i + 1);
        }
        System.out.format("%s mean %.2f\n", string, sum * 1.0 / totalCount);
    }
}
