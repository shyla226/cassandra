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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.bkd.BKDWriter;


/**
 * Handles intersection of a multi-dimensional shape in byte[] space with a block KD-tree previously written with
 * {@link BKDWriter}.
 */
public class BKDReader extends TraversingBKDReader implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final FileHandle postingsFile;
    private final BKDPostingsIndex postingsIndex;

    /**
     * Performs a blocking read.
     */
    public BKDReader(IndexComponents indexComponents, FileHandle kdtreeFile, long bkdIndexRoot,
                     FileHandle postingsFile, long bkdPostingsRoot) throws IOException
    {
        super(indexComponents, kdtreeFile, bkdIndexRoot);
        this.postingsFile = postingsFile;
        this.postingsIndex = new BKDPostingsIndex(postingsFile, bkdPostingsRoot);
    }

    public static int openPerIndexFiles()
    {
        // kd-tree, posting lists file
        return 2;
    }

    @Override
    public void close()
    {
        try
        {
            super.close();
        }
        finally
        {
            postingsFile.close();
        }
    }

    @SuppressWarnings("resource")
    public PostingList intersect(IntersectVisitor visitor, QueryEventListener.BKDIndexEventListener listener, QueryContext context)
    {
        Relation relation = visitor.compare(minPackedValue, maxPackedValue);

        if (relation == Relation.CELL_OUTSIDE_QUERY)
        {
            listener.onIntersectionEarlyExit();
            return null;
        }

        listener.onSegmentHit();
        IndexInput bkdInput = indexComponents.openInput(indexFile);
        IndexInput postingsInput = indexComponents.openInput(postingsFile);
        IndexInput postingsSummaryInput = indexComponents.openInput(postingsFile);
        PackedIndexTree index = new PackedIndexTree();

        Intersection completable =
                relation == Relation.CELL_INSIDE_QUERY ?
                new Intersection(bkdInput, postingsInput, postingsSummaryInput, index, listener, context) :
                new FilteringIntersection(bkdInput, postingsInput, postingsSummaryInput, index, visitor, listener, context);

        return completable.execute();
    }

    /**
     * Synchronous intersection of an multi-dimensional shape in byte[] space with a block KD-tree
     * previously written with {@link BKDWriter}.
     */
    class Intersection
    {
        private final Stopwatch queryExecutionTimer = Stopwatch.createStarted();
        final QueryContext context;

        final IndexInput bkdInput;
        final IndexInput postingsInput;
        final IndexInput postingsSummaryInput;
        final BKDReader.IndexTree index;
        final QueryEventListener.BKDIndexEventListener listener;

        Intersection(IndexInput bkdInput, IndexInput postingsInput, IndexInput postingsSummaryInput,
                     IndexTree index, QueryEventListener.BKDIndexEventListener listener, QueryContext context)
        {
            this.bkdInput = bkdInput;
            this.postingsInput = postingsInput;
            this.postingsSummaryInput = postingsSummaryInput;
            this.index = index;
            this.listener = listener;
            this.context = context;
        }

        public PostingList execute()
        {
            try
            {
                final List<PostingList> postingLists = new ArrayList<>();
                executeInternal(postingLists);

                FileUtils.closeQuietly(bkdInput);

                return mergePostings(postingLists);
            }
            catch (Throwable t)
            {
                if (!(t instanceof AbortedOperationException))
                    logger.error(indexComponents.logMessage("kd-tree intersection failed on {}"), indexFile.path(), t);

                closeOnException();
                throw Throwables.cleaned(t);
            }
        }

        protected void executeInternal(final List<PostingList> postingLists) throws IOException
        {
            collectPostingLists(postingLists);
        }

        protected void closeOnException()
        {
            FileUtils.closeQuietly(bkdInput, postingsInput, postingsSummaryInput);
        }

        protected PostingList mergePostings(List<PostingList> postingLists)
        {
            final long elapsedMicros = queryExecutionTimer.stop().elapsed(TimeUnit.MICROSECONDS);

            listener.onIntersectionComplete(elapsedMicros, TimeUnit.MICROSECONDS);
            listener.postingListsHit(postingLists.size());

            if (postingLists.isEmpty())
            {
                FileUtils.closeQuietly(postingsInput, postingsSummaryInput);
                return null;
            }
            else
            {
                if (logger.isTraceEnabled())
                    logger.trace(indexComponents.logMessage("[{}] Intersection completed in {} microseconds. {} leaf and internal posting lists hit."),
                                 indexFile.path(), elapsedMicros, postingLists.size());
                return MergePostingList.merge(postingLists, () -> FileUtils.close(postingsInput, postingsSummaryInput));
            }
        }

        public void collectPostingLists(List<PostingList> postingLists) throws IOException
        {
            context.checkpoint();

            final int nodeID = index.getNodeID();

            // if there is pre-built posting for entire subtree
            if (postingsIndex.exists(nodeID))
            {
                postingLists.add(initPostingReader(postingsIndex.getPostingsFilePointer(nodeID)));
                return;
            }

            Preconditions.checkState(!index.isLeafNode(), "Leaf node %s does not have kd-tree postings.", index.getNodeID());

            // Recurse on left sub-tree:
            index.pushLeft();
            collectPostingLists(postingLists);
            index.pop();

            // Recurse on right sub-tree:
            index.pushRight();
            collectPostingLists(postingLists);
            index.pop();
        }

        private PostingList initPostingReader(long offset) throws IOException
        {
            final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(postingsSummaryInput, offset);
            return initPostingReader(summary);
        }

        private PostingList initPostingReader(PostingsReader.BlocksSummary header) throws IOException
        {
            return new PostingsReader(postingsInput, header, listener.postingListEventListener());
        }
    }

    private class FilteringIntersection extends Intersection
    {
        private final BKDReader.IntersectVisitor visitor;
        private final byte[] scratchPackedValue1;
        private final int[] commonPrefixLengths;
        private final short[] origIndex;

        FilteringIntersection(IndexInput bkdInput, IndexInput postingsInput, IndexInput postingsSummaryInput,
                              IndexTree index, BKDReader.IntersectVisitor visitor,
                              QueryEventListener.BKDIndexEventListener listener, QueryContext context)
        {
            super(bkdInput, postingsInput, postingsSummaryInput, index, listener, context);
            this.visitor = visitor;
            this.commonPrefixLengths = new int[numDims];
            this.scratchPackedValue1 = new byte[packedBytesLength];
            this.origIndex = new short[maxPointsInLeafNode];
        }

        @Override
        public void executeInternal(final List<PostingList> postingLists) throws IOException
        {
            collectPostingLists(postingLists, minPackedValue, maxPackedValue);
        }

        public void collectPostingLists(List<PostingList> postingLists, byte[] cellMinPacked, byte[] cellMaxPacked) throws IOException
        {
            context.checkpoint();

            final Relation r = visitor.compare(cellMinPacked, cellMaxPacked);

            if (r == Relation.CELL_OUTSIDE_QUERY)
            {
                // This cell is fully outside of the query shape: stop recursing
                return;
            }

            if (r == Relation.CELL_INSIDE_QUERY)
            {
                // This cell is fully inside of the query shape: recursively add all points in this cell without filtering
                super.collectPostingLists(postingLists);
                return;
            }

            if (index.isLeafNode())
            {
                if (index.nodeExists())
                    filterLeaf(postingLists);
                return;
            }

            visitNode(postingLists, cellMinPacked, cellMaxPacked);
        }

        void filterLeaf(List<PostingList> postingLists) throws IOException
        {
            bkdInput.seek(index.getLeafBlockFP());

            final int count = bkdInput.readVInt();

            // loading doc ids occurred here prior

            final FixedBitSet[] holder = new FixedBitSet[1];

            final int orderMapLength = bkdInput.readVInt();

            final long orderMapPointer = bkdInput.getFilePointer();

            final SeekingRandomAccessInput randoInput = new SeekingRandomAccessInput(bkdInput);
            for (int x = 0; x < count; x++)
            {
                origIndex[x] = (short) LeafOrderMap.getValue(randoInput, orderMapPointer, maxPointsInLeafNode - 1, x);
            }

            // seek beyond the ordermap
            bkdInput.seek(orderMapPointer + orderMapLength);

            IndexInput leafInput = bkdInput;

            visitDocValues(commonPrefixLengths, scratchPackedValue1, leafInput, count, visitor, holder);

            final int nodeID = index.getNodeID();

            if (postingsIndex.exists(nodeID) && holder[0].cardinality() > 0)
            {
                final long pointer = postingsIndex.getPostingsFilePointer(nodeID);
                postingLists.add(initFilteringPostingReader(pointer, holder[0]));
            }
        }

        void visitNode(List<PostingList> postingLists, byte[] cellMinPacked, byte[] cellMaxPacked) throws IOException
        {
            int splitDim = index.getSplitDim();
            assert splitDim >= 0 : "splitDim=" + splitDim;
            assert splitDim < numDims;

            byte[] splitPackedValue = index.getSplitPackedValue();
            BytesRef splitDimValue = index.getSplitDimValue();
            assert splitDimValue.length == bytesPerDim;

            // make sure cellMin <= splitValue <= cellMax:
            assert FutureArrays.compareUnsigned(cellMinPacked, splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + bytesPerDim) <= 0 : "bytesPerDim=" + bytesPerDim + " splitDim=" + splitDim + " numDims=" + numDims;
            assert FutureArrays.compareUnsigned(cellMaxPacked, splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + bytesPerDim) >= 0 : "bytesPerDim=" + bytesPerDim + " splitDim=" + splitDim + " numDims=" + numDims;

            // Recurse on left sub-tree:
            System.arraycopy(cellMaxPacked, 0, splitPackedValue, 0, packedBytesLength);
            System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim * bytesPerDim, bytesPerDim);

            index.pushLeft();
            collectPostingLists(postingLists, cellMinPacked, splitPackedValue);
            index.pop();

            // Restore the split dim value since it may have been overwritten while recursing:
            System.arraycopy(splitPackedValue, splitDim * bytesPerDim, splitDimValue.bytes, splitDimValue.offset, bytesPerDim);
            // Recurse on right sub-tree:
            System.arraycopy(cellMinPacked, 0, splitPackedValue, 0, packedBytesLength);
            System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim * bytesPerDim, bytesPerDim);
            index.pushRight();
            collectPostingLists(postingLists, splitPackedValue, cellMaxPacked);
            index.pop();
        }

        /**
         * Modified copy of BKDReader#visitDocValues()
         */
        private int visitDocValues(int[] commonPrefixLengths,
                                   byte[] scratchPackedValue1,
                                   IndexInput in,
                                   int count,
                                   IntersectVisitor visitor,
                                   FixedBitSet[] holder) throws IOException
        {
            readCommonPrefixes(commonPrefixLengths, scratchPackedValue1, in);

            int compressedDim = readCompressedDim(in);
            if (compressedDim == -1)
            {
                return visitRawDocValues(commonPrefixLengths, scratchPackedValue1, in, count, visitor, holder);
            }
            else
            {
                return visitCompressedDocValues(commonPrefixLengths, scratchPackedValue1, in, count, visitor, compressedDim, holder);
            }
        }

        private PostingList initFilteringPostingReader(long offset, FixedBitSet filter) throws IOException
        {
            final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(postingsSummaryInput, offset);
            return initFilteringPostingReader(filter, summary);
        }

        @SuppressWarnings("resource")
        private PostingList initFilteringPostingReader(FixedBitSet filter, PostingsReader.BlocksSummary header) throws IOException
        {
            PostingsReader postingsReader = new PostingsReader(postingsInput, header, listener.postingListEventListener());
            return new FilteringPostingList(filter, postingsReader);
        }

        /**
         * Modified copy of {@link org.apache.lucene.util.bkd.BKDReader#readCompressedDim(IndexInput)}
         */
        @SuppressWarnings("JavadocReference")
        private int readCompressedDim(IndexInput in) throws IOException
        {
            int compressedDim = in.readByte();
            if (compressedDim < -1 || compressedDim >= numDims)
            {
                throw new CorruptIndexException(String.format("Dimension should be in the range [-1, %d), but was %d.", numDims, compressedDim), in);
            }
            return compressedDim;
        }

        /**
         * Modified copy of BKDReader#visitCompressedDocValues()
         */
        private int visitCompressedDocValues(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in,
                                             int count, IntersectVisitor visitor, int compressedDim,
                                             FixedBitSet[] holder) throws IOException
        {
            // the byte at `compressedByteOffset` is compressed using run-length compression,
            // other suffix bytes are stored verbatim
            final int compressedByteOffset = compressedDim * bytesPerDim + commonPrefixLengths[compressedDim];
            commonPrefixLengths[compressedDim]++;
            int i, collected = 0;

            final FixedBitSet bitSet = new FixedBitSet(maxPointsInLeafNode);

            for (i = 0; i < count; )
            {
                scratchPackedValue[compressedByteOffset] = in.readByte();
                final int runLen = Byte.toUnsignedInt(in.readByte());
                for (int j = 0; j < runLen; ++j)
                {
                    for (int dim = 0; dim < numDims; dim++)
                    {
                        int prefix = commonPrefixLengths[dim];
                        in.readBytes(scratchPackedValue, dim * bytesPerDim + prefix, bytesPerDim - prefix);
                    }
                    if (visitor.visit(scratchPackedValue))
                    {
                        final int rowIDIndex = origIndex[i + j];

                        bitSet.set(rowIDIndex);
                        collected++;
                    }
                }
                i += runLen;
            }
            if (i != count)
            {
                throw new CorruptIndexException(String.format("Expected %d sub-blocks but read %d.", count, i), in);
            }

            holder[0] = bitSet;

            return collected;
        }

        /**
         * Modified copy of BKDReader#visitRawDocValues()
         */
        private int visitRawDocValues(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in,
                                      int count, IntersectVisitor visitor,
                                      FixedBitSet[] holder) throws IOException
        {
            final FixedBitSet bitSet = new FixedBitSet(maxPointsInLeafNode);

            int collected = 0;
            for (int i = 0; i < count; ++i)
            {
                for (int dim = 0; dim < numDims; dim++)
                {
                    int prefix = commonPrefixLengths[dim];
                    in.readBytes(scratchPackedValue, dim * bytesPerDim + prefix, bytesPerDim - prefix);
                }
                if (visitor.visit(scratchPackedValue))
                {
                    final int rowIDIndex = origIndex[i];

                    bitSet.set(rowIDIndex);

                    collected++;
                }
            }
            holder[0] = bitSet;
            return collected;
        }

        /**
         * Copy of BKDReader#readCommonPrefixes()
         */
        private void readCommonPrefixes(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in) throws IOException
        {
            for (int dim = 0; dim < numDims; dim++)
            {
                int prefix = in.readVInt();
                commonPrefixLengths[dim] = prefix;
                if (prefix > 0)
                {
                    in.readBytes(scratchPackedValue, dim * bytesPerDim, prefix);
                }
            }
        }
    }

    public int getNumDimensions()
    {
        return numDims;
    }

    public int getBytesPerDimension()
    {
        return bytesPerDim;
    }

    public long getPointCount()
    {
        return pointCount;
    }

    /**
     * We recurse the BKD tree, using a provided instance of this to guide the recursion.
     */
    public interface IntersectVisitor
    {
        /**
         * Called for all values in a leaf cell that crosses the query.  The consumer
         * should scrutinize the packedValue to decide whether to accept it.  In the 1D case,
         * values are visited in increasing order, and in the case of ties, in increasing order
         * by segment row ID.
         */
        boolean visit(byte[] packedValue);

        /**
         * Called for non-leaf cells to test how the cell relates to the query, to
         * determine how to further recurse down the tree.
         */
        Relation compare(byte[] minPackedValue, byte[] maxPackedValue);
    }
}
