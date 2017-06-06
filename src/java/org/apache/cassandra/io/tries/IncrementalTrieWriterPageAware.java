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
package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.util.*;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.PageAware;

/**
 * Incremental builders of on-disk tries which packs trie stages into disk cache pages.
 *
 * The incremental core is as in {@code IncrementalTrieWriterSimple}, which this augments by:
 *   - calculating branch sizes reflecting the amount of data that needs to be written to store the trie
 *     branch rooted at each node
 *   - delaying writing any part of a completed node until its branch size is above the page size 
 *   - laying out (some of) its children branches (each smaller than a page) to be contained within a page
 *   - adjusting the branch size to reflect the fact that the children are now written (i.e. removing their size)
 * The process is bottom-up, i.e. pages are packed at the bottom and the root page is usually smaller. This is not
 * optimally efficient as the root page is normally always in cache and we should be using the most of it. Unfortunately
 * this is unavoidable for an incremental process.
 * 
 * As an example, taking a sample page size fitting 4 nodes, a simple trie would be split like this:
 * Node 0 |
 *   -a-> | Node 1
 *        |   -s-> Node 2
 *        |          -k-> Node 3 (payload 1)
 *        |          -s-> Node 4 (payload 2)
 *        -----------------------------------
 *   -b-> Node 5 |
 *          -a-> |Node 6
 *               |  -n-> Node 7
 *               |         -k-> Node 8 (payload 3)
 *               |                -s-> Node 9 (payload 4)
 * where lines denote page boundaries.
 * 
 * The process itself will start by adding "ask" which adds three nodes after the root to the stack. Adding "ass"
 * completes Node 3, setting its branch a size of 1 and replaces it on the stack with Node 4.
 * The step of adding "bank" starts by completing Node 4 (size 1), Node 2 (size 3), Node 1 (size 4), then adds 4 more
 * nodes to the stack. Adding "banks" descends one more node.
 * The trie completion step completes nodes 9 (size 1), 8 (size 2), 7 (size 3), 6 (size 4), 5 (size 5). Since the size
 * of node 5 is above the page size, the algorithm lays out its children. Nodes 6, 7, 8, 9 are written in order. The
 * size of node 5 is now just the size of it individually, 1. The process continues with completing Node 0 (size 6).
 * This is bigger than the page size, so some of its children need to be written. The algorithm takes the largest,
 * Node 1, and lays it out with its children in the file. Node 0 now has an adjusted size of 2 which is below the
 * page size and we can continue the process.
 * Since this was the root of the trie, the current page is padded and the remaining nodes 0, 5 are written.
 */
public class IncrementalTrieWriterPageAware<Value>
extends IncrementalTrieWriterBase<Value, DataOutputPlus, IncrementalTrieWriterPageAware.Node<Value>>
implements IncrementalTrieWriter<Value>
{
    private final static Comparator<Node<?>> branchSizeComparator = (l, r) ->
    {
        // Smaller branches first.
        int c = Integer.compare(l.branchSize + l.nodeSize, r.branchSize + r.nodeSize);
        if (c != 0)
            return c;

        // Make sure item we use for comparison key comes greater than all equals.
        c = Long.compare(l.filePos, r.filePos);
        if (c != 0)
            return c;

        // Then enforce inequality to allow duplicates.
        return Integer.compare(l.hashCode(), r.hashCode());
    };

    public IncrementalTrieWriterPageAware(TrieSerializer<Value, ? super DataOutputPlus> serializer, DataOutputPlus dest)
    {
        super(serializer, dest, new Node<>((byte) 0));
    }

    @Override
    Node<Value> performCompletion() throws IOException
    {
        Node<Value> root = super.performCompletion();

        int actualSize = recalcTotalSize(root, dest.position());
        int bytesLeft = bytesLeftInPage();
        if (actualSize > bytesLeft)
        {
            if (actualSize <= PageAware.PAGE_SIZE)
            {
                PageAware.pad(dest);
                bytesLeft = PageAware.PAGE_SIZE;
                // position changed, recalc again
                actualSize = recalcTotalSize(root, dest.position());
            }

            if (actualSize > bytesLeft)
            {
                // Still greater. Lay out children separately.
                layoutChildren(root);

                // Pad if needed and place.
                if (root.nodeSize > bytesLeftInPage())
                {
                    PageAware.pad(dest);
                    // Recalc again as pointer size may have changed, triggering assertion in writeRecursive.
                    recalcTotalSize(root, dest.position());
                }
            }
        }


        root.finalizeWithPosition(writeRecursive(root));
        return root;
    }

    @Override
    void complete(Node<Value> node) throws IOException
    {
        assert node.filePos == -1;

        int branchSize = 0;
        for (Node<Value> child : node.children)
            branchSize += child.branchSize + child.nodeSize;

        node.branchSize = branchSize;

        int nodeSize = serializer.sizeofNode(node, dest.position());
        if (nodeSize + branchSize < PageAware.PAGE_SIZE)
        {
            // Good. This node and all children will (most probably) fit page.
            node.nodeSize = nodeSize;
            node.hasOutOfPageChildren = false;
            node.hasOutOfPageInBranch = false;

            for (Node<Value> child : node.children)
                if (child.filePos != -1)
                    node.hasOutOfPageChildren = true;
                else if (child.hasOutOfPageChildren || child.hasOutOfPageInBranch)
                    node.hasOutOfPageInBranch = true;

            return;
        }

        // Cannot fit. Lay out children; The current node will be marked with one with out-of-page children.
        layoutChildren(node);
    }

    void layoutChildren(Node<Value> node) throws IOException
    {
        assert node.filePos == -1;

        NavigableSet<Node<Value>> children = new TreeSet<>(branchSizeComparator);
        for (Node<Value> child : node.children)
            if (child.filePos == -1)
                children.add(child);

        int bytesLeft = bytesLeftInPage();
        Node<Value> cmp = new Node<Value>((byte) 0);
        cmp.filePos = 0;        // goes after all equal-sized unplaced nodes (whose filePos is -1)
        cmp.nodeSize = 0;
        while (!children.isEmpty())
        {
            cmp.branchSize = bytesLeft;
            Node<Value> child = children.headSet(cmp, true).pollLast();    // grab biggest that could fit
            if (child == null)
            {
                PageAware.pad(dest);
                bytesLeft = PageAware.PAGE_SIZE;
                child = children.pollLast();       // just biggest
            }

            if (child.hasOutOfPageChildren || child.hasOutOfPageInBranch)
            {
                // We didn't know what size this branch will actually need to be, node's children may be far.
                // We now know where we would place it, so let's reevaluate size.
                int actualSize = recalcTotalSize(child, dest.position());
                if (actualSize > bytesLeft)
                {
                    if (bytesLeft == PageAware.PAGE_SIZE)
                    {
                        // Branch doesn't even fit in a page.

                        // Note: In this situation we aren't actually making the best choice as the layout should have
                        // taken place at the child (which could have made the current parent small enough to fit).
                        // This is not trivial to fix but should be very rare.

                        layoutChildren(child);
                        bytesLeft = bytesLeftInPage();

                        assert (child.filePos == -1);
                    }

                    // Doesn't fit, but that's probably because we don't have a full page. Put it back with the new
                    // size and retry when we do have enough space.
                    children.add(child);
                    continue;
                }
            }

            child.finalizeWithPosition(writeRecursive(child));
            bytesLeft = bytesLeftInPage();
        }

        // The sizing below will use the branch size, so make sure it's set.
        node.branchSize = 0;
        node.hasOutOfPageChildren = true;
        node.hasOutOfPageInBranch = false;
        node.nodeSize = serializer.sizeofNode(node, dest.position());
    }

    private int recalcTotalSize(Node<Value> node, long nodePosition)
    {
        if (node.hasOutOfPageInBranch)
        {
            int sz = 0;
            for (Node<Value> child : node.children)
                sz += recalcTotalSize(child, nodePosition + sz);
            node.branchSize = sz;
        }

        // The sizing below will use the branch size, so make sure it's set.
        if (node.hasOutOfPageChildren)
            node.nodeSize = serializer.sizeofNode(node, nodePosition + node.branchSize);

        return node.branchSize + node.nodeSize;
    }

    private long writeRecursive(Node<Value> node) throws IOException
    {
        long nodePosition = dest.position();
        for (Node<Value> child : node.children)
            if (child.filePos == -1)
                child.filePos = writeRecursive(child);

        nodePosition += node.branchSize;
        assert dest.position() == nodePosition;

        serializer.write(dest, node, nodePosition);

        assert dest.position() == nodePosition + node.nodeSize
               || PageAware.padded(dest.position()) == dest.position(); // For PartitionIndexTest.testPointerGrowth where position may jump on page boundaries.

        return nodePosition;
    }

    int bytesLeftInPage()
    {
        long position = dest.position();
        long bytesLeft = PageAware.pageLimit(position) - position;
        return (int) bytesLeft;
    }

    @Override
    public PartialTail makePartialRoot() throws IOException
    {
        // The expectation is that the partial tail will be in memory, so we don't bother with page-fitting.
        // We could also send some completed children to disk, but that could make suboptimal layout choices so we'd
        // rather not. Just write anything not written yet to a buffer, from bottom to top, and we're done.
        try (DataOutputBuffer buf = new DataOutputBuffer())
        {
            PTail tail = new PTail();
            // Readers ask rebufferers for page-aligned positions, so make sure tail starts at one.
            // "Padding" of the cutoff point may leave some unaddressable space in the constructed file view.
            // Nothing will point to it, though, so that's fine.
            tail.cutoff = PageAware.padded(dest.position());
            tail.count = count;
            tail.root = writePartialRecursive(stack.getFirst(), buf, tail.cutoff);
            tail.tail = buf.trimmedBuffer();
            return tail;
        }
    }

    private long writePartialRecursive(Node<Value> node, DataOutputPlus dest, long baseOffset) throws IOException
    {
        List<Node<Value>> childrenToClear = new ArrayList<>();
        for (Node<Value> child : node.children)
            if (child.filePos == -1)
            {
                childrenToClear.add(child);
                child.filePos = writePartialRecursive(child, dest, baseOffset);
            }

        long nodePosition = dest.position() + baseOffset;
        serializer.write(dest, node, nodePosition);
        for (Node<Value> child : childrenToClear)
            child.filePos = -1;
        return nodePosition;
    }

    static class Node<Value> extends IncrementalTrieWriterBase.BaseNode<Value, Node<Value>>
    {
        /**
         * Currently calculated size of the branch below this node, not including the node itself.
         * If hasOutOfPageInBranch is true, this may be underestimated as the size
         * depends on the position the branch is written.
         */
        int branchSize = -1;
        /**
         * Currently calculated node size. If hasOutOfPageChildren is true, this may be underestimated as the size
         * depends on the position the node is written.
         */
        int nodeSize = -1;

        /**
         * Whether there is an out-of-page, already written node in the branches below the immediate children of the
         * node.
         */
        boolean hasOutOfPageInBranch = false;
        /**
         * Whether a child of the node is out of page, already written.
         * Forced to true before being set to make sure maxPositionDelta performs its evaluation on non-completed
         * nodes for makePartialRoot.
         */
        boolean hasOutOfPageChildren = true;

        Node(byte transition)
        {
            super(transition);
        }

        @Override
        Node<Value> newNode(byte transition)
        {
            return new Node<Value>(transition);
        }

        public long serializedPositionDelta(int i, long nodePosition)
        {
            if (children.get(i).filePos != -1)
                return children.get(i).filePos - nodePosition;

            int dist = children.get(i).nodeSize;
            for (int j = i + 1; j < children.size(); ++j)
                dist += children.get(j).branchSize + children.get(j).nodeSize;
            return -dist;
        }

        /**
         * The max delta is the delta with either:
         * - the position where the first child not-yet-placed child will be laid out.
         * - the position of the furthest child that is already placed.
         *
         * This method assumes all children's branch and node sizes, as well as this node's branchSize, are already
         * calculated.
         */
        public long maxPositionDelta(long nodePosition)
        {
            // The max delta is the position the first child would be laid out.
            assert (childCount() > 0);

            if (!hasOutOfPageChildren)
                // We need to be able to address the first child. We don't need to cover its branch, though.
                return -(branchSize - children.get(0).branchSize);

            long minPlaced = 0;
            long minUnplaced = 1;
            for (Node<Value> child : children)
            {
                if (child.filePos != -1)
                    minPlaced = Math.min(minPlaced, child.filePos - nodePosition);
                else if (minUnplaced > 0)   // triggers once
                    minUnplaced = -(branchSize - child.branchSize);
            }

            return Math.min(minPlaced, minUnplaced);
        }

        void finalizeWithPosition(long position)
        {
            this.branchSize = 0;                // takes no space in current page
            this.nodeSize = 0;
            this.hasOutOfPageInBranch = false;  // its size no longer needs to be recalculated
            this.hasOutOfPageChildren = false;
            super.finalizeWithPosition(position);
        }

        @Override
        public String toString()
        {
            return String.format("%02x branchSize=%04x nodeSize=%04x", transition, branchSize, nodeSize);
        }
    }
}