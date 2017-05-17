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

import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteSource;

/**
 * Thread-unsafe reverse value iterator for on-disk tries. Uses the assumptions of Walker.
 */
public class ReverseValueIterator<Concrete extends ReverseValueIterator<Concrete>> extends Walker<Concrete>
{
    final ByteSource limit;
    IterationPosition stack;
    long next;
    boolean reportingPrefixes;

    static class IterationPosition
    {
        long node;
        int childIndex;
        int limit;
        IterationPosition prev;

        public IterationPosition(long node, int childIndex, int limit, IterationPosition prev)
        {
            super();
            this.node = node;
            this.childIndex = childIndex;
            this.limit = limit;
            this.prev = prev;
        }
    }

    protected ReverseValueIterator(Rebufferer source, long root, Rebufferer.ReaderConstraint rc)
    {
        super(source, root, rc);
        stack = new IterationPosition(root, -1, 256, null);
        limit = null;
        next = advanceNode();
    }

    protected ReverseValueIterator(Rebufferer source, long root, ByteSource start, ByteSource end, boolean admitPrefix, Rebufferer.ReaderConstraint rc)
    {
        super(source, root, rc);
        limit = start;
        IterationPosition prev = null;
        boolean atLimit = true;
        int childIndex;
        int limitByte;
        reportingPrefixes = admitPrefix;

        // Follow end position while we still have a prefix, stacking path.
        start.reset();
        end.reset();
        go(root);
        while (true)
        {
            int s = end.next();
            childIndex = search(s);

            limitByte = -1;
            if (atLimit)
            {
                limitByte = start.next();
                if (s > limitByte)
                    atLimit = false;
            }
            if (childIndex < 0)
                break;

            prev = new IterationPosition(position, childIndex, limitByte, prev);
            go(transition(childIndex));
        }

        // Advancing now gives us first match.
        childIndex = -1 - childIndex;
        stack = new IterationPosition(position, childIndex, limitByte, prev);
        next = advanceNode();
    }

    protected long nextPayloadedNode()     // returns payloaded node position
    {
        long toReturn = next;
        if (next != -1)
            next = advanceNode();
        return toReturn;
    }

    long advanceNode()
    {
        if (stack == null)
            return -1;

        long child;
        int transitionByte;

        go(stack.node);
        while (true)
        {
            // advance position in node
            --stack.childIndex;
            boolean beyondLimit = true;
            if (stack.childIndex >= 0)
            {
                transitionByte = transitionByte(stack.childIndex);
                beyondLimit = transitionByte < stack.limit;
                if (beyondLimit)
                {
                    assert stack.limit >= 0;    // we are at a limit position (not in a node that's completely within the span)
                    reportingPrefixes = false;  // there exists a smaller child than limit, no longer should report prefixes
                }
            }
            else
                transitionByte = Integer.MIN_VALUE;

            if (beyondLimit)
            {
                // ascend to parent, remove from stack
                IterationPosition stackTop = stack;
                stack = stack.prev;

                // Report payloads on the way up
                // unless we are at limit and there has been a smaller child
                if (reportingPrefixes && payloadFlags() != 0)
                {
                    if (stackTop.limit >= 0)    // if we are at limit position only report the closest prefix
                        reportingPrefixes = false;
                    return stackTop.node;
                }

                if (stack == null)        // exhausted whole trie
                    return -1;
                go(stack.node);
                continue;
            }

            child = transition(stack.childIndex);
            if (child != -1)
            {
                // descend, stack up position
                int l = -1;
                if (transitionByte == stack.limit)
                    l = limit.next();
                go(child);
                stack = new IterationPosition(child, transitionRange(), l, stack);
            }
        }
    }
}
