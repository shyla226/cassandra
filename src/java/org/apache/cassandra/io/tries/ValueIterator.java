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
 * Thread-unsafe value iterator for on-disk tries. Uses the assumptions of Walker.
 */
public class ValueIterator<Concrete extends ValueIterator<Concrete>> extends Walker<Concrete>
{
    final ByteSource limit;
    IterationPosition stack;
    long next;

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

    protected ValueIterator(Rebufferer source, long root)
    {
        super(source, root);
        stack = new IterationPosition(root, -1, 256, null);
        limit = null;
        go(root);
        if (payloadFlags() != 0)
            next = root;
        else
            next = advanceNode();
    }

    protected ValueIterator(Rebufferer source, long root, ByteSource start, ByteSource end, boolean admitPrefix)
    {
        super(source, root);
        limit = end;
        IterationPosition prev = null;
        boolean atLimit = true;
        int childIndex;
        int limitByte;
        long payloadedNode = -1;

        // Follow start position while we still have a prefix, stacking path and saving prefixes.
        start.reset();
        end.reset();
        go(root);
        while (true)
        {
            int s = start.next();
            childIndex = search(s);

            // For a separator trie the latest payload met along the prefix is a potential match for start
            if (admitPrefix)
                if (childIndex == 0 || childIndex == -1)
                {
                    if (payloadFlags() != 0)
                        payloadedNode = position;
                }
                else
                    payloadedNode = -1;

            limitByte = 256;
            if (atLimit)
            {
                limitByte = end.next();
                if (s < limitByte)
                    atLimit = false;
            }
            if (childIndex < 0)
                break;

            prev = new IterationPosition(position, childIndex, limitByte, prev);
            go(transition(childIndex));
        }

        childIndex = -1 - childIndex - 1;
        stack = new IterationPosition(position, childIndex, limitByte, prev);

        // Advancing now gives us first match if we didn't find one already.
        if (payloadedNode != -1)
            next = payloadedNode;
        else
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
        long child;
        int transitionByte;

        go(stack.node);
        while (true)
        {
            // advance position in node
            ++stack.childIndex;
            transitionByte = transitionByte(stack.childIndex);
            if (transitionByte > stack.limit)
            {
                // ascend
                stack = stack.prev;
                if (stack == null)        // exhausted whole trie
                    return -1;
                go(stack.node);
                continue;
            }
            child = transition(stack.childIndex);

            if (child != -1)
            {
                // descend
                int l = 256;
                if (transitionByte == stack.limit)
                    l = limit.next();
                stack = new IterationPosition(child, -1, l, stack);
                go(child);
                if (payloadFlags() != 0)
                    return child;
            }
        }

    }
}
