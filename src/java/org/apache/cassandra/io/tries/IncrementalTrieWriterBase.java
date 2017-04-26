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
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.apache.cassandra.utils.ByteSource;

/**
 * Helper base class for incremental trie builders.
 */
public abstract class IncrementalTrieWriterBase<Value, Dest, Node extends IncrementalTrieWriterBase.BaseNode<Value, Node>>
implements IncrementalTrieWriter<Value>
{

    protected final TrieSerializer<Value, ? super Dest> serializer;
    protected final Dest dest;
    protected ByteSource prev = null;
    protected Deque<Node> stack = new ArrayDeque<>();
    long count = 0;

    public IncrementalTrieWriterBase(TrieSerializer<Value, ? super Dest> serializer, Dest dest, Node root)
    {
        this.serializer = serializer;
        this.dest = dest;
        this.stack.addLast(root);
    }

    public void add(ByteSource next, Value value) throws IOException
    {
        ++count;
        int stackpos = 0;
        next.reset();
        int n = next.next();

        if (prev != null)
        {
            prev.reset();
            int p = prev.next();
            while ( n == p )
            {
                assert n != ByteSource.END_OF_STREAM : String.format("Incremental trie requires unique sorted keys, got equal %s after %s.", next, prev);
                ++stackpos;
                n = next.next();
                p = prev.next();
            }
            assert p < n : String.format("Incremental trie requires sorted keys, got %s after %s.", next, prev);
        }
        prev = next;

        while (stack.size() > stackpos + 1)
            completeLast();

        Node node = stack.getLast();
        while (n != ByteSource.END_OF_STREAM)
        {
            stack.addLast(node = node.addChild((byte) n));
            ++stackpos;
            n = next.next();
        }

        Value existingPayload = node.setPayload(value);
        assert existingPayload == null;
    }

    public long complete() throws IOException
    {
        Node root = stack.getFirst();
        if (root.filePos != -1)
            return root.filePos;

        return performCompletion().filePos;
    }

    Node performCompletion() throws IOException
    {
        Node root = null;
        while (!stack.isEmpty())
            root = completeLast();
        stack.addLast(root);
        return root;
    }

    public long count()
    {
        return count;
    }

    protected Node completeLast() throws IOException
    {
        Node node = stack.removeLast();
        complete(node);
        return node;
    }

    abstract void complete(Node value) throws IOException;
    abstract public PartialTail makePartialRoot() throws IOException;

    class PTail implements PartialTail
    {
        long root;
        long cutoff;
        long count;
        ByteBuffer tail;

        @Override
        public long root()
        {
            return root;
        }

        @Override
        public long cutoff()
        {
            return cutoff;
        }

        @Override
        public ByteBuffer tail()
        {
            return tail;
        }

        @Override
        public long count()
        {
            return count;
        }
    }

    static abstract class BaseNode<Value, Node extends BaseNode<Value, Node>> implements SerializationNode<Value>
    {
        Value payload;
        List<Node> children;
        final byte transition;
        long filePos = -1;

        BaseNode(byte transition)
        {
            children = new ArrayList<>();
            this.transition = transition;
        }

        public Value payload()
        {
            return payload;
        }

        public Value setPayload(Value newPayload)
        {
            Value p = payload;
            payload = newPayload;
            return p;
        }

        public Node addChild(byte b)
        {
            assert children.isEmpty() || (children.get(children.size() - 1).transition & 0xFF) < (b & 0xFF);
            Node node = newNode(b);
            children.add(node);
            return node;
        }

        public int childCount()
        {
            return children.size();
        }

        void finalizeWithPosition(long position)
        {
            this.filePos = position;

            // Make sure we are not holding on to pointers to data we no longer need
            // (otherwise we keep the whole trie in memory).
            children = null;
            payload = null;
        }

        public byte transition(int i)
        {
            return children.get(i).transition;
        }

        public long serializedPosition(int i)
        {
            return children.get(i).filePos;
        }

        @Override
        public String toString()
        {
            return String.format("%02x", transition);
        }

        abstract Node newNode(byte transition);
    }
}