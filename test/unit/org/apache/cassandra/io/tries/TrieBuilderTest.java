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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.io.util.AsynchronousChannelProxy;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.TailOverridingRebufferer;
import org.apache.cassandra.utils.ByteSource;

import static org.junit.Assert.assertEquals;

public class TrieBuilderTest
{
    boolean dump = false;

    TrieSerializer<Integer, DataOutput> serializer = new TrieSerializer<Integer, DataOutput>()
    {
        public int sizeofNode(SerializationNode<Integer> node, long nodePosition)
        {
            return TrieNode.typeFor(node, nodePosition).sizeofNode(node);
        }

        public void write(DataOutput dataOutput, SerializationNode<Integer> node, long nodePosition) throws IOException
        {
            if (dump)
                System.out.format("Writing at %x type %s size %d: %s\n", nodePosition, TrieNode.typeFor(node, nodePosition), TrieNode.typeFor(node, nodePosition).sizeofNode(node), node);
            TrieNode.typeFor(node, nodePosition).serialize(dataOutput, node, node.payload() != null ? node.payload() : 0, nodePosition);
        }
    };

    static final int BASE = 80;

    @Test
    public void testPartialBuild_Apollo1148() throws IOException
    {
        DataOutputBuffer buf = new DataOutputBuffer();
        IncrementalTrieWriter<Integer> builder = IncrementalTrieWriter.construct(serializer, buf);
        long count = 0;

        count += addUntilBytesWritten(buf, builder, "a", 1);            // Make a node whose children are written
        long reset = count;
        count += addUntilBytesWritten(buf, builder, "c", 64 * 1024);    // Finalize it and write long enough to grow its pointer size

        dump = true;
        IncrementalTrieWriter.PartialTail tail = builder.makePartialRoot();
        // The line above hit an assertion as that node's parent had a pre-calculated branch size which was no longer
        // correct and we didn't bother to reset it.
        dump = false;

        // Check that partial representation has the right content.
        Rebufferer source = new ByteBufRebufferer(buf.trimmedBuffer());
        source = new TailOverridingRebufferer(source, tail.cutoff(), tail.tail());
        verifyContent(count, source, tail.root(), reset);

        long reset2 = count;

        // Also check the completed trie.
        count += addUntilBytesWritten(buf, builder, "e", 16 * 1024);
        dump = true;
        long root = builder.complete();
        // The line above hit another size assertion as the size of a node's branch growing caused it to need to switch
        // format, but we didn't bother to recalculate its size.
        dump = false;

        source = new ByteBufRebufferer(buf.trimmedBuffer());
        verifyContent(count, source, root, reset, reset2);
    }

    public void verifyContent(long count, Rebufferer source, long root, long... resets)
    {
        Iterator iter = new Iterator(source, root);
        long found = 0;
        long ofs = 0;
        int rpos = 0;
        long pos;
        while ((pos = iter.nextPayloadedNode()) != -1)
        {
            iter.go(pos);
            assertEquals(valueFor(found - ofs), iter.payloadFlags());
            ++found;
            if (rpos < resets.length && found >= resets[rpos])
            {
                ofs = resets[rpos];
                ++rpos;
            }
        }
        assertEquals(count, found);
    }

    public long addUntilBytesWritten(DataOutputBuffer buf,
                                     IncrementalTrieWriter<Integer> builder,
                                     String prefix,
                                     long howMany) throws IOException
    {
        long pos = buf.position();
        long idx = 0;
        while (pos + howMany > buf.position())
        {
            builder.add(source(String.format("%s%8s", prefix, toBase(idx))), valueFor(idx));
            ++idx;
        }
        System.out.format("%s%8s\n", prefix, toBase(idx - 1));
        return idx;
    }

    public int valueFor(long found)
    {
        return Long.bitCount(found + 1) & 0xF;
    }

    ByteSource source(String s)
    {
        ByteBuffer buf = ByteBuffer.allocate(s.length());
        for (int i = 0; i < s.length(); ++i)
            buf.put((byte) s.charAt(i));
        buf.rewind();
        return ByteSource.fixedLength(buf);
    }

    String toBase(long v)
    {
        String s = "";
        while (v > 0)
        {
            s = ((char) ((v % BASE) + '0')) + s;
            v /= BASE;
        }
        return s;
    }

    class Iterator extends ValueIterator<Iterator>
    {
        public Iterator(Rebufferer source, long root)
        {
            super(source, root, Rebufferer.ReaderConstraint.NONE);
        }
    }

    class ByteBufRebufferer implements Rebufferer, Rebufferer.BufferHolder
    {
        final ByteBuffer buffer;

        ByteBufRebufferer(ByteBuffer buffer)
        {
            this.buffer = buffer;
        }

        public AsynchronousChannelProxy channel()
        {
            return null;
        }

        public long fileLength()
        {
            return buffer.remaining();
        }

        public double getCrcCheckChance()
        {
            return 0;
        }

        public BufferHolder rebuffer(long position, ReaderConstraint rc)
        {
            return this;
        }

        public ByteBuffer buffer()
        {
            return buffer.duplicate();
        }

        public long offset()
        {
            return 0;
        }

        public void release()
        {
            // nothing
        }

        public void close()
        {
            // nothing
        }

        public void closeReader()
        {
            // nothing
        }
    }
}