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
package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrenttrees.common.KeyValuePair;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.node.Node;
import com.googlecode.concurrenttrees.radix.node.concrete.SmartArrayBasedNodeFactory;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.AbstractIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUnionIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class TrieMemoryIndex extends MemoryIndex
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemoryIndex.class);

    private static final int MAX_TERM_SIZE = 1024;

    protected final ConcurrentTrie index;
    private final AbstractType<?> type;

    public TrieMemoryIndex(ColumnContext columnContext)
    {
        super(columnContext);
        this.index = new ConcurrentPrefixTrie(columnContext.getDefinition(), columnContext.clusteringComparator());
        this.type = columnContext.getValidator();
    }

    @Override
    public long add(DecoratedKey key, Clustering clustering, ByteBuffer value)
    {
        AbstractAnalyzer analyzer = columnContext.getAnalyzer();

        analyzer.reset(value.duplicate());


        long size = 0;
        while (analyzer.hasNext())
        {
            ByteBuffer term = analyzer.next();

            setMinMaxTerm(term);

            if (term.remaining() >= MAX_TERM_SIZE)
            {
                logger.warn(columnContext.logMessage("Can't add term of column {} to index for key: {}, term size {} max allowed size {}, use analyzed = true (if not yet set) for that column."),
                                                     columnContext.getColumnName(),
                                                     columnContext.keyValidator().getString(key.getKey()),
                                                     FBUtilities.prettyPrintMemory(term.remaining()),
                                                     FBUtilities.prettyPrintMemory(MAX_TERM_SIZE));
                continue;
            }

            String literal = analyzer.nextLiteral(type);
            size += index.add(literal, key, clustering);
        }

        return size;
    }

    @Override
    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        // Trie memory index only supports EQ match, it won't materialize large amount of keys due to large number of
        // matching indexes, so no need to filter matching keys by keyRange before hand.
        return index.search(expression);
    }

    @Override
    public Iterator<Pair<ByteBuffer, PrimaryKeys>> iterator()
    {
        return new AbstractIterator<Pair<ByteBuffer, PrimaryKeys>>()
        {
            final Iterator<KeyValuePair<PrimaryKeys>>  kvs = index.all().iterator();
            
            @Override
            protected Pair<ByteBuffer, PrimaryKeys> computeNext()
            {
                if (kvs.hasNext())
                {
                    KeyValuePair<PrimaryKeys> pair = kvs.next();
                    CharSequence key = pair.getKey();
                    ByteBuffer term = columnContext.getValidator().fromString(key.toString());
                    PrimaryKeys primaryKeys = pair.getValue();
                    return Pair.create(term, primaryKeys);
                }
                return endOfData();
            }
        };
    }

    protected static abstract class ConcurrentTrie
    {
        public static final SizeEstimatingNodeFactory NODE_FACTORY = new SizeEstimatingNodeFactory();

        protected final ColumnMetadata definition;
        private final ClusteringComparator clusteringComparator;

        ConcurrentTrie(ColumnMetadata column, ClusteringComparator clusteringComparator)
        {
            definition = column;
            this.clusteringComparator = clusteringComparator;
        }

        public long add(String value, DecoratedKey key, Clustering clustering)
        {
            long overhead = SkipListMemoryIndex.CSLM_OVERHEAD;
            PrimaryKeys keys = get(value);
            if (keys == null)
            {
                PrimaryKeys newKeys = PrimaryKeys.create(clusteringComparator);
                keys = putIfAbsent(value, newKeys);
                if (keys == null)
                {
                    overhead += SkipListMemoryIndex.CSLM_OVERHEAD + value.length();
                    keys = newKeys;
                }
            }

            keys.add(key, clustering);

            // get and reset new memory size allocated by current thread
            overhead += NODE_FACTORY.currentUpdateSize();
            NODE_FACTORY.reset();

            return overhead;
        }

        public RangeIterator search(Expression expression)
        {
            ByteBuffer prefix = expression.lower == null ? null : expression.lower.value;

            Iterable<PrimaryKeys> search = search(expression.getOp(), definition.cellValueType().getString(prefix));

            RangeUnionIterator.Builder builder = RangeUnionIterator.builder();
            for (PrimaryKeys keys : search)
            {
                if (!keys.isEmpty())
                    builder.add(new KeyRangeIterator(keys.partitionKeys()));
            }

            return builder.build();
        }

        protected abstract PrimaryKeys get(String value);
        protected abstract Iterable<PrimaryKeys> search(Expression.Op operator, String value);
        protected abstract PrimaryKeys putIfAbsent(String value, PrimaryKeys keys);
        protected abstract Iterable<KeyValuePair<PrimaryKeys>> all();
    }

    protected static class ConcurrentPrefixTrie extends ConcurrentTrie
    {
        private final ConcurrentRadixTree<PrimaryKeys> trie;

        private ConcurrentPrefixTrie(ColumnMetadata column, ClusteringComparator clusteringComparator)
        {
            super(column, clusteringComparator);
            trie = new ConcurrentRadixTree<>(NODE_FACTORY);
        }

        public PrimaryKeys get(String value)
        {
            return trie.getValueForExactKey(value);
        }

        public PrimaryKeys putIfAbsent(String value, PrimaryKeys newKeys)
        {
            return trie.putIfAbsent(value, newKeys);
        }

        @Override
        public Iterable<KeyValuePair<PrimaryKeys>> all()
        {
            return trie.getKeyValuePairsForKeysStartingWith("");
        }

        public Iterable<PrimaryKeys> search(Expression.Op operator, String value)
        {
            switch (operator)
            {
                case EQ:
                case MATCH:
                    PrimaryKeys keys = trie.getValueForExactKey(value);
                    return keys == null ? Collections.emptyList() : Collections.singletonList(keys);

                case PREFIX:
                    return trie.getValuesForKeysStartingWith(value);

                default:
                    throw new UnsupportedOperationException(String.format("operation %s is not supported.", operator));
            }
        }
    }

    // This relies on the fact that all of the tree updates are done under exclusive write lock,
    // method would overestimate in certain circumstances e.g. when nodes are replaced in place,
    // but it's still better comparing to underestimate since it gives more breathing room for other memory users.
    private static class SizeEstimatingNodeFactory extends SmartArrayBasedNodeFactory
    {
        private static final FastThreadLocal<Long> updateSize = new FastThreadLocal<Long>()
        {
            protected Long initialValue() throws Exception
            {
                return 0L;
            }
        };

        public Node createNode(CharSequence edgeCharacters, Object value, List<Node> childNodes, boolean isRoot)
        {
            Node node = super.createNode(edgeCharacters, value, childNodes, isRoot);
            updateSize.set(updateSize.get() + measure(node));
            return node;
        }

        public long currentUpdateSize()
        {
            return updateSize.get();
        }

        public void reset()
        {
            updateSize.set(0L);
        }

        private long measure(Node node)
        {
            // node with max overhead is CharArrayNodeLeafWithValue = 24B
            long overhead = 24;

            // array of chars (2 bytes) + CharSequence overhead
            overhead += 24 + node.getIncomingEdge().length() * 2;

            if (node.getOutgoingEdges() != null)
            {
                // 16 bytes for AtomicReferenceArray
                overhead += 16;
                overhead += 24 * node.getOutgoingEdges().size();
            }

            return overhead;
        }
    }
}
