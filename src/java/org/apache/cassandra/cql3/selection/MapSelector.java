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
package org.apache.cassandra.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import java.util.Objects;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.filter.ColumnFilter.Builder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Pair;

/**
 * <code>Selector</code> for literal map (e.g. {'min' : min(value), 'max' : max(value), 'count' : count(value)}).
 *
 */
final class MapSelector extends Selector
{
    protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        protected Selector deserialize(DataInputPlus in, ReadVersion version, TableMetadata metadata) throws IOException
        {
            MapType<?, ?> type = (MapType<?, ?>) readType(metadata, in);
            int size = (int) in.readUnsignedVInt();
            List<Pair<Selector, Selector>> entries = new ArrayList<>(size);
            Serializer serializer = serializers.get(version);
            for (int i = 0; i < size; i++)
            {
                entries.add(Pair.create(serializer.deserialize(in, metadata),
                                        serializer.deserialize(in, metadata)));
            }
            return new MapSelector(type, entries);
        }
    };

    /**
     * The map type.
     */
    private final MapType<?, ?> type;

    /**
     * The map elements
     */
    private final List<Pair<Selector, Selector>> elements;

    public static Factory newFactory(final AbstractType<?> type, final List<Pair<Factory, Factory>> factories)
    {
        return new Factory()
        {
            protected String getColumnName()
            {
                return Maps.mapToString(factories, Factory::getColumnName);
            }

            protected AbstractType<?> getReturnType()
            {
                return type;
            }

            protected final void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
            {
                SelectionColumnMapping tmpMapping = SelectionColumnMapping.newMapping();
                for (Pair<Factory, Factory> entry : factories)
                {
                    entry.left.addColumnMapping(tmpMapping, resultsColumn);
                    entry.right.addColumnMapping(tmpMapping, resultsColumn);
                }

                if (tmpMapping.getMappings().get(resultsColumn).isEmpty())
                    // add a null mapping for cases where the collection is empty
                    mapping.addMapping(resultsColumn, (ColumnMetadata)null);
                else
                    // collate the mapped columns from the child factories & add those
                    mapping.addMapping(resultsColumn, tmpMapping.getMappings().values());
            }

            public Selector newInstance(final QueryOptions options)
            {
                return new MapSelector(type,
                                        factories.stream()
                                                 .map(p -> Pair.create(p.left.newInstance(options),
                                                                       p.right.newInstance(options)))
                                                 .collect(Collectors.toList()));
            }

            @Override
            public boolean isAggregateSelectorFactory()
            {
                for (Pair<Factory, Factory> entry : factories)
                {
                    if (entry.left.isAggregateSelectorFactory() || entry.right.isAggregateSelectorFactory())
                        return true;
                }
                return false;
            }

            @Override
            public void addFunctionsTo(List<Function> functions)
            {
                for (Pair<Factory, Factory> entry : factories)
                {
                    entry.left.addFunctionsTo(functions);
                    entry.right.addFunctionsTo(functions);
                }
            }

            @Override
            public boolean isWritetimeSelectorFactory()
            {
                for (Pair<Factory, Factory> entry : factories)
                {
                    if (entry.left.isWritetimeSelectorFactory() || entry.right.isWritetimeSelectorFactory())
                        return true;
                }
                return false;
            }

            @Override
            public boolean isTTLSelectorFactory()
            {
                for (Pair<Factory, Factory> entry : factories)
                {
                    if (entry.left.isTTLSelectorFactory() || entry.right.isTTLSelectorFactory())
                        return true;
                }
                return false;
            }

            @Override
            boolean areAllFetchedColumnsKnown()
            {
                for (Pair<Factory, Factory> entry : factories)
                {
                    if (!entry.left.areAllFetchedColumnsKnown() || !entry.right.areAllFetchedColumnsKnown())
                        return false;
                }
                return true;
            }

            @Override
            void addFetchedColumns(Builder builder)
            {
                for (Pair<Factory, Factory> entry : factories)
                {
                    entry.left.addFetchedColumns(builder);
                    entry.right.addFetchedColumns(builder);
                }
            }
        };
    }

    @Override
    public void addFetchedColumns(Builder builder)
    {
        for (int i = 0, m = elements.size(); i < m; i++)
        {
            Pair<Selector, Selector> pair = elements.get(i);
            pair.left.addFetchedColumns(builder);
            pair.right.addFetchedColumns(builder);
        }
    }

    public void addInput(ProtocolVersion protocolVersion, InputRow input)
    {
        for (int i = 0, m = elements.size(); i < m; i++)
        {
            Pair<Selector, Selector> pair = elements.get(i);
            pair.left.addInput(protocolVersion, input);
            pair.right.addInput(protocolVersion, input);
        }
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion)
    {
        Map<ByteBuffer, ByteBuffer> map = new TreeMap<>(type.getKeysType());
        for (int i = 0, m = elements.size(); i < m; i++)
        {
            Pair<Selector, Selector> pair = elements.get(i);
            map.put(pair.left.getOutput(protocolVersion), pair.right.getOutput(protocolVersion));
        }

        List<ByteBuffer> buffers = new ArrayList<>(elements.size() * 2);
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : map.entrySet())
        {
            buffers.add(entry.getKey());
            buffers.add(entry.getValue());
        }
        return CollectionSerializer.pack(buffers, elements.size(), protocolVersion);
    }

    public void reset()
    {
        for (int i = 0, m = elements.size(); i < m; i++)
        {
            Pair<Selector, Selector> pair = elements.get(i);
            pair.left.reset();
            pair.right.reset();
        }
    }

    public AbstractType<?> getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return Maps.mapToString(elements);
    }

    @Override
    public boolean isTerminal()
    {
        for (int i = 0, m = elements.size(); i < m; i++)
        {
            Pair<Selector, Selector> pair = elements.get(i);
            if (!pair.left.isTerminal() || !pair.right.isTerminal())
                return false;
        }
        return true;
    }

    private MapSelector(AbstractType<?> type, List<Pair<Selector, Selector>> elements)
    {
        super(Kind.MAP_SELECTOR);
        this.type = (MapType<?, ?>) type;
        this.elements = elements;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof MapSelector))
            return false;

        MapSelector s = (MapSelector) o;

        return Objects.equals(type, s.type)
            && Objects.equals(elements, s.elements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, elements);
    }

    @Override
    protected int serializedSize(ReadVersion version)
    {
        int size = sizeOf(type) + TypeSizes.sizeofUnsignedVInt(elements.size());

        Serializer serializer = serializers.get(version);
        for (int i = 0, m = elements.size(); i < m; i++)
        {
            Pair<Selector, Selector> entry = elements.get(i);
            size += serializer.serializedSize(entry.left) + serializer.serializedSize(entry.right);
        }

        return size;
    }

    @Override
    protected void serialize(DataOutputPlus out, ReadVersion version) throws IOException
    {
        writeType(out, type);
        out.writeUnsignedVInt(elements.size());

        Serializer serializer = serializers.get(version);
        for (int i = 0, m = elements.size(); i < m; i++)
        {
            Pair<Selector, Selector> entry = elements.get(i);
            serializer.serialize(entry.left, out);
            serializer.serialize(entry.right, out);
        }
    }
}
