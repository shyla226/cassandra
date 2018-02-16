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

import java.util.Objects;

import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.transport.ProtocolVersion;

final class WritetimeOrTTLSelector extends Selector
{
    protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        protected Selector deserialize(DataInputPlus in, ReadVersion version, TableMetadata metadata) throws IOException
        {
            Selector selected = Selector.serializers.get(version).deserialize(in, metadata);
            int columnIndex = in.readInt();
            boolean isWritetime = in.readBoolean();
            boolean isMultiCell = in.readBoolean();
            return new WritetimeOrTTLSelector(selected, columnIndex, isWritetime, isMultiCell);
        }
    };

    private final Selector selected;
    private final int columnIndex;
    private final boolean isWritetime;
    private final boolean isMultiCell;
    private ByteBuffer output;
    private boolean isSet;

    public static Factory newFactory(final Selector.Factory factory, final int columnIndex, final boolean isWritetime, final boolean isMultiCell)
    {
        return new Factory()
        {
            protected String getColumnName()
            {
                return String.format("%s(%s)", isWritetime ? "writetime" : "ttl", factory.getColumnName());
            }

            protected AbstractType<?> getReturnType()
            {
                AbstractType<?> type = isWritetime ? LongType.instance : Int32Type.instance;
                return isMultiCell ? ListType.getInstance(type, false) : type;
            }

            protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
            {
                factory.addColumnMapping(mapping, resultsColumn);
            }

            public Selector newInstance(QueryOptions options)
            {
                return new WritetimeOrTTLSelector(factory.newInstance(options), columnIndex, isWritetime, isMultiCell);
            }

            public boolean isWritetimeSelectorFactory()
            {
                return isWritetime;
            }

            public boolean isTTLSelectorFactory()
            {
                return !isWritetime;
            }

            public boolean areAllFetchedColumnsKnown()
            {
                return true;
            }

            public void addFetchedColumns(ColumnFilter.Builder builder)
            {
                factory.addFetchedColumns(builder);
            }
        };
    }

    public void addFetchedColumns(ColumnFilter.Builder builder)
    {
        selected.addFetchedColumns(builder);
    }

    @Override
    public void addInput(InputRow input)
    {
        // Because the write timestamps and TTLs returned by the InputRow are mutables they cannot be safely stored in
        // the case of aggregation/group by queries with select columns without aggregates.
        // So we need to compute the output value and store this one.
        if (isSet)
            return;

        isSet = true;
        selected.addInput(input);
        ProtocolVersion protocolVersion = input.getProtocolVersion();
        output = isWritetime ? selected.getWritetimes(protocolVersion).toByteBuffer(protocolVersion)
                             : selected.getTTLs(protocolVersion).toByteBuffer(protocolVersion);
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion)
    {
        return output;
    }

    public void reset()
    {
        selected.reset();
        isSet = false;
        output = null;
    }

    public AbstractType<?> getType()
    {
        AbstractType<?> type = isWritetime ? LongType.instance : Int32Type.instance;
        return isMultiCell ? ListType.getInstance(type, false) : type;
    }

    @Override
    public String toString()
    {
        return selected.toString();
    }

    private WritetimeOrTTLSelector(Selector selected, final int columnIndex, boolean isWritetime, boolean isMultiCell)
    {
        super(Kind.WRITETIME_OR_TTL_SELECTOR);
        this.selected = selected;
        this.columnIndex = columnIndex;
        this.isWritetime = isWritetime;
        this.isMultiCell = isMultiCell;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof WritetimeOrTTLSelector))
            return false;

        WritetimeOrTTLSelector s = (WritetimeOrTTLSelector) o;

        return Objects.equals(selected, s.selected)
            && Objects.equals(isWritetime, s.isWritetime);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(selected, isWritetime);
    }

    @Override
    protected int serializedSize(ReadVersion version)
    {
        return serializers.get(version).serializedSize(selected)
                + TypeSizes.sizeof(columnIndex)
                + TypeSizes.sizeof(isWritetime);
    }

    @Override
    protected void serialize(DataOutputPlus out, ReadVersion version) throws IOException
    {
        serializers.get(version).serialize(selected, out);
        out.writeInt(columnIndex);
        out.writeBoolean(isWritetime);
        out.writeBoolean(isMultiCell);
    }
}
