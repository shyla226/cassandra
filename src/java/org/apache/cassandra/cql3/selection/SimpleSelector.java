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

import com.google.common.base.Objects;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.transport.ProtocolVersion;

public final class SimpleSelector extends Selector
{
    protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        protected Selector deserialize(DataInputPlus in, ReadVersion version, TableMetadata metadata) throws IOException
        {
            String columnName = in.readUTF();
            int idx = in.readInt();
            AbstractType<?> type = readType(metadata, in);
            return new SimpleSelector(columnName, idx, type);
        }
    };

    private final String columnName;
    private final int idx;
    private final AbstractType<?> type;
    private ByteBuffer current;
    private boolean isSet;

    public static Factory newFactory(final ColumnMetadata def, final int idx)
    {
        return new Factory()
        {
            @Override
            protected String getColumnName()
            {
                return def.name.toString();
            }

            @Override
            protected AbstractType<?> getReturnType()
            {
                return def.type;
            }

            protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultColumn)
            {
               mapping.addMapping(resultColumn, def);
            }

            @Override
            public Selector newInstance(QueryOptions options)
            {
                return new SimpleSelector(def.name.toString(), idx, def.type);
            }

            @Override
            public boolean isSimpleSelectorFactory(int index)
            {
                return index == idx;
            }
        };
    }

    @Override
    public void addInput(ProtocolVersion protocolVersion, InputRow input)
    {
        if (!isSet)
        {
            isSet = true;
            current = input.getValue(idx);
        }
    }

    @Override
    public ByteBuffer getOutput(ProtocolVersion protocolVersion)
    {
        return current;
    }

    @Override
    public void reset()
    {
        isSet = false;
        current = null;
    }

    @Override
    public AbstractType<?> getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return columnName;
    }

    private SimpleSelector(String columnName, int idx, AbstractType<?> type)
    {
        super(Kind.SIMPLE_SELECTOR);
        this.columnName = columnName;
        this.idx = idx;
        this.type = type;
    }

    @Override
    public void validateForGroupBy()
    {
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof SimpleSelector))
            return false;

        SimpleSelector s = (SimpleSelector) o;

        return Objects.equal(columnName, s.columnName)
            && Objects.equal(idx, s.idx)
            && Objects.equal(type, s.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(columnName, idx, type);
    }

    @Override
    protected int serializedSize(ReadVersion version)
    {
        return TypeSizes.sizeof(columnName)
                + TypeSizes.sizeof(idx)
                + sizeOf(type);
    }

    @Override
    protected void serialize(DataOutputPlus out, ReadVersion version) throws IOException
    {
        out.writeUTF(columnName);
        out.writeInt(idx);
        writeType(out, type);
    }
}
