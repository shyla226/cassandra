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

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

final class WritetimeOrTTLSelector extends Selector
{
    protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        protected Selector deserialize(DataInputPlus in, ReadVersion version, TableMetadata metadata) throws IOException
        {
            String columnName = in.readUTF();
            int idx = in.readInt();
            boolean isWritetime = in.readBoolean();
            return new WritetimeOrTTLSelector(columnName, idx, isWritetime);
        }
    };

    private final String columnName;
    private final int idx;
    private final boolean isWritetime;
    private ByteBuffer current;
    private boolean isSet;

    public static Factory newFactory(final ColumnMetadata def, final int idx, final boolean isWritetime)
    {
        return new Factory()
        {
            protected String getColumnName()
            {
                return String.format("%s(%s)", isWritetime ? "writetime" : "ttl", def.name.toString());
            }

            protected AbstractType<?> getReturnType()
            {
                return isWritetime ? LongType.instance : Int32Type.instance;
            }

            protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
            {
               mapping.addMapping(resultsColumn, def);
            }

            public Selector newInstance(QueryOptions options)
            {
                return new WritetimeOrTTLSelector(def.name.toString(), idx, isWritetime);
            }

            public boolean isWritetimeSelectorFactory()
            {
                return isWritetime;
            }

            public boolean isTTLSelectorFactory()
            {
                return !isWritetime;
            }
        };
    }

    public void addInput(ProtocolVersion protocolVersion, InputRow input)
    {
        if (isSet)
            return;

        isSet = true;

        if (isWritetime)
        {
            long ts = input.getTimestamp(idx);
            current = ts != Long.MIN_VALUE ? ByteBufferUtil.bytes(ts) : null;
        }
        else
        {
            int ttl = input.getTtl(idx);
            current = ttl > 0 ? ByteBufferUtil.bytes(ttl) : null;
        }
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion)
    {
        return current;
    }

    public void reset()
    {
        isSet = false;
        current = null;
    }

    public AbstractType<?> getType()
    {
        return isWritetime ? LongType.instance : Int32Type.instance;
    }

    @Override
    public String toString()
    {
        return columnName;
    }

    private WritetimeOrTTLSelector(String columnName, int idx, boolean isWritetime)
    {
        super(Kind.WRITETIME_OR_TTL_SELECTOR);
        this.columnName = columnName;
        this.idx = idx;
        this.isWritetime = isWritetime;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof WritetimeOrTTLSelector))
            return false;

        WritetimeOrTTLSelector s = (WritetimeOrTTLSelector) o;

        return Objects.equal(columnName, s.columnName)
            && Objects.equal(idx, s.idx)
            && Objects.equal(isWritetime, s.isWritetime);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(columnName, idx, isWritetime);
    }

    @Override
    protected int serializedSize(ReadVersion version)
    {
        return TypeSizes.sizeof(columnName)
                + TypeSizes.sizeof(idx)
                + TypeSizes.sizeof(isWritetime);
    }

    @Override
    protected void serialize(DataOutputPlus out, ReadVersion version) throws IOException
    {
        out.writeUTF(columnName);
        out.writeInt(idx);
        out.writeBoolean(isWritetime);
    }
}
