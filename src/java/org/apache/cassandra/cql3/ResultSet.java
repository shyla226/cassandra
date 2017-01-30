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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.*;

import io.netty.buffer.ByteBuf;

import org.apache.cassandra.cql3.selection.ResultBuilder;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ResultSet
{
    public static final Codec codec = new Codec();

    public final ResultMetadata metadata;
    public final List<List<ByteBuffer>> rows;

    public ResultSet(List<ColumnSpecification> metadata)
    {
        this(new ResultMetadata(metadata), new ArrayList<>());
    }

    public ResultSet(ResultMetadata metadata, List<List<ByteBuffer>> rows)
    {
        this.metadata = metadata;
        this.rows = rows;
    }

    public int size()
    {
        return rows.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public void addRow(List<ByteBuffer> row)
    {
        assert row.size() == metadata.valueCount();
        rows.add(row);
    }

    public void addColumnValue(ByteBuffer value)
    {
        if (rows.isEmpty() || lastRow().size() == metadata.valueCount())
            rows.add(new ArrayList<>(metadata.valueCount()));

        lastRow().add(value);
    }

    private List<ByteBuffer> lastRow()
    {
        return rows.get(rows.size() - 1);
    }

    public void reverse()
    {
        Collections.reverse(rows);
    }

    public void trim(int limit)
    {
        int toRemove = rows.size() - limit;
        if (toRemove > 0)
        {
            for (int i = 0; i < toRemove; i++)
                rows.remove(rows.size() - 1);
        }
    }

    @Override
    public String toString()
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.append(metadata).append('\n');
            for (List<ByteBuffer> row : rows)
            {
                for (int i = 0; i < row.size(); i++)
                {
                    ByteBuffer v = row.get(i);
                    if (v == null)
                    {
                        sb.append(" | null");
                    }
                    else
                    {
                        sb.append(" | ");
                        if (metadata.flags.contains(Flag.NO_METADATA))
                            sb.append("0x").append(ByteBufferUtil.bytesToHex(v));
                        else
                            sb.append(metadata.names.get(i).type.getString(v));
                    }
                }
                sb.append('\n');
            }
            sb.append("---");
            return sb.toString();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static class Codec implements CBCodec<ResultSet>
    {
        /*
         * Format:
         *   - metadata
         *   - rows count (4 bytes)
         *   - rows
         */
        public ResultSet decode(ByteBuf body, ProtocolVersion version)
        {
            ResultMetadata m = ResultMetadata.codec.decode(body, version);
            int rowCount = body.readInt();
            ResultSet rs = new ResultSet(m, new ArrayList<>(rowCount));

            // rows
            int totalValues = rowCount * m.columnCount;
            for (int i = 0; i < totalValues; i++)
                rs.addColumnValue(CBUtil.readValue(body));

            return rs;
        }

        /**
         * Encode all the rows in the result set, including the header (metadata and num rows).
         */
        public void encode(ResultSet rs, ByteBuf dest, ProtocolVersion version)
        {
            encodeHeader(rs.metadata, dest, rs.rows.size(), version);
            for (List<ByteBuffer> row : rs.rows)
                encodeRow(row, rs.metadata, dest, false);
        }

        /**
         * Write the header to the Netty byte buffer.
         * @param metadata the result set metadata
         * @param dest the Netty byte buffer
         * @param numRows the number of rows
         */
        public void encodeHeader(ResultMetadata metadata, ByteBuf dest, int numRows, ProtocolVersion version)
        {
            ResultMetadata.codec.encode(metadata, dest, version);
            dest.writeInt(numRows);
        }

        /**
         * Write the current row to the Netty byte buffer.
         * <p>
         * If checkSpace is true, that means that the Netty byte buffer was created with a fixed size and the
         * current row may not fit, so before writing each value we check if there is enough space. If there isn't
         * enough space, we return false. Note that the row will have been partially written in this case, so the
         * caller must handle this possibility.
         * <p>
         * If checkSpace is false, it means the caller ensured that there is enough space in the buffer to fit
         * the entire row, for example by calling encodedSize() to create the byte buffer.
         * <p>
         * Note that we do only want to serialize only the first columnCount values, even if the row
         * has more: see comment on ResultMetadata.names field.
         *
         * @param row the current row
         * @param metadata the result set metadata
         * @param dest the Netty byte buffer
         * @param checkSpace when true check if there is sufficient space in the Netty byte buffer before adding each value
         *                   .
         * @return true if the row was written entirely, false if it was written only partially, or not at all.
         */
        public boolean encodeRow(List<ByteBuffer> row, ResultMetadata metadata, ByteBuf dest, boolean checkSpace)
        {
            for (int i = 0; i < metadata.columnCount; i++)
            {
                if (checkSpace && dest.writableBytes() < CBUtil.sizeOfValue(row.get(i)))
                    return false;

                CBUtil.writeValue(row.get(i), dest);
            }

            return true;
        }

        public int encodedSize(ResultSet rs, ProtocolVersion version)
        {
            int size = encodedHeaderSize(rs.metadata, version);
            for (List<ByteBuffer> row : rs.rows)
                size += encodedRowSize(row, rs.metadata);

            return size;
        }

        public int encodedHeaderSize(ResultMetadata metadata, ProtocolVersion version)
        {
            return ResultMetadata.codec.encodedSize(metadata, version) + 4;
        }

        public int encodedRowSize(List<ByteBuffer> row, ResultMetadata metadata)
        {
            int size = 0;
            for (int i = 0; i < metadata.columnCount; i++)
                size += CBUtil.sizeOfValue(row.get(i));
            return size;
        }
    }

    /**
     * The metadata for the results of executing a query or prepared statement.
     */
    public static class ResultMetadata
    {
        public static final CBCodec<ResultMetadata> codec = new Codec();

        public static final ResultMetadata EMPTY = new ResultMetadata(EnumSet.of(Flag.NO_METADATA), null, 0, PagingResult.NONE);

        private final EnumSet<Flag> flags;
        // Please note that columnCount can actually be smaller than names, even if names is not null. This is
        // used to include columns in the resultSet that we need to do post-query re-orderings
        // (SelectStatement.orderResults) but that shouldn't be sent to the user as they haven't been requested
        // (CASSANDRA-4911). So the serialization code will exclude any columns in name whose index is >= columnCount.
        public final List<ColumnSpecification> names;
        private final int columnCount;
        private PagingResult pagingResult;

        public ResultMetadata(List<ColumnSpecification> names)
        {
            this(EnumSet.noneOf(Flag.class), names, names.size(), PagingResult.NONE);
            if (!names.isEmpty() && ColumnSpecification.allInSameTable(names))
                flags.add(Flag.GLOBAL_TABLES_SPEC);
        }

        private ResultMetadata(EnumSet<Flag> flags, List<ColumnSpecification> names, int columnCount, PagingResult pagingResult)
        {
            this.flags = flags;
            this.names = names;
            this.columnCount = columnCount;
            this.pagingResult = pagingResult;
        }

        public ResultMetadata copy()
        {
            return new ResultMetadata(EnumSet.copyOf(flags), names, columnCount, pagingResult);
        }

        /**
         * Return only the column names requested by the user, excluding those added for post-query re-orderings,
         * see definition of names and columnCount.
         **/
        public List<ColumnSpecification> requestNames()
        {
            return names.subList(0, columnCount);
        }

        // The maximum number of values that the ResultSet can hold. This can be bigger than columnCount due to CASSANDRA-4911
        public int valueCount()
        {
            return names == null ? columnCount : names.size();
        }

        /**
         * Adds the specified column which will not be serialized.
         *
         * @param name the column
         */
        public void addNonSerializedColumn(ColumnSpecification name)
        {
            // See comment above. Because columnCount doesn't account the newly added name, it
            // won't be serialized.
            names.add(name);
        }

        public void setPagingResult(PagingResult pagingResult)
        {
            this.pagingResult = pagingResult;

            if (pagingResult.state != null)
                flags.add(Flag.HAS_MORE_PAGES);
            else
                flags.remove(Flag.HAS_MORE_PAGES);

            boolean continuous = pagingResult.seqNo > 0;

            if (continuous)
                flags.add(Flag.CONTINUOUS_PAGING);
            else
                flags.remove(Flag.CONTINUOUS_PAGING);

            if (continuous && pagingResult.last)
                flags.add(Flag.LAST_CONTINOUS_PAGE);
            else
                flags.remove(Flag.LAST_CONTINOUS_PAGE);
        }

        public void setSkipMetadata()
        {
            flags.add(Flag.NO_METADATA);
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
                return true;

            if (!(other instanceof ResultMetadata))
                return false;

            ResultMetadata that = (ResultMetadata) other;

            return Objects.equals(flags, that.flags)
                   && Objects.equals(names, that.names)
                   && columnCount == that.columnCount
                   && Objects.equals(pagingResult, that.pagingResult);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(flags, names, columnCount, pagingResult);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            if (names == null)
            {
                sb.append("[").append(columnCount).append(" columns]");
            }
            else
            {
                for (ColumnSpecification name : names)
                {
                    sb.append("[").append(name.name);
                    sb.append("(").append(name.ksName).append(", ").append(name.cfName).append(")");
                    sb.append(", ").append(name.type).append("]");
                }
            }

            if (pagingResult != null)
                sb.append("[").append(pagingResult.toString()).append("]");

            if (flags.contains(Flag.HAS_MORE_PAGES))
                sb.append(" (to be continued)");

            return sb.toString();
        }

        private static class Codec implements CBCodec<ResultMetadata>
        {
            public ResultMetadata decode(ByteBuf body, ProtocolVersion version)
            {
                // flags & column count
                int iflags = body.readInt();
                int columnCount = body.readInt();

                EnumSet<Flag> flags = Flag.deserialize(iflags);

                PagingState state = flags.contains(Flag.HAS_MORE_PAGES)
                                    ? PagingState.deserialize(CBUtil.readValue(body), version)
                                    : null;

                PagingResult pagingResult = flags.contains(Flag.CONTINUOUS_PAGING)
                                            ? new PagingResult(state, body.readInt(), flags.contains(Flag.LAST_CONTINOUS_PAGE))
                                            : state == null ? PagingResult.NONE : new PagingResult(state);

                if (flags.contains(Flag.NO_METADATA))
                    return new ResultMetadata(flags, null, columnCount, pagingResult);

                boolean globalTablesSpec = flags.contains(Flag.GLOBAL_TABLES_SPEC);

                String globalKsName = null;
                String globalCfName = null;
                if (globalTablesSpec)
                {
                    globalKsName = CBUtil.readString(body);
                    globalCfName = CBUtil.readString(body);
                }

                // metadata (names/types)
                List<ColumnSpecification> names = new ArrayList<ColumnSpecification>(columnCount);
                for (int i = 0; i < columnCount; i++)
                {
                    String ksName = globalTablesSpec ? globalKsName : CBUtil.readString(body);
                    String cfName = globalTablesSpec ? globalCfName : CBUtil.readString(body);
                    ColumnIdentifier colName = new ColumnIdentifier(CBUtil.readString(body), true);
                    AbstractType type = DataType.toType(DataType.codec.decodeOne(body, version));
                    names.add(new ColumnSpecification(ksName, cfName, colName, type));
                }
                return new ResultMetadata(flags, names, names.size(), pagingResult);
            }

            public void encode(ResultMetadata m, ByteBuf dest, ProtocolVersion version)
            {
                boolean noMetadata = m.flags.contains(Flag.NO_METADATA);
                boolean globalTablesSpec = m.flags.contains(Flag.GLOBAL_TABLES_SPEC);
                boolean hasMorePages = m.flags.contains(Flag.HAS_MORE_PAGES);
                boolean continuousPaging = m.flags.contains(Flag.CONTINUOUS_PAGING);

                assert version.isGreaterThan(ProtocolVersion.V1) || (!hasMorePages && !noMetadata)
                    : "version = " + version + ", flags = " + m.flags;

                dest.writeInt(Flag.serialize(m.flags));
                dest.writeInt(m.columnCount);

                if (hasMorePages)
                    CBUtil.writeValue(m.pagingResult.state.serialize(version), dest);

                if (continuousPaging)
                {
                    assert version.isGreaterOrEqualTo(ProtocolVersion.DSE_V1)
                        : "version = " + version + ", does not support optimized paging.";
                    dest.writeInt(m.pagingResult.seqNo);
                }

                if (!noMetadata)
                {
                    if (globalTablesSpec)
                    {
                        CBUtil.writeString(m.names.get(0).ksName, dest);
                        CBUtil.writeString(m.names.get(0).cfName, dest);
                    }

                    for (int i = 0; i < m.columnCount; i++)
                    {
                        ColumnSpecification name = m.names.get(i);
                        if (!globalTablesSpec)
                        {
                            CBUtil.writeString(name.ksName, dest);
                            CBUtil.writeString(name.cfName, dest);
                        }
                        CBUtil.writeString(name.name.toString(), dest);
                        DataType.codec.writeOne(DataType.fromType(name.type, version), dest, version);
                    }
                }
            }

            public int encodedSize(ResultMetadata m, ProtocolVersion version)
            {
                boolean noMetadata = m.flags.contains(Flag.NO_METADATA);
                boolean globalTablesSpec = m.flags.contains(Flag.GLOBAL_TABLES_SPEC);
                boolean hasMorePages = m.flags.contains(Flag.HAS_MORE_PAGES);
                boolean continuousPaging = m.flags.contains(Flag.CONTINUOUS_PAGING);

                int size = 8;
                if (hasMorePages)
                    size += CBUtil.sizeOfValue(m.pagingResult.state.serializedSize(version));

                if (continuousPaging)
                    size += 4;

                if (!noMetadata)
                {
                    if (globalTablesSpec)
                    {
                        size += CBUtil.sizeOfString(m.names.get(0).ksName);
                        size += CBUtil.sizeOfString(m.names.get(0).cfName);
                    }

                    for (int i = 0; i < m.columnCount; i++)
                    {
                        ColumnSpecification name = m.names.get(i);
                        if (!globalTablesSpec)
                        {
                            size += CBUtil.sizeOfString(name.ksName);
                            size += CBUtil.sizeOfString(name.cfName);
                        }
                        size += CBUtil.sizeOfString(name.name.toString());
                        size += DataType.codec.oneSerializedSize(DataType.fromType(name.type, version), version);
                    }
                }
                return size;
            }
        }
    }

    /**
     * The metadata for the query parameters in a prepared statement.
     */
    public static class PreparedMetadata
    {
        public static final CBCodec<PreparedMetadata> codec = new Codec();

        private final EnumSet<Flag> flags;
        public final List<ColumnSpecification> names;
        private final short[] partitionKeyBindIndexes;

        public PreparedMetadata(List<ColumnSpecification> names, short[] partitionKeyBindIndexes)
        {
            this(EnumSet.noneOf(Flag.class), names, partitionKeyBindIndexes);
            if (!names.isEmpty() && ColumnSpecification.allInSameTable(names))
                flags.add(Flag.GLOBAL_TABLES_SPEC);
        }

        private PreparedMetadata(EnumSet<Flag> flags, List<ColumnSpecification> names, short[] partitionKeyBindIndexes)
        {
            this.flags = flags;
            this.names = names;
            this.partitionKeyBindIndexes = partitionKeyBindIndexes;
        }

        public PreparedMetadata copy()
        {
            return new PreparedMetadata(EnumSet.copyOf(flags), names, partitionKeyBindIndexes);
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
                return true;

            if (!(other instanceof PreparedMetadata))
                return false;

            PreparedMetadata that = (PreparedMetadata) other;
            return this.names.equals(that.names) &&
                   this.flags.equals(that.flags) &&
                   Arrays.equals(this.partitionKeyBindIndexes, that.partitionKeyBindIndexes);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(names, flags) + Arrays.hashCode(partitionKeyBindIndexes);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            for (ColumnSpecification name : names)
            {
                sb.append("[").append(name.name);
                sb.append("(").append(name.ksName).append(", ").append(name.cfName).append(")");
                sb.append(", ").append(name.type).append("]");
            }

            sb.append(", bindIndexes=[");
            if (partitionKeyBindIndexes != null)
            {
                for (int i = 0; i < partitionKeyBindIndexes.length; i++)
                {
                    if (i > 0)
                        sb.append(", ");
                    sb.append(partitionKeyBindIndexes[i]);
                }
            }
            sb.append("]");
            return sb.toString();
        }

        private static class Codec implements CBCodec<PreparedMetadata>
        {
            public PreparedMetadata decode(ByteBuf body, ProtocolVersion version)
            {
                // flags & column count
                int iflags = body.readInt();
                int columnCount = body.readInt();

                EnumSet<Flag> flags = Flag.deserialize(iflags);

                short[] partitionKeyBindIndexes = null;
                if (version.isGreaterOrEqualTo(ProtocolVersion.V4))
                {
                    int numPKNames = body.readInt();
                    if (numPKNames > 0)
                    {
                        partitionKeyBindIndexes = new short[numPKNames];
                        for (int i = 0; i < numPKNames; i++)
                            partitionKeyBindIndexes[i] = body.readShort();
                    }
                }

                boolean globalTablesSpec = flags.contains(Flag.GLOBAL_TABLES_SPEC);

                String globalKsName = null;
                String globalCfName = null;
                if (globalTablesSpec)
                {
                    globalKsName = CBUtil.readString(body);
                    globalCfName = CBUtil.readString(body);
                }

                // metadata (names/types)
                List<ColumnSpecification> names = new ArrayList<>(columnCount);
                for (int i = 0; i < columnCount; i++)
                {
                    String ksName = globalTablesSpec ? globalKsName : CBUtil.readString(body);
                    String cfName = globalTablesSpec ? globalCfName : CBUtil.readString(body);
                    ColumnIdentifier colName = new ColumnIdentifier(CBUtil.readString(body), true);
                    AbstractType type = DataType.toType(DataType.codec.decodeOne(body, version));
                    names.add(new ColumnSpecification(ksName, cfName, colName, type));
                }
                return new PreparedMetadata(flags, names, partitionKeyBindIndexes);
            }

            public void encode(PreparedMetadata m, ByteBuf dest, ProtocolVersion version)
            {
                boolean globalTablesSpec = m.flags.contains(Flag.GLOBAL_TABLES_SPEC);
                dest.writeInt(Flag.serialize(m.flags));
                dest.writeInt(m.names.size());

                if (version.isGreaterOrEqualTo(ProtocolVersion.V4))
                {
                    // there's no point in providing partition key bind indexes if the statements affect multiple tables
                    if (m.partitionKeyBindIndexes == null || !globalTablesSpec)
                    {
                        dest.writeInt(0);
                    }
                    else
                    {
                        dest.writeInt(m.partitionKeyBindIndexes.length);
                        for (Short bindIndex : m.partitionKeyBindIndexes)
                            dest.writeShort(bindIndex);
                    }
                }

                if (globalTablesSpec)
                {
                    CBUtil.writeString(m.names.get(0).ksName, dest);
                    CBUtil.writeString(m.names.get(0).cfName, dest);
                }

                for (ColumnSpecification name : m.names)
                {
                    if (!globalTablesSpec)
                    {
                        CBUtil.writeString(name.ksName, dest);
                        CBUtil.writeString(name.cfName, dest);
                    }
                    CBUtil.writeString(name.name.toString(), dest);
                    DataType.codec.writeOne(DataType.fromType(name.type, version), dest, version);
                }
            }

            public int encodedSize(PreparedMetadata m, ProtocolVersion version)
            {
                boolean globalTablesSpec = m.flags.contains(Flag.GLOBAL_TABLES_SPEC);
                int size = 8;
                if (globalTablesSpec)
                {
                    size += CBUtil.sizeOfString(m.names.get(0).ksName);
                    size += CBUtil.sizeOfString(m.names.get(0).cfName);
                }

                if (m.partitionKeyBindIndexes != null && version.isGreaterOrEqualTo(ProtocolVersion.V4))
                    size += 4 + 2 * m.partitionKeyBindIndexes.length;

                for (ColumnSpecification name : m.names)
                {
                    if (!globalTablesSpec)
                    {
                        size += CBUtil.sizeOfString(name.ksName);
                        size += CBUtil.sizeOfString(name.cfName);
                    }
                    size += CBUtil.sizeOfString(name.name.toString());
                    size += DataType.codec.oneSerializedSize(DataType.fromType(name.type, version), version);
                }
                return size;
            }
        }
    }

    public enum Flag
    {
        // public flags
        GLOBAL_TABLES_SPEC(0),
        HAS_MORE_PAGES(1),
        NO_METADATA(2),

        // private flags
        CONTINUOUS_PAGING(30),
        LAST_CONTINOUS_PAGE(31);

        /** The position of the bit that must be set to one to indicate a flag is set */
        private final int pos;

        Flag(int pos)
        {
            this.pos = pos;
        }

        public static EnumSet<Flag> deserialize(int flags)
        {
            EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
            Flag[] values = Flag.values();
            for (Flag flag : values)
            {
                if ((flags & (1 << flag.pos)) != 0)
                    set.add(flag);
            }
            return set;
        }

        public static int serialize(EnumSet<Flag> flags)
        {
            int i = 0;
            for (Flag flag : flags)
                i |= 1 << flag.pos;
            return i;
        }
    }

    /**
     * Return an estimated size of a CQL row. For each column, if the column type is fixed, return its
     * fixed length. Otherwise estimate a mean cell size by dividing the mean partition size by the mean
     * number of cells.
     * <p>
     * This method really could use some improvement since avgColumnSize is not correct for partition or
     * primary columns, as well as complex columns and ignores disk format overheads.
     * @return - an estimated size of a CQL row.
     */
    public static int estimatedRowSize(TableMetadata cfm, List<ColumnMetadata> columns)
    {
        ColumnFamilyStore cfs = Keyspace.open(cfm.keyspace).getColumnFamilyStore(cfm.name);

        int avgColumnSize = (int)(cfs.getMeanCells() > 0
                                  ? cfs.getMeanPartitionSize() / cfs.getMeanCells()
                                  : cfs.getMeanPartitionSize());

        int ret = 0;
        for (ColumnMetadata def : columns)
        {
            int fixedLength = def.type.valueLengthIfFixed();
            ret += CBUtil.sizeOfValue(fixedLength > 0 ? fixedLength : avgColumnSize);
        }
        return ret;
    }

    public static Builder makeBuilder(QueryOptions options, boolean isJson, Selection selection)
    {
        return new ResultSet.Builder(options, isJson, null, selection);
    }

    public static Builder makeBuilder(QueryOptions options, boolean isJson, AggregationSpecification aggregationSpec, Selection selection)
    {
        return aggregationSpec == null ? new ResultSet.Builder(options, isJson, null, selection)
                                       : new ResultSet.Builder(options, isJson, aggregationSpec.newGroupMaker(), selection);
    }

    /**
     * Build a ResultSet by implementing the abstract methods of {@link ResultBuilder}.
     */
    public final static class Builder extends ResultBuilder
    {
        private ResultSet resultSet;

        public Builder(QueryOptions options, boolean isJson, GroupMaker groupMaker, Selection selection)
        {
            super(options, isJson, groupMaker, selection);
            this.resultSet = new ResultSet(selection.getResultMetadata(isJson).copy(), new ArrayList<>());
        }

        public boolean onRowCompleted(List<ByteBuffer> row, boolean nextRowPending)
        {
            resultSet.addRow(row);
            return true;
        }

        public boolean resultIsEmpty()
        {
            return resultSet.isEmpty();
        }

        public ResultSet build() throws InvalidRequestException
        {
            complete();
            return resultSet;
        }
    }
}
